// Command fleet-console runs a web dashboard demonstrating billet's fleet
// convergence with live fault injection. It uses a simulated transport so no
// hardware is required.
//
// Usage:
//
//	go run .                              # 8 devices, seed 42, localhost:8080
//	go run . -devices 20 -seed 7 -addr :9090
package main

import (
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/davidhrinaldo/billet/converge"
	"github.com/davidhrinaldo/billet/fleet"
	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/internal/sim"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store/memstore"
	"github.com/davidhrinaldo/billet/transport"
)

//go:embed web/index.html
var webFS embed.FS

// sseClient is a connected SSE listener.
type sseClient struct {
	ch chan []byte
}

// hub fans out messages to connected SSE clients.
type hub struct {
	mu      sync.Mutex
	clients []*sseClient
}

func (h *hub) add() *sseClient {
	c := &sseClient{ch: make(chan []byte, 64)}
	h.mu.Lock()
	h.clients = append(h.clients, c)
	h.mu.Unlock()
	return c
}

func (h *hub) remove(c *sseClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, cl := range h.clients {
		if cl == c {
			h.clients = append(h.clients[:i], h.clients[i+1:]...)
			return
		}
	}
}

func (h *hub) broadcast(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.clients {
		select {
		case c.ch <- data:
		default:
			// Drop if client is slow.
		}
	}
}

// snapshotMsg is the JSON message sent to clients.
type snapshotMsg struct {
	Type    string       `json:"type"`
	Devices []deviceView `json:"devices"`
	Net     netView      `json:"net"`
}

type deviceView struct {
	ID          string            `json:"id"`
	State       string            `json:"state"`
	SinceMs     int64             `json:"sinceMs"`
	Reported    map[string]string `json:"reported"`
	Desired     map[string]string `json:"desired"`
	Delta       []string          `json:"delta"`
	Partitioned bool              `json:"partitioned"`
	LossPct     int               `json:"lossPct"`
	MinDelayMs  int64             `json:"minDelayMs"`
	MaxDelayMs  int64             `json:"maxDelayMs"`
}

type netView struct {
	Inflight int `json:"inflight"`
	Dropped  int `json:"dropped"`
}

// eventMsg is a state-change event sent to clients.
type eventMsg struct {
	Type     string `json:"type"`
	DeviceID string `json:"deviceId"`
	From     string `json:"from"`
	To       string `json:"to"`
	AtMs     int64  `json:"atMs"`
}

// linkInfo tracks per-device link configuration for the UI.
type linkInfo struct {
	Down       bool
	LossPct    int // 0–100
	MinDelayMs int64
	MaxDelayMs int64
}

// linkState tracks per-device link status for the UI. SimNet doesn't expose
// per-link config queries, so we mirror it here.
type linkState struct {
	mu      sync.Mutex
	links   map[string]linkInfo
	allDown bool
}

func newLinkState() *linkState {
	return &linkState{links: make(map[string]linkInfo)}
}

func (ls *linkState) get(id string) linkInfo {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	li := ls.links[id]
	if ls.allDown {
		li.Down = true
	}
	return li
}

func buildSnapshot(mgr *fleet.Manager, net *sim.SimNet, nowNs int64, ls *linkState) []byte {
	snaps := mgr.FleetSnapshot()
	views := make([]deviceView, len(snaps))
	for i, s := range snaps {
		reported := make(map[string]string, len(s.Reported))
		for k, v := range s.Reported {
			reported[k] = string(v)
		}
		desired := make(map[string]string, len(s.Desired))
		for k, v := range s.Desired {
			desired[k] = string(v)
		}
		var delta []string
		for k := range s.Delta.Diffs {
			delta = append(delta, k)
		}
		var sinceMs int64
		if s.Since > 0 {
			sinceMs = (nowNs - s.Since) / 1e6
		}
		li := ls.get(s.DeviceID)
		views[i] = deviceView{
			ID:          s.DeviceID,
			State:       s.State.String(),
			SinceMs:     sinceMs,
			Reported:    reported,
			Desired:     desired,
			Delta:       delta,
			Partitioned: li.Down,
			LossPct:     li.LossPct,
			MinDelayMs:  li.MinDelayMs,
			MaxDelayMs:  li.MaxDelayMs,
		}
	}
	msg := snapshotMsg{
		Type:    "snapshot",
		Devices: views,
		Net: netView{
			Inflight: net.Inflight(),
			Dropped:  net.Dropped(),
		},
	}
	data, _ := json.Marshal(msg)
	return data
}

// simulateDevices reads pending frames from the device-side transport, decodes
// desired ops, and echoes back reported ops — simulating a fleet of devices
// that immediately adopt configuration.
func simulateDevices(dev transport.Transport, nowNs int64) {
	for {
		select {
		case d := <-dev.Inbound():
			opID, _, _, payload, err := converge.ParseFragmentHeader(d.Frame)
			if err != nil {
				continue
			}
			op, err := converge.DecodeOp(opID, payload)
			if err != nil {
				continue
			}

			reportOp := shadow.Op{
				ID:        shadow.OpID{NodeID: 2, Seq: opID.Seq},
				DeviceID:  op.DeviceID,
				Section:   shadow.SectionReported,
				Key:       op.Key,
				Data:      op.Data,
				Timestamp: hlc.Timestamp{Physical: nowNs, NodeID: 2},
			}
			reportPayload := converge.EncodeOp(reportOp)
			frames, err := converge.Fragment(reportOp.ID, reportPayload, 1024)
			if err != nil {
				continue
			}
			for _, frame := range frames {
				_ = dev.Send(transport.Channel(op.DeviceID), frame)
			}
		default:
			return
		}
	}
}

func main() {
	seed := flag.Int64("seed", 42, "PRNG seed for the simulated network")
	numDevices := flag.Int("devices", 8, "number of simulated devices")
	tickMs := flag.Int("tick", 50, "simulation tick interval in milliseconds")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	net := sim.New(*seed, 1024)
	net.SetDefault(sim.LinkConfig{MinDelay: 5e6, MaxDelay: 20e6}) // 5–20ms

	st := memstore.New()
	clock := hlc.NewClock(1, nil)

	mgr := fleet.NewManager(fleet.ManagerConfig{
		Store:     st,
		Transport: net.Controller(),
		Clock:     clock,
		Timeout:   5 * time.Second,
		Budget:    fleet.BudgetConfig{MaxTokens: 5, RefillInterval: 500 * time.Millisecond},
		EventSize: 4096,
	})

	// Register devices and set initial desired state.
	deviceIDs := make([]shadow.DeviceID, *numDevices)
	for i := range *numDevices {
		id := shadow.DeviceID(fmt.Sprintf("dev-%04d", i))
		deviceIDs[i] = id
		mgr.Add(id)
	}
	for _, id := range deviceIDs {
		if err := mgr.SetDesired(id, "mode", []byte("auto")); err != nil {
			log.Fatalf("SetDesired(%s): %v", id, err)
		}
		if err := mgr.SetDesired(id, "interval", []byte("60")); err != nil {
			log.Fatalf("SetDesired(%s): %v", id, err)
		}
	}

	// Assign random link impairments so the dashboard is interesting on startup.
	h := &hub{}
	ls := newLinkState()
	rng := rand.New(rand.NewSource(*seed))
	for _, id := range deviceIDs {
		ch := transport.Channel(id)
		roll := rng.Float64()
		switch {
		case roll < 0.15: // ~15% chance: partitioned
			net.Partition(ch)
			ls.mu.Lock()
			li := ls.links[id]
			li.Down = true
			ls.links[id] = li
			ls.mu.Unlock()
		case roll < 0.40: // ~25% chance: lossy/delayed
			lossPct := 10 + rng.Intn(41) // 10–50%
			minD := int64(20 + rng.Intn(80))
			maxD := minD + int64(50+rng.Intn(200))
			net.SetLink(ch, sim.LinkConfig{
				Loss:     float64(lossPct) / 100,
				MinDelay: minD * 1e6,
				MaxDelay: maxD * 1e6,
			})
			ls.mu.Lock()
			ls.links[id] = linkInfo{
				LossPct:    lossPct,
				MinDelayMs: minD,
				MaxDelayMs: maxD,
			}
			ls.mu.Unlock()
		}
		// remaining ~60%: healthy (use default 5–20ms)
	}

	// Simulation loop.
	go func() {
		ticker := time.NewTicker(time.Duration(*tickMs) * time.Millisecond)
		defer ticker.Stop()

		var tickCount int
		for range ticker.C {
			nowNs := time.Now().UnixNano()

			net.Deliver(nowNs)
			mgr.DrainInbound()
			mgr.Tick(nowNs)
			simulateDevices(net.Device(), nowNs)

			// Forward events.
			for {
				select {
				case ev := <-mgr.Events():
					msg := eventMsg{
						Type:     "event",
						DeviceID: ev.DeviceID,
						From:     ev.From.String(),
						To:       ev.To.String(),
						AtMs:     ev.At / 1e6,
					}
					data, _ := json.Marshal(msg)
					h.broadcast(data)
				default:
					goto doneEvents
				}
			}
		doneEvents:

			// Broadcast snapshot every 4th tick (~200ms at 50ms tick).
			tickCount++
			if tickCount%4 == 0 {
				data := buildSnapshot(mgr, net, nowNs, ls)
				h.broadcast(data)
			}
		}
	}()

	// --- HTTP handlers ---

	// Serve embedded web UI.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data, err := webFS.ReadFile("web/index.html")
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	})

	// SSE endpoint.
	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		client := h.add()
		defer h.remove(client)

		// Send initial snapshot.
		initial := buildSnapshot(mgr, net, time.Now().UnixNano(), ls)
		fmt.Fprintf(w, "data: %s\n\n", initial)
		flusher.Flush()

		for {
			select {
			case data := <-client.ch:
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	})

	// Set desired state.
	http.HandleFunc("/api/desired", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			DeviceID string `json:"deviceId"`
			Key      string `json:"key"`
			Value    string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := mgr.SetDesired(shadow.DeviceID(req.DeviceID), req.Key, []byte(req.Value)); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		go func() {
			data := buildSnapshot(mgr, net, time.Now().UnixNano(), ls)
			h.broadcast(data)
		}()
		w.WriteHeader(http.StatusOK)
	})

	// Fault injection.
	http.HandleFunc("/api/fault", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Action     string  `json:"action"`
			DeviceID   string  `json:"deviceId"`
			Loss       float64 `json:"loss"`
			MinDelayMs int64   `json:"minDelayMs"`
			MaxDelayMs int64   `json:"maxDelayMs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		ch := transport.Channel(req.DeviceID)
		switch req.Action {
		case "partition":
			net.Partition(ch)
			ls.mu.Lock()
			li := ls.links[req.DeviceID]
			li.Down = true
			ls.links[req.DeviceID] = li
			ls.mu.Unlock()
		case "heal":
			net.Heal(ch)
			ls.mu.Lock()
			li := ls.links[req.DeviceID]
			li.Down = false
			ls.links[req.DeviceID] = li
			ls.mu.Unlock()
		case "partitionAll":
			net.PartitionAll()
			ls.mu.Lock()
			ls.allDown = true
			ls.mu.Unlock()
		case "healAll":
			net.HealAll()
			ls.mu.Lock()
			ls.allDown = false
			for k := range ls.links {
				li := ls.links[k]
				li.Down = false
				ls.links[k] = li
			}
			ls.mu.Unlock()
		case "setLink":
			net.SetLink(ch, sim.LinkConfig{
				Loss:     req.Loss,
				MinDelay: req.MinDelayMs * 1e6,
				MaxDelay: req.MaxDelayMs * 1e6,
			})
			ls.mu.Lock()
			ls.links[req.DeviceID] = linkInfo{
				LossPct:    int(req.Loss * 100),
				MinDelayMs: req.MinDelayMs,
				MaxDelayMs: req.MaxDelayMs,
			}
			ls.mu.Unlock()
		default:
			http.Error(w, "unknown action: "+req.Action, http.StatusBadRequest)
			return
		}
		// Broadcast updated snapshot immediately so UI reflects the change.
		go func() {
			data := buildSnapshot(mgr, net, time.Now().UnixNano(), ls)
			h.broadcast(data)
		}()
		w.WriteHeader(http.StatusOK)
	})

	// Reboot a device.
	http.HandleFunc("/api/reboot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			DeviceID string `json:"deviceId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		id := shadow.DeviceID(req.DeviceID)
		mgr.Remove(id)
		mgr.Add(id)
		// Re-set desired so the device re-converges.
		_ = mgr.SetDesired(id, "mode", []byte("auto"))
		_ = mgr.SetDesired(id, "interval", []byte("60"))
		go func() {
			data := buildSnapshot(mgr, net, time.Now().UnixNano(), ls)
			h.broadcast(data)
		}()
		w.WriteHeader(http.StatusOK)
	})

	// Push a config change to all devices — triggers convergence traffic.
	var pushSeq int
	http.HandleFunc("/api/push", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		pushSeq++
		val := []byte(fmt.Sprintf("v%d", pushSeq))
		for _, id := range deviceIDs {
			_ = mgr.SetDesired(id, "config", val)
		}
		go func() {
			data := buildSnapshot(mgr, net, time.Now().UnixNano(), ls)
			h.broadcast(data)
		}()
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("Fleet console: http://localhost%s  (%d devices, seed %d)", *addr, *numDevices, *seed)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
