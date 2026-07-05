// Command chirpstack-billet demonstrates wiring a ChirpStack LoRaWAN network
// server into billet's shadow and convergence system. It connects to the
// ChirpStack gRPC API, registers a set of devices, and runs a tick loop that
// drives fleet convergence over LoRaWAN downlinks.
//
// Usage:
//
//	go run . -addr localhost:8080 -devices 0011223344556677,AABBCCDDEEFF0011
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/davidhrinaldo/billet/fleet"
	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store/memstore"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "ChirpStack gRPC address")
	devicesFlag := flag.String("devices", "", "comma-separated DevEUIs to manage")
	tickInterval := flag.Duration("tick", 10*time.Second, "tick interval")
	maxFrame := flag.Int("max-frame", 51, "LoRaWAN max payload bytes (DR0=51)")
	flag.Parse()

	if *devicesFlag == "" {
		log.Fatal("no devices specified; use -devices dev1,dev2,...")
	}
	devices := strings.Split(*devicesFlag, ",")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Connect to ChirpStack.
	tr, err := newChirpTransport(ctx, *addr, *maxFrame)
	if err != nil {
		log.Fatalf("transport: %v", err)
	}
	defer tr.Close()

	// Set up billet fleet manager.
	st := memstore.New()
	clock := hlc.NewClock(1, nil) // uses real wall clock
	mgr := fleet.NewManager(fleet.ManagerConfig{
		Store:     st,
		Transport: tr,
		Clock:     clock,
		Timeout:   30 * time.Second,
		Budget:    fleet.BudgetConfig{MaxTokens: 1, RefillInterval: *tickInterval},
		EventSize: 64,
	})

	for _, devEUI := range devices {
		mgr.Add(shadow.DeviceID(devEUI))
		log.Printf("registered device %s", devEUI)
	}

	// Example: set an initial desired state for all devices.
	for _, devEUI := range devices {
		if err := mgr.SetDesired(devEUI, "reporting_interval", []byte("60")); err != nil {
			log.Printf("SetDesired(%s): %v", devEUI, err)
		}
	}

	// Tick loop.
	ticker := time.NewTicker(*tickInterval)
	defer ticker.Stop()

	log.Printf("running tick loop every %s (ctrl-c to stop)", *tickInterval)
	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down")
			return

		case t := <-ticker.C:
			nowNs := t.UnixNano()
			mgr.DrainInbound()
			mgr.Tick(nowNs)

			// Drain and log events.
			for {
				select {
				case ev := <-mgr.Events():
					if ev.Kind == fleet.EventError {
						log.Printf("event: %s error on %s: %v", ev.Kind, ev.DeviceID, ev.Err)
					} else {
						log.Printf("event: %s %s → %s for %s", ev.Kind, ev.From, ev.To, ev.DeviceID)
					}
				default:
					goto drained
				}
			}
		drained:

			// Log stall report.
			report := mgr.StallReport()
			if len(report.Stalled) > 0 {
				log.Printf("stalled: %d devices", len(report.Stalled))
			}
		}
	}
}
