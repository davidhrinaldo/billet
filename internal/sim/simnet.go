package sim

import (
	"container/heap"
	"errors"
	"math/rand"
	"sync"

	"github.com/davidhrinaldo/billet/transport"
)

// ErrClosed is returned when Send is called on a closed transport.
var ErrClosed = errors.New("sim: transport closed")

// ErrFrameTooLarge is returned when a frame exceeds MaxFrameBytes.
var ErrFrameTooLarge = errors.New("sim: frame too large")

// LinkConfig describes fault behavior for a single directional link.
// A zero-value LinkConfig means lossless, instant, connected delivery.
type LinkConfig struct {
	// Loss is the probability of dropping a frame, in [0.0, 1.0].
	Loss float64
	// MinDelay is the minimum delivery latency in nanoseconds.
	MinDelay int64
	// MaxDelay is the maximum delivery latency in nanoseconds. Must be >= MinDelay.
	MaxDelay int64
	// Down means the link is partitioned — all frames are silently dropped.
	Down bool
}

// SimNet is a deterministic network simulator. It models a star topology
// (controller ↔ N devices, multiplexed by Channel) with per-device link
// configuration, latency, reordering, and partitions. All randomness comes
// from a seeded PRNG. No goroutines — Deliver must be called explicitly to
// advance the network.
type SimNet struct {
	mu       sync.Mutex
	rng      *rand.Rand
	ctrl     *simTransport
	dev      *simTransport
	links    map[transport.Channel]*LinkConfig
	dflt     LinkConfig
	queue    deliveryHeap
	seq      uint64
	nowNs    int64
	maxFrame int
	dropped  int
}

// New creates a SimNet with the given PRNG seed and maximum frame size. Both
// the controller and device transports are created immediately.
func New(seed int64, maxFrameBytes int) *SimNet {
	n := &SimNet{
		rng:      rand.New(rand.NewSource(seed)),
		links:    make(map[transport.Channel]*LinkConfig),
		maxFrame: maxFrameBytes,
	}

	n.ctrl = &simTransport{
		net:     n,
		id:      0,
		inbound: make(chan transport.Delivery, 4096),
		caps: transport.Capabilities{
			MaxFrameBytes: maxFrameBytes,
			Ordered:       false,
			Reliable:      false,
			Duplex:        true,
		},
	}
	n.dev = &simTransport{
		net:     n,
		id:      1,
		inbound: make(chan transport.Delivery, 4096),
		caps: transport.Capabilities{
			MaxFrameBytes: maxFrameBytes,
			Ordered:       false,
			Reliable:      false,
			Duplex:        true,
		},
	}

	return n
}

// Controller returns the controller-side transport. Plug this into
// fleet.ManagerConfig.Transport.
func (n *SimNet) Controller() transport.Transport {
	return n.ctrl
}

// Device returns the device-side transport. The test's device simulator reads
// from this transport's Inbound channel and sends reported ops back through it.
func (n *SimNet) Device() transport.Transport {
	return n.dev
}

// SetDefault sets the default link configuration used for channels that have no
// per-device override.
func (n *SimNet) SetDefault(cfg LinkConfig) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dflt = cfg
}

// SetLink sets link configuration for a specific channel (device). This
// overrides the default for frames sent to or from this channel.
func (n *SimNet) SetLink(ch transport.Channel, cfg LinkConfig) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.links[ch] = &cfg
}

// Partition marks a channel as down, silently dropping all frames in both
// directions.
func (n *SimNet) Partition(ch transport.Channel) {
	n.mu.Lock()
	defer n.mu.Unlock()
	link, ok := n.links[ch]
	if !ok {
		cfg := n.dflt
		cfg.Down = true
		n.links[ch] = &cfg
		return
	}
	link.Down = true
}

// Heal restores connectivity for a channel, preserving other link config.
func (n *SimNet) Heal(ch transport.Channel) {
	n.mu.Lock()
	defer n.mu.Unlock()
	link, ok := n.links[ch]
	if !ok {
		return
	}
	link.Down = false
}

// PartitionAll marks every channel as down by setting the default link's Down
// flag and all per-device overrides.
func (n *SimNet) PartitionAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dflt.Down = true
	for _, link := range n.links {
		link.Down = true
	}
}

// HealAll restores connectivity for all channels by clearing the default
// link's Down flag and all per-device overrides.
func (n *SimNet) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dflt.Down = false
	for _, link := range n.links {
		link.Down = false
	}
}

// Deliver pops all frames from the heap with deliverAt <= nowNs and pushes
// them onto the destination's inbound channel. Returns the number of frames
// delivered.
func (n *SimNet) Deliver(nowNs int64) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.nowNs = nowNs
	count := 0

	for n.queue.Len() > 0 && n.queue[0].deliverAt <= nowNs {
		pd := heap.Pop(&n.queue).(pendingDelivery)
		var dest *simTransport
		if pd.dest == 0 {
			dest = n.ctrl
		} else {
			dest = n.dev
		}
		select {
		case dest.inbound <- pd.delivery:
		default:
			// Channel full — drop. This matches fleet.Manager's emitEvent
			// pattern: never block on delivery.
		}
		count++
	}

	return count
}

// Inflight returns the number of frames in the delivery heap that have not
// yet been delivered.
func (n *SimNet) Inflight() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.queue.Len()
}

// Dropped returns the total number of frames dropped since creation (due to
// loss or partition).
func (n *SimNet) Dropped() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dropped
}

// linkFor returns the link config for a channel, falling back to the default.
// Caller must hold n.mu.
func (n *SimNet) linkFor(ch transport.Channel) *LinkConfig {
	if link, ok := n.links[ch]; ok {
		return link
	}
	return &n.dflt
}

// enqueue is called by simTransport.Send to schedule a frame for delivery.
// Caller must NOT hold n.mu — this method acquires it.
func (n *SimNet) enqueue(from int, ch transport.Channel, frame transport.Frame) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if len(frame) > n.maxFrame {
		return ErrFrameTooLarge
	}

	link := n.linkFor(ch)

	// Partition check.
	if link.Down {
		n.dropped++
		return nil
	}

	// Loss check.
	if link.Loss > 0 && n.rng.Float64() < link.Loss {
		n.dropped++
		return nil
	}

	// Copy frame to prevent caller mutation.
	cp := make(transport.Frame, len(frame))
	copy(cp, frame)

	// Compute delivery time.
	delay := link.MinDelay
	if link.MaxDelay > link.MinDelay {
		delay += n.rng.Int63n(link.MaxDelay - link.MinDelay + 1)
	}
	deliverAt := n.nowNs + delay

	// Destination is the opposite side.
	dest := 1 - from

	n.seq++
	heap.Push(&n.queue, pendingDelivery{
		deliverAt: deliverAt,
		seq:       n.seq,
		dest:      dest,
		delivery:  transport.Delivery{Channel: ch, Frame: cp},
	})

	return nil
}

// simTransport implements transport.Transport. Send enqueues frames into
// SimNet's delivery heap; Inbound returns a buffered channel populated by
// SimNet.Deliver.
type simTransport struct {
	net     *SimNet
	id      int // 0 = controller, 1 = device
	inbound chan transport.Delivery
	caps    transport.Capabilities
	mu      sync.Mutex
	closed  bool
}

// Send enqueues a frame for delivery to the peer. The frame may be dropped
// (loss/partition) or delayed (latency) depending on the link configuration
// for the given channel.
func (t *simTransport) Send(ch transport.Channel, frame transport.Frame) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}
	t.mu.Unlock()

	return t.net.enqueue(t.id, ch, frame)
}

// Inbound returns the channel that delivers received frames.
func (t *simTransport) Inbound() <-chan transport.Delivery {
	return t.inbound
}

// Caps returns the capabilities of this simulated transport.
func (t *simTransport) Caps() transport.Capabilities {
	return t.caps
}

// Close shuts down the transport. Subsequent Send calls return ErrClosed.
func (t *simTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

// Compile-time check that simTransport implements transport.Transport.
var _ transport.Transport = (*simTransport)(nil)

// pendingDelivery is a frame scheduled for future delivery.
type pendingDelivery struct {
	deliverAt int64
	seq       uint64
	dest      int
	delivery  transport.Delivery
}

// deliveryHeap is a min-heap of pendingDelivery, ordered by (deliverAt, seq).
type deliveryHeap []pendingDelivery

func (h deliveryHeap) Len() int { return len(h) }

func (h deliveryHeap) Less(i, j int) bool {
	if h[i].deliverAt != h[j].deliverAt {
		return h[i].deliverAt < h[j].deliverAt
	}
	return h[i].seq < h[j].seq
}

func (h deliveryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *deliveryHeap) Push(x any) {
	*h = append(*h, x.(pendingDelivery))
}

func (h *deliveryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
