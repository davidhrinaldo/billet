package testutil

import (
	"sync"

	"github.com/davidhrinaldo/billet/transport"
)

// LossyMux is a transport that optionally drops frames based on a configurable
// loss function. It can be wired as a pair to simulate bidirectional lossy
// communication between a controller and device fleet. It implements
// transport.Transport.
type LossyMux struct {
	mu       sync.Mutex
	maxFrame int
	inbound  chan transport.Delivery
	peer     *LossyMux
	lossFn   func(transport.Channel) bool
	closed   bool
}

// NewLossyMuxPair creates two connected lossy transports. Frames sent on one
// are delivered to the other's Inbound channel unless the loss function returns
// true, in which case the frame is silently dropped. A nil lossFn means no
// loss.
func NewLossyMuxPair(maxFrameBytes int, lossFn func(transport.Channel) bool) (controller, device *LossyMux) {
	a := &LossyMux{
		maxFrame: maxFrameBytes,
		inbound:  make(chan transport.Delivery, 4096),
		lossFn:   lossFn,
	}
	b := &LossyMux{
		maxFrame: maxFrameBytes,
		inbound:  make(chan transport.Delivery, 4096),
		lossFn:   lossFn,
	}
	a.peer = b
	b.peer = a
	return a, b
}

// Send transmits a frame to the peer's Inbound channel. If the loss function
// returns true for the given channel, the frame is silently dropped. Returns
// ErrClosed if the transport is closed, or ErrFrameTooLarge if the frame
// exceeds MaxFrameBytes.
func (l *LossyMux) Send(ch transport.Channel, frame transport.Frame) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}
	if len(frame) > l.maxFrame {
		return ErrFrameTooLarge
	}

	// Check loss before delivering.
	if l.lossFn != nil && l.lossFn(ch) {
		return nil
	}

	cp := make(transport.Frame, len(frame))
	copy(cp, frame)

	l.peer.inbound <- transport.Delivery{Channel: ch, Frame: cp}
	return nil
}

// Inbound returns the channel that delivers received frames.
func (l *LossyMux) Inbound() <-chan transport.Delivery {
	return l.inbound
}

// Caps returns the capabilities of this lossy transport. It advertises
// unreliable, unordered delivery to simulate constrained transports like
// LoRaWAN.
func (l *LossyMux) Caps() transport.Capabilities {
	return transport.Capabilities{
		MaxFrameBytes: l.maxFrame,
		Ordered:       false,
		Reliable:      false,
		Duplex:        true,
	}
}

// Close shuts down the lossy transport.
func (l *LossyMux) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	return nil
}

// Verify LossyMux implements Transport at compile time.
var _ transport.Transport = (*LossyMux)(nil)
