// Package testutil provides test helpers for billet, including a loopback
// transport that delivers frames locally without network I/O.
package testutil

import (
	"errors"
	"sync"

	"github.com/davidhrinaldo/billet/transport"
)

// ErrClosed is returned when Send is called on a closed transport.
var ErrClosed = errors.New("loopback: transport closed")

// ErrFrameTooLarge is returned when a frame exceeds MaxFrameBytes.
var ErrFrameTooLarge = errors.New("loopback: frame exceeds max size")

// Loopback is a transport that delivers frames to its own Inbound channel or
// to a paired Loopback. It is reliable, ordered, and duplex — the best-case
// transport, useful for testing core logic without transport concerns.
type Loopback struct {
	mu            sync.Mutex
	maxFrame      int
	inbound       chan transport.Delivery
	peer          *Loopback // nil means self-loopback
	closed        bool
}

// NewLoopback creates a self-loopback transport: frames sent are delivered back
// to the same instance's Inbound channel.
func NewLoopback(maxFrameBytes int) *Loopback {
	return &Loopback{
		maxFrame: maxFrameBytes,
		inbound:  make(chan transport.Delivery, 64),
	}
}

// NewLoopbackPair creates two connected transports. Frames sent on one are
// delivered to the other's Inbound channel, simulating a bidirectional link.
func NewLoopbackPair(maxFrameBytes int) (*Loopback, *Loopback) {
	a := &Loopback{
		maxFrame: maxFrameBytes,
		inbound:  make(chan transport.Delivery, 64),
	}
	b := &Loopback{
		maxFrame: maxFrameBytes,
		inbound:  make(chan transport.Delivery, 64),
	}
	a.peer = b
	b.peer = a
	return a, b
}

// Send transmits a frame. In self-loopback mode, it delivers to its own Inbound.
// In pair mode, it delivers to the peer's Inbound.
func (l *Loopback) Send(ch transport.Channel, frame transport.Frame) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}
	if len(frame) > l.maxFrame {
		return ErrFrameTooLarge
	}

	// Copy the frame to avoid caller mutation.
	cp := make(transport.Frame, len(frame))
	copy(cp, frame)

	target := l.inbound
	if l.peer != nil {
		target = l.peer.inbound
	}

	target <- transport.Delivery{Channel: ch, Frame: cp}
	return nil
}

// Inbound returns the channel that delivers received frames.
func (l *Loopback) Inbound() <-chan transport.Delivery {
	return l.inbound
}

// Caps returns the capabilities of this loopback transport.
func (l *Loopback) Caps() transport.Capabilities {
	return transport.Capabilities{
		MaxFrameBytes: l.maxFrame,
		Ordered:       true,
		Reliable:      true,
		Duplex:        true,
	}
}

// Close shuts down the loopback transport.
func (l *Loopback) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	return nil
}

// Verify Loopback implements Transport at compile time.
var _ transport.Transport = (*Loopback)(nil)
