// Package transport defines the interface and value types for billet's
// transport layer. The contract is committed to the weakest transport billet
// will ever support: unreliable, unordered, non-duplex, size-capped datagrams.
// Stronger transports advertise capabilities; core adapts.
package transport

// Transport is the minimal contract for moving opaque frames between peers.
// Implementations must not parse frame payloads. Delivery is push-based:
// frames arrive on the Inbound channel, and Send dispatches outbound frames.
type Transport interface {
	// Send transmits a frame to the specified channel. It returns an error if
	// the frame exceeds MaxFrameBytes or the transport is unavailable.
	Send(ch Channel, frame Frame) error

	// Inbound returns a channel that delivers received frames. The transport
	// must never close this channel while running. Callers must not block the
	// receive loop.
	Inbound() <-chan Delivery

	// Caps returns the capabilities of this transport.
	Caps() Capabilities

	// Close shuts down the transport and releases resources. After Close
	// returns, Send will return an error and Inbound will no longer deliver.
	Close() error
}

// Connected is an optional interface that transports with a connection concept
// may implement. Core never requires it.
type Connected interface {
	// IsConnected reports whether the transport currently has an active link.
	IsConnected() bool
}

// Channel identifies a logical destination for frames (e.g., a device address,
// a topic, a multicast group).
type Channel string

// Frame is an opaque byte slice. The transport never inspects its contents.
type Frame []byte

// Delivery pairs a received frame with the channel it arrived on.
type Delivery struct {
	Channel Channel
	Frame   Frame
}

// Capabilities describes what a transport can do. Core reads these to decide
// whether it needs to handle ordering, dedup, and fragmentation itself.
type Capabilities struct {
	// MaxFrameBytes is the maximum payload size the transport will accept in a
	// single Send call. Core fragments ops that exceed this.
	MaxFrameBytes int

	// Ordered indicates the transport guarantees FIFO delivery per channel.
	// If false, core must handle reordering.
	Ordered bool

	// Reliable indicates the transport guarantees delivery (e.g., TCP, QoS 1+).
	// If false, core handles retransmission via application-level acks.
	Reliable bool

	// Duplex indicates the transport supports simultaneous send and receive.
	// If false (half-duplex), core must not assume it can send at arbitrary times.
	Duplex bool
}
