// Package main demonstrates integrating billet with a ChirpStack LoRaWAN
// network server. The chirpTransport adapter wraps ChirpStack's gRPC API to
// implement transport.Transport: uplink events become inbound frames, and
// downlink enqueues implement Send.
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"sync"

	"github.com/chirpstack/chirpstack/api/go/v4/api"
	"github.com/davidhrinaldo/billet/transport"
	"google.golang.org/grpc"
)

// chirpTransport adapts ChirpStack gRPC streams to billet's transport.Transport.
type chirpTransport struct {
	mu       sync.Mutex
	conn     *grpc.ClientConn
	device   api.DeviceServiceClient
	inbound  chan transport.Delivery
	cancel   context.CancelFunc
	closed   bool
	maxFrame int
}

// newChirpTransport connects to a ChirpStack gRPC endpoint and returns a
// transport that delivers uplink events as inbound frames.
func newChirpTransport(ctx context.Context, addr string, maxFrame int) (*chirpTransport, error) {
	conn, err := grpc.NewClient(addr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("chirp: dial %s: %w", addr, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	ct := &chirpTransport{
		conn:     conn,
		device:   api.NewDeviceServiceClient(conn),
		inbound:  make(chan transport.Delivery, 256),
		cancel:   cancel,
		maxFrame: maxFrame,
	}

	// Start streaming uplink events in the background.
	go ct.streamEvents(ctx)

	return ct, nil
}

// streamEvents reads the ChirpStack event stream and converts uplink payloads
// into transport.Delivery values on the inbound channel.
func (ct *chirpTransport) streamEvents(ctx context.Context) {
	// In a real integration you would use the Integration service's
	// StreamDeviceEvents RPC. This example uses a simplified polling loop
	// to illustrate the pattern without requiring a running ChirpStack.
	//
	// Production code would look like:
	//
	//   stream, err := integrationClient.StreamDeviceEvents(ctx, &api.StreamDeviceEventsRequest{})
	//   for {
	//       event, err := stream.Recv()
	//       ...
	//       ct.inbound <- transport.Delivery{
	//           Channel: transport.Channel(event.DevEui),
	//           Frame:   event.Data,
	//       }
	//   }
	<-ctx.Done()
}

// Send enqueues a downlink payload to the device via ChirpStack's Enqueue RPC.
func (ct *chirpTransport) Send(ch transport.Channel, frame transport.Frame) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	if ct.closed {
		return fmt.Errorf("chirp: transport closed")
	}
	if len(frame) > ct.maxFrame {
		return fmt.Errorf("chirp: frame %d bytes exceeds max %d", len(frame), ct.maxFrame)
	}

	_, err := ct.device.Enqueue(context.Background(), &api.EnqueueDeviceQueueItemRequest{
		QueueItem: &api.DeviceQueueItem{
			DevEui: string(ch),
			Data:   frame,
			FPort:  10,
		},
	})
	if err != nil {
		return fmt.Errorf("chirp: enqueue to %s: %w", ch, err)
	}

	log.Printf("chirp: enqueued %d bytes to %s (hex: %s)", len(frame), ch, hex.EncodeToString(frame))
	return nil
}

// Inbound returns the channel delivering uplink frames from devices.
func (ct *chirpTransport) Inbound() <-chan transport.Delivery {
	return ct.inbound
}

// Caps returns the transport capabilities. LoRaWAN is unreliable, unordered,
// half-duplex, and size-constrained.
func (ct *chirpTransport) Caps() transport.Capabilities {
	return transport.Capabilities{
		MaxFrameBytes: ct.maxFrame,
		Ordered:       false,
		Reliable:      false,
		Duplex:        false,
	}
}

// Close shuts down the gRPC connection and stops the event stream.
func (ct *chirpTransport) Close() error {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	if ct.closed {
		return nil
	}
	ct.closed = true
	ct.cancel()
	return ct.conn.Close()
}

// Compile-time check that chirpTransport implements transport.Transport.
var _ transport.Transport = (*chirpTransport)(nil)
