package testutil

import (
	"testing"
	"time"

	"github.com/davidhrinaldo/billet/transport"
)

func TestLoopbackSendReceive(t *testing.T) {
	tests := []struct {
		name    string
		channel transport.Channel
		frame   transport.Frame
	}{
		{
			name:    "simple message",
			channel: "dev-1",
			frame:   transport.Frame("hello"),
		},
		{
			name:    "empty frame",
			channel: "dev-1",
			frame:   transport.Frame{},
		},
		{
			name:    "max size frame",
			channel: "sensor-42",
			frame:   make(transport.Frame, 242),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lb := NewLoopback(242)

			err := lb.Send(tt.channel, tt.frame)
			if err != nil {
				t.Fatalf("Send: %v", err)
			}

			select {
			case d := <-lb.Inbound():
				if d.Channel != tt.channel {
					t.Errorf("channel = %q, want %q", d.Channel, tt.channel)
				}
				if len(d.Frame) != len(tt.frame) {
					t.Errorf("frame len = %d, want %d", len(d.Frame), len(tt.frame))
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatal("timed out waiting for delivery")
			}
		})
	}
}

func TestLoopbackFrameTooLarge(t *testing.T) {
	lb := NewLoopback(100)

	err := lb.Send("dev-1", make(transport.Frame, 101))
	if err == nil {
		t.Fatal("expected error for oversized frame, got nil")
	}
}

func TestLoopbackCaps(t *testing.T) {
	lb := NewLoopback(200)
	caps := lb.Caps()

	if caps.MaxFrameBytes != 200 {
		t.Errorf("MaxFrameBytes = %d, want 200", caps.MaxFrameBytes)
	}
	if caps.Ordered != true {
		t.Error("expected Ordered = true")
	}
	if caps.Reliable != true {
		t.Error("expected Reliable = true")
	}
	if caps.Duplex != true {
		t.Error("expected Duplex = true")
	}
}

func TestLoopbackClose(t *testing.T) {
	lb := NewLoopback(100)

	err := lb.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	err = lb.Send("dev-1", transport.Frame("hello"))
	if err == nil {
		t.Fatal("expected error after Close, got nil")
	}
}

func TestLoopbackPair(t *testing.T) {
	tests := []struct {
		name    string
		channel transport.Channel
		frame   transport.Frame
	}{
		{
			name:    "a sends to b",
			channel: "ch-1",
			frame:   transport.Frame("from-a"),
		},
		{
			name:    "b sends to a",
			channel: "ch-2",
			frame:   transport.Frame("from-b"),
		},
	}

	a, b := NewLoopbackPair(200)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sender, receiver transport.Transport
			if tt.name == "b sends to a" {
				sender, receiver = b, a
			} else {
				sender, receiver = a, b
			}

			err := sender.Send(tt.channel, tt.frame)
			if err != nil {
				t.Fatalf("Send: %v", err)
			}

			select {
			case d := <-receiver.Inbound():
				if d.Channel != tt.channel {
					t.Errorf("channel = %q, want %q", d.Channel, tt.channel)
				}
				if string(d.Frame) != string(tt.frame) {
					t.Errorf("frame = %q, want %q", d.Frame, tt.frame)
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatal("timed out waiting for delivery")
			}
		})
	}
}
