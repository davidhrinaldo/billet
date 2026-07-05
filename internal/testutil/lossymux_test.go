package testutil

import (
	"testing"
	"time"

	"github.com/davidhrinaldo/billet/transport"
)

func TestLossyMux(t *testing.T) {
	tests := []struct {
		name       string
		maxFrame   int
		lossFn     func(transport.Channel) bool
		channel    transport.Channel
		frame      transport.Frame
		wantErr    error
		wantDeliv  bool // expect delivery on peer
	}{
		{
			name:      "no loss delivers to peer",
			maxFrame:  256,
			lossFn:    nil,
			channel:   "dev-01",
			frame:     transport.Frame("hello"),
			wantErr:   nil,
			wantDeliv: true,
		},
		{
			name:     "100% loss drops frame silently",
			maxFrame: 256,
			lossFn:   func(transport.Channel) bool { return true },
			channel:  "dev-01",
			frame:    transport.Frame("hello"),
			wantErr:  nil,
			wantDeliv: false,
		},
		{
			name:     "frame too large returns error",
			maxFrame: 4,
			lossFn:   nil,
			channel:  "dev-01",
			frame:    transport.Frame("hello"),
			wantErr:  ErrFrameTooLarge,
			wantDeliv: false,
		},
		{
			name:     "selective loss by channel",
			maxFrame: 256,
			lossFn:   func(ch transport.Channel) bool { return ch == "drop-me" },
			channel:  "keep-me",
			frame:    transport.Frame("kept"),
			wantErr:  nil,
			wantDeliv: true,
		},
		{
			name:     "selective loss drops matching channel",
			maxFrame: 256,
			lossFn:   func(ch transport.Channel) bool { return ch == "drop-me" },
			channel:  "drop-me",
			frame:    transport.Frame("dropped"),
			wantErr:  nil,
			wantDeliv: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, device := NewLossyMuxPair(tt.maxFrame, tt.lossFn)
			defer controller.Close()
			defer device.Close()

			err := controller.Send(tt.channel, tt.frame)
			if err != tt.wantErr {
				t.Fatalf("Send() error = %v, want %v", err, tt.wantErr)
			}

			if tt.wantDeliv {
				select {
				case d := <-device.Inbound():
					if d.Channel != tt.channel {
						t.Errorf("delivery channel = %q, want %q", d.Channel, tt.channel)
					}
					if string(d.Frame) != string(tt.frame) {
						t.Errorf("delivery frame = %q, want %q", d.Frame, tt.frame)
					}
				case <-time.After(100 * time.Millisecond):
					t.Fatal("expected delivery but timed out")
				}
			} else if err == nil {
				// No delivery expected — verify channel is empty.
				select {
				case d := <-device.Inbound():
					t.Fatalf("unexpected delivery: %+v", d)
				case <-time.After(10 * time.Millisecond):
					// Good — nothing delivered.
				}
			}
		})
	}
}

func TestLossyMuxSendAfterClose(t *testing.T) {
	controller, device := NewLossyMuxPair(256, nil)
	defer device.Close()

	controller.Close()
	err := controller.Send("dev-01", transport.Frame("hello"))
	if err != ErrClosed {
		t.Fatalf("Send after Close = %v, want %v", err, ErrClosed)
	}
}

func TestLossyMuxBidirectional(t *testing.T) {
	controller, device := NewLossyMuxPair(256, nil)
	defer controller.Close()
	defer device.Close()

	// Controller → Device
	if err := controller.Send("dev-01", transport.Frame("downlink")); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-device.Inbound():
		if string(d.Frame) != "downlink" {
			t.Errorf("device got %q, want %q", d.Frame, "downlink")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("device: expected delivery")
	}

	// Device → Controller
	if err := device.Send("dev-01", transport.Frame("uplink")); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-controller.Inbound():
		if string(d.Frame) != "uplink" {
			t.Errorf("controller got %q, want %q", d.Frame, "uplink")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("controller: expected delivery")
	}
}

func TestLossyMuxCaps(t *testing.T) {
	controller, _ := NewLossyMuxPair(512, nil)
	caps := controller.Caps()

	if caps.MaxFrameBytes != 512 {
		t.Errorf("MaxFrameBytes = %d, want 512", caps.MaxFrameBytes)
	}
	if caps.Ordered {
		t.Error("Ordered = true, want false")
	}
	if caps.Reliable {
		t.Error("Reliable = true, want false")
	}
	if !caps.Duplex {
		t.Error("Duplex = false, want true")
	}
}

func TestLossyMuxFrameCopy(t *testing.T) {
	controller, device := NewLossyMuxPair(256, nil)
	defer controller.Close()
	defer device.Close()

	data := []byte("original")
	if err := controller.Send("dev-01", transport.Frame(data)); err != nil {
		t.Fatal(err)
	}

	// Mutate the original after send.
	data[0] = 'X'

	select {
	case d := <-device.Inbound():
		if string(d.Frame) != "original" {
			t.Errorf("frame = %q, want %q (frame was not copied)", d.Frame, "original")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected delivery")
	}
}
