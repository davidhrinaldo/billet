package sim

import (
	"testing"

	"github.com/davidhrinaldo/billet/transport"
)

func TestDeliverOrdering(t *testing.T) {
	// Frames with different delays should arrive in delay order.
	net := New(1, 1024)
	net.SetDefault(LinkConfig{MinDelay: 0, MaxDelay: 0})

	ctrl := net.Controller()

	// Send three frames with explicit per-channel delays so they arrive
	// out of send order.
	net.SetLink("fast", LinkConfig{MinDelay: 100, MaxDelay: 100})
	net.SetLink("medium", LinkConfig{MinDelay: 200, MaxDelay: 200})
	net.SetLink("slow", LinkConfig{MinDelay: 300, MaxDelay: 300})

	if err := ctrl.Send("slow", []byte("A")); err != nil {
		t.Fatalf("Send slow: %v", err)
	}
	if err := ctrl.Send("fast", []byte("B")); err != nil {
		t.Fatalf("Send fast: %v", err)
	}
	if err := ctrl.Send("medium", []byte("C")); err != nil {
		t.Fatalf("Send medium: %v", err)
	}

	// Deliver at time 350 — all three should be due.
	n := net.Deliver(350)
	if n != 3 {
		t.Fatalf("Deliver returned %d, want 3", n)
	}

	dev := net.Device()
	want := []string{"B", "C", "A"} // fast(100), medium(200), slow(300)
	for i, w := range want {
		select {
		case d := <-dev.Inbound():
			if string(d.Frame) != w {
				t.Errorf("frame %d: got %q, want %q", i, d.Frame, w)
			}
		default:
			t.Fatalf("frame %d: no delivery", i)
		}
	}
}

func TestDeliverStableOrder(t *testing.T) {
	// Frames with equal delivery times should arrive in insertion order.
	net := New(1, 1024)
	net.SetDefault(LinkConfig{MinDelay: 0, MaxDelay: 0})

	ctrl := net.Controller()
	for _, msg := range []string{"A", "B", "C", "D", "E"} {
		if err := ctrl.Send("ch", []byte(msg)); err != nil {
			t.Fatalf("Send %s: %v", msg, err)
		}
	}

	net.Deliver(1)

	dev := net.Device()
	for _, want := range []string{"A", "B", "C", "D", "E"} {
		select {
		case d := <-dev.Inbound():
			if string(d.Frame) != want {
				t.Errorf("got %q, want %q", d.Frame, want)
			}
		default:
			t.Fatalf("expected frame %q, got nothing", want)
		}
	}
}

func TestLossRate(t *testing.T) {
	tests := []struct {
		name     string
		loss     float64
		sends    int
		wantMin  int // minimum expected drops
		wantMax  int // maximum expected drops
	}{
		{
			name:    "zero loss",
			loss:    0.0,
			sends:   100,
			wantMin: 0,
			wantMax: 0,
		},
		{
			name:    "total loss",
			loss:    1.0,
			sends:   100,
			wantMin: 100,
			wantMax: 100,
		},
		{
			name:    "50% loss statistical",
			loss:    0.5,
			sends:   1000,
			wantMin: 400, // generous bounds for seeded PRNG
			wantMax: 600,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			net := New(42, 1024)
			net.SetDefault(LinkConfig{Loss: tt.loss})

			ctrl := net.Controller()
			for i := range tt.sends {
				_ = ctrl.Send("dev", []byte{byte(i)})
			}

			dropped := net.Dropped()
			if dropped < tt.wantMin || dropped > tt.wantMax {
				t.Errorf("dropped %d frames, want [%d, %d]", dropped, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestLatencyBounds(t *testing.T) {
	tests := []struct {
		name     string
		minDelay int64
		maxDelay int64
	}{
		{name: "zero latency", minDelay: 0, maxDelay: 0},
		{name: "fixed latency", minDelay: 1000, maxDelay: 1000},
		{name: "variable latency", minDelay: 100, maxDelay: 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			net := New(42, 1024)
			net.SetDefault(LinkConfig{MinDelay: tt.minDelay, MaxDelay: tt.maxDelay})

			ctrl := net.Controller()
			sendTime := int64(1000)
			net.Deliver(sendTime) // set nowNs

			for i := range 20 {
				_ = ctrl.Send("dev", []byte{byte(i)})
			}

			// Nothing should be delivered before MinDelay.
			if tt.minDelay > 0 {
				n := net.Deliver(sendTime + tt.minDelay - 1)
				if n != 0 {
					t.Errorf("delivered %d frames before MinDelay", n)
				}
			}

			// Everything should be delivered by MaxDelay.
			n := net.Deliver(sendTime + tt.maxDelay)
			if n != 20 {
				t.Errorf("delivered %d frames at MaxDelay, want 20", n)
			}
		})
	}
}

func TestPartition(t *testing.T) {
	net := New(1, 1024)
	ctrl := net.Controller()
	dev := net.Device()

	// Send before partition — should deliver.
	_ = ctrl.Send("dev-1", []byte("before"))
	net.Deliver(1)

	select {
	case d := <-dev.Inbound():
		if string(d.Frame) != "before" {
			t.Errorf("got %q, want %q", d.Frame, "before")
		}
	default:
		t.Fatal("expected frame before partition")
	}

	// Partition and send — should be dropped.
	net.Partition("dev-1")
	_ = ctrl.Send("dev-1", []byte("during"))
	net.Deliver(2)

	select {
	case d := <-dev.Inbound():
		t.Errorf("got frame during partition: %q", d.Frame)
	default:
		// expected
	}

	if net.Dropped() != 1 {
		t.Errorf("dropped %d, want 1", net.Dropped())
	}

	// Heal and send — should deliver.
	net.Heal("dev-1")
	_ = ctrl.Send("dev-1", []byte("after"))
	net.Deliver(3)

	select {
	case d := <-dev.Inbound():
		if string(d.Frame) != "after" {
			t.Errorf("got %q, want %q", d.Frame, "after")
		}
	default:
		t.Fatal("expected frame after heal")
	}
}

func TestPartitionAll(t *testing.T) {
	net := New(1, 1024)
	ctrl := net.Controller()
	dev := net.Device()

	net.PartitionAll()

	_ = ctrl.Send("dev-1", []byte("a"))
	_ = ctrl.Send("dev-2", []byte("b"))
	net.Deliver(1)

	select {
	case d := <-dev.Inbound():
		t.Errorf("got frame during PartitionAll: %q", d.Frame)
	default:
		// expected
	}

	if net.Dropped() != 2 {
		t.Errorf("dropped %d, want 2", net.Dropped())
	}

	net.HealAll()

	_ = ctrl.Send("dev-1", []byte("c"))
	net.Deliver(2)

	select {
	case d := <-dev.Inbound():
		if string(d.Frame) != "c" {
			t.Errorf("got %q, want %q", d.Frame, "c")
		}
	default:
		t.Fatal("expected frame after HealAll")
	}
}

func TestPerDeviceLink(t *testing.T) {
	net := New(42, 1024)
	net.SetDefault(LinkConfig{Loss: 0.0})

	// One device is lossy, another is reliable.
	net.SetLink("lossy-dev", LinkConfig{Loss: 1.0})
	net.SetLink("reliable-dev", LinkConfig{Loss: 0.0})

	ctrl := net.Controller()
	for i := range 10 {
		_ = ctrl.Send("lossy-dev", []byte{byte(i)})
		_ = ctrl.Send("reliable-dev", []byte{byte(i)})
	}

	net.Deliver(1)

	if net.Dropped() != 10 {
		t.Errorf("dropped %d, want 10 (all lossy-dev)", net.Dropped())
	}

	// Drain the reliable deliveries.
	dev := net.Device()
	count := 0
	for {
		select {
		case <-dev.Inbound():
			count++
		default:
			goto done
		}
	}
done:
	if count != 10 {
		t.Errorf("received %d reliable frames, want 10", count)
	}
}

func TestDeterminism(t *testing.T) {
	// Two SimNets with the same seed must produce identical behavior.
	run := func(seed int64) []transport.Delivery {
		net := New(seed, 1024)
		net.SetDefault(LinkConfig{Loss: 0.3, MinDelay: 10, MaxDelay: 100})

		ctrl := net.Controller()
		for i := range 50 {
			_ = ctrl.Send(transport.Channel("dev"), []byte{byte(i)})
		}

		net.Deliver(200)

		var deliveries []transport.Delivery
		dev := net.Device()
		for {
			select {
			case d := <-dev.Inbound():
				deliveries = append(deliveries, d)
			default:
				return deliveries
			}
		}
	}

	a := run(99)
	b := run(99)

	if len(a) != len(b) {
		t.Fatalf("different delivery counts: %d vs %d", len(a), len(b))
	}

	for i := range a {
		if string(a[i].Frame) != string(b[i].Frame) {
			t.Errorf("frame %d differs: %q vs %q", i, a[i].Frame, b[i].Frame)
		}
		if a[i].Channel != b[i].Channel {
			t.Errorf("channel %d differs: %q vs %q", i, a[i].Channel, b[i].Channel)
		}
	}
}

func TestFrameTooLarge(t *testing.T) {
	net := New(1, 64)
	ctrl := net.Controller()

	tests := []struct {
		name    string
		size    int
		wantErr error
	}{
		{name: "within limit", size: 64, wantErr: nil},
		{name: "exceeds limit", size: 65, wantErr: ErrFrameTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ctrl.Send("dev", make([]byte, tt.size))
			if err != tt.wantErr {
				t.Errorf("Send(%d bytes): got %v, want %v", tt.size, err, tt.wantErr)
			}
		})
	}
}

func TestClosedTransport(t *testing.T) {
	net := New(1, 1024)
	ctrl := net.Controller()

	if err := ctrl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := ctrl.Send("dev", []byte("hello"))
	if err != ErrClosed {
		t.Errorf("Send after Close: got %v, want %v", err, ErrClosed)
	}
}

func TestInflightCount(t *testing.T) {
	net := New(1, 1024)
	net.SetDefault(LinkConfig{MinDelay: 100, MaxDelay: 100})

	ctrl := net.Controller()
	_ = ctrl.Send("dev", []byte("a"))
	_ = ctrl.Send("dev", []byte("b"))
	_ = ctrl.Send("dev", []byte("c"))

	if net.Inflight() != 3 {
		t.Errorf("Inflight: got %d, want 3", net.Inflight())
	}

	net.Deliver(50) // too early — nothing delivered
	if net.Inflight() != 3 {
		t.Errorf("Inflight after early deliver: got %d, want 3", net.Inflight())
	}

	net.Deliver(100) // exactly on time
	if net.Inflight() != 0 {
		t.Errorf("Inflight after deliver: got %d, want 0", net.Inflight())
	}
}

func TestBidirectional(t *testing.T) {
	// Verify that device can send to controller and vice versa.
	net := New(1, 1024)
	ctrl := net.Controller()
	dev := net.Device()

	// Controller → device.
	_ = ctrl.Send("dev-1", []byte("c2d"))
	net.Deliver(1)

	select {
	case d := <-dev.Inbound():
		if string(d.Frame) != "c2d" {
			t.Errorf("dev got %q, want %q", d.Frame, "c2d")
		}
	default:
		t.Fatal("dev: expected frame from controller")
	}

	// Device → controller.
	_ = dev.Send("dev-1", []byte("d2c"))
	net.Deliver(2)

	select {
	case d := <-ctrl.Inbound():
		if string(d.Frame) != "d2c" {
			t.Errorf("ctrl got %q, want %q", d.Frame, "d2c")
		}
	default:
		t.Fatal("ctrl: expected frame from device")
	}
}
