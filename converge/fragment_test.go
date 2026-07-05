package converge

import (
	"bytes"
	"errors"
	"testing"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/transport"
)

func TestEncodeDecodeOp(t *testing.T) {
	tests := []struct {
		name string
		op   shadow.Op
	}{
		{
			name: "reported op with data",
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: 42},
				DeviceID:  "dev-1",
				Section:   shadow.SectionReported,
				Key:       "temp",
				Data:      []byte("22.5"),
				Timestamp: hlc.Timestamp{Physical: 1000, Logical: 3, NodeID: 1},
			},
		},
		{
			name: "desired op with empty data",
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 2, Seq: 1},
				DeviceID:  "device-with-long-id",
				Section:   shadow.SectionDesired,
				Key:       "mode",
				Data:      nil,
				Timestamp: hlc.Timestamp{Physical: 500, Logical: 0, NodeID: 2},
			},
		},
		{
			name: "op with empty key",
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: 99},
				DeviceID:  "d",
				Section:   shadow.SectionReported,
				Key:       "",
				Data:      []byte{0xFF, 0x00, 0x01},
				Timestamp: hlc.Timestamp{Physical: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeOp(tt.op)
			decoded, err := DecodeOp(tt.op.ID, encoded)
			if err != nil {
				t.Fatalf("DecodeOp: %v", err)
			}

			if decoded.DeviceID != tt.op.DeviceID {
				t.Errorf("DeviceID = %q, want %q", decoded.DeviceID, tt.op.DeviceID)
			}
			if decoded.Section != tt.op.Section {
				t.Errorf("Section = %d, want %d", decoded.Section, tt.op.Section)
			}
			if decoded.Key != tt.op.Key {
				t.Errorf("Key = %q, want %q", decoded.Key, tt.op.Key)
			}
			if !bytes.Equal(decoded.Data, tt.op.Data) {
				t.Errorf("Data = %v, want %v", decoded.Data, tt.op.Data)
			}
			if hlc.Compare(decoded.Timestamp, tt.op.Timestamp) != 0 {
				t.Errorf("Timestamp = %v, want %v", decoded.Timestamp, tt.op.Timestamp)
			}
		})
	}
}

func TestFragment(t *testing.T) {
	id := shadow.OpID{NodeID: 1, Seq: 10}

	tests := []struct {
		name       string
		payload    []byte
		maxBytes   int
		wantFrames int
		wantErr    error
	}{
		{
			name:       "single frame fits exactly",
			payload:    make([]byte, 100),
			maxBytes:   100 + fragmentHeaderSize,
			wantFrames: 1,
		},
		{
			name:       "splits into two",
			payload:    make([]byte, 100),
			maxBytes:   64 + fragmentHeaderSize,
			wantFrames: 2,
		},
		{
			name:       "splits into many",
			payload:    make([]byte, 1000),
			maxBytes:   fragmentHeaderSize + 10,
			wantFrames: 100,
		},
		{
			name:       "empty payload produces one frame",
			payload:    []byte{},
			maxBytes:   fragmentHeaderSize + 1,
			wantFrames: 1,
		},
		{
			name:     "max bytes too small",
			payload:  make([]byte, 10),
			maxBytes: fragmentHeaderSize,
			wantErr:  ErrFrameTooSmall,
		},
		{
			name:       "one byte over triggers extra frame",
			payload:    make([]byte, 101),
			maxBytes:   100 + fragmentHeaderSize,
			wantFrames: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frames, err := Fragment(id, tt.payload, tt.maxBytes)
			if err != tt.wantErr {
				t.Fatalf("Fragment error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if len(frames) != tt.wantFrames {
				t.Errorf("got %d frames, want %d", len(frames), tt.wantFrames)
			}

			// Every frame must respect maxBytes.
			for i, f := range frames {
				if len(f) > tt.maxBytes {
					t.Errorf("frame[%d] size %d exceeds max %d", i, len(f), tt.maxBytes)
				}
			}
		})
	}
}

func TestFragmentReassembleRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		payload  []byte
		maxBytes int
	}{
		{
			name:     "single frame",
			payload:  []byte("hello world"),
			maxBytes: 256,
		},
		{
			name:     "multi frame",
			payload:  bytes.Repeat([]byte("x"), 500),
			maxBytes: fragmentHeaderSize + 50,
		},
		{
			name:     "exact fit",
			payload:  make([]byte, 100),
			maxBytes: 100 + fragmentHeaderSize,
		},
	}

	id := shadow.OpID{NodeID: 7, Seq: 42}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frames, err := Fragment(id, tt.payload, tt.maxBytes)
			if err != nil {
				t.Fatalf("Fragment: %v", err)
			}

			gotID, got, err := Reassemble(frames)
			if err != nil {
				t.Fatalf("Reassemble: %v", err)
			}
			if gotID != id {
				t.Errorf("reassembled ID = %v, want %v", gotID, id)
			}
			if !bytes.Equal(got, tt.payload) {
				t.Errorf("reassembled payload length = %d, want %d", len(got), len(tt.payload))
			}
		})
	}
}

func TestReassembleErrors(t *testing.T) {
	id := shadow.OpID{NodeID: 1, Seq: 1}
	payload := []byte("test payload data here")
	frames, err := Fragment(id, payload, fragmentHeaderSize+5)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}

	tests := []struct {
		name    string
		frames  []transport.Frame
		wantErr error
	}{
		{
			name:    "empty frame set",
			frames:  nil,
			wantErr: ErrIncomplete,
		},
		{
			name:    "missing fragment",
			frames:  frames[:len(frames)-1],
			wantErr: ErrIncomplete,
		},
		{
			name:    "frame too short",
			frames:  []transport.Frame{make([]byte, 5)},
			wantErr: ErrBadFragment,
		},
		{
			name: "mismatched OpID",
			frames: func() []transport.Frame {
				// Make two frames with different OpIDs.
				f1, _ := Fragment(shadow.OpID{NodeID: 1, Seq: 1}, []byte("ab"), fragmentHeaderSize+1)
				f2, _ := Fragment(shadow.OpID{NodeID: 2, Seq: 2}, []byte("cd"), fragmentHeaderSize+1)
				return []transport.Frame{f1[0], f2[0]}
			}(),
			wantErr: ErrFragmentMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Reassemble(tt.frames)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			// Use errors.Is for wrapped errors.
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseFragmentHeader(t *testing.T) {
	tests := []struct {
		name    string
		frame   transport.Frame
		wantErr error
	}{
		{
			name:    "too short",
			frame:   make([]byte, 10),
			wantErr: ErrBadFragment,
		},
		{
			name:  "valid header no payload",
			frame: make([]byte, fragmentHeaderSize),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, err := ParseFragmentHeader(tt.frame)
			if err != tt.wantErr {
				t.Errorf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
