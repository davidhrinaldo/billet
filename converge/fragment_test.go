package converge

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
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

// TestEncodeOpVersionPrefix verifies that EncodeOp produces the versioned
// format with the 0xBE marker and v1 version byte.
func TestEncodeOpVersionPrefix(t *testing.T) {
	op := shadow.Op{
		Section:   shadow.SectionReported,
		Key:       "k",
		Timestamp: hlc.Timestamp{Physical: 1},
	}
	buf := EncodeOp(op)
	if buf[0] != wireVersion {
		t.Errorf("byte[0] = 0x%02X, want 0x%02X (wireVersion)", buf[0], wireVersion)
	}
	if buf[1] != wireV1 {
		t.Errorf("byte[1] = 0x%02X, want 0x%02X (wireV1)", buf[1], wireV1)
	}
}

// TestDecodeOpLegacy verifies that DecodeOp can decode payloads encoded with
// the original unversioned format (first byte is SectionType, not 0xBE).
func TestDecodeOpLegacy(t *testing.T) {
	// Build a legacy payload by hand: the old format without version prefix.
	tests := []struct {
		name string
		op   shadow.Op
	}{
		{
			name: "legacy reported op",
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: 10},
				DeviceID:  "dev-1",
				Section:   shadow.SectionReported,
				Key:       "temp",
				Data:      []byte("22.5"),
				Timestamp: hlc.Timestamp{Physical: 1000, Logical: 3, NodeID: 1},
			},
		},
		{
			name: "legacy desired op empty data",
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 2, Seq: 1},
				DeviceID:  "d",
				Section:   shadow.SectionDesired,
				Key:       "mode",
				Timestamp: hlc.Timestamp{Physical: 500, Logical: 0, NodeID: 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			legacy := encodeLegacy(tt.op)

			// First byte must not be wireVersion — it should be the section type.
			if legacy[0] == wireVersion {
				t.Fatal("legacy encoding unexpectedly starts with wireVersion")
			}

			decoded, err := DecodeOp(tt.op.ID, legacy)
			if err != nil {
				t.Fatalf("DecodeOp(legacy): %v", err)
			}

			if decoded.Section != tt.op.Section {
				t.Errorf("Section = %d, want %d", decoded.Section, tt.op.Section)
			}
			if decoded.Key != tt.op.Key {
				t.Errorf("Key = %q, want %q", decoded.Key, tt.op.Key)
			}
			if decoded.DeviceID != tt.op.DeviceID {
				t.Errorf("DeviceID = %q, want %q", decoded.DeviceID, tt.op.DeviceID)
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

// encodeLegacy produces the original unversioned encoding (no 0xBE prefix).
func encodeLegacy(op shadow.Op) []byte {
	keyBytes := []byte(op.Key)
	devBytes := []byte(op.DeviceID)
	size := 1 + 8 + 2 + 2 + 2 + len(keyBytes) + 2 + len(devBytes) + len(op.Data)
	buf := make([]byte, size)

	buf[0] = byte(op.Section)
	binary.BigEndian.PutUint64(buf[1:9], uint64(op.Timestamp.Physical))
	binary.BigEndian.PutUint16(buf[9:11], op.Timestamp.Logical)
	binary.BigEndian.PutUint16(buf[11:13], op.Timestamp.NodeID)
	binary.BigEndian.PutUint16(buf[13:15], uint16(len(keyBytes)))
	copy(buf[15:], keyBytes)
	off := 15 + len(keyBytes)
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(devBytes)))
	copy(buf[off+2:], devBytes)
	off += 2 + len(devBytes)
	copy(buf[off:], op.Data)
	return buf
}

// TestDecodeOpUnknownVersion verifies that DecodeOp rejects payloads with
// an unrecognized version number.
func TestDecodeOpUnknownVersion(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr error
	}{
		{
			name:    "version 0xFF",
			data:    []byte{wireVersion, 0xFF, 0x01},
			wantErr: ErrUnknownWireVersion,
		},
		{
			name:    "version 0x02",
			data:    []byte{wireVersion, 0x02, 0x01},
			wantErr: ErrUnknownWireVersion,
		},
		{
			name:    "too short for any format",
			data:    []byte{0x01},
			wantErr: nil, // generic "too short" error, not ErrUnknownWireVersion
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeOp(shadow.OpID{}, tt.data)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestDecodeOpTruncated verifies both v1 and legacy decoders reject truncated
// payloads at each field boundary.
func TestDecodeOpTruncated(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "v1 truncated before timestamp",
			data: []byte{wireVersion, wireV1, 0x01},
		},
		{
			name: "v1 truncated at key length",
			data: func() []byte {
				// Valid header through timestamp (15 bytes), but key length
				// says 100 bytes which aren't there.
				buf := make([]byte, 17)
				buf[0] = wireVersion
				buf[1] = wireV1
				buf[2] = byte(shadow.SectionReported)
				binary.BigEndian.PutUint16(buf[15:17], 100)
				return buf
			}(),
		},
		{
			name: "legacy truncated before device ID",
			data: func() []byte {
				buf := make([]byte, 15)
				buf[0] = byte(shadow.SectionReported)
				// keyLen = 0, so device ID length starts at offset 15 — but
				// we only have 15 bytes.
				return buf
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeOp(shadow.OpID{}, tt.data)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// TestGoldenV1Vectors generates and verifies golden test vectors to ensure
// wire format stability. If the golden file does not exist, the test writes it
// (test generation). On subsequent runs, it verifies EncodeOp still produces
// the exact same bytes, and DecodeOp correctly round-trips.
func TestGoldenV1Vectors(t *testing.T) {
	vectors := []struct {
		tag string
		op  shadow.Op
	}{
		{
			tag: "reported_with_data",
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
			tag: "desired_nil_data",
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 2, Seq: 1},
				DeviceID:  "device-with-long-id",
				Section:   shadow.SectionDesired,
				Key:       "mode",
				Timestamp: hlc.Timestamp{Physical: 500, Logical: 0, NodeID: 2},
			},
		},
		{
			tag: "empty_key",
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: 99},
				DeviceID:  "d",
				Section:   shadow.SectionReported,
				Key:       "",
				Data:      []byte{0xFF, 0x00, 0x01},
				Timestamp: hlc.Timestamp{Physical: 1},
			},
		},
		{
			tag: "binary_data_256_bytes",
			op: shadow.Op{
				ID:       shadow.OpID{NodeID: 0xFFFF, Seq: 0xDEADBEEF},
				DeviceID: "gw-01",
				Section:  shadow.SectionDesired,
				Key:      "firmware",
				Data: func() []byte {
					b := make([]byte, 256)
					for i := range b {
						b[i] = byte(i)
					}
					return b
				}(),
				Timestamp: hlc.Timestamp{Physical: 1719100000000000000, Logical: 65535, NodeID: 0xFFFF},
			},
		},
	}

	goldenDir := filepath.Join("testdata")

	for _, v := range vectors {
		t.Run(v.tag, func(t *testing.T) {
			encoded := EncodeOp(v.op)
			goldenPath := filepath.Join(goldenDir, "op_v1_"+v.tag+".hex")

			golden, err := os.ReadFile(goldenPath)
			if errors.Is(err, os.ErrNotExist) {
				// First run: write the golden file.
				if err := os.WriteFile(goldenPath, []byte(hex.EncodeToString(encoded)), 0644); err != nil {
					t.Fatalf("writing golden file: %v", err)
				}
				t.Logf("wrote golden file %s (%d bytes)", goldenPath, len(encoded))
				golden = []byte(hex.EncodeToString(encoded))
			} else if err != nil {
				t.Fatalf("reading golden file: %v", err)
			}

			want, err := hex.DecodeString(string(golden))
			if err != nil {
				t.Fatalf("decoding golden hex: %v", err)
			}

			// Verify EncodeOp produces identical bytes.
			if !bytes.Equal(encoded, want) {
				t.Errorf("EncodeOp output changed:\n  got: %s\n  want: %s",
					hex.EncodeToString(encoded), string(golden))
			}

			// Verify DecodeOp round-trips.
			decoded, err := DecodeOp(v.op.ID, want)
			if err != nil {
				t.Fatalf("DecodeOp(golden): %v", err)
			}
			if decoded.Section != v.op.Section {
				t.Errorf("Section = %d, want %d", decoded.Section, v.op.Section)
			}
			if decoded.Key != v.op.Key {
				t.Errorf("Key = %q, want %q", decoded.Key, v.op.Key)
			}
			if decoded.DeviceID != v.op.DeviceID {
				t.Errorf("DeviceID = %q, want %q", decoded.DeviceID, v.op.DeviceID)
			}
			if !bytes.Equal(decoded.Data, v.op.Data) {
				t.Errorf("Data mismatch: got %d bytes, want %d bytes", len(decoded.Data), len(v.op.Data))
			}
			if hlc.Compare(decoded.Timestamp, v.op.Timestamp) != 0 {
				t.Errorf("Timestamp = %v, want %v", decoded.Timestamp, v.op.Timestamp)
			}
		})
	}
}
