package converge

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/transport"
)

// Fragment header layout (fixed 14 bytes):
//
//	[0:2]   NodeID     uint16 big-endian
//	[2:10]  Seq        uint64 big-endian
//	[10:12] FragIndex  uint16 big-endian
//	[12:14] FragTotal  uint16 big-endian
//	[14:]   Payload
const fragmentHeaderSize = 14

// ErrFrameTooSmall is returned when maxBytes is too small to hold even the
// fragment header plus one byte of payload.
var ErrFrameTooSmall = errors.New("converge: max frame bytes too small for fragment header")

// ErrTooManyFragments is returned when an op requires more than 65535 fragments.
var ErrTooManyFragments = errors.New("converge: op requires too many fragments")

// ErrBadFragment is returned when a frame is too short to be a valid fragment.
var ErrBadFragment = errors.New("converge: frame too short for fragment header")

// ErrIncomplete is returned when reassembly is attempted with missing fragments.
var ErrIncomplete = errors.New("converge: incomplete fragment set")

// ErrFragmentMismatch is returned when fragments have inconsistent totals or OpIDs.
var ErrFragmentMismatch = errors.New("converge: fragment total or OpID mismatch")

// EncodeOp serializes an Op into a byte slice suitable for fragmentation.
// Format:
//
//	[0:1]   SectionType  uint8
//	[1:9]   Timestamp.Physical  int64 big-endian
//	[9:11]  Timestamp.Logical   uint16 big-endian
//	[11:13] Timestamp.NodeID    uint16 big-endian
//	[13:15] KeyLen      uint16 big-endian
//	[15:15+KeyLen] Key  []byte
//	[15+KeyLen:17+KeyLen] DeviceIDLen uint16 big-endian
//	[17+KeyLen:17+KeyLen+DeviceIDLen] DeviceID []byte
//	[17+KeyLen+DeviceIDLen:] Data []byte
func EncodeOp(op shadow.Op) []byte {
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

// DecodeOp deserializes an Op from bytes produced by EncodeOp. The OpID must
// be supplied separately (it is carried in the fragment header).
func DecodeOp(id shadow.OpID, data []byte) (shadow.Op, error) {
	if len(data) < 15 {
		return shadow.Op{}, fmt.Errorf("converge: encoded op too short (%d bytes)", len(data))
	}
	op := shadow.Op{ID: id}
	op.Section = shadow.SectionType(data[0])
	op.Timestamp.Physical = int64(binary.BigEndian.Uint64(data[1:9]))
	op.Timestamp.Logical = binary.BigEndian.Uint16(data[9:11])
	op.Timestamp.NodeID = binary.BigEndian.Uint16(data[11:13])

	keyLen := int(binary.BigEndian.Uint16(data[13:15]))
	if len(data) < 15+keyLen+2 {
		return shadow.Op{}, fmt.Errorf("converge: encoded op truncated at key")
	}
	op.Key = string(data[15 : 15+keyLen])

	off := 15 + keyLen
	devLen := int(binary.BigEndian.Uint16(data[off : off+2]))
	if len(data) < off+2+devLen {
		return shadow.Op{}, fmt.Errorf("converge: encoded op truncated at device ID")
	}
	op.DeviceID = string(data[off+2 : off+2+devLen])

	off += 2 + devLen
	if off < len(data) {
		op.Data = make([]byte, len(data)-off)
		copy(op.Data, data[off:])
	}
	return op, nil
}

// Fragment splits a serialized op payload into frames, each at most maxBytes.
// The OpID is encoded in every fragment header for reassembly.
func Fragment(id shadow.OpID, payload []byte, maxBytes int) ([]transport.Frame, error) {
	maxPayload := maxBytes - fragmentHeaderSize
	if maxPayload < 1 {
		return nil, ErrFrameTooSmall
	}

	nFrags := (len(payload) + maxPayload - 1) / maxPayload
	if len(payload) == 0 {
		nFrags = 1
	}
	if nFrags > 0xFFFF {
		return nil, ErrTooManyFragments
	}

	frames := make([]transport.Frame, nFrags)
	for i := range nFrags {
		start := i * maxPayload
		end := start + maxPayload
		if end > len(payload) {
			end = len(payload)
		}
		chunk := payload[start:end]

		frame := make(transport.Frame, fragmentHeaderSize+len(chunk))
		binary.BigEndian.PutUint16(frame[0:2], id.NodeID)
		binary.BigEndian.PutUint64(frame[2:10], id.Seq)
		binary.BigEndian.PutUint16(frame[10:12], uint16(i))
		binary.BigEndian.PutUint16(frame[12:14], uint16(nFrags))
		copy(frame[fragmentHeaderSize:], chunk)

		frames[i] = frame
	}
	return frames, nil
}

// ParseFragmentHeader extracts the OpID, fragment index, and fragment total
// from a frame produced by Fragment.
func ParseFragmentHeader(frame transport.Frame) (id shadow.OpID, index, total uint16, payload []byte, err error) {
	if len(frame) < fragmentHeaderSize {
		return shadow.OpID{}, 0, 0, nil, ErrBadFragment
	}
	id.NodeID = binary.BigEndian.Uint16(frame[0:2])
	id.Seq = binary.BigEndian.Uint64(frame[2:10])
	index = binary.BigEndian.Uint16(frame[10:12])
	total = binary.BigEndian.Uint16(frame[12:14])
	payload = frame[fragmentHeaderSize:]
	return id, index, total, payload, nil
}

// Reassemble combines a set of fragments into the original payload. Fragments
// may be in any order. Returns ErrIncomplete if any are missing.
func Reassemble(frames []transport.Frame) (shadow.OpID, []byte, error) {
	if len(frames) == 0 {
		return shadow.OpID{}, nil, ErrIncomplete
	}

	id0, _, total0, _, err := ParseFragmentHeader(frames[0])
	if err != nil {
		return shadow.OpID{}, nil, err
	}

	if int(total0) != len(frames) {
		return shadow.OpID{}, nil, ErrIncomplete
	}

	// Collect fragments by index.
	parts := make([][]byte, total0)
	for _, frame := range frames {
		id, idx, total, payload, err := ParseFragmentHeader(frame)
		if err != nil {
			return shadow.OpID{}, nil, err
		}
		if id != id0 || total != total0 {
			return shadow.OpID{}, nil, ErrFragmentMismatch
		}
		if idx >= total {
			return shadow.OpID{}, nil, ErrFragmentMismatch
		}
		parts[idx] = payload
	}

	// Check completeness and compute total size.
	totalSize := 0
	for i, p := range parts {
		if p == nil {
			return shadow.OpID{}, nil, fmt.Errorf("%w: missing fragment %d", ErrIncomplete, i)
		}
		totalSize += len(p)
	}

	result := make([]byte, 0, totalSize)
	for _, p := range parts {
		result = append(result, p...)
	}
	return id0, result, nil
}
