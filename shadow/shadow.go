// Package shadow defines the core domain types for billet's device shadow:
// documents with reported/desired sections, operations, and delta computation.
package shadow

import (
	"bytes"

	"github.com/davidhrinaldo/billet/hlc"
)

// DeviceID uniquely identifies a device within a fleet.
type DeviceID = string

// Document is the shadow state for a single device. It holds the reported state
// (owned by the device), desired state (owned by the controller), and provides
// delta computation between them.
type Document struct {
	DeviceID DeviceID
	Reported Section
	Desired  Section
	Version  hlc.Timestamp
}

// Delta returns the current delta between desired and reported.
func (d *Document) Delta() Delta {
	return ComputeDelta(d.Reported, d.Desired)
}

// Section is one half of a shadow document (reported or desired). It maps keys
// to timestamped values.
type Section struct {
	Values map[string]Value
}

// Value is a single timestamped entry within a section.
type Value struct {
	Data      []byte
	Timestamp hlc.Timestamp
}

// Op represents a single idempotent operation on a shadow document.
type Op struct {
	ID        OpID
	DeviceID  DeviceID
	Section   SectionType
	Key       string
	Data      []byte
	Timestamp hlc.Timestamp
}

// OpID uniquely identifies an operation for deduplication.
type OpID struct {
	// NodeID is the originating node.
	NodeID uint16
	// Seq is a node-local monotonic sequence number.
	Seq uint64
}

// SectionType indicates which section an operation targets.
type SectionType uint8

const (
	// SectionReported targets the reported section (device-owned).
	SectionReported SectionType = iota + 1
	// SectionDesired targets the desired section (controller-owned).
	SectionDesired
)

// Delta represents the set of keys where desired differs from reported.
type Delta struct {
	Diffs map[string]Diff
}

// IsEmpty reports whether the delta contains no differences.
func (d Delta) IsEmpty() bool {
	return len(d.Diffs) == 0
}

// Diff is a single key's divergence between desired and reported.
type Diff struct {
	Key      string
	Desired  []byte
	Reported []byte // nil if the key has no reported value
}

// ComputeDelta computes the delta between reported and desired sections.
// A diff exists for each key in desired whose value differs from reported.
// Keys present only in reported do not produce diffs (reported state the device
// already has is not a divergence).
func ComputeDelta(reported, desired Section) Delta {
	diffs := make(map[string]Diff)

	for key, dv := range desired.Values {
		rv, ok := reported.Values[key]
		if !ok {
			diffs[key] = Diff{Key: key, Desired: dv.Data, Reported: nil}
			continue
		}
		if !bytes.Equal(rv.Data, dv.Data) {
			diffs[key] = Diff{Key: key, Desired: dv.Data, Reported: rv.Data}
		}
	}

	return Delta{Diffs: diffs}
}
