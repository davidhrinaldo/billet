package resolver

import (
	"errors"

	"github.com/davidhrinaldo/billet/shadow"
)

// ErrDeviceMismatch is returned when an op targets a different device than the document.
var ErrDeviceMismatch = errors.New("resolver: op device ID does not match document")

// ErrInvalidSection is returned when an op has an unrecognized section type.
var ErrInvalidSection = errors.New("resolver: invalid section type")

// SectionAuthority is a resolver that enforces split authority: any writer may
// update the section they target, with last-write-wins (by HLC timestamp)
// within that section. It does not enforce which node is allowed to write which
// section — that is an access-control concern above the resolver.
type SectionAuthority struct{}

// Resolve applies an op to the document under section-authority rules.
// The op is accepted if it targets the correct device and a valid section.
// Within a section, a key is updated only if the op's timestamp is newer than
// the existing value's timestamp.
func (r *SectionAuthority) Resolve(doc *shadow.Document, op shadow.Op) (*shadow.Document, error) {
	if op.DeviceID != doc.DeviceID {
		return doc, ErrDeviceMismatch
	}

	var section *shadow.Section
	switch op.Section {
	case shadow.SectionReported:
		section = &doc.Reported
	case shadow.SectionDesired:
		section = &doc.Desired
	default:
		return doc, ErrInvalidSection
	}

	existing, ok := section.Values[op.Key]
	if ok && !existing.Timestamp.Before(op.Timestamp) {
		// Existing value is newer or equal; op is a no-op.
		return doc, nil
	}

	if section.Values == nil {
		section.Values = make(map[string]shadow.Value)
	}

	section.Values[op.Key] = shadow.Value{
		Data:      op.Data,
		Timestamp: op.Timestamp,
	}

	// Advance document version to the latest timestamp seen.
	if doc.Version.Before(op.Timestamp) {
		doc.Version = op.Timestamp
	}

	return doc, nil
}

// Verify SectionAuthority implements Resolver at compile time.
var _ Resolver = (*SectionAuthority)(nil)

