// Package resolver defines the interface for resolving conflicts in shadow
// documents and provides the SectionAuthority implementation.
package resolver

import (
	"github.com/davidhrinaldo/billet/shadow"
)

// Resolver determines how to apply an incoming op to a document. It decides
// whether the op is accepted, rejected, or merged.
type Resolver interface {
	// Resolve applies an op to a document and returns the updated document.
	// If the op is rejected (e.g., wrong authority), it returns the document
	// unchanged and a non-nil error.
	Resolve(doc *shadow.Document, op shadow.Op) (*shadow.Document, error)
}
