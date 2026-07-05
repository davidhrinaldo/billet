// Package store defines the interface for billet's durable state storage.
// The store holds current shadow documents and the operation log.
package store

import (
	"errors"

	"github.com/davidhrinaldo/billet/shadow"
)

// ErrNotFound is returned when a requested document does not exist.
var ErrNotFound = errors.New("store: document not found")

// ErrOpExists is returned when attempting to append an op whose ID already exists.
var ErrOpExists = errors.New("store: op already exists")

// Store is the persistence interface for shadow documents and the op-log.
// Implementations must be safe for concurrent use.
type Store interface {
	// GetDocument retrieves a device's shadow document.
	// Returns ErrNotFound if the device has no stored document.
	GetDocument(deviceID shadow.DeviceID) (*shadow.Document, error)

	// PutDocument persists a shadow document, creating or replacing it.
	PutDocument(doc *shadow.Document) error

	// AppendOp appends an operation to the log. Returns ErrOpExists if the op
	// ID has already been recorded (deduplication).
	AppendOp(op shadow.Op) error

	// ListOps returns all ops for a device in timestamp order.
	ListOps(deviceID shadow.DeviceID) ([]shadow.Op, error)

	// HasOp reports whether an op with the given ID has been recorded.
	HasOp(id shadow.OpID) (bool, error)
}
