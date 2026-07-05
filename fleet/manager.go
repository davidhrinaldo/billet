package fleet

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/davidhrinaldo/billet/converge"
	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/oplog"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store"
	"github.com/davidhrinaldo/billet/transport"
)

// ErrUnknownDevice is returned when an operation targets a device that has not
// been added to the Manager.
var ErrUnknownDevice = errors.New("fleet: unknown device")

// defaultEventSize is the event channel capacity when ManagerConfig.EventSize
// is zero.
const defaultEventSize = 256

// ManagerConfig holds the parameters for creating a Manager.
type ManagerConfig struct {
	// Store is the shared store for all device documents and op-logs.
	Store store.Store
	// Transport is the controller-side transport for sending and receiving
	// frames.
	Transport transport.Transport
	// Clock is the HLC clock used by all reconcilers.
	Clock *hlc.Clock
	// Timeout is the per-device ack timeout passed to each Reconciler.
	Timeout time.Duration
	// Budget is the default rate-limiter configuration for each device.
	Budget BudgetConfig
	// EventSize is the buffered event channel capacity. Zero uses the default
	// of 256.
	EventSize int
	// Policy is the op-log compaction policy for each device's log.
	Policy oplog.Policy
}

// Manager orchestrates convergence for a fleet of devices. It is safe for
// concurrent use — internal locking serializes access to per-device
// reconcilers.
type Manager struct {
	mu      sync.Mutex
	cfg     ManagerConfig
	devices map[shadow.DeviceID]*deviceEntry
	events  chan Event
}

// deviceEntry holds per-device state managed by the Manager.
type deviceEntry struct {
	reconciler *converge.Reconciler
	log        *oplog.Log
	budget     budget
	lastState  converge.State
	stalledAt  int64 // physical ns when device entered current non-Synced state
}

// NewManager creates a Manager for orchestrating fleet convergence.
func NewManager(cfg ManagerConfig) *Manager {
	evSize := cfg.EventSize
	if evSize <= 0 {
		evSize = defaultEventSize
	}
	return &Manager{
		cfg:     cfg,
		devices: make(map[shadow.DeviceID]*deviceEntry),
		events:  make(chan Event, evSize),
	}
}

// Add registers a device in the fleet. It creates a per-device op-log and
// reconciler. If the device is already registered, Add is a no-op.
func (m *Manager) Add(id shadow.DeviceID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.devices[id]; ok {
		return
	}

	var opts []oplog.Option
	if m.cfg.Policy.MaxOps > 0 || m.cfg.Policy.MaxBytes > 0 {
		opts = append(opts, oplog.WithPolicy(m.cfg.Policy))
	}
	lg := oplog.New(m.cfg.Store, opts...)

	rec := converge.NewReconciler(converge.ReconcilerConfig{
		DeviceID:  id,
		Log:       lg,
		Store:     m.cfg.Store,
		Transport: m.cfg.Transport,
		Clock:     m.cfg.Clock,
		Timeout:   m.cfg.Timeout,
	})

	m.devices[id] = &deviceEntry{
		reconciler: rec,
		log:        lg,
		budget:     newBudget(m.cfg.Budget, 0),
		lastState:  converge.Synced,
	}
}

// Remove unregisters a device from the fleet. If the device is not registered,
// Remove is a no-op.
func (m *Manager) Remove(id shadow.DeviceID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.devices, id)
}

// SetDesired sets a desired key-value pair for a single device. Returns
// ErrUnknownDevice if the device has not been added.
func (m *Manager) SetDesired(id shadow.DeviceID, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.devices[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownDevice, id)
	}
	return entry.reconciler.SetDesired(key, data)
}

// GroupError collects per-device errors from a group operation.
type GroupError struct {
	// Errors maps device IDs to their individual errors.
	Errors map[shadow.DeviceID]error
}

// Error returns a summary of the group errors.
func (e *GroupError) Error() string {
	var b strings.Builder
	b.WriteString("fleet: group operation failed for devices: ")
	first := true
	for id, err := range e.Errors {
		if !first {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s (%s)", id, err)
		first = false
	}
	return b.String()
}

// SetDesiredGroup sets a desired key-value pair for a set of devices. It
// attempts all devices and collects errors. Returns nil if all succeed, or a
// *GroupError with per-device errors.
func (m *Manager) SetDesiredGroup(ids []shadow.DeviceID, key string, data []byte) *GroupError {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs map[shadow.DeviceID]error
	for _, id := range ids {
		entry, ok := m.devices[id]
		if !ok {
			if errs == nil {
				errs = make(map[shadow.DeviceID]error)
			}
			errs[id] = fmt.Errorf("%w: %s", ErrUnknownDevice, id)
			continue
		}
		if err := entry.reconciler.SetDesired(key, data); err != nil {
			if errs == nil {
				errs = make(map[shadow.DeviceID]error)
			}
			errs[id] = err
		}
	}
	if errs != nil {
		return &GroupError{Errors: errs}
	}
	return nil
}

// DrainInbound reads all pending deliveries from the transport and routes them
// to the appropriate per-device reconcilers. Frames addressed to unknown
// devices are silently ignored.
//
// Each delivery is decoded as a single-fragment op and applied as a reported
// update via OnReported. Multi-fragment reassembly is not performed here — the
// transport is assumed to deliver complete ops (or the device-side encoder
// fits ops into single frames).
func (m *Manager) DrainInbound() {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := m.cfg.Transport.Inbound()
	for {
		select {
		case d := <-ch:
			m.handleDelivery(d)
		default:
			return
		}
	}
}

// handleDelivery processes a single inbound delivery. Caller must hold mu.
func (m *Manager) handleDelivery(d transport.Delivery) {
	id := shadow.DeviceID(d.Channel)
	entry, ok := m.devices[id]
	if !ok {
		return
	}

	// Parse the fragment header to extract the OpID, then decode the op.
	opID, _, _, payload, err := converge.ParseFragmentHeader(d.Frame)
	if err != nil {
		return
	}
	op, err := converge.DecodeOp(opID, payload)
	if err != nil {
		return
	}

	_ = entry.reconciler.OnReported(op)
}

// Tick advances the fleet by one time step. It refills rate-limiter budgets,
// flushes pending devices that have available budget, ticks all reconcilers to
// detect timeouts, and emits observability events for state transitions.
func (m *Manager) Tick(nowPhysicalNs int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Phase 1: refill all budgets.
	for _, entry := range m.devices {
		entry.budget.refill(nowPhysicalNs)
	}

	// Phase 2: flush pending devices with available budget.
	for _, entry := range m.devices {
		if entry.reconciler.CurrentState() == converge.Pending && entry.budget.consume() {
			if err := entry.reconciler.Flush(); err != nil {
				return err
			}
		}
	}

	// Phase 3: tick all reconcilers (timeout/retry).
	for _, entry := range m.devices {
		state := entry.reconciler.CurrentState()
		if state == converge.Inflight || state == converge.TimedOut {
			if err := entry.reconciler.Tick(nowPhysicalNs); err != nil {
				return err
			}
		}
	}

	// Phase 4: detect state changes and emit events.
	for id, entry := range m.devices {
		cur := entry.reconciler.CurrentState()
		if cur != entry.lastState {
			m.emitEvent(Event{
				Kind:     stateChangeKind(cur),
				DeviceID: id,
				From:     entry.lastState,
				To:       cur,
				At:       nowPhysicalNs,
			})

			// Update stall tracking.
			if cur == converge.Synced {
				entry.stalledAt = 0
			} else if entry.lastState == converge.Synced {
				entry.stalledAt = nowPhysicalNs
			}
			entry.lastState = cur
		}
	}

	return nil
}

// stateChangeKind returns the appropriate EventKind for a state transition.
func stateChangeKind(to converge.State) EventKind {
	switch to {
	case converge.Synced:
		return EventConverged
	case converge.Diverged:
		return EventDiverged
	default:
		return EventStateChange
	}
}

// emitEvent sends an event to the events channel. If the channel is full the
// event is dropped — the Manager must never block on observability.
func (m *Manager) emitEvent(ev Event) {
	select {
	case m.events <- ev:
	default:
	}
}

// Events returns the channel that delivers fleet events. The caller should
// drain this channel to avoid dropped events.
func (m *Manager) Events() <-chan Event {
	return m.events
}

// StallReport returns a snapshot of all devices that have not reached the
// Synced state.
func (m *Manager) StallReport() StallReport {
	m.mu.Lock()
	defer m.mu.Unlock()

	var report StallReport
	for id, entry := range m.devices {
		state := entry.reconciler.CurrentState()
		if state != converge.Synced {
			report.Stalled = append(report.Stalled, StalledDevice{
				DeviceID: id,
				State:    state,
				Since:    entry.stalledAt,
			})
		}
	}
	return report
}

// DeviceState returns the current convergence state of a device. The second
// return value is false if the device is not registered.
func (m *Manager) DeviceState(id shadow.DeviceID) (converge.State, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.devices[id]
	if !ok {
		return 0, false
	}
	return entry.reconciler.CurrentState(), true
}

// Lagging returns the device IDs of all devices not in the Synced state.
func (m *Manager) Lagging() []shadow.DeviceID {
	m.mu.Lock()
	defer m.mu.Unlock()

	var ids []shadow.DeviceID
	for id, entry := range m.devices {
		if entry.reconciler.CurrentState() != converge.Synced {
			ids = append(ids, id)
		}
	}
	return ids
}

// Devices returns the number of devices registered in the fleet.
func (m *Manager) Devices() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.devices)
}
