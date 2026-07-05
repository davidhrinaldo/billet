// Package hlc implements a Hybrid Logical Clock suitable for ordering events
// across distributed nodes with unreliable wall clocks. It combines a physical
// timestamp (nanoseconds) with a bounded logical counter to preserve causality
// without requiring synchronized time.
//
// Based on the algorithm described in:
//
//	Kulkarni, S., Demirbas, M., Madappa, D., Avva, B., & Leone, M. (2014).
//	"Logical Physical Clocks and Consistent Snapshots in Globally Distributed
//	Databases." https://cse.buffalo.edu/tech-reports/2014-04.pdf
package hlc

import (
	"errors"
	"sync"
	"time"
)

// maxLogical is the maximum value of the logical counter before overflow.
const maxLogical = uint16(0xFFFF)

// Timestamp is a single HLC value: physical nanoseconds, a logical counter for
// sub-physical ordering, and a node ID for tie-breaking.
type Timestamp struct {
	Physical int64
	Logical  uint16
	NodeID   uint16
}

// Before reports whether ts is causally before other.
func (ts Timestamp) Before(other Timestamp) bool {
	return Compare(ts, other) < 0
}

// IsZero reports whether ts is the zero timestamp.
func (ts Timestamp) IsZero() bool {
	return ts == Timestamp{}
}

// Compare returns -1, 0, or 1 comparing a and b.
// Ordering: physical first, then logical, then node ID as tiebreaker.
func Compare(a, b Timestamp) int {
	switch {
	case a.Physical < b.Physical:
		return -1
	case a.Physical > b.Physical:
		return 1
	case a.Logical < b.Logical:
		return -1
	case a.Logical > b.Logical:
		return 1
	case a.NodeID < b.NodeID:
		return -1
	case a.NodeID > b.NodeID:
		return 1
	default:
		return 0
	}
}

// TimeSource abstracts wall-clock access for testing.
type TimeSource interface {
	Now() time.Time
}

// wallClock is the default TimeSource using time.Now.
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// ErrLogicalOverflow is returned when the logical counter would exceed uint16 max.
var ErrLogicalOverflow = errors.New("hlc: logical counter overflow")

// Clock is a hybrid logical clock bound to a specific node.
type Clock struct {
	mu     sync.Mutex
	nodeID uint16
	time   TimeSource
	// Last issued timestamp's physical and logical components.
	lastPhysical int64
	lastLogical  uint16
}

// NewClock creates a Clock for the given node ID. If ts is nil, the real wall
// clock is used.
func NewClock(nodeID uint16, ts TimeSource) *Clock {
	if ts == nil {
		ts = wallClock{}
	}
	return &Clock{
		nodeID: nodeID,
		time:   ts,
	}
}

// Now generates a new timestamp that is strictly greater than the last issued.
// Panics on logical counter overflow — see NowE for an error-returning variant.
func (c *Clock) Now() Timestamp {
	ts, err := c.NowE()
	if err != nil {
		panic(err)
	}
	return ts
}

// NowE generates a new timestamp, returning an error if the logical counter
// would overflow (physical clock stalled for 65535 consecutive events).
func (c *Clock) NowE() (Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := c.time.Now().UnixNano()

	if phys > c.lastPhysical {
		c.lastPhysical = phys
		c.lastLogical = 0
	} else {
		// Wall clock has not advanced; increment logical.
		if c.lastLogical >= maxLogical {
			return Timestamp{}, ErrLogicalOverflow
		}
		c.lastLogical++
	}

	return Timestamp{
		Physical: c.lastPhysical,
		Logical:  c.lastLogical,
		NodeID:   c.nodeID,
	}, nil
}

// Update merges a remote timestamp into the local clock state and returns a new
// timestamp that is strictly after both the local state and the remote timestamp.
// This is the "receive" operation in HLC literature.
func (c *Clock) Update(remote Timestamp) Timestamp {
	ts, err := c.updateE(remote)
	if err != nil {
		panic(err)
	}
	return ts
}

func (c *Clock) updateE(remote Timestamp) (Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := c.time.Now().UnixNano()

	// Pick the maximum of wall, local last, and remote physical.
	maxPhys := phys
	if c.lastPhysical > maxPhys {
		maxPhys = c.lastPhysical
	}
	if remote.Physical > maxPhys {
		maxPhys = remote.Physical
	}

	var logical uint16
	switch {
	case maxPhys == c.lastPhysical && maxPhys == remote.Physical:
		// All three tied: advance past both logical counters.
		logical = c.lastLogical
		if remote.Logical > logical {
			logical = remote.Logical
		}
		logical++
	case maxPhys == c.lastPhysical:
		// Local wins over remote and wall.
		logical = c.lastLogical + 1
	case maxPhys == remote.Physical:
		// Remote wins over local and wall.
		logical = remote.Logical + 1
	default:
		// Wall clock is ahead of both; reset logical.
		logical = 0
	}

	if logical == 0 && c.lastLogical == maxLogical {
		// Overflow check for the increment cases.
	}
	// The increment cases above can overflow if the source was maxLogical.
	if logical < c.lastLogical && maxPhys == c.lastPhysical && c.lastLogical == maxLogical {
		return Timestamp{}, ErrLogicalOverflow
	}

	c.lastPhysical = maxPhys
	c.lastLogical = logical

	return Timestamp{
		Physical: maxPhys,
		Logical:  logical,
		NodeID:   c.nodeID,
	}, nil
}

// setStateForTest allows tests to inject clock state directly.
func (c *Clock) setStateForTest(physical int64, logical uint16, nodeID uint16) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastPhysical = physical
	c.lastLogical = logical
	c.nodeID = nodeID
}
