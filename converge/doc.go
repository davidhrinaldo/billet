// Package converge implements the reconciliation state machine, op encoding,
// fragmentation, and ack tracking for driving device convergence. The
// Reconciler drives a single device toward agreement between desired and
// reported state. EncodeOp/DecodeOp provide a versioned wire format for ops,
// and Fragment/Reassemble handle splitting oversized ops into transport frames.
//
// v0.1 API — do not break.
package converge
