// Package sim provides a deterministic network simulator for property-based
// testing of billet's fleet convergence. It is a test artifact, not a
// production library.
//
// SimNet models a star topology (controller ↔ N devices, multiplexed by
// Channel) with per-device link fault injection: tunable loss, latency,
// reordering, and partitions. All randomness is seeded, so every test run is
// fully reproducible given its seed.
//
// The simulator uses a delivery heap instead of immediate channel sends.
// Frames are enqueued with a computed delivery time on Send; Deliver(nowNs)
// pops due frames and pushes them onto the destination's inbound channel.
// Variable latency produces natural reordering without any explicit shuffle.
//
// Property tests in this package assert three invariants across randomized
// scenarios:
//
//   - Eventual convergence: given eventual connectivity, all devices converge.
//   - No double-apply: no OpID is received by a device more than once.
//   - No lost writes: every SetDesired value appears in the converged state.
//
// Run with -short to skip property tests:
//
//	go test -short ./sim/...
//
// Reproduce a single seed:
//
//	go test -v -run TestConvergenceProperty/seed_42 ./sim/...
package sim
