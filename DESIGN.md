# billet — Design & Milestones

## Package Layout

```
billet/
├── shadow/          # Core domain: Document, Section, Op, OpID, Delta
├── hlc/             # Hybrid Logical Clock (standalone, no deps beyond std)
├── transport/       # Transport interface, Frame, Channel, Caps
├── store/           # Store interface + in-memory impl
│   └── memstore/
├── history/         # History interface + in-memory stub
│   └── memhistory/
├── resolver/        # Resolver interface + SectionAuthority impl
├── converge/        # (M2+) Reconciler state machine
├── oplog/           # (M2+) Op-log snapshot/truncate
└── internal/
    ├── testutil/    # Shared test helpers (loopback transport, clock fakes)
    └── sim/         # Deterministic network simulator + property-based convergence tests
```

### Why this layout

- `hlc` is leaf-level: no imports from billet. Useful standalone.
- `shadow` imports `hlc` only. This is the domain vocabulary.
- `transport` is pure interface + value types (Frame, Channel, Caps). No implementation here — adapters are external; the loopback lives in testutil.
- `store` and `history` define interfaces at the package root; sub-packages provide implementations. You can import the interface without pulling Pebble.
- `resolver` depends on `shadow` (needs Section, Op). SectionAuthority is the only resolver shipped in M1.
- `converge` and `oplog` are seams: exported interface types defined now, implementations in M2.

## Build Order (M1)

1. `hlc` — zero internal deps, testable in isolation.
2. `shadow` — domain types, imports hlc.
3. `transport` — interface + value types only.
4. `store` — interface, then `memstore`.
5. `history` — interface, then `memhistory` stub.
6. `resolver` — interface + SectionAuthority, tests use memstore.
7. `internal/testutil` — loopback transport for integration tests.

## Milestones

### M1 — Domain & Seams

Deliverables:
- Go module, package structure as above.
- Core domain types: `Document`, `Section`, `Op`, `OpID`, `Delta`.
- HLC with full test coverage (monotonicity, causality, skew, tick/update/merge).
- Interfaces: `Store`, `History`, `Transport`, `Resolver`.
- `Frame`, `Channel`, `Capabilities` value types.
- SectionAuthority resolver + tests.
- In-memory Store, History, loopback Transport.

Done when: `go test ./...` passes, the in-memory variants satisfy their interfaces, and the resolver computes deltas correctly under section authority rules.

---

### M2 — Op-Log & Reconciler

Deliverables:
- Op-log: append, replay, dedup (by OpID), snapshot, truncate.
- Reconciler state machine: drives a single device toward convergence.
  - States: `Synced | Pending | Inflight | TimedOut | Diverged`.
  - On reported update: compare to desired, transition state.
  - On timeout: re-issue from op-log tail.
- Fragmentation layer: split oversized ops into frames respecting `Caps.MaxFrameBytes`.
- Ack tracking: application-level ack/nack per OpID.
- Integration test: loopback transport, memstore, full cycle desired→inflight→reported→synced.

Done when: a device goes offline, misses commands, reconnects, and converges to desired — all in a deterministic test with no real I/O.

---

### M3 — Pebble Store & ingot History

Deliverables:
- Pebble-backed Store implementation (separate build tag or sub-module to isolate CGo).
- Wire ingot as the real History backend.
- Benchmarks: op-log replay throughput, snapshot size, ingot write path.
- Flash-budget enforcement: configurable max op-log size, auto-truncate policy.

Done when: sustained write load on Pi-class hardware stays within flash-write budget; ingot holds 90+ days of 1-min data for 64 channels in <100 MB.

#### Storage validation

`TestCompacted90DayStorage` (history/ingothistory/bench_test.go) simulates 90
days × 64 channels × 1-min samples with flush every 2h and compaction every
day. Result: 91.56 MB (11.58 bytes/sample). Head-only extrapolation was 307 MB;
compacted XOR encoding is 3.3× smaller. Skipped with `-short`.

---

### M4 — Fleet Convergence

Deliverables:
- Multi-device convergence manager: tracks N devices, surfaces "who's lagging."
- Group desired: set desired for a device set, fan-out to per-device reconcilers.
- Backpressure: respect duty-cycle / downlink budget (configurable rate limiter per device).
- Observability hooks: callbacks or channel for state transitions, stall detection.

Done when: 400-device simulated fleet over lossy loopback converges within bounded time; stall report correctly identifies the non-converged subset.

---

### M5 — Hardening & Ergonomics

Deliverables:
- Wire format stability (versioned encoding, backward-compat test suite).
- Defined degradation behavior for each failure mode: Store unavailable, History full, Transport down.
- Example integration: ChirpStack gRPC stream → billet shadow + convergence.
- API surface review and freeze for v0.1.

#### v0.1 API Freeze

All public packages carry a `// v0.1 API — do not break.` marker in their
package doc. The exported surface was audited and frozen as of M5. Packages
covered: hlc, shadow, transport, store, history, resolver, converge, oplog,
fleet. Implementation sub-packages (memstore, pebblestore, memhistory,
ingothistory) and internal/testutil are not frozen.

#### Wire Format

EncodeOp prepends a 0xBE marker byte + version byte (currently 0x01). DecodeOp
auto-detects legacy (unversioned) payloads by checking byte 0: if it is not
0xBE, the legacy decoder is used. Golden test vectors in
`converge/testdata/op_v1_*.hex` lock the encoding.

#### Degradation Modes

| Failure | Behavior |
|---------|----------|
| Store unavailable | Per-device error emitted as `EventError`; device skipped for that tick, state unchanged. |
| Transport down | `Flush`/`Tick` error emitted as `EventError`; device stays in current state, retried next tick. |
| History full | Caller's responsibility — billet does not write history from the fleet manager. |
| Corrupt inbound frame | `EventError` emitted with decode error; frame dropped, device state unchanged. |

---

## Key Design Decisions

**HLC over LWW wall-clock.** Edge RTCs drift minutes after a reboot with no NTP. Wall-clock LWW is a footgun. HLC preserves causality cheaply: 64-bit physical + 16-bit logical + 16-bit node ID fits in a struct.

**Op-log in KV, not ingot.** Ops are heterogeneous (config blobs, enums, strings). Access pattern is tail-append + replay + prefix-scan-by-device. ingot is float-only with Gorilla compression — wrong tool for this.

**Transport is push, not poll.** Core receives frames via `Inbound() <-chan Frame`. This matches LoRaWAN (uplink arrives when the device wakes) and avoids a polling loop that doesn't map to half the target transports.

**SectionAuthority ships first.** Resolver is pluggable, but CRDT merge is deferred until a real use case forces it. Section authority (device owns reported, controller owns desired) covers >95% of device-state semantics without the complexity.

**Fragmentation in core.** Only core knows MaxFrameBytes + op boundaries. Transport must not parse payloads. Core splits; transport ships opaque chunks.
