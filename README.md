# billet

Device shadow, convergence, and on-device history for intermittently-connected edge fleets. A Go library you import into your gateway or application server.

> **Status: pre-alpha.** Design is settling; almost nothing is built yet. APIs below are intent, not contract. Expect breaking changes.

In steelmaking, an ingot is the raw cast form and a billet is the worked intermediate shaped from it. [`ingot`](https://github.com/davidhrinaldo/ingot) is the embedded time-series store; billet is the fleet-state layer built on top of it.

---

## The problem

Cloud device-shadow platforms (AWS IoT Device Shadow, Azure Device Twin, Eclipse Ditto) live in the cloud and assume the device can reach them. When a device goes dark (cellular handoff, satellite gap, duty-cycled radio, a Class A LoRaWAN sensor) the shadow is stale and the device is on its own. Cloud platforms own the backend abstraction. They don't run on the box, and they leave the device-side to developers.

Every edge team hand-rolls the same four things:

- a local store for current device state, so control loops don't block on the network
- a durable buffer for readings and commands taken while the uplink is down
- a reconnect sync that drains that buffer without duplicates or gaps
- a local history store, because streaming every reading to a cloud TSDB is a metered bill and a data-loss risk when the WAN drops

There's a second failure the transport layer papers over: **command delivery is not command adoption.** A LoRaWAN network server hands you a fire-and-forget downlink queue. You enqueue bytes; they get sent on the device's next receive window; a confirmed downlink that times out is silently discarded from the queue. Nothing tracks whether the device *adopted* the config. Push a new schedule to 400 sleepy valve controllers and you have no answer to "which twelve haven't taken it yet" without building that yourself.

## What billet does

billet is the device-side / gateway-side half:

- **Shadow state** — `reported` / `desired` / `delta`, authority split by section (device owns reported, controller owns desired), so the common case needs no general conflict resolution.
- **Convergence engine** — turns fire-and-forget command delivery into a state machine. Set `desired`, and billet drives each device toward it, re-issuing on the next contact until `reported` matches, and tells you who's lagging.
- **On-device history** — every numeric reported value streams into ingot: compressed, queryable, 90+ days on a Pi-class box, surviving WAN outages.
- **Offline autonomy** — local reads of current state with zero network dependency, so control logic keeps running when the uplink is gone.

## How it works

### Storage split

Two stores, chosen by fit — billet does not force everything into ingot:

| Data | Store | Why |
|------|-------|-----|
| Current state + op-log | embedded KV (Pebble/bbolt) | heterogeneous values, transactional, tail-read/replay pattern |
| Reported numeric history | [ingot](https://git.dvdt.dev) | float time-series, Gorilla-compressed, range-scan native |

Using ingot for the op-log would be the wrong call — the log's values are heterogeneous and its access pattern is tail-read, neither of which is ingot's job. ingot earns its place as the compressed on-box history engine and nothing else.

### Correctness

- **Hybrid logical clocks (HLC)** for ordering. Edge devices have RTC drift, reboots, and no NTP while offline; wall-clock LWW is a footgun. HLC preserves causality without trusting the wall clock.
- **Idempotent op replay** over an at-least-once transport. Every op carries a stable ID; the receiver dedups; a half-acked batch replayed after a dropped link does not double-apply.
- **Snapshot + truncate** on the op-log so it doesn't grow unbounded and kill the flash.

### Transport interface (floor-first)

The contract is committed to the *weakest* transport billet will ever support: an unreliable, unordered, non-duplex, size-capped datagram pipe. Every stronger guarantee (MQTT ordered QoS, a persistent TCP session) is an optimization core may exploit, never a requirement. Reliability, ordering, dedup, and fragmentation live in core, because only core holds the HLC and op IDs — and a transport-level ack ("broker got it") never means "op durably applied at the peer" anyway.

Frames are opaque bytes; the transport never parses payloads. Acks are application-level. Delivery is push. Capabilities are advertised, not assumed. An adapter for LoRa/Meshtastic (~200-byte frames, best-effort, half-duplex) and one for MQTT sit behind the same interface.

## Primary use case: LoRaWAN fleet convergence

An on-prem application server above ChirpStack, consuming its integration stream:

- decoded sensor uplinks → shadow `reported` + ingot history
- operator sets `desired` config for a device group → billet enqueues downlinks and reconciles each device's next uplink against `desired`, re-issuing until it converges
- dashboards and control logic read local state and local history, working through WAN outages

billet's primary job is the convergence piece. History has a well-trodden path already (ChirpStack → InfluxDB → Grafana). billet tracks which devices have actually adopted pushed config and which are still lagging.

## Non-goals

- **Not a platform.** Billet has no web UI, no auth, no twin registry.
- **Not a cloud service.** billet converges *with* AWS/Azure/Ditto/ChirpStack; it does not reimplement them.
- **Not a network server or LNS.** It sits above one.
- **No transport adapters in core.** Adapters live outside the main tree. The interface is the contract.
- **No CRDT resolver until a real use case forces it.** Section authority covers the overwhelming majority of device state.

## Design principles

- Design against the floor, not the mode. An interface that encodes one delivery model leaks the moment you add a second.
- Keep scope tight. Extension happens through the adapter interface, not by growing core.
- Correctness over features. The reconciler and wire format produce subtle bugs; the API surface should stay small and stabilize early.

## License

Apache 2.0