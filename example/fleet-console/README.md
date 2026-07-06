# Fleet Console

Web dashboard demonstrating billet's fleet convergence with live fault injection.
Uses a simulated transport — no hardware required.

## Quick Start

```bash
go run .
# open http://localhost:8080
```

## Flags

| Flag       | Default  | Description                          |
|------------|----------|--------------------------------------|
| `-seed`    | `42`     | PRNG seed for the simulated network  |
| `-devices` | `8`      | Number of simulated devices          |
| `-tick`    | `50`     | Simulation tick interval (ms)        |
| `-addr`    | `:8080`  | HTTP listen address                  |

The seed controls both the simulated network's PRNG and the randomized startup
link impairments, so the same seed produces the same initial conditions.

## What You See

On startup, devices are assigned random link conditions — some healthy, some
lossy/delayed, some partitioned — so the dashboard is immediately active with
mixed convergence states and events flowing.

- **Device grid**: Each card shows device ID (color-coded, click to select),
  convergence state badge, reported/desired values, delta keys, link status
  (loss %, delay range, or "LINK DOWN"), and time in current state.
- **Chaos panel**: Partition/heal individual devices or the entire fleet, set
  per-device loss rate and delay, reboot a device, push new desired values, or
  push a config update to all devices at once. Selecting a device syncs the
  loss/delay controls to that device's current settings.
- **Event log**: Real-time state transition stream with color-coded device names
  matching the grid cards.

## Demo Flow

1. Observe the initial mixed state — some devices Synced, others retrying
2. Click **Push Update to All** to trigger fresh convergence traffic
3. Watch healthy devices converge quickly while impaired ones struggle
4. Click a device name on a card to select it in the chaos panel
5. **Partition** the device — its card shows "LINK DOWN" with a dashed red border
6. **Push Update to All** — the partitioned device gets stuck at Inflight/TimedOut
7. **Heal** the device — watch it reconverge to Synced
8. Adjust **Loss** and **Delay** sliders, click **Apply Link**, then push another
   update to see probabilistic failures

## How It Works

The console talks to billet through its public API (`fleet.Manager`). A
background goroutine drives the simulation loop: `SimNet.Deliver` →
`Manager.DrainInbound` → `Manager.Tick` → device simulator. State snapshots
stream to the browser via Server-Sent Events.

Fault injection calls `SimNet.Partition`, `Heal`, and `SetLink` — the same
runtime-safe methods used by billet's property-based convergence tests.
