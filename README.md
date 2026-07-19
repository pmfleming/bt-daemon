# bt-daemon

Rust Bluetooth policy and API daemon for Shelllist.

The project uses BlueZ (`bluetoothd`) as its system backend. It uses the official [`bluer`](https://github.com/bluez/bluer) Rust bindings and keeps `zbus` available for the session D-Bus API and BlueZ interfaces not covered by BlueR.

## Nix development environment

```sh
direnv allow
# or without direnv:
nix develop
```

The shell provides Rust, Cargo, Clippy, rustfmt, rust-analyzer, BlueZ tools, D-Bus development headers, `busctl`, `wpctl`, LLVM coverage tools, and `just`.

Run the development checks:

```sh
just check
nix flake check
```

Verify access to BlueZ and print the current adapter/device snapshot:

```sh
just probe
```

Build or run through Nix:

```sh
nix build
nix run -- probe-bluez
```

## Current status

The current `bt-api` v1 slice provides adapter/device snapshots, adapter power and owned discovery, and pair/connect/disconnect/trust/block/wake/rename/remove operations. Pair is a bounded pair → trust → connect workflow; remove disconnects first. The daemon monitors BlueZ adapter hotplug, adapter properties, device additions/removals, and device properties, then publishes debounced `bluetooth.changed` snapshots.

`bt-daemon daemon` owns `org.laufan.BluetoothDaemon` on the session bus. `bt-daemon client` bridges newline-delimited frontend JSON through that service, with a direct BlueZ fallback for ordinary calls. Device operations receive opaque request IDs and publish queued/running/completed/failed/cancelled lifecycle events; active operations can be cancelled through the transport. Errors distinguish timeout, unavailable-device, rejected, BlueZ-unavailable, validation, and generic failures. The application-specific BlueR pairing agent covers PIN input/display, passkey input/display, numeric confirmation, pairing authorization, service authorization, BlueZ cancellation, and a 60-second prompt timeout without requesting default-agent ownership from Blueman.

Inspect the canonical protocol metadata and checked fixture with:

```sh
bt-daemon debug protocol-registry
bt-daemon debug contract-fixture
bt-daemon debug audio-probe
```

`audio-probe` uses the native PipeWire API (not `wpctl` output parsing) to enumerate Bluetooth audio cards, active profiles, available A2DP/HSP/HFP profiles, priorities, availability, codecs, sink/source state, and default-route status. `bluetooth.audio.snapshot` exposes only daemon-issued device/profile keys and sanitized metadata; `bluetooth.audio.setProfile` resolves those keys internally and switches profiles through PipeWire SPA parameters. A persistent native PipeWire monitor publishes debounced `bluetooth.audio.changed` events when cards, nodes, profiles, states, or default routes change.

Object-push transfers use typed `org.bluez.obex` D-Bus calls. The daemon validates canonical outgoing files, creates/removes OPP sessions, and owns an incoming authorization agent for paired, unblocked devices. Incoming names are sanitized, collision-safe, and confined to the configured download directory. Both directions expose only opaque request/device IDs plus sanitized metadata, stream byte progress, and support cancellation.

Device identities are random opaque IDs persisted in a private, versioned state registry and shared by snapshots, pairing prompts, and operations. They survive daemon and machine restarts for identities resolved by BlueZ; truly unknown, unpaired devices that rotate private addresses cannot be safely correlated and remain separate until BlueZ resolves them. Audio and OBEX hardware/restart coverage remain staged work.
