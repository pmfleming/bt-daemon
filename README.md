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

`bt-daemon daemon` owns `org.laufan.BluetoothDaemon` on the session bus. `bt-daemon client` bridges newline-delimited frontend JSON through that service, with a direct BlueZ fallback for ordinary calls. The application-specific BlueR pairing agent covers PIN input/display, passkey input/display, numeric confirmation, pairing authorization, service authorization, BlueZ cancellation, and a 60-second prompt timeout without requesting default-agent ownership from Blueman.

Inspect the canonical protocol metadata and checked fixture with:

```sh
bt-daemon debug protocol-registry
bt-daemon debug contract-fixture
```

Operation request IDs/cancellation, durable private-address identity, audio profiles, and OBEX remain staged work.
