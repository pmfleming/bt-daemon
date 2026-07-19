# bt-daemon

Rust Bluetooth policy and API daemon for Shelllist.

The project will use BlueZ (`bluetoothd`) as its system backend. The initial scaffold uses the official [`bluer`](https://github.com/bluez/bluer) Rust bindings and keeps `zbus` available for the session D-Bus API and BlueZ interfaces not covered by BlueR.

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

The first `bt-api` v1 slice provides adapter/device snapshots, adapter power and discovery control, and pair/connect/disconnect/trust/block/wake/rename/remove operations. `bt-daemon client` exposes newline-delimited JSON for Shelllist, while `bt-daemon daemon` exports the initial session D-Bus endpoint. Pairing prompts, operation events/cancellation, durable private-address identity, audio profiles, and OBEX remain staged work.
