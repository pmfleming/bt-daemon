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

Verify access to the running BlueZ service and list adapters:

```sh
just probe
```

Build or run through Nix:

```sh
nix build
nix run -- --probe-bluez
```

## Current status

This is the Phase 0 environment and BlueZ connectivity probe. The versioned `bt-api`, pairing agent, operation state machines, cache, and Shelllist integration are not implemented yet.
