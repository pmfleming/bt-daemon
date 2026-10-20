# Upstream integration upgrades — 2026-10-20

Seven independently committed changes implement the upstream review:

1. **ANC:** current four-byte Hearable Controls payload, independent of account
   keys/nonces. Pairing, connection, blocking, user policy, live stream, protocol
   version and settable modes still gate controls. Audio Switch retains HMAC.
2. **GFPS 3.3.1 advertisements:** bounded parsing of legacy Model IDs and new
   length/type fields; version, truncation, duplicate and account-filter checks.
   Capability-map bits are untrusted hints, not proof of ownership. Temporary
   account keys and full LE Audio Sharing remain outside this implementation.
3. **Runtime dependencies:** locked BlueZ 5.87 / PipeWire 1.6.9 / WirePlumber
   0.5.18 package set, keeping the framework's required Rust/Cargo 1.95.0.
   Version and enabled/disabled NixOS-module evaluation checks are included.
   See [deployment](../audio-stack-upgrade.md); no host switch was performed.
4. **Audio confirmation:** bounded event-driven profile activation, explicit
   parameter subscriptions, core/removal errors, and configured/effective route
   outcomes. Sync replies alone do not indicate completed hardware changes.
5. **LE Audio metadata and identity:** BAP/ASHA modes, machine-readable codec
   data, distinct logical endpoints, filtered internal transport nodes, stable
   compatibility aliases, and adapter/address plus object-serial validation
   before writes. Listeners are dropped before their retained proxies. Policy
   retries do not automatically replay failed/uncertain mutation attempts.
6. **Disconnect diagnostics:** owner-pinned `Device1.Disconnected` subscription,
   normalized reasons and bounded plain text, in-memory per-adapter history,
   removal/recovery cleanup, and existing debounced change notifications. No
   automatic reconnect policy was added; remote termination is not attributed
   to multipoint without evidence.
7. **Rust bindings:** `pipewire`, `pipewire-sys`, `libspa`, and `libspa-sys`
   upgraded from 0.10.0 to 0.10.1; the `v1_2_0` API floor remains unchanged.

## Additive API fields

- `fast_pair.controls_available` and `fast_pair.advertised_capabilities`.
- Audio-device `endpoints`, with opaque keys, kind, codec and state. Existing
  `sink`/`source` fields and their keys remain accepted compatibility aliases.
- `default_route_outcome.configured` and `.effective` on routing responses.
- Device `last_disconnect`, omitted when no current-generation report exists.

## Validation

- `cargo test --all-features`: **57 unit tests + 1 integration test passed**.
- Strict all-target/all-feature Clippy and rustfmt passed.
- Python quality-gate tests: **2 passed**.
- `cargo llvm-cov --all-features --fail-under-lines 50`: **58.87% line coverage**.
- `../daemon-framework/tools/local-build check . --print-build-logs --max-jobs 2
  --cores 4`: passed, including the release build/tests against PipeWire 1.6.9
  and runtime/module checks. The disconnect test uses a private D-Bus config,
  not `/etc/dbus-1/session.conf`, so it also runs in the Nix sandbox.
- `cargo audit`: no vulnerability errors; three existing unmaintained-package
  warnings for `zbus_names`, `zvariant`, and `zvariant_derive` (RUSTSEC-2026-0371,
  -0370, -0372). Their coordinated migration with the framework is separate.

Tests cover ANC wire bytes and capability gating, malformed/extended advertising,
late event-loop completion and deadlines, route confirmation semantics, multiple
logical/internal nodes, stale serials and adapter mismatches, private-bus sender
filtering, malformed signals, owner replacement and bounded diagnostic history.

**Hardware acceptance is still required:** account-key-free ANC on real providers,
codec/profile timing, logical LE Audio endpoints, effective WirePlumber routing,
suspend/resume and independent service restarts. No connected device was mutated,
no Bluetooth/audio service restarted, and no host configuration deployed.
