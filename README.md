# bt-daemon

Rust Bluetooth policy and API daemon for Shelllist.

The project uses BlueZ (`bluetoothd`) as its system backend. It uses the official [`bluer`](https://github.com/bluez/bluer) Rust bindings and keeps `zbus` available for the session D-Bus API and BlueZ interfaces not covered by BlueR.

## Nix development environment

Keep `daemon-framework` beside this checkout. Cargo and the Nix helper use that
same current framework, including tracked dirty edits. Never vendor or
revision-pin it; persistent locks select third-party dependencies only.

```sh
direnv allow
# or without direnv:
python3 ../daemon-framework/tools/local-build.py develop .
```

The shell provides Rust, Cargo, Clippy, rustfmt, rust-analyzer, BlueZ tools, D-Bus development headers, `busctl`, `wpctl`, LLVM coverage tools, and `just`.

Run the development checks:

```sh
just check
python3 ../daemon-framework/tools/local-build.py check .
```

Verify access to BlueZ and print the current adapter/device snapshot, or run the read-only live acceptance smoke test:

```sh
just probe
just hardware-smoke
```

The tracked interactive hardware matrix and rollout gates are in [`docs/hardware-acceptance.md`](docs/hardware-acceptance.md).

Build or run through Nix:

```sh
python3 ../daemon-framework/tools/local-build.py build .
python3 ../daemon-framework/tools/local-build.py run . -- probe-bluez
```

## Current status

The current `bt-api` v1 slice provides rich adapter/device snapshots, global radio/rfkill state, adapter power/settings, bounded caller-owned discovery, and pair/connect/disconnect/trust/block/wake/rename/reset-name/remove operations. Discovery leases are released when their D-Bus caller disappears without interrupting overlapping leases from other clients. Pair and connect are staged, power-aware workflows with progress events and bounded service resolution; remove disconnects first. Only one mutating operation may run per device, while unrelated devices may be managed concurrently. The daemon monitors BlueZ adapter hotplug, adapter properties, device additions/removals, and device properties, then publishes debounced `bluetooth.changed` snapshots. Snapshots distinguish no-adapter, powered-off, soft-rfkill, hard-rfkill, and operational states. Global power operations update rfkill when permitted and retain adapter-power fallback behavior when soft-blocking is unavailable. Snapshots include adapter discoverability/timeouts and device bonding, service resolution, address type, modalias, UUID/service summaries, battery provenance, RSSI and session last-seen state. Management policy is persisted privately through `bluetooth.management.update`: login behavior (`remember`, `enable`, or `disable`), reconnect-after-resume, trust-after-pair, preferred adapter, and blocked/recent-device visibility. `bluetooth.device.policy.update` adds per-device reconnect, trust, connect-power, service-resolution, preferred-audio-profile, default-output routing, and `fast_pair_controls_enabled` overrides; null values restore global/default behavior. Setting `fast_pair_controls_enabled` to `false` disables authenticated ANC/multipoint commands and automatic Fast Pair provisioning without deleting account keys, changing Bluetooth pairing, or stopping read-only mode/battery reporting. Setting it back to `true` reuses the stored key; the default is `true`. The daemon records adapter power and connected opaque device keys, watches logind suspend/resume, restores power, and optionally reconnects devices with bounded attempts. For paired devices, the latest non-empty BlueZ icon, battery reports, Fast Pair model ID, and verified component topology are persisted. Stable presentation metadata is restored immediately after a restart or at the beginning of a connection while live services initialize; stale battery percentages remain hidden while connected. The first reliable semantic device type is retained across transient connection metadata, while stronger evidence (such as left/right component reports identifying earbuds) may refine a generic type; forgetting a device clears this presentation history. Nearby unpaired devices are retained in an in-memory five-minute cache after discovery stops, with their last signal marked non-live; discovery cache entries are never persisted and rotating private addresses are not merged.

For connected devices exposing Google's documented Fast Pair services, an optional backend provider opens either the authenticated RFCOMM Message Stream or the secure BLE L2CAP Message Stream. The stream is bidirectional: the daemon consumes component battery updates, queries Audio Switch/multipoint and Hearable Controls/ANC state, and sends authenticated control messages when it owns an account key. RFCOMM channels are resolved through SDP. For BLE-only providers, the daemon reads the encrypted `FE2C1239-8366-4814-8EB0-01DE32100BEA` GATT characteristic, validates its ready state and dynamic PSM, and opens an LE credit-based L2CAP stream on that PSM. Verified left, right, and case readings replace BlueZ's aggregate value while the stream is live; unknown components are omitted and the aggregate remains the fallback. Device eligibility and transport selection use only standard Fast Pair UUIDs, so no manufacturer, model, address, RFCOMM channel, or L2CAP PSM is hardcoded.

The daemon supports Google's retroactive account-key procedure immediately after traditional pairing. A production Provider's 64-byte anti-spoofing public key must be supplied from trusted Fast Pair model metadata; it cannot be derived from the advertised model ID. The daemon recognizes the standard three-byte discoverable Model ID in BlueZ service data without treating account-key-filter advertisements as identities. Shelllist exposes a Fast Pair setup card and the reason provisioning is unavailable. Install trusted, model-specific metadata in `/etc/bluetooth/fast-pair-models.json` (or set `BT_DAEMON_FAST_PAIR_METADATA` to an operator-controlled file):

```json
{"version":1,"models":{"aabbcc":{"name":"Device model","anti_spoofing_public_key":"<128 hexadecimal characters from trusted model metadata>"}}}
```

This example is a schema, **not a usable key**. Protect the file and its parent directories from other users; group/world-writable files are rejected. Restart the daemon after installing metadata. No production keys are bundled or downloaded. After pairing, the daemon automatically provisions a matching catalog entry; `provision-fast-pair` with no key parameter also uses this catalog. Advanced API clients can instead supply `fast_pair_anti_spoofing_public_key` on `pair`, or `anti_spoofing_public_key` on `provision-fast-pair`. A pairing observed within the last minute is required; already paired devices must be re-paired rather than pretending their retroactive window remains open. Account keys are generated with the required `0x04` type byte, written through the ECDH/AES GATT handshake, and persisted in a private mode-`0600` state file. Secrets and control MAC material are never logged.

Authenticated controls use `bluetooth.device.operation` with `set-multipoint` plus an `enabled` boolean, or `set-noise-control` plus one of `transparent`, `adaptive`, `off`, and `noise-cancelling`. For example:

```json
{"key":"device-opaque","operation":"set-multipoint","enabled":true}
{"key":"device-opaque","operation":"set-noise-control","mode":"noise-cancelling"}
```

Snapshot `fast_pair` state includes the public model ID, extension state, and whether authenticated controls are available. Capability flags indicate which operations are currently valid. Fast Pair support does not imply that either optional extension is present. Documented multipoint-switch notifications are decoded and logged with direction/reason, and exposed as `fast_pair.last_switch`. Shelllist can explain a reported move to another device. **Full advertisement-driven Audio Switch seeker policy is not implemented**: capability queries are answered with authenticated version zero when credentials are available (otherwise the specified no-response/unsupported behavior applies). `audio_switch_seeker_supported` is false; enabling Provider multipoint does not claim otherwise.

The implementation follows Google's public [Fast Pair BLE Device](https://developers.google.com/nearby/fast-pair/specifications/bledevice#message_stream_PSM), [Message Stream](https://developers.google.com/nearby/fast-pair/specifications/extensions/messagestream), [Audio Switch](https://developers.google.com/nearby/fast-pair/specifications/extensions/sass), [Hearable Controls](https://developers.google.com/nearby/fast-pair/specifications/extensions/hearablecontrols), and [message authentication](https://developers.google.com/nearby/fast-pair/specifications/extensions/mac) specifications. It does not fetch Google model metadata, synchronize account keys with a Google account, or implement the encrypted-advertisement-driven Fast Pair initial/subsequent pairing flows. Onboarding uses traditional BlueZ pairing followed by Google's documented retroactive procedure, not a claim of complete Fast Pair seeker support. Open-source Fast Pair seeker examples for the PSM characteristic were not available during implementation; the socket layer follows [BlueR's official BLE L2CAP client](https://github.com/bluez/bluer/blob/master/bluer/examples/l2cap_client.rs) and Google's Apache-licensed [Nearby BLE L2CAP implementation](https://github.com/google/nearby/tree/main/internal/platform/implementation/apple/Mediums/BLE) patterns.

`bt-daemon daemon` owns `org.laufan.BluetoothDaemon` on the session bus. `bt-daemon client` bridges newline-delimited frontend JSON through that service and does not bypass daemon policy with direct BlueZ mutations. Device and scan operations receive opaque request IDs and publish lifecycle events; active operations can be cancelled through the transport. Lifecycle streams emit a `lagged` event with a `skipped` count if a slow subscriber misses events. Consumers can then call `bluetooth.requests.snapshot` to reconcile active scans, operations, pairing prompts, and recently completed device operations instead of silently retaining stale state. Dropping BlueR's Pair future invokes BlueZ `CancelPairing`. Errors distinguish timeout, unavailable-device, rejected, BlueZ-unavailable, validation, and generic failures. If BlueZ disappears, the session API remains owned, publishes unavailable snapshots, and rebuilds its BlueZ session, monitors, Fast Pair provider, and pairing agent when BlueZ returns instead of restarting the daemon and discarding subscriptions. The application-specific BlueR pairing agent covers PIN input/display, passkey input/display, numeric confirmation, pairing authorization, service authorization, BlueZ cancellation, and a 60-second prompt timeout without requesting default-agent ownership from Blueman.

## Operational logs

The daemon writes structured lifecycle, operation, transfer, subscription, and error-chain logs to stderr at `debug` level by default. The systemd user service captures them in the journal:

```sh
journalctl --user -u bt-daemon.service -f
RUST_LOG=bt_daemon=trace bt-daemon daemon
```

Request parameters, pairing PINs/passkeys, and full transfer paths are not logged. Errors at D-Bus, BlueZ, OBEX, Fast Pair, PipeWire, task, and client boundaries include their operation context and available error chain for later review.

Inspect the canonical protocol metadata and checked fixture with the commands below, or call `bluetooth.protocol.describe` at runtime. Introspection includes JSON parameter schemas, operation-specific parameters, capability requirements, streams, cancellation behavior, compatibility rules, and stable error metadata:

```sh
bt-daemon debug protocol-registry
bt-daemon debug contract-fixture
bt-daemon debug audio-probe
```

`audio-probe` uses the native PipeWire API (not `wpctl` output parsing) to enumerate Bluetooth audio cards, active profiles, available A2DP/HSP/HFP profiles, priorities, availability, codecs, sink/source state, and default-route status. `bluetooth.audio.snapshot` exposes only daemon-issued device/profile/endpoint keys and sanitized metadata; `bluetooth.audio.setProfile` resolves profile keys internally and switches profiles through PipeWire SPA parameters, while `bluetooth.audio.setDefault` writes PipeWire default sink/source metadata using opaque endpoint keys. A persistent native PipeWire monitor publishes debounced `bluetooth.audio.changed` events when cards, nodes, profiles, states, or default routes change.

**File-transfer support boundary:** OBEX is implemented in `bt-daemon`, but **Shelllist does not support file transfers**: there is no send, approval, progress, or cancellation UI. Use Blueman or another transfer client. The incoming authorization agent is disabled by default so it cannot steal transfers and present invisible prompts. Set `BT_DAEMON_OBEX_INCOMING=1` only when running a client that subscribes to `bluetooth.obex.transfer` and answers `bluetooth.obex.respond`. Outgoing transfers remain available through the daemon API.

Object-push transfers use typed `org.bluez.obex` D-Bus calls. The daemon validates canonical outgoing files, creates/removes OPP sessions, and owns an incoming authorization agent for paired, unblocked devices. Incoming names are sanitized, collision-safe, and confined to the configured download directory. Both directions expose only opaque request/device IDs plus sanitized metadata, stream byte progress, and support cancellation.

Device identities are random opaque IDs persisted in a private, versioned state registry and shared by snapshots, pairing prompts, and operations. They survive daemon and machine restarts for identities resolved by BlueZ; the same private registry retains non-secret icon and battery presentation state for known devices. Truly unknown, unpaired devices that rotate private addresses cannot be safely correlated and remain separate until BlueZ resolves them. Audio and OBEX hardware/restart coverage remain staged work. See the [review remediation status](docs/review-remediation.md) for implemented fixes, validation evidence, and remaining Fast Pair limitations.
