# bt-daemon

The daemon also exposes same-user JSONL at `$XDG_RUNTIME_DIR/bt-daemon.sock`
(0700 runtime directory and socket, peer UID checked). The socket uses the same
service admission and coordinators as D-Bus. Connection closure releases only
that connection's subscriptions, scans, operations and outgoing transfers;
accepted effects are not treated as rolled back when a reply is lost.
D-Bus and `bt-daemon client` remain supported. The Shelllist socket prototype is
opt-in (`SHELLLIST_BT_SOCKET=1`); wider rollout is gated on real Quickshell,
restart/backpressure, large-record and comparative performance evidence. See
`../shelllist/contracts/unix-jsonl-stream-recovery.md` and the framework's `docs/unix-jsonl.md`.

The explicit ignored `unix_socket_acceptance_probe` test uses a private bus and
in-memory backend, real Quickshell, and a matching `bt-daemon client` binary. It
records p50/p99, process-group PSS, fragmented/large records, blocked writes,
no-replay and service-recreation evidence. Set `SHELLLIST_SOURCE`, `BT_DAEMON_BIN`
and `SOCKET_PROBE_REPORT`; the Shelllist migration document has the full command
and checked evidence. Ordinary tests do not run this experiment. Its debug,
isolated results do not establish whole-desktop or OS-process-crash acceptance;
the cross-daemon stream revision contract and wider rollout remain unfinished.

`bluetooth.changed` and `bluetooth.audio.changed` now carry `data.state` with an
opaque publisher epoch and an exact positive state revision. Their cached snapshot
RPCs capture the same value/watermark atomically, including loading/unavailable
errors. Each publisher is independent; unchanged observations do not advance and
watch coalescing may skip revisions. Counter exhaustion starts a fresh epoch at 1.
Mutation acknowledgements and embedded operation/scan snapshots are **not** part
of this publication order: consumers must reconcile through the authoritative
watch/read rather than overwrite live state with those observations. Old clients
may ignore additive metadata; ordering-aware Shelllist requires matched builds.
This does not yet order request/pairing/OBEX recovery or qualify quiet terminal
loss, process crashes, sustained load or rollout. Native tests cover captured reads,
coalescing, rollover, errors and real socket/D-Bus watermark parity; the offline
fixture uses the live state/event constructors (only epochs normalized).

Rust Bluetooth policy and API daemon for Shelllist.

The project uses BlueZ (`bluetoothd`) as its system backend. It uses the official [`bluer`](https://github.com/bluez/bluer) Rust bindings and keeps `zbus` available for the session D-Bus API and BlueZ interfaces not covered by BlueR.

## Nix development environment

Keep `daemon-framework` beside this checkout. Cargo and the Nix helper use that
same current framework, including tracked dirty edits. Never vendor or
revision-pin it; persistent locks select third-party dependencies only.

```sh
direnv allow
# or without direnv:
../daemon-framework/tools/local-build develop .
```

The shell provides Rust, Cargo, Clippy, rustfmt, rust-analyzer, BlueZ tools, D-Bus development headers, `busctl`, `wpctl`, LLVM coverage tools, and `just`.

Run the development checks:

```sh
just check
../daemon-framework/tools/local-build check .
```

Verify access to BlueZ and print the current adapter/device snapshot, or run the read-only live acceptance smoke test:

```sh
just probe
just hardware-smoke
```

The tracked interactive hardware matrix and rollout gates are in [`docs/hardware-acceptance.md`](docs/hardware-acceptance.md).

Build or run through Nix:

```sh
../daemon-framework/tools/local-build build .
../daemon-framework/tools/local-build run . -- probe-bluez
```

The Nix package set includes BlueZ 5.87+ and PipeWire 1.6.9+. Building this project
alone does not upgrade running host services. See [audio stack deployment and
acceptance](docs/audio-stack-upgrade.md) for the opt-in NixOS module.

## Current status

The current `bt-api` v1 slice provides rich adapter/device snapshots, global radio/rfkill state, adapter power/settings, bounded caller-owned discovery, and pair/connect/disconnect/trust/block/wake/rename/reset-name/remove operations. Discovery leases are released when their D-Bus caller disappears without interrupting overlapping leases from other clients. Pair and connect are staged, power-aware workflows with progress events and bounded service resolution; remove disconnects first. Only one mutating operation may run per device, while unrelated devices may be managed concurrently. The daemon monitors BlueZ adapter hotplug, adapter properties, device additions/removals, and device properties, then publishes debounced `bluetooth.changed` snapshots. Snapshots distinguish no-adapter, powered-off, soft-rfkill, hard-rfkill, and operational states. Global power operations update rfkill when permitted and retain adapter-power fallback behavior when soft-blocking is unavailable. Snapshots include adapter discoverability/timeouts and device bonding, service resolution, address type, modalias, UUID/service summaries, battery provenance, RSSI and session last-seen state. Management policy is persisted privately through `bluetooth.management.update`: login behavior (`remember`, `enable`, or `disable`), reconnect-after-resume, trust-after-pair, preferred adapter, and blocked/recent-device visibility. `bluetooth.device.policy.update` adds per-device reconnect, trust, connect-power, service-resolution, preferred-audio-profile, default-output routing, and `fast_pair_controls_enabled` overrides; null values restore global/default behavior. Setting `fast_pair_controls_enabled` to `false` disables ANC and authenticated multipoint commands and automatic Fast Pair provisioning without deleting account keys, changing Bluetooth pairing, or stopping read-only mode/battery reporting. Setting it back to `true` reuses the stored key; the default is `true`. The daemon records adapter power and connected opaque device keys, watches logind suspend/resume, restores power, and optionally reconnects devices with bounded attempts. For paired devices, the latest non-empty BlueZ icon, battery reports, Fast Pair model ID, and verified component topology are persisted. Stable presentation metadata is restored immediately after a restart or at the beginning of a connection while live services initialize; stale battery percentages remain hidden while connected. The first reliable semantic device type is retained across transient connection metadata, while stronger evidence (such as left/right component reports identifying earbuds) may refine a generic type; forgetting a device clears this presentation history. Nearby unpaired devices are retained in an in-memory five-minute cache after discovery stops, with their last signal marked non-live; discovery cache entries are never persisted and rotating private addresses are not merged.

For connected devices exposing Google's documented Fast Pair services, an optional backend provider opens either the authenticated RFCOMM Message Stream or the secure BLE L2CAP Message Stream. The stream is bidirectional: the daemon consumes component battery updates, queries Audio Switch/multipoint and Hearable Controls/ANC state, and sends authenticated control messages when it owns an account key. RFCOMM channels are resolved through SDP. For BLE-only providers, the daemon reads the encrypted `FE2C1239-8366-4814-8EB0-01DE32100BEA` GATT characteristic, validates its ready state and dynamic PSM, and opens an LE credit-based L2CAP stream on that PSM. Verified left, right, and case readings replace BlueZ's aggregate value while the stream is live; unknown components are omitted and the aggregate remains the fallback. Device eligibility and transport selection use only standard Fast Pair UUIDs, so no manufacturer, model, address, RFCOMM channel, or L2CAP PSM is hardcoded. Connected BR/EDR devices with the cached Message Stream UUID may reconnect even while BlueZ reports `ServicesResolved=false`: `ConnectProfile` can itself finish service resolution. BLE L2CAP still waits for resolved GATT services before reading its dynamic PSM. Busy failures retain bounded retries/backoff. A pending Fast Pair attempt with no delivered stream expires after 30 seconds on reconciliation and enters failure backoff; disconnect observations also retire pending attempts. Attempt IDs fence late outbound results, and live readers retain ownership of their teardown.

The daemon supports Google's retroactive account-key procedure immediately after traditional pairing. A production Provider's 64-byte anti-spoofing public key must be supplied from trusted Fast Pair model metadata; it cannot be derived from the advertised model ID. The daemon recognizes both the legacy three-byte discoverable Model ID and GFPS 3.3.1 length/type-tagged LE Audio Sharing advertisements in BlueZ service data, without treating account-key-filter advertisements as identities. The new `fast_pair.advertised_capabilities` reports untrusted `le_audio_sharing` and `factory_new` hints; neither proves ownership or enables sharing. Unknown versions, malformed fields, duplicate known fields, and Model IDs outside the supported 24-bit catalog are rejected. Temporary-session (`0xFF`) keys and full LE Audio Sharing are not implemented; persistent keys remain `0x04` only. Shelllist exposes a Fast Pair setup card and the reason provisioning is unavailable. Install trusted, model-specific metadata in `/etc/bluetooth/fast-pair-models.json` (or set `BT_DAEMON_FAST_PAIR_METADATA` to an operator-controlled file):

```json
{"version":1,"models":{"aabbcc":{"name":"Device model","anti_spoofing_public_key":"<128 hexadecimal characters from trusted model metadata>"}}}
```

This example is a schema, **not a usable key**. Protect the file and its parent directories from other users; group/world-writable files are rejected. Restart the daemon after installing metadata. No production keys are bundled or downloaded. After pairing, the daemon automatically provisions a matching catalog entry; `provision-fast-pair` with no key parameter also uses this catalog. Advanced API clients can instead supply `fast_pair_anti_spoofing_public_key` on `pair`, or `anti_spoofing_public_key` on `provision-fast-pair`. A pairing observed within the last minute is required; already paired devices must be re-paired rather than pretending their retroactive window remains open. Account keys are generated with the required `0x04` type byte, written through the ECDH/AES GATT handshake, and persisted in a private mode-`0600` state file. Secrets and control MAC material are never logged.

Controls use `bluetooth.device.operation` with `set-multipoint` plus an `enabled` boolean, or `set-noise-control` plus one of `transparent`, `adaptive`, `off`, and `noise-cancelling`. For example:

```json
{"key":"device-opaque","operation":"set-multipoint","enabled":true}
{"key":"device-opaque","operation":"set-noise-control","mode":"noise-cancelling"}
```

ANC follows the current Hearable Controls specification: four-byte control data with no account key, nonce, or MAC requirement. Pairing, a live connection/stream, unblocked state, supported/settable modes, and `fast_pair_controls_enabled` are still required. Multipoint commands continue to require account-key authentication.

Sony headphones (WF-1000XM4 and newer, using the v2 command set) keep noise control off the Fast Pair stream, even when they use it for battery. For connected devices exposing the Sony Headphones Connect RFCOMM service `956c7b26-d49a-4ba8-b03f-b17d393cb6e2`, a second optional provider opens that channel, queries the noise-cancelling/ambient-sound state, follows its state notifications, and maps `transparent`, `off`, and `noise-cancelling` to it; Sony's separate adaptive sound control is not offered. Focus-on-voice and the ambient level are preserved when only the mode changes. Sony noise control is not a Fast Pair control, so `fast_pair_controls_enabled` does not gate it. Like Fast Pair RFCOMM, its connected-device reconciler accepts the cached Sony service UUID without waiting indefinitely for `ServicesResolved`; its existing duplicate-connection guard and busy/failure backoff remain.

Every device snapshot carries vendor-neutral `noise_control` (`source`, `available_modes`, `settable_modes`, `active_mode`) from whichever channel reports it. Sony state is preferred; otherwise it mirrors `fast_pair.noise_control`. `can_set_noise_control` is true when either channel can accept a mode.

Snapshot `fast_pair` state includes the public model ID, extension state, `controls_available` for live policy-enabled controls, and `authenticated_controls` for Audio Switch commands requiring credentials. Capability flags indicate which operations are currently valid. Fast Pair support does not imply that either optional extension is present. Documented multipoint-switch notifications are decoded and logged with direction/reason, and exposed as `fast_pair.last_switch`. Shelllist can explain a reported move to another device. **Full advertisement-driven Audio Switch seeker policy is not implemented**: capability queries are answered with authenticated version zero when credentials are available (otherwise the specified no-response/unsupported behavior applies). `audio_switch_seeker_supported` is false; enabling Provider multipoint does not claim otherwise.

The implementation follows Google's public [Fast Pair BLE Device](https://developers.google.com/nearby/fast-pair/specifications/bledevice#message_stream_PSM), [Message Stream](https://developers.google.com/nearby/fast-pair/specifications/extensions/messagestream), [Audio Switch](https://developers.google.com/nearby/fast-pair/specifications/extensions/sass), [Hearable Controls](https://developers.google.com/nearby/fast-pair/specifications/extensions/hearablecontrols), and [message authentication](https://developers.google.com/nearby/fast-pair/specifications/extensions/mac) specifications. It does not fetch Google model metadata, synchronize account keys with a Google account, or implement the encrypted-advertisement-driven Fast Pair initial/subsequent pairing flows. Onboarding uses traditional BlueZ pairing followed by Google's documented retroactive procedure, not a claim of complete Fast Pair seeker support. Open-source Fast Pair seeker examples for the PSM characteristic were not available during implementation; the socket layer follows [BlueR's official BLE L2CAP client](https://github.com/bluez/bluer/blob/master/bluer/examples/l2cap_client.rs) and Google's Apache-licensed [Nearby BLE L2CAP implementation](https://github.com/google/nearby/tree/main/internal/platform/implementation/apple/Mediums/BLE) patterns.

`bt-daemon daemon` owns `org.laufan.BluetoothDaemon` on the session bus. `bt-daemon client` bridges newline-delimited frontend JSON through that service and does not bypass daemon policy with direct BlueZ mutations. Device and scan operations receive opaque request IDs and publish lifecycle events; active operations can be cancelled through the transport. Lifecycle streams emit a `lagged` event with a `skipped` count if a slow subscriber misses events. Consumers can then call `bluetooth.requests.snapshot` to reconcile active scans, operations, pairing prompts, and recently completed device operations instead of silently retaining stale state. Dropping BlueR's Pair future invokes BlueZ `CancelPairing`. Errors distinguish timeout, unavailable-device, rejected, BlueZ-unavailable, validation, and generic failures. If BlueZ is unavailable at startup or disappears later, the session API remains available, publishes unavailable snapshots, and builds or rebuilds its BlueZ session, monitors, Fast Pair provider, and pairing agent when BlueZ becomes available instead of restarting the daemon and discarding subscriptions. The application-specific BlueR pairing agent covers PIN input/display, passkey input/display, numeric confirmation, pairing authorization, service authorization, BlueZ cancellation, and a 60-second prompt timeout without requesting default-agent ownership from Blueman.

## Disconnect diagnostics

Device snapshots optionally expose `last_disconnect` with a normalized `reason`
(`unknown`, `timeout`, `local`, `remote`, `authentication`, or `suspend`), bounded
plain-text `message`, and `observed_at_ms`. This is the last BlueZ
`Device1.Disconnected` report, not an inferred current failure. Remote termination
does not prove multipoint switching, low battery, or a specific remote action.
Reports trigger the existing debounced `bluetooth.changed` stream. Only the current
BlueZ owner's signals for watched devices are accepted; diagnostics are bounded,
in-memory, cleared on removal/unpairing/adapter removal and backend recovery, and
never change reconnect policy. Older BlueZ versions that do not emit this signal
continue to work without a reason field.

## Connected-link signal readings

The unprivileged daemon polls the optional **`bt-rssi-helper`** system service
for already-connected devices. Only that small, separately confined service gets
**CAP_NET_ADMIN**. It sends Linux Bluetooth management **Get Connection
Information** (`0x0031`), never discovery, connection or mutation commands. The
session daemon has no direct management-socket fallback.

Polling runs every five seconds, with four concurrent calls maximum. BlueZ
enumeration has a separate four-second deadline; helper requests are bounded to
four seconds including authorization, with a two-second kernel deadline and a
five-second client deadline. Batches finish before the next poll, skipping missed
ticks rather than overlapping. Missing helper, authorization, transport and
unsupported-controller failures back off for thirty seconds and preserve cached
readings. Installation, helper restart or session authorization changes recover
without restarting bt-daemon. Snapshot requests never wait for hardware I/O.

The helper accepts only typed adapter/address reads, verifies that BlueZ reports
the target connected, and derives transport type itself. Polkit authenticates the
unique D-Bus caller; the default policy permits active local sessions only, with
no password prompts. It enforces global admission/concurrency and per-target rate
limits. It does not persist or return cached readings. Framework reuse is limited
to generic lifecycle support; the privileged executable, authorization policy and
Bluetooth protocol stay in this repository.

Installation of the user daemon alone grants no capabilities. See
[RSSI helper deployment and security](docs/rssi-helper.md) for the opt-in NixOS
module and non-Nix deployment requirements.

Fresh connected readings take priority; fresh discovery readings are the next
choice, then the most recent available cached sample. Failed reads never clear
or refresh the last successful number. Connected samples become non-live on read
failure, disconnect/removal/suspend, or after twelve seconds without a successful
read. The optional sampler retains samples in memory for up to five minutes;
the ordinary device snapshot cache remains the fallback (including its existing
connected-device retention). No RSSI is persisted across daemon/BlueZ restarts.

`rssi_kind` is an optional, additive bt-api v1 field:

- `dbm`: discovery or connected LE RSSI. `signal_strength` retains the existing
  clamped -100..-40 dBm → 0..100 heuristic; it is not a calibrated link-quality
  percentage.
- `bredr-relative`: connected classic Bluetooth RSSI, in dB relative to the
  controller's golden receive-power range. Zero is a valid reading, not unknown
  or 100%. `signal_strength` is null because an absolute percentage cannot be
  inferred.

`signal_level` (1–3, null without RSSI) is a coarse bar quality for both units.
For `dbm` it buckets `signal_strength` at 67/34%. For `bredr-relative`, at or
above the golden range is 3, down to 10 dB below it is 2, and weaker is 1. It is
a quality hint, not a percentage; Shelllist draws bars from it and labels
relative readings Good/Fair/Weak, keeping the dB value in the label.

An unavailable controller value (127) is never shown as a reading. Cached
fallback preserves the original units and `signal_live=false`; lack of RSSI
never means that an otherwise connected device has disconnected.

See [hardware acceptance](docs/hardware-acceptance.md#connected-link-rssi) for the
remaining privileged/hardware checks.

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

`audio-probe` uses the native PipeWire API (not `wpctl` output parsing) to enumerate Bluetooth audio cards, active profiles, available A2DP/HSP/HFP/BAP/ASHA profiles, priorities, availability, codecs, endpoint state, and default-route status. Profile modes distinguish `le-audio` and `hearing-aid`. Codec labels use machine profile identifiers and active `api.bluez5.codec` properties, never translated descriptions. Audio snapshots include an additive `endpoints` array with distinct opaque keys, kind, codec, state, and default status. Internal component transport nodes are excluded from routable endpoints. Legacy `sink`/`source` fields and keys remain deterministic compatibility aliases (effective default first, then session priority/name). Mutations validate current PipeWire object serials, and policy lookup includes the adapter so two controllers cannot target each other's devices. `bluetooth.audio.snapshot` exposes only daemon-issued device/profile/endpoint keys and sanitized metadata; `bluetooth.audio.setProfile` resolves profile keys internally and switches profiles through PipeWire SPA parameters, while `bluetooth.audio.setDefault` writes PipeWire default sink/source metadata using opaque endpoint keys. A persistent native PipeWire monitor subscribes to profile parameters and publishes debounced `bluetooth.audio.changed` events when cards, nodes, profiles, states, or default routes change. Profile writes wait up to eight seconds for an observed active profile, not merely a synchronization reply; disconnects, removals, and errors fail the operation. A timeout is an uncertain write and must not be automatically replayed. Default-route writes confirm configured metadata separately from policy's effective route: `default_route_outcome` reports `configured` and `effective`, so saving a preference is not presented as successful routing. A configured preference can succeed with `effective=false` after the deadline; WirePlumber still owns routing policy.

**File-transfer support boundary:** OBEX is implemented in `bt-daemon`, but **Shelllist does not support file transfers**: there is no send, approval, progress, or cancellation UI. Use Blueman or another transfer client. The incoming authorization agent is disabled by default so it cannot steal transfers and present invisible prompts. Set `BT_DAEMON_OBEX_INCOMING=1` only when running a client that subscribes to `bluetooth.obex.transfer` and answers `bluetooth.obex.respond`. Outgoing transfers remain available through the daemon API.

Object-push transfers use typed `org.bluez.obex` D-Bus calls. The daemon validates canonical outgoing files, creates/removes OPP sessions, and owns an incoming authorization agent for paired, unblocked devices. Incoming names are sanitized, collision-safe, and confined to the configured download directory. Both directions expose only opaque request/device IDs plus sanitized metadata, stream byte progress, and support cancellation.

Device identities are random opaque IDs persisted in a private, versioned state registry and shared by snapshots, pairing prompts, and operations. They survive daemon and machine restarts for identities resolved by BlueZ; the same private registry retains non-secret icon and battery presentation state for known devices. Truly unknown, unpaired devices that rotate private addresses cannot be safely correlated and remain separate until BlueZ resolves them. Audio and OBEX hardware/restart coverage remain staged work. See the [review remediation status](docs/review-remediation.md) for implemented fixes, validation evidence, and remaining Fast Pair limitations.
