# Bluetooth review remediation — 2026-09-07

Changes were committed locally in `bt-daemon` and `shelllist`, in scoped solution commits. No system rebuild, service restart, device reconnection, or remote Git push was performed. The earlier earbud disconnects still need an HCI capture to prove their initial cause; these fixes are not proof of that cause.

## Implemented reliability fixes

- Own, cancel and join each BlueZ backend generation's workers; retain shared durable identity/management state through recovery.
- Keep device/adapter subscriptions alive, record connection transitions, timestamp real discovery observations, and expire stale signal observations.
- Keep unpaired discovery identities ephemeral, batch durable writes off the async runtime, and suppress identical snapshot fanout. These are architectural improvements, not measured performance claims.
- Use WirePlumber's configured-default metadata keys and rebuild PipeWire monitoring after core errors/panics.
- Make Fast Pair command reservations cancellation-safe, bound writer/response waits, back off stream failures, and honor session-scoped unavailable PSM responses, including rapid physical reconnections.
- Preserve display pairing prompts, queue simultaneous requests, publish answered events, and retain input when responding fails.
- Invalidate stale UI devices, audio routes, capabilities and prompts during outages, then reconcile fresh state.
- Revoke stored Fast Pair keys on device removal; serialize and bound resume reconnection and apply device audio policy.

## Functionality and support boundaries

| Area | Current result |
|---|---|
| OBEX file transfer | Implemented by **bt-daemon, not supported by Shelllist**. Comments and README explicitly say so. Incoming authorization is opt-in (`BT_DAEMON_OBEX_INCOMING=1`) to avoid invisible approvals or replacing another client's agent. |
| Blocked devices | Visibility setting is honored and exposed. |
| Component batteries | Preserve and display charging bits; unknown values remain unknown. |
| Audio management | Default input/output selection and per-device policy/reset UI; backend rejects unknown policy fields without rejecting the routing key. |
| Hearable controls | UI requires actual authenticated ANC capability and settable modes; backend refuses unsupported control protocol versions. Hardware/account-key acceptance remains pending. |
| Multipoint | Capability/state/configuration extensions and switch direction/reason presentation; required capability acknowledgements and explicit unsupported full seeker-policy negotiation. |
| Trusted local onboarding | Discoverable three-byte Model IDs can match an operator-controlled P-256 public-key catalog. Traditional BlueZ pairing can automatically run retroactive provisioning within the one-minute window. UI explains missing metadata/windows and can request provisioning without supplying keys. No production keys are bundled. |
| Full Google Fast Pair | **Still incomplete**: no Google account synchronization, trusted online metadata service integration, or encrypted-advertisement-driven initial/subsequent pairing and Audio Switch seeker policy. Local retroactive onboarding is not equivalent to these flows or certification. |

## Validation

- Rust: **57 tests passed**; `cargo fmt`; strict `cargo clippy --all-targets -- -D warnings` passed.
- Bluetooth JavaScript checks: **121 checks** (battery 38, glyphs 18, noise control 16, lifecycle 49).
- API fixture/frontend registry agreement and daemon-boundary checks passed.
- Offscreen QML suite: **96 passed**, including the new Bluetooth recovery/control tests (10 including setup/cleanup). Daemon transports were mocked; no live device commands were sent.
- Coverage run passed the existing 34% line gate (35.74% before the final protocol-boundary follow-up).
- Bluetooth QML lint still reports three `BluetoothController to BluetoothController` type-identity warnings. Running the same linter against the pre-change `a9e333e` archive reproduced all three; this is not a clean-lint claim. Runtime QML tests pass.

Interactive BlueZ/PipeWire restarts, suspend/resume, concurrent discovery, BLE L2CAP, provisioning and authenticated controls require the [hardware acceptance checklist](hardware-acceptance.md). Prior baseline results do not count as acceptance of these new commits. Retain Blueman for unsupported workflows.

Protocol references and trusted metadata setup are in the [README](../README.md).
