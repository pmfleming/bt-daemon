# Bluetooth hardware acceptance

This checklist records evidence for the Shelllist Bluetooth rollout gates. Automated checks and the read-only hardware smoke test are necessary but do not replace interactive pairing and transfer tests.

## Read-only smoke test

Run against the active session daemon:

```sh
just hardware-smoke
```

The test validates the live `bt-api` envelope, adapter/device collections, unique opaque keys, audio snapshot shape, and OBEX capability snapshot without changing Bluetooth state.

## 2026-07-20 ThinkPad baseline

Environment observed:

- BlueZ `bluetooth.service`: active.
- `bt-daemon.service`: enabled and active.
- `shelllist-bluetooth.service`: enabled and active.
- `org.bluez.obex` and `org.laufan.BluetoothDaemon`: owned on the session bus.
- Adapter: one powered `hci0` controller.
- Known devices: Sony WF-1000XM5, HX, and Buds.
- Read-only smoke result: **pass**.
- Connected/audio devices during the initial smoke run: none; audio profile and battery behavior were therefore not exercised.

Follow-up acceptance on the same machine:

- Adapter power off/on: **pass**.
- Sony WF-1000XM5 connect/disconnect: **pass**.
- Sony WF-1000XM5 pair and remove pairing: **pass**.
- Sony WF-1000XM5 standard aggregate battery report: **pass**. BlueZ exposes one correct figure.
- Sony WF-1000XM5 Fast Pair RFCOMM Message Stream component battery report: **pass**. The authenticated stream reported left `100%`, right `100%`, and case `78%`; the case value was not inferred from BlueZ's aggregate `100%`. BlueZ resolved the documented profile UUID to SDP channel 23 during this run.
- Pixel Buds Fast Pair RFCOMM Message Stream component battery report: **partial**. The device reported left `100%`, right `100%`, and case `unknown` as the standards-defined `0xFF`; the daemon correctly omitted the unknown case percentage.
- Fast Pair BLE L2CAP Message Stream: **implementation complete; compatible hardware pending**. GATT PSM state/range parsing and the secure LE credit-based socket path are automated, but neither connected acceptance device selects the BLE-only transport because both expose the RFCOMM Message Stream UUID.
- Fast Pair Audio Switch/multipoint extensions: **partial; hardware/account-key acceptance pending**. Capability/state discovery, authenticated multipoint configuration, ACK/NAK, and switch-reason notifications are implemented. Full advertisement-driven Audio Switch seeker policy remains unsupported and is explicitly advertised as such; this is not complete Audio Switch support.
- Fast Pair Hearable Controls/ANC state and control: **partial; unsigned version-1 set verified on real earbuds**. With the daemon stopped, RFCOMM channel 2 reported `01 e8 e8 08` (all four modes settable, noise-cancelling active). Sending `08 12 00 04 01 e8 e8 80` without an account key, nonce, or signature received an ACK and state notification `01 e8 e8 80` (transparent active). After restart, the daemon snapshot confirmed `active_mode: "transparent"`. End-to-end setting through the updated daemon, the remaining modes, version-2 hardware, and NAK handling remain to be verified.
- Sony WF-1000XM5 noise control over the Sony Headphones Connect channel: **pass (2026-10-21)**. The XM5 answers Fast Pair's ANC query with nothing, so the daemon opens the vendor RFCOMM service `956c7b26-d49a-4ba8-b03f-b17d393cb6e2` (SDP channel 9 during this run). Init returned the eight-byte v2 reply `01 00 03 00 30 18 00 00`; NC/ASM inquiry type `0x17` reported `67 17 01 01 00 00 14` (noise cancelling, ambient level 20) and type `0x15` was only acknowledged. Through the local daemon build, `set-noise-control` to `transparent`, `off`, and `noise-cancelling` each received an ACK and a `0x69` notification, and the snapshot's `noise_control.active_mode` (source `sony-headphones-control`) followed every change. Mode changes made on the earbuds themselves, v1 (four-byte init) Sony models, and wind-noise-capable models remain to be verified.
- Fast Pair retroactive account-key provisioning: **implementation complete; trusted model metadata and hardware acceptance pending**. Verify ECDH response, encrypted account-key write, reconnect persistence, and rejection outside the Provider's one-minute window.
- The alternate no-trust pairing policy, audio playback/profile readiness, component-update behavior as batteries discharge, and reconnect behavior remain to be checked separately.

## Fast Pair reconnect latency regression

The original 2026-10-05 fix added fast busy retries and a blanket
`ServicesResolved` gate. The latter caused a confirmed reconnect deadlock on
2026-10-22: the WF-1000XM5 was connected with cached UUIDs and working audio, but
BlueZ never changed `ServicesResolved` back to true. Both Fast Pair and the later
Sony provider skipped it. Only BlueZ's aggregate 97% remained in the snapshot.
The operator reported that manually calling `ConnectProfile` for the standard
Fast Pair Message Stream restored left 99%, right 97%, case 26% within 300 ms;
BlueZ then reported resolved services and Sony control recovered automatically.
This is diagnostic evidence, not acceptance of the new automatic recovery code.

Updated implementation: connected devices advertising a cached RFCOMM profile
UUID can attempt it without service resolution. BLE L2CAP still requires resolved
GATT services. Fast Pair pending attempts expire after 30 seconds on the next
reconcile pass and enter normal exponential backoff if no stream arrived, even
when `ConnectProfile` returned success. Observed disconnects retire pending
attempts immediately; attempt IDs keep old outbound failures/completions from
retiring or replacing a newer attempt. Live streams do not expire under this rule
and retain reader-owned cleanup.

Private-bus regression tests exercise the actual daemon reconcilers with cached
Fast Pair/Sony UUIDs, `Connected=true`, and `ServicesResolved=false`. They also
check disconnected/unadvertised peers are not contacted, unresolved BLE PSMs are
not read, and GATT reads resume on resolution. Virtual-time lifecycle tests cover
missing stream callbacks, bounded backoff, disconnect/reconnect, stale results,
and preservation of live streams. No hardware or user services are touched.

Pending post-deployment hardware validation:

1. Rebuild/deploy `bt-daemon` and follow `journalctl --user -u bt-daemon.service -f`
   with `RUST_LOG=bt_daemon=debug` enabled.
2. Disconnect/reconnect compatible earbuds through Shelllist. Compare the
   `BlueZ device services changed resolved=true`, operation completion,
   `Fast Pair battery stream started`, and `Fast Pair component battery updated`
   timestamps. For cached RFCOMM profiles, stream startup may now precede
   `resolved=true`; service resolution must not be a permanent prerequisite.
3. If BlueZ still returns `InProgress` / `br-connection-busy`, expect retries after
   250ms, 500ms, then 1s (at most six short retries per connection session), not
   the old 15–18s backoff followed by a 10s polling boundary. Persistent failures
   retain exponential backoff; its deadlines also wake the reconciler directly.
4. Confirm left/right/case readings appear promptly after the first battery frame,
   then disconnect and reconnect again, including an audio-only reconnect with
   `Connected=true` and `ServicesResolved=false`. Verify cached RFCOMM profiles
   recover without a manual `ConnectProfile` call. Disconnected peers must not be
   contacted; failures must neither spin nor create duplicate live streams.
   Repeat with a BLE-only Fast Pair device when available; its PSM read must still
   wait for GATT resolution.

The original 2026-10-05 journal showed connection completion at 13:44:02,
`br-connection-busy` failures during setup, and the first component report at
13:44:26. Snapshot publication took only about 85ms after that report. Automated
virtual-time tests cover the bounded retry budget, error classification, deadline
ordering/consumption, and preservation of live streams after late failures;
post-fix automatic recovery timing is not yet measured. Do not restart services,
re-pair devices, or disconnect active audio without operator approval.

## Connected-link RSSI

Hardware acceptance is **pending**; synthetic socket and virtual-time tests do
not establish controller support. Do not automatically grant capabilities or
restart the user's Bluetooth service to run these checks.

1. With the ordinary unprivileged user service and no RSSI helper, verify bounded
   read failures/backoff and unchanged discovery/cached readings and operations.
2. In an explicitly approved [RSSI helper deployment](rssi-helper.md), verify the
   helper alone has CAP_NET_ADMIN and the session daemon still has no effective
   capabilities. Keep a known device connected and discovery stopped. Within a
   polling interval (or the thirty-second recovery backoff), check `rssi`,
   `rssi_kind`, `signal_live` and `signal_strength` in snapshots. Check active-local
   versus inactive/remote Polkit authorization, with no authentication prompts.
3. Exercise classic BR/EDR, LE public and LE random connections where hardware is
   available. BR/EDR zero is relative to the golden receive range and must not
   become 100%; LE retains the existing dBm percentage heuristic. Verify that
   raw unavailable value 127 is never rendered.
4. Disconnect, suspend/resume, remove the adapter, and restart BlueZ in an
   approved session. Cached values retain their units and become non-live;
   retired/in-flight reads cannot relabel them live. Reconnect gets new readings.
5. With a controller that rejects/does not implement the command, verify bounded
   reads, thirty-second failure backoff, and cached fallback without UI errors.
6. Edit a Bluetooth name while readings update. Selection and the native draft
   remain intact; Escape still discards, with no unsolicited save.
7. Stop/restart the helper in an approved session. Readings become cached and
   recover without restarting the user daemon. Test missing Polkit and helper
   capability denial: the latter must decode the queued `0x14` management reply,
   not become a silent WriteZero error. No deployment/privilege change is part
   of the automated test suite.

## Interactive Rofi-replacement gate

Record a date, device, and result for every row before moving `SUPER+B`.

| Scenario | Required evidence | Status |
|---|---|---|
| Power off/on | Snapshot and UI converge; active scan terminates on power-off | Pass — 2026-07-20 |
| Bounded scan | Start, live additions, timeout, explicit cancellation, and stop-on-hide | Pending |
| No-controller boot | Clear unavailable state and recovery after controller appears | Pending |
| PIN input | Correct input validation, accept, reject, and timeout | Pending |
| Passkey input | 1–6 digit validation, accept, reject, and timeout | Pending |
| PIN/passkey display | Display updates and cancellation clear the modal | Pending |
| Numeric confirmation | Accept, reject, timeout, and operation terminal state | Pending |
| Pair authorization | Prompt cannot be bypassed by closing the popup | Pending |
| Service authorization | UUID/service is shown and rejection reaches BlueZ | Pending |
| Pair → optional trust → connect | Both trust policies and typed failures | Partial — WF-1000XM5 default-trust pair/connect/remove passed; no-trust policy pending |
| Connect/disconnect | Sony WF-1000XM5, HX, and Buds | Partial — WF-1000XM5 passed 2026-07-20; HX and Buds pending |
| Operation cancellation | Pair and connect cancellation produce one terminal event | Pending |
| BlueZ restart | API remains available with an unavailable snapshot; retired backend workers terminate; one new generation resumes subscriptions without identity drift | Pending |
| Adapter hotplug | Stale actions disappear and adapter selection recovers | Pending |

## Blueman-parity hardware matrix

| Device/workflow | Status |
|---|---|
| WF-1000XM5 A2DP playback, battery, reconnect | Partial — pair/connect/disconnect/remove, aggregate battery, and Fast Pair left/right/case battery passed; playback, live battery-change notification, and reconnect remain pending. |
| WF-1000XM5 headset microphone profile | Pending |
| HX audio profiles | Pending |
| Buds audio profiles and wake behavior | Pending |
| Keyboard passkey entered-count flow | Pending hardware |
| Mouse/gamepad just-works and wake | Pending hardware |
| Phone pairing and OBEX send/receive | Daemon API hardware acceptance pending; **file transfer is intentionally unsupported by Shelllist**. Incoming daemon agent requires an explicit opt-in and a client capable of approvals. |
| Suspend/resume and rfkill recovery | Pending |
| PipeWire restart while Bluetooth UI is open | Pending — stop/restart PipeWire only in an approved hardware session; expect an unavailable audio snapshot followed by fresh cards/routes, with exactly one monitor thread. Core errors and panics now trigger monitor reconstruction. |
| Two simultaneous discovery clients | Pending |

Do not remove Blueman until the actually used audio, input, incoming pairing, OBEX, networking, and serial rows have explicit pass or intentionally-unsupported decisions.
