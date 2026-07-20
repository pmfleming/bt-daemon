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
- The alternate no-trust pairing policy, audio playback/profile readiness, component-update behavior as batteries discharge, and reconnect behavior remain to be checked separately.

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
| BlueZ restart | Daemon restarts, client reconnects, subscriptions resume | Pending |
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
| Phone pairing and OBEX send/receive | Pending hardware |
| Suspend/resume and rfkill recovery | Pending |
| Two simultaneous discovery clients | Pending |

Do not remove Blueman until the actually used audio, input, incoming pairing, OBEX, networking, and serial rows have explicit pass or intentionally-unsupported decisions.
