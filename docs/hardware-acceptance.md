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
- Connected/audio devices during the run: none; audio profile and battery behavior were therefore not exercised.

## Interactive Rofi-replacement gate

Record a date, device, and result for every row before moving `SUPER+B`.

| Scenario | Required evidence | Status |
|---|---|---|
| Power off/on | Snapshot and UI converge; active scan terminates on power-off | Pending |
| Bounded scan | Start, live additions, timeout, explicit cancellation, and stop-on-hide | Pending |
| No-controller boot | Clear unavailable state and recovery after controller appears | Pending |
| PIN input | Correct input validation, accept, reject, and timeout | Pending |
| Passkey input | 1–6 digit validation, accept, reject, and timeout | Pending |
| PIN/passkey display | Display updates and cancellation clear the modal | Pending |
| Numeric confirmation | Accept, reject, timeout, and operation terminal state | Pending |
| Pair authorization | Prompt cannot be bypassed by closing the popup | Pending |
| Service authorization | UUID/service is shown and rejection reaches BlueZ | Pending |
| Pair → optional trust → connect | Both trust policies and typed failures | Pending |
| Connect/disconnect | Sony WF-1000XM5, HX, and Buds | Pending |
| Operation cancellation | Pair and connect cancellation produce one terminal event | Pending |
| BlueZ restart | Daemon restarts, client reconnects, subscriptions resume | Pending |
| Adapter hotplug | Stale actions disappear and adapter selection recovers | Pending |

## Blueman-parity hardware matrix

| Device/workflow | Status |
|---|---|
| WF-1000XM5 A2DP playback, battery, reconnect | Pending |
| WF-1000XM5 headset microphone profile | Pending |
| HX audio profiles | Pending |
| Buds audio profiles and wake behavior | Pending |
| Keyboard passkey entered-count flow | Pending hardware |
| Mouse/gamepad just-works and wake | Pending hardware |
| Phone pairing and OBEX send/receive | Pending hardware |
| Suspend/resume and rfkill recovery | Pending |
| Two simultaneous discovery clients | Pending |

Do not remove Blueman until the actually used audio, input, incoming pairing, OBEX, networking, and serial rows have explicit pass or intentionally-unsupported decisions.
