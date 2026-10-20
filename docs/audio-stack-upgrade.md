# Bluetooth/audio runtime upgrade

The repository selects a separate locked `runtimeNixpkgs` (`nixos-unstable`)
package set for BlueZ 5.87+, PipeWire 1.6.9+, and matching WirePlumber 0.5.18.
The stable channel still provided BlueZ 5.86 and PipeWire 1.6.6 at review time.
Only these three packages are overlaid into the build/development package set;
the framework's required Rust/Cargo 1.95.0 toolchain stays on the existing pin.
`checks.runtime-versions` rejects a dependency override below these versions.
The `v1_2_0` Rust feature remains an API floor, not a runtime version pin.

## Deployment is separate from building

A Cargo/Nix build or lock update does **not** update running system services.
An optional NixOS module selects the same package set for the host without
changing codec policy, enabling experimental BlueZ interfaces, granting daemon
capabilities, or enabling services implicitly:

```nix
imports = [ inputs.bt-daemon.nixosModules.audio-stack ];
services.bt-daemon.audioStack.enable = true;
hardware.bluetooth.enable = true;
services.pipewire.enable = true;
services.pipewire.wireplumber.enable = true;
```

The module uses bt-daemon's `runtimeNixpkgs` input. Do not override it with an old
stable channel: the version assertions will fail. Explicit host package overrides
remain possible; validate the complete PipeWire/WirePlumber/plugin combination.
The RSSI helper is still a separate opt-in module.

First build the host configuration without switching it. Applying that host
configuration and restarting services is a separate operator-controlled action;
it can interrupt sound and disconnect Bluetooth devices. Retain the previous
NixOS generation for rollback. On other distributions upgrade BlueZ, PipeWire
and WirePlumber through the distribution's package manager instead.

## Acceptance before rollout

- Compare executable versions of the **running** services with the intended
  packages; development-shell binaries can be different.
- Check A2DP playback, microphone/profile changes, selected default routes, codec
  changes, device unplug/reconnect, and independent PipeWire/BlueZ restarts.
- Test suspend/resume, GATT/Fast Pair control traffic, and LE Audio on supported
  adapters/headphones. A successful build does not demonstrate interoperability.
- Confirm WirePlumber's session ownership and routing still work; do not disable
  logind seat monitoring merely to hide an unavailable Bluetooth audio card.

Upstream release evidence:
- https://github.com/PipeWire/pipewire/blob/1.6.9/NEWS
- https://github.com/bluez/bluez/blob/5.87/ChangeLog
