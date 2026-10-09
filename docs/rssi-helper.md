# Connected RSSI helper

## Boundary

```
Shelllist -> session bt-daemon -> system bt-rssi-helper -> Linux HCI control channel
```

`bt-rssi-helper` is a separate executable from the same repository/package. It
initializes no pairing agent, Fast Pair provider, audio backend, state store or
session mutation API. It reuses daemon-framework's shutdown utility; Bluetooth
commands, schemas and authorization stay domain-owned. There is no universal
privileged framework process and no ability to supply arbitrary HCI packets,
commands, files, programs or D-Bus paths.

The system service runs as the dedicated `bt-rssi-helper` account, not root,
with ambient/bounding **CAP_NET_ADMIN** only. The user daemon receives no
capabilities. Systemd restricts address families to Unix and Bluetooth, makes the
filesystem read-only, hides home/device access, forbids privilege escalation and
namespaces, and bounds tasks/memory. Do not use `PrivateNetwork`: it can hide the
host Bluetooth controllers. No setuid binary, sudo subprocess or runtime tool
output parsing is involved.

CAP_NET_ADMIN is still a broad kernel capability: an exploited helper could send
other Bluetooth management commands. Its narrow audited API, dedicated identity,
Rust parsing and separate systemd confinement reduce that risk; they do not turn
the capability itself into an RSSI-only kernel permission.

## API and policy

- Bus: system bus, `org.laufan.BluetoothRssiHelper`.
- Object: `/org/laufan/BluetoothRssiHelper`.
- Interface: `org.laufan.BluetoothRssiHelper1`.
- Method: `Read(q index, s address) -> (s status, n rssi, b relative)` (one struct).
- Statuses: `ok`, `not-connected`, `unavailable`, `permission-denied`, `timeout`.
  The numeric fields are meaningful only for `ok`. Unknown/invalid replies fail
  closed in the client; 127 is the controller's unavailable sentinel.
- D-Bus errors cover invalid arguments, unauthorized callers, admission limits,
  missing BlueZ properties and whole-request timeouts.

Polkit action `org.laufan.bt-daemon.read-connected-rssi` uses the actual unique
system-bus sender as its subject. No caller-supplied UID/PID is accepted. Flags
are zero: polling never invokes an authentication dialog. Default authorization
is **yes for an active local session, no for inactive/remote callers**. There is
no session lock override or cached authorization in the helper. Polkit decides
what constitutes an active session. Missing/unreachable Polkit fails closed.

Any process authorized by this policy can read connected-device RSSI; it is not
an executable-identity allowlist for bt-daemon. System bus policy restricts name
ownership to the helper account and only allows Read/Introspect/Ping requests.
The helper canonicalizes a 17-character Bluetooth address and rejects the
reserved controller index. It constructs the BlueZ path itself, requires
`Device1.Connected`, and derives random/public address type from BlueZ. It never
connects a target merely to measure it.

Admission is non-queued: four simultaneous requests maximum, sixteen admitted
requests per second globally, and at most one attempt per target every two
seconds. Rate maps are bounded and expired. The two-second management deadline
is inside a four-second authorization/BlueZ/read deadline; the client allows five
seconds for the D-Bus round trip. Malformed/failing calls never refresh a sample.

Linux can return **zero from send() while queuing a Command Status error**. The
transport accepts that return and reads the bounded reply. A positive partial
write is still rejected. This preserves errors such as `0x14 Permission Denied`
instead of silently treating them as WriteZero.

## NixOS deployment (explicit opt-in)

In the host's NixOS modules, using its existing bt-daemon flake input:

```nix
imports = [ inputs.bt-daemon.nixosModules.rssi-helper ];
services.bt-rssi-helper.enable = true;
```

An optional `services.bt-rssi-helper.package` override must contain the matching
helper binary and policies. The module creates the system user/group, installs
D-Bus and Polkit policies and enables the system service. The helper has a system
D-Bus activation file as well. Importing the module without enabling it does
nothing; simply building/installing the user daemon does not grant privileges.

Rebuild the coordinated source graph through `daemon-framework/tools/local-build`
and deploy using the host's normal approved rebuild procedure. New source files
must be Git-tracked or intent-to-add for that tool's snapshot contract. Building
or testing this repository does **not** activate the helper or restart services.
No /etc/nixos edits are made by the implementation.

On a non-Nix system, create the dedicated system user/group and install the
packaged unit under the system unit search path, the `.conf` under
`dbus-1/system.d`, the activation file under `dbus-1/system-services` and the
Polkit action under `polkit-1/actions`. Substitute the executable installation
path for `@out@` when using source templates. Policy reload and service activation
are explicit administrator deployment operations. Direct D-Bus activation without
systemd cannot grant the ambient capability; use the supplied system service.

After an approved deployment, read-only checks include:

```sh
systemctl show bt-rssi-helper.service -p User -p AmbientCapabilities -p CapabilityBoundingSet
journalctl -u bt-rssi-helper.service
journalctl --user -u bt-daemon.service
busctl --system introspect org.laufan.BluetoothRssiHelper /org/laufan/BluetoothRssiHelper
```

Introspection can activate the helper if installed; it is a deployment check, not
an offline test. Hardware/authorization acceptance is recorded separately in
[hardware acceptance](hardware-acceptance.md#connected-link-rssi).

## Fallback and validation

The existing session sampler owns freshness, connection-generation fencing and
cached fallback. Fresh connected samples win over discovery; fresh discovery wins
over stale cached samples. Original units survive fallback, and classic relative
RSSI never becomes an invented percentage. Permission loss, helper disappearance
and timeouts mark retained readings non-live and back off; retrying a read does
not reconnect a device or refresh the cached timestamp.

Automated tests use a private D-Bus with no host activation directories, synthetic
Polkit/BlueZ services, fake hardware and Unix datagram management replies. They
cover caller identity, no-prompt policy checks, denial, unavailable Polkit, target
validation, disconnected devices, admission/concurrency/deadline limits, helper
restart, wire units/sentinels and zero-byte management sends. They do not prove
real controller support or host Polkit session classification.
