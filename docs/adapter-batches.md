# Submitted adapter-setting patches

`bluetooth.adapter.update` accepts:

```json
{"key":"adapter-opaque","changes":{"alias":"Computer","discoverable_timeout":120,"pairable_timeout":0}}
```

`key` is a captured opaque adapter identity. `changes` must be a nonempty object
containing only those three optional settings. Alias must be a nonempty string;
timeouts are unsigned 32-bit integers, including zero. Null, fractional, negative,
out-of-range and unknown fields are rejected before any setter is invoked.
Hardware availability and BlueZ validation remain authoritative at execution.

After validation an owned, non-cancellable worker executes the supplied settings
in alias/discoverable-timeout/pairable-timeout order. It holds one adapter-setting
gate across the sequence and final snapshot. Legacy `bluetooth.adapter.operation`
requests use the same gate; different adapters remain independent. Dropping the
request waiter does not abort the worker or release its gate early. Power changes,
external BlueZ clients and hardware changes are not excluded by this gate.

The response is terminal, not an admission acknowledgement. `ok: true` means an
outcome is available, **not** that every setting succeeded. `data.adapter_batch`
contains the captured `key` and an `outcomes` array, one item for each supplied
field, echoing `field`, `value` and one of:

- `applied`: BlueZ acknowledged the setter; not a guarantee against later changes.
- `unknown`: the backend returned an error; it may have happened after dispatch.
  The item retains the typed `error`. Do not infer rollback or absence of effects.
- `not-attempted`: an earlier setting failed, so this setter was never invoked.

The first error stops the sequence. There is no rollback or automatic replay.
A final read failure adds `adapter_batch.snapshot_error` instead of erasing known
write acknowledgements; otherwise `data.snapshot` contains the fresh observation.
A worker failure without a result returns `adapter-outcome-unknown`. Transport
loss or daemon restart leaves the caller's unobserved outcome unknown. Batches
are not durable jobs and are not replayed on restart. Inspect fresh settings
before explicitly retrying retained changes. Discarding a UI draft cancels no
already-submitted setter.

Local field drafts, Enter/Tab saves and Escape discard remain frontend-owned.
Do not assemble a patch from unsaved editor buffers, or silently resend uncertain
fields when another field is saved. Deploy the new consumer with a matching daemon;
there is no client fallback that reconstructs the native sequence.

Hardware-free tests exercise whole-patch rejection, every failure position,
zero/max timeouts, snapshot failure, one-field saves, captured keys, independent
adapters, legacy serialization and waiter cancellation. Regenerate the checked
partial-outcome fixture with:

```
BT_DAEMON_UPDATE_CONTRACT_FIXTURE=1 cargo test --lib adapter_batch_fixture_is_current
```

Then run the normal tests/build and copy the fixture/generate bindings in Shelllist.
Live BlueZ/hardware acceptance and the pinned deployment gate remain separate.
