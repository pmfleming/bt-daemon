# RQLens-guided refactor — 2026-10-23

Baseline: `2bc7116` (clean working tree). Used the local
`../rust-quality-lens/target/debug/rqlens` binary and the existing `rqlens.toml`.

## Review and changes

- **Duplicated vendor framing:** `src/framing.rs` now owns byte stuffing,
  unescaping, noise removal and incomplete-frame bounds. Sony and Maestro retain
  their own payload/checksum validation. Sony consumes its closing delimiter and
  rejects empty frames; Maestro retains shared closing/opening flags and ignores
  idle flags. The shared module has no hardware dependencies. An intermediate
  placement under `vendor_channel` failed the existing architecture rules; moving
  it to this independent module restored zero observed violations.
- **Redundant operation state:** `src/daemon/operation.rs` uses the framework's
  `OwnedOperations` as the only active-operation registry. Removed the parallel
  device index, `OperationState`, `OperationExecution` and duplicate cleanup.
  Workers share the coordinator instead of independently cloning its fields.
  Admission's device check is now a scan bounded by the existing 256-task limit,
  rather than an additional hash lookup. Ownership, start barriers, progress and
  single-terminal-result behavior remain covered by tests.
- **Unnecessary ownership:** vendor startup takes the receiver it needs instead
  of requiring temporary sender clones. Presentation component merging compares
  borrowed strings and only copies the merged result when it changes, including
  normalization of unsorted/duplicated persisted components.
- **Branching:** shared optional-provider error handling preserves independent
  startup failures; profile classification uses a prefix table; Maestro packet
  fields share checked numeric conversion without accepting wrong wire types.
- **Dependency direction:** generic vendor channels no longer depend on Fast
  Pair's private retry implementation. The common BlueZ busy-error classifier
  lives in `vendor_channel`; both implementations use it.

Public Rust APIs and wire formats are unchanged. The removed types/index were
redundant, not unreachable code. Clippy reports no dead-code warnings; no unused
public API was deleted merely because this repository has no caller.

## Measurements

Final comparable artifacts: `target/refactor-review/{before,after}`;
summary: `target/refactor-review/comparison.json` (ignored local artifacts).
Concurrent edits in the sibling `daemon-framework` checkout caused RQLens to
reject two live runs. Final measurements therefore use source copies under
`target/refactor-review/snapshot/{before,after}`, sharing one frozen copy of the
path dependency. Before is `git archive 2bc7116`; after contains the final Rust
sources. The copies omit Git history, so these runs do not establish history-based
change pressure. Producers: `hotspots`, `clones`, `escape-hatches`, `reliability`,
`locality`, `leverage`, `map`, `architecture-rules`.

| Measurement | Before | After |
| --- | ---: | ---: |
| Sum of function cognitive complexity | 1,184 | 1,163 |
| Sum of function cyclomatic complexity | 2,718 | 2,699 |
| Sum of function Halstead effort | 10,623,292 | 10,612,999 |
| Maximum cognitive / cyclomatic complexity | 14 / 16 | 14 / 16 |
| BlueZ initialization cognitive / cyclomatic | 4 / 13 | 1 / 7 |
| BlueZ initialization Halstead effort | 75,462 | 48,057 |
| Profile classification cognitive / cyclomatic | 7 / 7 | 1 / 2 |
| RQLens duplicated lines | 382 | 356 |
| Explicit `.clone()` / `::clone(...)` sites | 292 | 283 |
| `.cloned()` sites | 11 | 12 |
| Physical Rust lines, including integration tests | 19,187 | 19,175 |
| RQLens source nonblank lines | 17,098 | 17,078 |
| Mean observed leverage (higher better) | 23.6923 | 23.7879 |
| Mean leverage pressure (lower better) | 33.8000 | 33.7955 |
| Mean locality risk (lower better) | 1.5692 | 1.5909 |
| Escape hatches | 2 | 2 |
| Configured architecture violations | 0 | 0 |

**Tradeoffs and limits:** aggregate locality risk regresses slightly: the generic
vendor module loses its one outbound dependency, but Fast Pair gains a direct
vendor-channel dependency (9 → 10 outbound modules). Global locality improvement
therefore remains open; the dependency direction and framing boundary improve.
Observed leverage is advisory, and the new module changes the averaging population.
Profile classification and packet decoding reduce decisions but individually
increase Halstead effort; the aggregate decreases only modestly. Clone-site counts
include cheap Arc clones and are not allocation or runtime measurements.

The two escape hatches are the required `repr(C)` socket address and documented
unsafe Linux HCI `bind` in `src/rssi_helper/mgmt.rs`; neither was hidden or removed.
RQLens observes 240 reliability findings, all test-scoped, before and after.
Macro expansion/impl ownership remains partial, with 22 unresolved dependency
references in each architecture run (97.07% → 97.11% resolved). These observations
are not proof of the absence of defects or dead code.

## Validation

Passed on the working checkout:

- `cargo fmt --package bt-daemon -- --check`
- `cargo clippy --all-targets --locked -- -D warnings`
- `cargo test --all-targets --locked`: 64 unit tests and 2 integration tests
- `cargo test --doc --locked`: no doctests defined
- `git diff --check`

The final `cargo fmt --all -- --check` attempt also traversed the sibling path
dependency and reported formatting differences in its concurrently edited tests;
use the package-scoped command for this checkout. No sibling fixes are included.

New/extended regressions cover delimiter-specific empty/idle behavior, exact
incomplete-frame bounds and recovery, strict profile prefixes, unchanged component
storage and persisted-order normalization, and accepting a new device operation
after completion or cancellation. Existing protocol vectors, fragmented/coalesced
transport tests and private-bus recovery tests still pass.

No coverage, mutation, benchmark or physical Bluetooth/PipeWire acceptance run was
performed. Hardware checks in `docs/hardware-acceptance.md` remain necessary.
