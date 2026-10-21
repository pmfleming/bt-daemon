# RQLens-guided refactor — 2026-10-21

Baseline: `6a41e54` (initially clean working tree). Used the local checkout at
`../rust-quality-lens` (the installed project directory has hyphens), building
its `rqlens` binary with `cargo build --locked --bin rqlens`.

## Findings and changes

1. **Routing callback mixed identity validation, binding, writes and confirmation.**
   In `src/audio.rs`, `RouteChange` now owns the operation's shared state and
   separates these responsibilities. Device and endpoint serials, adapter/address,
   endpoint ownership, removal/error handling, deadlines and write-once behavior
   remain enforced. Listeners are retained outside callback state to avoid Rc
   cycles; replacing default metadata releases its previous listener.
2. **Unnecessary owned copies.** Profile callbacks now retain only device identity,
   not a cloned `AudioDevice` with profiles/endpoints. Sony active-mode polling reads
   the static mode directly instead of building/cloning a presentation snapshot.
   Routing shares one state rather than separately cloning many callback captures.
3. **Scattered snapshot representation and duplicated test infrastructure.**
   `SharedSnapshot`, result conversion and response encoding now live beside
   subscription encoding in `src/daemon/subscription.rs`. The daemon no longer
   directly depends on the snapshot model. `src/test_bus.rs` shares isolated bus
   startup, connection building and process cleanup across unit and integration
   tests. No host service activation or parent-process environment mutation.
4. **Avoidable branching and indirection.** Flattened Fast Pair command result
   handling; consolidated advertisement duplicate-field detection and Sony retry
   initialization. Removed the `DeviceEndpoints` wrapper and `ProbeState::retain`
   forwarding method. Removed two wildcard imports, not the necessary HCI FFI.

Public Rust APIs and wire schemas are retained. No unused public API was deleted
merely because it has no in-repository caller. This is targeted redundancy removal,
not a claim that all dead code has been identified.

## Measurements

Raw artifacts and the comparison are saved locally under
`target/review-request/{before,after}` and `target/review-request/comparison.json`.
These are ignored build artifacts, not committed evidence.

| Measurement | Before | After |
| --- | ---: | ---: |
| `set_default_node` cognitive complexity | 44 | 6 |
| `set_default_node` cyclomatic complexity | 31 | 8 |
| `set_default_node` Halstead effort | 346,490 | 69,028 |
| Maximum function cognitive complexity | 44 | 14 |
| Maximum function cyclomatic complexity | 31 | 16 |
| Sum of function cognitive complexity | 1,125 | 1,097 |
| Sum of function cyclomatic complexity | 2,537 | 2,539 |
| Sum of function Halstead effort | 10,135,210 | 9,898,704 |
| RQLens duplicated lines | 389 | 315 |
| RQLens escape hatches | 4 | 2 |
| Mean locality risk (lower better) | 1.5984 | 1.5847 |
| Mean observed leverage (higher better) | 23.9344 | 24.1935 |
| Mean legacy leverage pressure (lower better) | 33.7541 | 33.8065 |
| Daemon outbound module dependencies | 10 | 9 |
| Daemon locality risk | 15 | 12 |
| Explicit `.clone()` / `::clone(...)` call sites | 294 | 278 |
| Physical Rust lines, including integration tests | 17,633 | 17,649 |
| Nonblank Rust lines, including integration tests | 16,566 | 16,567 |

**Tradeoffs:** complexity is distributed into smaller responsibilities; aggregate
cyclomatic complexity increases by two despite much lower maxima. Total LOC is
essentially flat, not reduced: explicit routing state and new regression coverage
outweigh deletion by 16 physical lines. The line-reduction objective therefore
remains open. Leverage pressure also increases slightly; observed reuse and legacy
pressure are different metrics. No tests or safety checks were removed to meet
numeric targets.

**Evidence limits:** RQLens marks the artifacts partial because item macros and
some impl owners are not fully inventoried. Both architecture runs have 22
unresolved dependency references (96.75% → 96.80% resolution). Explicit imports
also reveal edges previously hidden by a glob. Mean architecture scores include
tests and a new helper module, so their small improvements are advisory, not proof
of a global architecture improvement. Clone call counts include cheap Rc/Arc
clones and are not allocation measurements. Halstead effort is not developer time.
The configured `source_roots = ["src"]` excludes the integration-test file from
RQLens's line count; the manual Rust line counts above include it. RQLens source
nonblank lines increase from 15,934 to 15,966.

## Validation and remaining work

Passed:

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --locked -- -D warnings`
- `cargo test --all-targets --locked`: 52 unit tests + 1 integration test
- `cargo test --doc --locked`: no doctests defined
- `git diff --check`

Added a hardware-free regression covering sink/source confirmation, pre-write
observations, invalid/wrong-name metadata, unrelated keys and metadata clearing.
Existing tests cover failure overriding completion, reused object IDs, profile
properties, command cancellation/timeouts, malformed advertisements, retry budgets,
private-bus sender filtering and process-level service recovery.

Focused RQLens producers: `hotspots`, `clones`, `escape-hatches`, `reliability`,
`locality`, `leverage`, `map`, `architecture-rules`, using `--config rqlens.toml`.
The final architecture report observes zero violations of the two configured rules;
this remains partial evidence. Production reliability findings remain zero observed.

No new coverage, mutation, benchmark or physical Bluetooth/PipeWire acceptance run
was performed. In particular, real default-route/profile changes and metadata
replacement during a PipeWire restart still need the hardware checks documented
in `docs/hardware-acceptance.md`. The larger BlueZ orchestration module remains the
highest locality-risk module; further restructuring should preserve its recovery
and cancellation invariants rather than optimize the scores alone.
