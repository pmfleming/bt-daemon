# RQLens refactor review

Baseline: `ea046a2`. Tool: `../rust-quality-lens/target/debug/rqlens`, SHA-256
`09b72768d645f97886ae4b22ced05553bbcac988fac290745e66f97405e94053`.
The existing configuration, thresholds and source roots were retained.

## Findings and changes

- **Adapter batches carried redundant execution state.** In `src/api/adapter.rs`,
  stop iteration on the first uncertain write and project the remaining settings
  as not attempted. `Setting::outcome` shares response construction. Validation
  still precedes every effect; field order, snapshot-error reporting, serialization
  gates and survival of caller cancellation are unchanged. Submit cognitive /
  cyclomatic complexity falls from **13 / 11 to 9 / 9**.
- **RSSI enumeration fetched an unused address type.** Remove that D-Bus read and
  the discarded boolean from `src/bluez/link_signal.rs`. The privileged helper
  remains responsible for authoritatively validating connection/address type;
  no authorization or transport checks were removed. Explicit test imports replace
  wildcard imports here and in `src/rssi_helper/mgmt.rs`.
- **Native telemetry parsing duplicated bounds/error branches.** Use checked slice
  access and integer conversion in `src/rssi_helper/mgmt.rs`. Preserve negative
  syscall errors, zero-byte management sends, EOF, packet correlation and rejection
  of truncated/oversized datagrams. Extend the socket transport regression test
  with an oversized datagram before the valid reply. Exchange cognitive /
  cyclomatic complexity falls from **10 / 8 to 5 / 6**.
- **Audio identity checks were split between mutation and projection.** Both now
  use `resolved_device_key` in `src/daemon/audio.rs`, including invalid-address and
  missing-adapter handling. In `src/audio.rs`, move the final callback owners into
  their closures rather than cloning handles that have no other consumer.
- **Fast Pair provisioning copied unrelated report fields.** In `src/fast_pair.rs`,
  return only the known model ID and optional BLE address from the metadata wait.
  This removes a deep report clone and downstream checks of an already-established
  model-ID invariant. Borrow the provider across joined stream futures; retain
  ownership clones for independently spawned tasks. Consolidate successful frame
  completion without changing malformed-frame isolation or ACK ordering.
- **Protocol constraints repeated mutually exclusive tests.** A single match in
  `src/protocol.rs` keeps each method/property rule together; the checked-in wire
  fixture remains unchanged. Cognitive / cyclomatic complexity: **8 / 13 to 1 / 9**.
- **Observation timestamp conversion was duplicated four times.** `src/time.rs`
  provides the common wall-clock conversion for Fast Pair and BlueZ observations.
  Expiry, retry and freshness decisions continue using their existing clocks.

No public API or wire-contract changes were intended. Dead-code reduction here is
specifically the unused RSSI address-type plumbing and redundant local state, not
an assertion that every public function is reachable. Necessary native socket FFI
remains; no lint allowances, unsafe shortcuts, macros or quality-gate exceptions
were introduced to hide complexity.

## Comparable measurements

Two live after-scans refused publication because the sibling `daemon-framework`
was changing. The final comparison therefore uses one frozen dependency/tool
snapshot and the same copied project path for baseline and refactored source.
Both full scans completed successfully. Frozen inputs, dependency revision/patch
and hashes, logs, `comparison.json`, and `summarize.py` are in
`target/review-refactor/`; authoritative artifacts are `frozen-before/` and
`frozen-after/`, **not** the failed live after captures.

Production function totals exclude module paths containing `tests` or ending in
`_tests`; this is a naming heuristic, not cfg-aware reachability. Physical lines
and clone sites include `src/` and `test_support/`. Halstead effort is a syntax
heuristic, not developer hours or a runtime benchmark.

| Metric | Before | After |
| --- | ---: | ---: |
| Production cognitive complexity sum | 810 | 788 |
| Production cyclomatic complexity sum | 2,038 | 2,029 |
| Production Halstead effort sum | 5,521,808 | 5,472,466 |
| Production function SLOC sum | 9,107 | 9,077 |
| Production maximum cognitive / cyclomatic | 13 / 19 | 11 / 19 |
| All-function cognitive / cyclomatic sums | 991 / 2,326 | 969 / 2,317 |
| All-function Halstead effort sum | 9,057,924 | 9,009,424 |
| Rust physical lines, including integration tests | 16,768 | 16,762 |
| Rust nonblank lines | 15,754 | 15,746 |
| Syntactic `clone(` sites, including Arc and tests | 278 | 272 |
| Mean observed-reuse leverage | 24.9091 | 25.1786 |
| Mean locality score | 98.3909 | 98.3125 |
| Mean leverage pressure | 33.6545 | 33.6607 |
| Observed escape hatches | 12 | 10 |
| Unsafe sites | 5 | 5 |
| Observed production reliability findings | 0 | 0 |
| Token / AST clone groups | 15 / 1 | 15 / 1 |
| Line coverage | 60.24% | 60.36% |
| Passing tests | 95 | 95 |

**Trade-offs:** locality's aggregate slightly worsens, despite consolidating audio
identity checks and improving observed reuse. The new timestamp utility adds four
module dependencies and changes the denominator from 55 to 56 modules; leverage
pressure also slightly increases. Adapter batch/parse Halstead effort increases
locally, while project effort decreases. Total physical LOC falls by only six
lines: explicit test imports and the shared utility consume most production
savings. These are incremental improvements, not a claim that every requested
metric improved. Clone-site counts are not allocation measurements; escape-hatch
reduction is two test wildcard imports, not removal of necessary FFI.

## Validation and remaining limits

Passed:

- `cargo fmt --package bt-daemon -- --check` and `git diff --check`.
- `cargo test --locked --all-features`: 94 unit tests and one integration test,
  none ignored. Existing coverage includes every adapter failure position,
  cancellation/serialization, protocol fixture equality, audio identity/selection,
  RSSI freshness/fencing/backoff and privileged-helper authorization.
- `cargo clippy --locked --all-targets --all-features -- -D warnings`.
- Python quality-gate unit tests: two passing tests.
- Frozen RQLens `measure all` before/after and `verify`: zero verification errors;
  four existing repository-documentation warnings. One earlier live verification
  observed a transient formatting failure in the concurrently edited dependency.

Strict RQLens policy and `scripts/quality_gate.py` still fail on incomplete static
inventory/API-health evidence. Seven item-macro invocations remain unexpanded,
there are two unresolved lexical impl owners, and 19 dependency references remain
unresolved. Zero observed reliability findings is not proof of complete analysis.
No gates were relaxed. Hardware, live Bluetooth/PipeWire services and allocation
benchmarks were not exercised; no service was restarted.
