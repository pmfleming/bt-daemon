# RQLens ownership and locality refactor

Baseline: `b9a9b01c25f9124e5f4d31e1921fef7f78d7212b`.
Tool: `../rust-quality-lens/target/debug/rqlens`, SHA-256
`09b72768d645f97886ae4b22ced05553bbcac988fac290745e66f97405e94053`.
No quality thresholds, source roots, dependencies or wire fixtures were changed.

## Findings and changes

- **Requests borrowed routing keys while cloning their entire payloads.**
  `src/api.rs` now gives each validated backend request ownership of its inputs.
  Adapter workers receive the original JSON value; device-policy routing removes
  `key` from the owned object instead of copying all settings. Validation still
  precedes effects, and adapter mutations retain their cancellation-safe worker.
  Trade-off: owned routing keys require small string allocations, including one
  for adapter-scoped power requests that previously borrowed their key.
- **Incoming and outgoing OBEX events used separate update paths.**
  `src/obex.rs` and `src/daemon/obex.rs` share event publication. Immutable event
  templates are borrowed, and updated strings reuse cloned buffer capacity.
  Remove the one-field `PendingAuthorization` wrapper, the redundant incoming
  monitoring wrapper and manual error propagation. Subscription-before-refresh,
  obexd cancellation acknowledgement, reservation cleanup and failure events are
  preserved. Event tests now assert identity/file fields independently rather
  than comparing two implementations that have become the same path.
- **RSSI batching mixed orchestration with per-peer effects.**
  `src/bluez/link_signal.rs` separates `poll_peer` from enumeration and batching.
  `poll` cognitive/cyclomatic complexity falls **11/8 → 1/3**; the new peer
  function is **2/6**. Both pre-read and post-read epoch fences, four-request
  concurrency, independent enumeration deadlines, units and failure backoff
  remain intact. The extraction adds one aggregate cyclomatic decision, while
  reducing nesting and combined Halstead effort.
- **Snapshot-only cache data lived in the backend's root module.**
  `CachedDevice` now belongs to `src/bluez/snapshot.rs`, removing assembly's
  dependency on the backend root. Snapshot assembly locality rises **97 → 100**.
  The shared cache lifetime remains backend-owned; the discovery live lifetime
  is named locally in `src/bluez/observations.rs`. Small observation values are
  copied explicitly instead of going through `Clone`.
- **PipeWire callbacks retained unnecessarily shared ownership.**
  `src/audio.rs` moves callback-exclusive retention state into closures as
  `RefCell`s, removing two `Rc` allocations and their clones. Shared completion
  flags and listeners remain owned for the required lifetimes.
- **Pairing validation allocated a PIN that it immediately discarded.**
  `src/pairing.rs` validates borrowed text and allocates only when accepting it.
  Rejection/cancellation dispatch belongs to `PendingResponse`, replacing two
  forwarding functions. A focused test covers empty/oversized PINs, Unicode
  character limits and exact accepted text.
- **Remaining wildcard imports obscured test dependencies.** Replace all four
  with explicit imports. Necessary management-socket FFI remains unchanged.
  `src/protocol.rs` groups identical parameter requirements without changing
  the checked-in wire contract; BlueZ timeout propagation uses the existing
  typed error conversion directly.

These changes remove redundant plumbing and retained state, not every potentially
unreachable public API. No suppression, unsafe shortcut or macro was introduced
for metric reduction.

## Comparable measurements

A live after-scan refused publication because the sibling `daemon-framework`
was being edited. Both authoritative scans therefore ran at the same frozen
project path, against dependency revision
`aa831877d49247c88fba6ead149d5aacea713b27`, with identical configuration/toolchain.
The frozen baseline and final source both completed `rqlens measure all`.

Artifacts, source patch, dependency snapshot, tool hash, logs, `comparison.json`
and `summarize.py` are in `target/review-current/`. Use **`before/` and `after/`**;
`live-before/`, `interim-after/` and the failed live after log are not the final
comparison. The frozen checkout is `target/review-current/frozen/bt-daemon/`.

Production function totals exclude module paths containing a `tests` segment or
ending in `_tests`; this is a naming heuristic, not compiler reachability.
Reliability findings instead use RQLens's explicit test/production scope.
Physical lines and syntactic `clone(` sites include `src/` and `test_support/`.
All 56 measured modules remain in the architecture averages.

| Metric | Before | After |
| --- | ---: | ---: |
| Production cognitive complexity sum | 788 | 778 |
| Production cyclomatic complexity sum | 2,029 | 2,023 |
| Production Halstead effort sum | 5,472,466 | 5,443,386 |
| Production function SLOC sum | 9,077 | 9,034 |
| Production maximum cognitive / cyclomatic | 11 / 19 | 10 / 19 |
| All-function cognitive / cyclomatic sums | 969 / 2,317 | 962 / 2,314 |
| All-function Halstead effort sum | 9,009,424 | 8,991,253 |
| Rust physical lines | 16,762 | 16,751 |
| Rust nonblank lines | 15,746 | 15,736 |
| Explicit clone sites, including Arc/Rc and tests | 272 | 265 |
| Mean observed-reuse leverage | 25.1786 | 25.5357 |
| Mean locality score | 98.3125 | 98.3527 |
| Mean leverage pressure | 33.6607 | 33.7321 |
| Observed escape hatches | 10 | 6 |
| Unsafe blocks | 5 | 5 |
| Observed production reliability findings | 0 | 0 |
| Token / AST clone groups | 15 / 1 | 15 / 1 |
| Line coverage | 60.36% | 60.66% |
| Passing tests | 95 | 96 |

**Limits and trade-offs:** these are incremental improvements. Leverage pressure
slightly worsens, and some individual function effort increases. Explicit test
imports expose previously hidden consumers, contributing to the observed-reuse
increase; this is not all new production reuse. Structural duplicate groups are
unchanged. Added tests/imports consume most physical LOC savings. Halstead effort
is a syntax heuristic, not developer hours; clone counts are not allocation or
runtime benchmarks.

## Validation and remaining gates

Passed on the final frozen source:

- `rqlens measure all` and `rqlens verify` (zero verification errors).
- `cargo test --locked --all-features`: 95 unit tests and one integration test,
  none ignored; includes contract equality, adapter cancellation/serialization,
  pairing, private D-Bus authorization, RSSI fencing and recovery coverage.
- `cargo clippy --locked --all-targets --all-features -- -D warnings`.
- Formatting and diff whitespace checks; both Python quality-gate tests.

Strict `rqlens check` and `scripts/quality_gate.py` still reject incomplete static
evidence. Seven item-macro invocations, two unresolved lexical impl owners and
19 unresolved dependency references remain. The strict policy's test/reliability/
architecture labels therefore must not be read as executed test failures or
observed production defects. No architecture violations were observed, but the
analysis is partial. Four existing repository-documentation warnings remain:
contribution instructions, code of conduct, security policy and changelog.

No gates were relaxed. Live Bluetooth/PipeWire hardware and allocation benchmarks
were not exercised, and no service was restarted.
