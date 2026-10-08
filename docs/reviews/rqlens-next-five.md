# Next five quality improvements

Baseline: `37b2cca`, including the newer adapter-batch and remembered-audio-profile
features. Tool: `../rust-quality-lens/target/debug/rqlens`, SHA-256
`09b72768d645f97886ae4b22ced05553bbcac988fac290745e66f97405e94053`.
Configuration, source roots and gates are unchanged.

## Implemented

1. **Shared scan cleanup selection — `src/daemon/scan.rs`.** Explicit stops and
   timeout cleanup now share `select_scans`, preserving leases held by other
   requests. Deduplicate borrowed adapter keys before cloning the stop list;
   sort it to make the first backend failure deterministic. Remove the separate
   timeout-selection and retained-adapter helpers. Serialize the start response
   before transferring the event to its broadcaster, avoiding another event copy.
   The transition lock still covers selection and effects. Tests cover timeout
   cleanup with overlapping leases, unknown requests and deduplicated stop-all.
2. **Staged account-key writes without cloning the live map —
   `src/fast_pair/keys.rs`.** Persistence receives a borrowed candidate iterator
   excluding the replaced/removed entry and optionally including its replacement.
   Commit to the live map only after the write succeeds. This removes two full-map
   clone sites while retaining the existing disk format and private-file policy.
   Encoding still allocates the serialized key map; this is not zero-copy I/O.
   Tests cover failed insert/replace/remove, invalid keys, missing-key removal,
   unrelated entries, successful retries and reloading the saved result.
3. **Retryable failed policy removal — `src/management.rs`.** Previously, a failed
   save discarded the in-memory override, so a retry could become a no-op even
   though the old file still contained the override. Retain the removed entry and
   restore it on a persistence error while holding the existing mutex. No map
   cloning is required. Tests cover failed removal, disk/memory agreement, missing
   entries, repeated removal, successful retry and preservation of another device.
   The existing best-effort/logged-error API is unchanged.
4. **Cheaper snapshot assembly — `src/bluez/snapshot.rs`.** Reuse the adapter address
   already read for stable identity instead of reading and formatting it again.
   Cache merging borrows live keys and stages references to missing cached entries,
   rather than cloning every live key. This adds a temporary vector of references;
   it is an allocation trade-off, not a measured runtime speedup. Tests cover live
   precedence, adapter isolation, exact TTL boundaries, stale entries and unchanged
   cache contents. Cached views still clear live connection/presentation state.
5. **Testable audio request and outcome policy — `src/daemon/audio.rs`.** Move
   validation into `Change::request` and successful-application result projection
   into `applied_response`. Regression tests exercise supported/invalid `remember`
   values and all combinations of persistence and refresh success/failure without
   probing hardware. The non-abortable worker still owns the device gate and both
   apply/persist effects; partial outcomes and wire envelopes are unchanged.

## RQLens comparison

Production excludes module paths named `tests` or ending in `_tests`; this is a
naming heuristic, not cfg-aware reachability. Means cover the same 49 modules.
Halstead effort is an advisory syntactic metric, not developer hours.

| Metric | Before | After |
| --- | ---: | ---: |
| Production cognitive sum / maximum | 713 / 18 | 710 / 13 |
| Production cyclomatic sum | 1,871 | 1,869 |
| Production Halstead effort sum | 5,093,774 | 5,044,748 |
| Audio `apply_change` cognitive / cyclomatic | 18 / 18 | 11 / 12 |
| Scan `stop` Halstead effort | 95,747 | 47,503 |
| Production function SLOC | 8,409 | 8,409 |
| Clone calls inside observed production functions | 201 | 198 |
| All Rust physical lines, including tests | 14,517 | 14,701 |
| All-source clone-call sites, including tests | 232 | 241 |
| Mean locality / observed-reuse leverage | 98.4541 / 25.7143 | 98.4541 / 25.7143 |
| Mean leverage pressure | 33.6735 | 33.6837 |
| Token / AST duplicate groups | 12 / 1 | 13 / 1 |
| Observed escape hatches / production reliability findings | 1 / 0 | 1 / 0 |
| Line coverage | 54.62% | 55.90% |
| Passing tests | 70 | 75 |

These changes prioritize a correctness fix, cheaper ownership and lower hotspot
complexity. They do **not** reduce total source size: tests account for most of
its growth, and production function SLOC is flat. Locality/reuse means are flat;
pressure increases slightly. The extra duplicate group is shared setup in two
management persistence tests. No unrelated code was removed to improve a score.

## Validation and evidence

Passed `cargo fmt --all -- --check`, `cargo test --locked --all-features`, strict
all-target/all-feature Clippy, both Python gate unit tests, and RQLens `measure all`
and `verify`. Verification reports zero error failures and four existing
repository-documentation warnings. No hardware or live service was exercised.

Strict partial-evidence checking and `scripts/quality_gate.py` still reject
incomplete evidence. Seven item macros and two lexical impl owners remain outside
complete inventory; dependency identity still has 19 unresolved references. No
thresholds or exclusions were relaxed. One measurement refused publication when
inputs changed and was rerun successfully; the sibling framework is a live local
dependency, so these are not isolated dependency/performance experiments.

Artifacts, logs, comparison data and its `summarize.py` are retained under
`target/review-next-five/`; current results are in `target/analysis/`.
