# RQLens review — 2026-10-24

Baseline: `dd7b762` plus the pre-existing changes in the two recovery integration
tests. Those changes are preserved. Used the local `../rust-quality-lens` checkout;
a copy of the built executable is retained under `target/review-20261024/rqlens`.
Public APIs and wire formats are unchanged.

## Changes

- **Shared framed-stream reader:** `src/framing.rs` owns read/EOF handling and
  pending-message delivery for Sony and Maestro. It consumes the decoder's vector
  directly, without another queue allocation. Decoding and invalid-frame logging
  remain vendor-specific. Removed Maestro's bespoke reader and the Sony test's
  duplicate reader, which previously discarded coalesced messages.
- **Single connection-state representation:** Fast Pair's connected timestamp is
  now part of `LinkState`, removing the parallel `connected_since` map. Existing
  59/60-second, duplicate-notification and stale-attempt regressions remain.
  Retry classification now belongs to `RetryPolicy`; orchestration passes the
  actual error instead of interpreting it through a vendor-channel dependency.
- **Ownership and redundancy:** audio projections move owned codec/state strings;
  compatibility endpoints share one selection expression. Removed unused `Clone`
  implementations on three private frame/packet types and unused `LinkState`
  derives. Identity loading no longer accepts an unused no-path branch, and its
  presentation update no longer constructs an intermediate tuple/scope.
- **Safer boundaries:** Fast Pair advertisement slicing uses checked splitting.
  Explicit imports replace two glob imports. The isolated acceptance probe uses
  Rustix's safe process-group signaling with checked PID conversion instead of
  an unsafe `libc::kill` call; its armed/disarmed ownership rules are retained.
  The additional Rustix feature is dev-only.
- **Tests:** combined overlapping Sony mode/layout cases and Maestro session
  lifecycle cases, retaining previous assertions and adding wind/type variants,
  invalid ambient/voice values, corrupt/coalesced input, queued reports before
  EOF, and exact report cardinality. Added real temporary-sysfs rfkill aggregation
  and failure cases, and corrupt/unsupported identity-file cases.

## Measurements

Live before/after artifacts: `target/review-20261024/{before,after}`. Both runs
executed `measure all`, including coverage and tests. Default test results are
69 → 68 passed (67 → 66 unit tests, two integration tests in each run); the same
one environment-dependent acceptance experiment remains ignored.

Auto semantic identity resolution varied between runs. Architecture comparisons
therefore use paired source snapshots with one frozen framework dependency,
`identity_resolution = "disabled"`, and the same executable. Their artifacts are
`{before,after}-static`, with `static-summary.json` alongside them. These are
syntax-based advisory observations, not compiler-verified architecture or Git
history measurements. Production sums exclude authored test modules/fixtures
and the individually test-gated helpers; macro expansion remains partial.

| Measurement | Before | After |
| --- | ---: | ---: |
| Production function cognitive complexity, sum | 1,024 | 1,011 |
| Production function cyclomatic complexity, sum | 2,484 | 2,477 |
| Production function Halstead effort, sum | 7,031,052 | 7,025,751 |
| All-function cognitive / cyclomatic sums | 1,247 / 2,797 | 1,238 / 2,797 |
| All-function Halstead effort, sum | 11,690,124 | 11,781,147 |
| Mean locality risk, lower better | 1.4271 | 1.3854 |
| Mean observed leverage, higher better | 20.5556 | 20.8333 |
| Mean leverage pressure, lower better | 33.5903 | 33.6042 |
| Duplicated lines | 408 | 386 |
| Explicit `.clone()` / `::clone(...)` sites | 298 | 294 |
| `.cloned()` sites | 12 | 11 |
| Escape hatches | 5 | 2 |
| Physical Rust lines, including integration tests | 20,264 | 20,277 |
| Production function SLOC, sum | 11,077 | 11,058 |
| Test/fixture function SLOC, sum | 4,324 | 4,335 |
| Line coverage | 56.92% | 57.10% |
| Function coverage | 52.26% | 52.29% |
| Region coverage | 55.88% | 56.30% |
| High-CRAP functions | 84 | 82 |

**Not every requested metric improved.** Production complexity/effort, observed
reuse/locality, duplication, clone sites, escape hatches and coverage improved.
Test count fell, but test-function SLOC and test-inclusive effort increased.
Total Rust lines increased by 13, principally offsetting production reductions
with explicit imports and new coverage. Leverage pressure increased slightly.
Further total-LOC/test-LOC reduction remains open; no tests or safety checks were
removed merely to force every metric downward. Clone-site counts include cheap
shared-ownership clones and are not allocation benchmarks.

## Verification and remaining findings

- `cargo clippy --all-targets --locked -- -D warnings`: passed.
- RQLens `verify`: formatting, compilation, Clippy, tests, doctests and rustdoc
  passed; zero error-level practice failures. Four documentation-policy warnings
  remain (contributing, code of conduct, security policy and changelog).
- Production reliability findings: zero observed before and after.
- Configured architecture violations: zero observed; identity evidence remains
  partial, with unresolved references. Branch coverage is unavailable.
- Strict `rqlens check --fail-on partial --fail-on test-failure --fail-on
  practice-failure --fail-on reliability-finding` still exits 1. The repository
  quality gate stops on incomplete API-health evidence. Gate thresholds were not
  relaxed; remaining duplication, high-CRAP, escape-hatch and ignored-test counts
  also exceed their configured ceilings.
- The remaining escapes are the HCI socket address's `repr(C)` and documented
  unsafe bind in `src/rssi_helper/mgmt.rs`. Removing them needs a suitable safe
  HCI-address API, not hiding the boundary in another module.
- `audio::finalize_devices` and `audio::apply_profile` remain cognitive hotspots
  (11 and 10). Hardware-event/error-path coverage is a better next investment
  than further consolidation of unrelated tests.

No hardware acceptance, ignored Qt experiment, mutation campaign or performance
benchmark was executed. The probe signaling change is compilation-checked, not
claimed as an executed acceptance result. `git diff --check` passes.
