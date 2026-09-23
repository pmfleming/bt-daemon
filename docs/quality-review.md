# RQLens review and refactor

Baseline: the working tree **including its five pre-existing modified files**, not HEAD.
Tool: `../rust-quality-lens/target/debug/rqlens`, using unchanged `rqlens.toml`
and quality gates. Baseline artifacts and the original patch are saved under
`target/refactor-review/`; final artifacts are in `target/analysis/`.

## Changes

- **Subscription delivery:** keep event formatting with forwarding; share the
  event-envelope and watched-snapshot paths. Avoid cloning subscription IDs,
  emitters and JSON values unnecessarily. Separate scan orchestration from dispatch.
- **BlueZ:** place audio-policy effects with other operation effects; reuse
  Fast Pair service detection in snapshot assembly. Classify adapter errors once.
- **PipeWire:** share session initialization and synchronization; move device and
  endpoint results out of probe state instead of deep-cloning them. Use `Reverse`
  for profile priorities, avoiding overflow at `i32::MIN`.
- **Fast Pair:** share connection reservation, timeout and failure handling while
  preserving asynchronous RFCOMM acceptance and L2CAP suppression. Compare only
  the updated runtime field; avoid cloning whole reports on each update/retry.
- **Policy:** validate names beside their mutations, eliminating separate allowed
  lists. Retain transactional copies needed for validation/persistence rollback.
- **Protocol:** parse parameter examples once, fail closed on malformed examples,
  remove the production `expect`, and build operation schemas once.
- Remove redundant wrappers and unused derived implementations; retain public APIs.

## Measurements

These are RQLens measurements, not estimates of developer time. Complexity sums
include tests, so added regression coverage remains in the denominator.

| Metric | Before | After |
| --- | ---: | ---: |
| Cognitive complexity, sum | 781 | 768 |
| Cyclomatic complexity, sum | 2,099 | 2,084 |
| Worst function hotspot score | 67.70 | 65.86 |
| Nonblank Rust source lines | 13,209 | 13,185 |
| Duplicated lines | 298 | 213 |
| Mean locality risk, lower is better | 1.0053 | 0.8138 |
| Mean leverage pressure, lower is better | 33.3191 | 33.3085 |
| Line coverage | 50.71% | 51.78% |
| Passing Rust tests | 117 | 119 |
| Escape hatches / production reliability findings | 0 / 0 | 0 / 0 |
| Cyclic modules / architecture violations | 0 / 0 | 0 / 0 |

Leverage improvement is small overall: BlueZ's pressure falls from 41.5 to 35.5,
while the operation-effects module deliberately takes on the audio dependencies.
RQLens has no dedicated effort metric; source size, complexity and duplication
are proxies only. Reductions exclude the pre-existing edits.

## Validation and remaining risks

RQLens `measure all`, `verify`, and configured `check` policies pass, as do
`scripts/quality_gate.py`, its six Python tests, and Clippy with `-D warnings`.
Regression checks cover the unchanged protocol fixture, malformed examples,
policy validation/reset/rollback, connection suppression, subscription payloads,
transport backpressure and extreme profile priorities. No gates were relaxed.

Hardware integration was not exercised. Branch coverage remains unavailable,
73 high-CRAP functions remain, and the maximum cognitive complexity remains 9.
Four existing repository-documentation warnings remain (contributing guide,
code of conduct, security policy and changelog). Compiler checks do not establish
that every exported API is used; no blanket dead-code elimination is claimed.
