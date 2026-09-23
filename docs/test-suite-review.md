# Test-suite reduction

Baseline: commit `406bc04`. Count runnable test functions reported by Cargo and
Python unittest discovery, not individual table rows or subtests.

| Suite | Baseline | Final |
| --- | ---: | ---: |
| Rust (all targets) | 119 | 80 |
| Python quality tooling | 6 | 4 |
| Total | 125 | 84 |

`125 × 0.67 = 83.75`; 84 is the nearest whole-test target (67.2% retained,
32.8% removed). No tests were ignored or disabled, and no gates were relaxed.

## Selection

- Remove the duplicate daemon-framework watch-forwarding test, trivial constant
  assertions, visibility-predicate truth-table duplication, and repeated helper
  checks already exercised by retained workflows.
- Test malformed commands, ownership, cancellation events and device-busy rejection
  through daemon dispatch rather than repeating coordinator/helper-only tests.
- Exercise PipeWire profile parsing through serialized PODs, and endpoint readiness
  through the device snapshot contract rather than private field-mutator tests.
- Consolidate BlueZ watch/recovery lifecycles; assert delivered events and candidate
  cleanup instead of handle counts or `Arc` reference counts.
- Consolidate connection/retry lifecycles and transport success/failure checks.
  Keep suppression, backpressure, EOF, framing limits and stale-command cleanup.
- Keep independent crypto reference vectors; test authenticated-message integrity
  at encoded-frame level instead of repeating the same nonce/payload checks below it.
- Keep persistence failure/retry and shutdown flushing. Exercise concurrent flushes
  through the writer rather than its private barrier-collection representation.
- Consolidate per-device policy persistence with the existing validation/reset matrix;
  all supported settings now undergo reload and peer-isolation checks.
- Preserve pairing secrecy, prompt recovery, confirmation/passkey acceptance,
  cancellation races, timeout and owner-loss behavior in complete lifecycle tests.
- Check OBEX sanitization plus file reservation together, including collision safety;
  retain scoped transfer cancellation, authorization cleanup and late-answer rejection.
- Consolidate invalid configuration and unknown-evidence matrices in Python while
  retaining independent artifact corruption and numerical-boundary checks.

Public protocol fixtures, identity privacy/reload, device gates, client correlation,
rfkill aggregation, scan lease overlap/owner loss and failed cleanup remain covered.
Changes are limited to tests and removal of an unused `#[cfg(test)]` scan accessor.

## Validation

- 80 Rust tests and 4 Python tests pass; zero ignored tests.
- Formatting, Clippy (`--all-targets --locked -- -D warnings`), RQLens `measure all`,
  `verify`, configured `check` policies, and `scripts/quality_gate.py` pass.
- Line coverage: **51.78% → 51.37%** (unchanged gate: at least 50%).
- Function coverage: **48.79% → 46.69%**; region coverage: **51.14% → 49.68%**.
  These aggregates include test-code changes; coverage is not claimed unchanged.
- High-CRAP functions: **73 → 73**. Production reliability findings, escape hatches,
  architecture violations and cyclic modules remain zero.

Evidence and test inventories are in `target/test-reduction-review/`; current
RQLens artifacts are in `target/analysis/`. Hardware behavior was not exercised.
