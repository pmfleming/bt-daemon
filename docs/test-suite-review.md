# Test-suite reduction

## Current pass: 84 → 56 tests

Baseline: commit `54f7b5b`, after the previous reduction. Counts are runnable test
functions, not table rows or Python subtests.

| Suite | Baseline | Final |
| --- | ---: | ---: |
| Rust (all targets) | 80 | 54 |
| Python quality tooling | 4 | 2 |
| Total | 84 | 56 |

56 retains **66.7%** of the current baseline: **28 tests removed (33.3%)**.
No tests were ignored or disabled; production behavior and gate thresholds are
unchanged. The only non-test-body removal is an unused `#[cfg(test)]` operation
accessor.

### Selection and retained protection

- **API/BlueZ (18 → 12):** exercise contextual error classification with real BlueZ
  errors, and combine operation sequencing with failure/cancellation at each stage.
  Combine snapshot unknown/live/cached/connected states. Remove exhaustive label
  spelling and malformed-UUID helper cases; retain the Bluetooth-base versus vendor
  UUID distinction through assembled snapshots.
- **Daemon (17 → 10):** move watch deduplication into snapshot routing; combine valid
  and invalid command dispatch. Exercise scan eligibility and caller-scoped operation
  and scan cancellation through dispatch, including keeping the other request alive.
  Combine overlapping scan leases with owner loss, and successful operation events
  with completion/cancellation terminal-result recovery. Retain failed-stop retries.
  Remove private stream-flag assertions while retaining empty/unknown stream rejection.
- **Fast Pair (21 → 14):** consolidate command outcomes and stale-reservation cleanup,
  connection failure isolation, transport selection, suppression and writer teardown.
  Exercise fragmented/coalesced frames through bounded transport queues instead of a
  second decoder-only test. Keep backpressure, EOF, frame limits, failed writes and
  dropped receivers. Check provisioning prerequisites through advertised features.
  Independent crypto vectors, authenticated-frame integrity and secure-key reload stay.
- **Identity/management (8 → 5):** follow discovery through promotion, adapter rename,
  persistence and presentation recovery. Check ephemeral-address privacy on disk, not
  private registry serialization. Fold failed-save rollback into policy persistence,
  validation, reset and peer-isolation checks. Keep writer failure/retry and shutdown
  flush tests independent.
- **OBEX/pairing (10 → 7):** combine scoped authorization/active-transfer cancellation
  and file-path safety contracts. Consolidate accepted/rejected prompt responses and
  cancellation causes, retaining timeout, abandonment, owner loss, opaque identities,
  secret clearing, one terminal event and late-answer rejection.
- **Python (4 → 2):** share numerical-boundary/regression/unknown-evidence cases and
  invalid-input checks; retain corrupt/incomplete/missing artifacts, invalid bounds
  and unavailable coverage semantics.

The six audio, client, protocol, rfkill and device-gate tests are unchanged.
This is a mix of redundant-check removal and lifecycle consolidation, not a claim
that one third of behavioral scenarios were discarded. Related assertions remain
together; table cases are not counted as separate tests.

### Validation and tradeoffs

- All 54 Rust tests pass both in parallel and sequentially; both Python tests pass.
- Formatting, strict Clippy, RQLens `measure all`, `verify`, configured `check` policies
  and `scripts/quality_gate.py` pass without changing configuration.
- Line coverage: **51.37% → 50.91%** (gate remains 50%); function coverage:
  **46.69% → 45.84%**; region coverage: **49.69% → 49.08%**.
- Duplicated lines: **213 → 191**; high-CRAP functions remain **73**. Production
  reliability findings, escape hatches, architecture violations, cyclic modules and
  test-quality findings remain **zero**.
- Consolidation is not free: fewer independently reported failures, lower aggregate
  coverage, and the maximum function hotspot score rises **65.86 → 70.22**. These
  measurements include tests. Branch coverage remains unavailable; four existing
  repository-documentation warnings remain. Hardware integration was not exercised.

Inventories, baseline/final measurements and validation logs are under
`target/test-reduction-round2/`; RQLens artifacts are in `target/analysis/`.

## Previous pass: 125 → 84 tests

Baseline: commit `406bc04`. Count runnable test functions reported by Cargo and
Python unittest discovery, not individual table rows or subtests.

| Suite | Baseline | Final |
| --- | ---: | ---: |
| Rust (all targets) | 119 | 80 |
| Python quality tooling | 6 | 4 |
| Total | 125 | 84 |

`125 × 0.67 = 83.75`; 84 is the nearest whole-test target (67.2% retained,
32.8% removed). No tests were ignored or disabled, and no gates were relaxed.

### Selection

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

### Validation

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
