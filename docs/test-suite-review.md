# Test-suite reduction

## Current pass: 98 → 66 tests

Baseline is the working tree after the [ownership/locality refactor](reviews/rqlens-ownership-refactor.md),
not the older counts below. Cargo discovery confirmed 95 unit tests and one
integration test; Python discovery confirmed two tests.

| Suite | Baseline | Final |
| --- | ---: | ---: |
| Rust unit tests | 95 | 63 |
| Rust integration tests | 1 | 1 |
| Python quality tooling | 2 | 2 |
| **Total** | **98** | **66** |

`98 × 0.67 = 65.66`, rounded to **66**: 32 fewer tests, **32.65% removed**.
Considering Rust alone, **96 → 64** removes **33.33%** and also gives the nearest
whole-test target to 67%. Counts are runnable test functions, not table rows.
No tests were ignored, feature-gated away or removed from discovery configuration.

### Selection and retained protection

- **Identity:** delete legacy migration/serialization-only tests and assertions
  about shared battery allocation addresses. Retain discovery-address privacy,
  identity promotion, adapter renaming, presentation recovery and current-format
  persistence. The writer now has one lifecycle test covering concurrent flushes,
  failed saves, recovery and shutdown draining instead of separate fixtures.
- **API and recovery:** remove duplicate error-envelope and routing-key helper
  tests. Exercise contextual error codes/retryability through BlueZ-to-API error
  conversion and validate routed policy updates with the real management store.
  The private-bus integration test retains both initially absent and already
  running BlueZ, failed agent registration, owner replacement, policy persistence
  and existing-client recovery; remove the overlapping startup/unavailable unit
  tests, but retain cancellation of in-flight stale recovery candidates.
- **Daemon:** fold read-only method routing into command dispatch, and recover
  operation terminal results through the public requests route. Drop private
  watch-deduplication/burst-drain assertions and repeated empty-field checks.
  Share scan-lease setup for timeout and owner-loss cases, preserving other owners'
  adapters, terminal events, final-stop deduplication and failed-stop retryability.
  Preserve caller-scoped cancellation tests separately.
- **Adapter contracts:** check the exact batch fixture in the existing failure
  matrix rather than dispatching the same failing batch in a standalone test.
  Keep full validation-before-effects and submitted-worker cancellation/serialization
  tests. Move the device/adapter gate namespace check there and remove the duplicate
  mutation-gate mechanics test. The fixture regeneration command is updated in
  [adapter batches](adapter-batches.md).
- **Audio:** remove duplicate missing-parameter, exact-error-wording and selector
  fixtures. Keep invalid `remember` requests at daemon dispatch; test emitted opaque
  keys against selectors in the snapshot test. Combine apply/persistence/refresh
  failures into one outcome test driven by actual helper results. Retain typed POD
  parsing, profile availability/identity checks and default-route projection.
- **Connection policy and Fast Pair:** replace the private policy-flag truth table with observed ordered
  effects, including power overrides, preferred profiles and failure/cancellation
  at each stage. Consolidate retry classification, bounds, earliest deadlines and
  session-scoped suppression; consolidate command deadlines with reservation
  lifecycle outcomes. Remove repeated fixed-length decoder matrices, keeping
  representative truncation/trailing-byte and invalid-mode cases. Delete the
  display-only Audio Switch label test. Independent crypto/MAC vectors, malformed
  transport frames, PSM validation and trusted provisioning prerequisites remain.
  Key-store rollback/reload now also checks generated keys and secure permissions.
- **OBEX and pairing:** test destination cleanup alongside confined, non-overwriting
  file reservation; test abandoned prompts alongside scoped authorization/transfer
  cancellation. Remove the PIN channel-plumbing test, preserving PIN length/Unicode
  validation in the existing response test. Keep secret clearing, display progress,
  timeouts, owner loss, late-answer rejection and exactly one terminal event.
- **RSSI:** consolidate stalled enumeration with bounded multi-batch polling, pure
  admission limits with stalled-request cleanup, and send/status cases with the
  existing decoder/transport tests. Remove private container-size assertions and
  repeated status mappings. Preserve first-read invalidation, suspend notification,
  freshness/backoff, address-type fallback, signed/unknown values, zero-byte sends,
  permissions and real private-bus Polkit caller-identity checks.

This is selective removal plus consolidation, not a claim that one third of
behavioral scenarios disappeared. Compared with the starting working tree,
`src/` loses **472 lines** and **74 assertion sites**. Production source text is
unchanged apart from blank lines; the integration fixture and Python tests are
unchanged. Shared setup and retained assertions execute directly in the remaining
tests, rather than hiding removed tests behind ignored attributes or a runner.

### Validation and trade-offs

- All **64 Rust tests** pass in parallel and sequentially; both Python tests pass.
- Formatting, diff whitespace checks, strict Clippy (`--all-targets --all-features
  --locked -- -D warnings`), RQLens `measure all` and `verify` pass.
- RQLens line coverage: **60.66% → 58.39%**; function coverage:
  **54.65% → 51.98%**; region coverage: **59.65% → 57.21%**. Aggregate coverage
  includes changes to inline tests; it is not claimed unchanged. Branch coverage
  is unavailable. No coverage threshold was changed.
- Legacy migration/layout and Audio Switch display-label parsing lose their direct
  tests intentionally. Recovery still executes in the retained daemon-subprocess
  integration test; the coverage artifact no longer attributes hits to
  `src/bluez/recovery.rs` after removing its direct unit test. Passing integration
  behavior is not a claim of complete subprocess coverage accounting.
- Consolidation reduces independent failure isolation and lengthens some tests.
  Existing incomplete macro/type/dependency evidence still prevents strict quality
  policy acceptance; four repository-documentation warnings remain. Hardware was
  not exercised and no service was restarted.

Inventories, the starting source snapshot, source-only audit, logs and RQLens
artifacts are under `target/test-reduction-current/`. Baseline artifacts match the
previously measured 96-test source. Final scans use a frozen dependency checkout
at `aa831877d49247c88fba6ead149d5aacea713b27`, matching that baseline rather than the
concurrently edited live sibling. `audit.json` records the before/after test names
and verifies unchanged non-test source text.

## Previous pass: 84 → 56 tests

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
