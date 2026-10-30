# BT quality campaign — 2026-10-30

Baseline: clean `8fe2f64d8528b66289548af0c1ea5238d9247195`. Only this repository
is changed; public APIs, dependencies, protocol fixture and quality policy remain
unchanged. Raw reports/logs are ignored under `target/quality-campaign/`.

## Evaluator and reproducibility

Commands use the existing Nix environment:
`source .direnv/flake-profile-*.rc`, with `CARGO_BUILD_JOBS=2`.
Rust/Cargo are 1.95.0; Python is the Nix-provided 3.13.13.

The current RQLens source (`dfe016202134f1ea5fc5fd0e0c5da73c4b2b32b0`)
was built privately with `cargo build --locked --manifest-path
../rust-quality-lens/Cargo.toml --target-dir "$PWD/target/campaign-lens"`.
Its executable SHA-256 is
`91e241668805513445e72d2a01aebf0f03fbd8ab7e9b90944a0129d737d3a27c`.
It cannot extract this project's facts: 1,446 dependency targets produce a
149,329-byte `RQLENS_CRATE_ROOTS`, exceeding Linux's per-string exec limit.
The retained executable `target/next-five/rqlens` works, SHA-256
`423a8b2f8222b9840871efdabc82f29e700a949ba5d83cfd7f8c3c27386f0158`.
Its original source revision is not established; the exact binary is retained.
Both comparison runs use it and the same current helper sources, worker-private
`XDG_CACHE_HOME=$PWD/target/quality-campaign/cache`, unchanged `rqlens.toml`
(SHA-256 `aac4a80902e3222faeadc1a7350fddf34f6eabf03d4dbe06150e3bbf7e762512`),
and unchanged source scope `src`.

Live baseline publication was correctly rejected when the sibling framework
changed during measurement. Never use the old `target/analysis` artifacts as
fresh evidence after a failed run. Equivalent comparisons instead use tracked
BT source snapshots in `target/quality-campaign/frozen/bt-daemon`, with one
fixed sibling-source copy in `frozen/daemon-framework`. This is analysis data,
not an implementation checkout. Framework capture HEAD was `98349a6`, including
the then-current dirty decimal changes; its 101-file tree digest is
`de243e8bdbeb7843c5257e27661089dca865797ca9cc17b107a02b63919a7e75`.
Baseline BT's 138-file digest is
`5d37c690e9c77b2edc844fe6eba466f63be3556c7a8aa8f1e80c3f967c6208bf`.
Digests hash JSON-encoded sorted `(relative path, SHA-256 contents)` pairs.
No sibling sources, settings or pins are modified.

From the snapshot, run the retained CLI with `measure all --config rqlens.toml`;
unset `CARGO_TARGET_DIR` and set
`GIT_CEILING_DIRECTORIES=/home/laufan/Projects/bt-daemon/target/quality-campaign/frozen`
to avoid inheriting the live checkout's changing Git identity. This removes Git
history from both comparison snapshots; history-derived risk is not comparable
to the live project. Macro/semantic incompleteness is reported, not waived.
The frozen framework is held constant for the final snapshot. Native final
checks separately use the live sibling framework. Cargo's target root stays
inside the snapshot, below its source root; only its `debug` and
`llvm-cov-target` cache subdirectories link to the existing ignored caches.
An earlier provisional pair incorrectly set the target directory to an ancestor
of the source snapshot, causing freshness fingerprints to omit BT/framework
inputs. Those reports are superseded, not accepted as final evidence. The
corrected pair includes 349 inputs and checks distinct baseline/final source
digests; no metric/config exclusion is changed.

## Baseline checks

- `cargo test --locked --all-features`: 83 library + two process integration
  tests passed; two explicit-only probes ignored; zero doctests.
- `cargo clippy --locked --all-targets --all-features -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `python3 -m unittest discover -s scripts -p test_quality_gate.py`: passed.
- Tracked Rust: 21,773 physical lines, 78 files, 319 explicit clone calls,
  13 `.cloned()` calls. Baseline/final evaluator results are recorded below when
  the frozen runs finish; an invalidated live run is not a passing measurement.

## Initial refactor

Operation events mutate their scalar transition fields in place; status reads
serialize borrowed active/retained events. Cancellation uses the same event
ownership predicate as publication, removing the checked-then-`expect` lookup.
Publication clones required for independent consumers remain. Terminal retention,
exact revisions, ownership, cancellation reaping and wire constructors are kept.
The existing operation lifecycle, foreign-owner, abandoned-waiter, status race,
revision-exhaustion and protocol-fixture tests own the behavioral evidence.

Validation: `cargo test --locked --all-features` passed all 85 runnable tests
(two probes ignored), strict all-target/all-feature Clippy passed, and formatting
and `git diff --check` passed. Stage logs: `stage0-{tests,clippy}.log`.

## Next five, ranked after the initial refactor

1. **Framing state machine:** the fresh static extraction identifies `push` as
   the largest production cognitive hotspot (11). Express the existing states
   directly; retain both protocol decoders and the independent boundary matrix.
2. **Scan lease selection and failure evidence:** selection currently evaluates
   the same ownership predicate twice and scans tasks twice. Partition leased
   adapters in one pass; strengthen the existing failed-stop owner with timeout
   retries and single-terminal publication. Preserve overlapping leases.
3. **Authenticated command reservations:** `fetch_add` can wrap the generation
   token used by cancellation-safe Drop. Fail closed on exhaustion and use map
   entries to avoid repeated lookup; retain timeout/write/ACK/cancellation rules.
4. **Remote presentation text:** disconnect diagnostics and Audio Switch repeat
   the same Unicode/control/bidi filter. Give that policy one model owner, retain
   their distinct 256/128-character limits and invalid-UTF-8 behavior, and exercise
   both real decoding paths. Do not apply it to filenames or trusted identities.
5. **Credential persistence:** repository-wide caller inspection finds no caller
   of `AccountKeyStore::load` with `None`. Remove the private no-path mode and
   its write bypass, retaining public interfaces, atomic private writes and
   transactional insert/replace/remove behavior. Extend the existing persistence
   owner with malformed/unsupported stored credentials.

These outrank cosmetic module movement, unrelated match-shaped clone reports and
additional forwarding wrappers. Safety branches are retained even when their
cyclomatic entry counts rise. No assertion or policy exclusion is removed.

## Improvement 1 — framing

`DelimitedFrames::push` now matches the closed/end/full/body states explicitly.
End delimiters are still processed before the size bound; shared HDLC idle flags,
Sony empty-frame rejection and the exact encoded-length limit are unchanged.
The existing matrix covers 2 delimiter styles × 4 boundary lengths × 3 chunk
sizes = 24 framing cases, each with recovery and a bounded-buffer assertion.
Independent Sony/Maestro wire vectors and transport tests remain.
`cargo test --locked --lib`: 83 passed, two probes ignored (`stage1-tests.log`).
Formatting and `git diff --check` passed.

## Improvement 2 — scan leases

`select_scans` evaluates ownership/ID selection once per task and subtracts
retained adapter leases after collecting borrowed keys. The deterministic sorted
stop order is unchanged. The existing failure test now uses paused time and
covers explicit failed stop, timeout failure, another cleanup attempt two seconds
later, continued active-state retention and no duplicate terminal event.
The independent shared-adapter/owner-loss matrix remains unchanged.
`cargo test --locked --lib`: 83 passed, two probes ignored (`stage2-tests.log`).
Formatting and diff checks passed. Additional failure evidence increases test LOC;
this is not a claim of lower test volume or a measured throughput improvement.

## Improvement 3 — authenticated command reservations

Map entry ownership now performs admission/removal without a second lookup.
Generation allocation uses checked atomic advancement: exhaustion is a terminal
admission error, never wraparound into an earlier guard's identity. The existing
paused-time lifecycle test retains stalled writer/ACK deadlines, cancellation,
negative ACK, transport failure, disconnect and stale-guard cases, and now checks
last-token admission, exhaustion, unchanged maximum counter and no orphan slot.
`cargo test --locked --lib`: 83 passed, two probes ignored (`stage3-tests.log`).
Formatting and diff checks passed. The new exhaustion guard deliberately adds
code; no allocation benchmark or normal-runtime performance gain is claimed.

## Improvement 4 — remote display text

`model::remote_text` owns the existing control/bidi stripping and Unicode-character
bound; both callers retain their own limits. The Audio Switch owner now compares
exact outputs for sanitized names, long ASCII, multibyte text and control-only
names, retaining malformed UTF-8 rejection. The real private-bus disconnect
listener checks control/bidi stripping and a 256-character multibyte bound,
retaining sender identity, malformed signals and owner-replacement regressions.
`cargo test --locked --lib`: 83 passed, two probes ignored (`stage4-tests.log`).
Formatting and diff checks passed. No remote identifier/filename rules changed.

## Improvement 5 — mandatory credential persistence

The private store now always owns a path; there is no unreachable `None` branch
that can skip a credential write. Existing transactional persistence still tests
failed insert, replacement and removal, retry, reload and mode 0600. Its existing
owner adds five bad-file cases (syntax, version, hex, width and type), verifies
that failed reloads leave the live key untouched, then restores and reloads valid
credentials. Public interfaces and file encoding remain unchanged.
`cargo test --locked --all-features`: 83 library + two integration tests passed,
two explicit probes ignored, zero doctests (`stage5-tests.log`). Strict Clippy,
formatting, diff checks and both Python tests passed against the live framework.

## Final measurements and tradeoffs

Both frozen `measure all` runs published successfully. Reports are under
`baseline/analysis-corrected` and `final/analysis-corrected` in the ignored campaign directory.
The same executable/config/helper/scope are confirmed by helper digest
`86462b346ebcd322` and config digest `216c43f77027d2d1` in both fingerprints.
The tracked JSON companion records exact totals and confidence flags. Source
digests differ (`6b993c60e5ec0ed0` → `b1185c76abfbd3b8`), with 349 fingerprinted
inputs in each run. Aggregation command: `python3
target/quality-campaign/summarize.py <artifacts> <source-snapshot> <metrics.json>`;
the retained script SHA-256 is
`3efd1dfd3519fe1b70dcc4b2729c774b5dfb5275f240f17025ce90f5c80052f8`.
Baseline source is restored from the baseline commit before aggregation; final
source is the implementation copied into the same snapshot location.

| Measurement | Baseline | Final |
| --- | ---: | ---: |
| Production physical Rust LOC | 15,226 | 15,214 |
| Test/fixture physical Rust LOC | 6,547 | 6,605 |
| Total physical Rust LOC | 21,773 | 21,819 |
| Production function SLOC | 11,380 | 11,365 |
| Test/fixture function SLOC | 5,376 | 5,434 |
| Production cognitive complexity | 1,026 | 1,024 |
| Largest production cognitive complexity | 11 | 9 |
| Production cyclomatic complexity | 2,520 | 2,522 |
| Production Halstead effort | 7,262,296 | 7,252,992 |
| Duplicated lines | 555 | 553 |
| Mean locality risk | 1.5888 | 1.5888 |
| Mean observed leverage / pressure | 24.4737 / 33.7697 | 24.4737 / 33.7697 |
| Explicit clone / cloned call sites | 319 / 13 | 321 / 10 |
| Production reliability findings | 4 | 3 |
| Escape hatches / architecture violations | 6 / 1 | 6 / 1 |
| High-CRAP functions | 81 | 81 |
| LLVM line coverage | 8,102 / 13,526 (59.90%) | 8,125 / 13,532 (60.04%) |
| LLVM function coverage | 865 / 1,593 (54.30%) | 865 / 1,591 (54.37%) |
| LLVM region coverage | 12,306 / 20,731 (59.36%) | 12,376 / 20,773 (59.58%) |
| Runnable Rust / Python tests passed | 85 / 2 | 85 / 2 |

Static metrics are authored-syntax observations, **not complete compiler facts**.
Function sums use `src`; physical LOC includes `src` and `test_support`. Test LOC
includes test-gated files/modules and the four individually test-gated helpers;
it does not relabel runtime offline-fixture constructors as excluded tests.
No generated/excluded code movement changes those denominators. New evidence
files are additional maintenance cost and are outside Rust LOC.

There is no overall LOC/test-volume reduction: 12 fewer production lines are
outweighed by 58 added regression lines. Test count stays fixed by expanding the
existing owners, not deleting assertions. Cyclomatic and test-inclusive effort
increase; locality/leverage and escape counts do not improve. The framing match
reduces cognitive complexity 11→9 but raises cyclomatic complexity 7→10 and
Halstead effort 35,652→43,156. Scan selection reduces effort 38,308→26,577 and
one task traversal, but its explicit partition raises cognitive/cyclomatic 2/3→5/4.
These are deliberate representation/behavior tradeoffs, not universal gains.
Clone-site totals include added test setup and do not measure allocations.
Halstead effort is a static proxy, not elapsed engineering effort.

Coverage percentages include inline test code. Covered function count is unchanged;
its percentage rises because the denominator shrinks. Production `AccountKeyStore::load`
has observed line evidence 19/23→20/20; `persist` 9/10→7/7 (including removal of
the unused no-path branch). Frame parsing, scan selection, command reservation
and the shared text filter have all observed lines hit. These are not branch
coverage or physical Bluetooth/PipeWire qualification. Exact mapped line counts
are in the JSON companion.

## Gates and remaining limitations

- Final native commands: `cargo test --locked --all-features`,
  `cargo clippy --locked --all-targets --all-features -- -D warnings`,
  `cargo fmt --all -- --check`, `git diff --check`, and
  `python3 -m unittest discover -s scripts -p test_quality_gate.py`: passed.
  Native final tests used live framework HEAD `5e07711`; frozen comparison uses
  the constant capture described above. Sibling edits are never staged here.
- `rqlens verify --config rqlens.toml`: official formatting, compilation, Clippy,
  tests, doctests and rustdoc passed, zero error-level practice failures. Four
  existing documentation warnings remain; 11 optional practices are skipped,
  including mutation, Miri, semver and RustSec (audit is run separately below).
- The corrected final strict `rqlens check --config rqlens.toml --fail-on partial
  --fail-on test-failure --fail-on practice-failure --fail-on reliability-finding`
  exits 1. Both `python3 scripts/quality_gate.py --artifacts <report-directory>`
  runs exit 1 on incomplete API-health evidence. No thresholds/exclusions changed.
  Macro expansion and 23 unresolved semantic references remain incomplete.
  Reported duplication 553>312, high-CRAP 81>73, escape count 6>0, reliability
  findings 3>0, architecture violations 1>0 and the two ignored-probe test-quality
  findings also exceed configured ceilings; partial evidence fails first.
- Three production-classified `expect` findings remain in offline snapshot
  fixture construction in `daemon/subscription.rs`. The existing protocol→daemon
  fixture dependency violates the configured hardware-independent-data rule.
  Resolving it needs a separate fixture/API ownership design; duplicating its
  constructors or moving them out of measured scope would weaken the evidence.
  The six escape observations are four existing test glob imports plus the two
  documented HCI layout/unsafe-bind boundaries.
- `cargo audit`: **failed**, `crypto-bigint 0.5.5`, `RUSTSEC-2026-0387`, plus four
  warnings (three unmaintained D-Bus support crates and Tokio I/O-safety warning).
  `Cargo.lock` is unchanged (SHA-256
  `50cfd2b36edf52b68485bf5ab71f220dcf7ea5b48d37f57bf142483d372f6996`).
  Dependency upgrades are outside the authorized version-preserving scope.
- No hardware acceptance, ignored Qt probes, mutation campaign, benchmark,
  deployment or Nix package/check derivation is claimed. All builds/checks use
  the cached Nix development environment; no live settings/services are changed.

The explicit coverage threshold check `cargo llvm-cov --locked --all-features
--fail-under-lines 50 --summary-only` passed. The corrected snapshot's fresh
coverage data also passed `cargo llvm-cov report --summary-only
--fail-under-lines 50` (60.04%). No skipped optional practice counts as passing.
