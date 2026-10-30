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
set `CARGO_TARGET_DIR=/home/laufan/Projects/bt-daemon/target` and
`GIT_CEILING_DIRECTORIES=/home/laufan/Projects/bt-daemon/target/quality-campaign/frozen`
to avoid inheriting the live checkout's changing Git identity. This removes Git
history from both comparison snapshots; history-derived risk is not comparable
to the live project. Macro/semantic incompleteness is reported, not waived.
The frozen framework is held constant for the final snapshot. Native final
checks separately use the live sibling framework.

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
