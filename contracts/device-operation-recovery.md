# Device-operation recovery checkpoint

`bluetooth.operation.status {request_id}` is read-only on both supported ingress
paths. It uses the authenticated transport caller, not a parameter-supplied owner.
Its `data.operation_status` result is deliberately distinct from the
`data.operation` admission receipt, so a read cannot register or rebind accepted
work in the retained client. Active/retained status, the operation section of `bluetooth.requests.snapshot`,
and `bluetooth.operation` records are caller-owned. Pairing and scan observations
are **not** made private or authorized by this operation ownership.

An operation's opaque UUID request ID is never reused. `revision` is an exact,
positive JavaScript-safe integer scoped to that ID: queued receipt, started,
accepted progress, and one terminal transition. Revisions are not transport
sequences; skipped progress is permissible. State, revision, retained terminal,
and event publication share one mutex. A status read racing completion therefore
sees active or retained terminal, never a removal/retention hole. Status, aggregate
operation records and events serialize the same `OperationEvent`; owner identity
is never serialized. Global broadcast lag is explicit loss, not a sequence after
owner filtering.

Up to 64 terminal results survive for five minutes (capacity eviction can be
earlier). Active work does not expire with terminal history. Missing, foreign,
evicted, expired and retired-owner IDs produce `request-not-found`, not evidence
that an effect succeeded, failed, or was never dispatched. There is no durable
journal and no effect replay. A fresh connection has no authority over an old
connection's operations.

Cancellation requests that the owned worker stop. An independent reaper retains
the active entry/admission until its future drops and terminal state is recorded,
even if the cancellation RPC waiter disappears. A repeated cancellation can
acknowledge the existing cancellation intent; it is not completion or rollback.
Owner loss uses cancellation, preserving another owner's work and device gates.
D-Bus operation clients must retain their connection through completion; losing
it cancels owned work, not already-dispatched effects. The retained D-Bus/stdio
bridge remains supported; the socket remains opt-in.

Offline lifecycle witnesses come from the same constructors in
`src/daemon/operation.rs`, through `debug contract-fixture` / `operation_ordering`.
Tests in `src/daemon/operation/tests.rs` and
`src/daemon/tests/socket/operation.rs` use fake pending/completing backends and
private no-activation buses only.

This is not pairing, discovery-lease or OBEX outcome recovery. Those coordinators
still need their own ordering, authority, expiry and quiet-loss contracts.

## Fresh fixture-parity checkpoint (20261026T202406Z)

The current family inherits this operation implementation at `a5869d8` from
original baseline `3e0ecb4`; inheritance is not integration approval. This
checkpoint adds a serializer/registry regression in `src/protocol.rs`, without
changing production policy. It checks status/event identity, exact positive
revisions, absent owner identities and the retained client's rule that a status
reply cannot register accepted work.

Shelllist's now-assigned global BT contract was regenerated from this producer,
matching `test_support/bt-api-v1.json` byte-for-byte (SHA-256
`402f15f77c8d0b7c81e70b1bff04cc1c87ecd1a875d148ba184debd7d5155d84`). Its
BT-only generator checks that input plus all frontend fixture/binding companions
using the unchanged shared generator. An actual Qt regression and synthetic
orchestration failure/drift tests accompany it.

Fresh private checks passed: 83 library + two process integration tests, strict
all-target/all-feature Clippy and formatting; actual Qt 472 passes / one existing
renderer skip; the explicit Quickshell operation probe (zero active / one retained
terminal); sibling-source Nix BT contract/package, Qt and build-input checks.
Release BT tests also pass 83 + two, with the two explicit-only probes excluded
normally. The lifecycle probe was selected explicitly, never the benchmark.
All heavy checks used the current run's `check.sh timeout 1800`, matching family
dev shells/capture and private HOME/XDG/unavailable host endpoints. No host
activation directories, hardware, live services, deployment or final performance
measurement were used. The native producer is in-process and fake-backed, not
an OS-process-crash qualification.

Full commands, source/binary hashes, the retained inherited-fixture negatives,
logs and remaining dependent-lane contracts are in the matching Shelllist
`tests/bluetooth-lifecycle-qualification.md` and this task's implementation report.
The required full-original-baseline Rust/QML quality continuation is still pending;
these functional passes do not supply the previously missing Rust extraction or
approve rollout. Pairing/scan/aggregate/OBEX and CQ-02/03 remain separate work.

## Full-baseline quality continuation (20261026T202406Z)

This section supersedes only the preceding checkpoint's pending-quality statement,
not its remaining lifecycle/qualification limits. The exact session resumed at the
clean implementation tuple BT `a7d275a` / Shelllist `208789b`. Review covers all
**13 BT + 15 Shelllist original-baseline paths**, including inherited seeds, tests,
generators and generated companions. No implementation history is rewritten.

The previously missing Rust extraction now ran. Pinned RQLens source `d28f3d9`
and CLI SHA-256 `622b2c793a574781cd40a190a457f58af4b8c222ee5efce04c4e77488c50929a`
remain unchanged. Its normal analyzer-managed `cargo build --locked` provisioned
`rust_facts` and `ast_hasher` from that pinned source's `rust_helpers/Cargo.toml`
into this task's private XDG cache. Helper digest is `6a098384d350404d`. This is
explicitly authorized helper provisioning, not rebuilding/replacing the main CLI.

Static producers: hotspots, clones, reliability, escape-hatches, type-health,
test-quality, architecture-rules and map. BEFORE/AFTER use identical task configs,
source scope and tools, with no compiler expansion, coverage, audit, runtime or
ignored-test campaign. Raw artifacts, identities, commands and comparison are in
this task's `quality/{before,after}`, `quality/TRIAGE.md` and
`quality/comparison.json`.

**Evidence remains partial, not a universal quality pass:**

- 78 Cargo-identified Rust files parse without fallback/read errors; 991 hotspot
  records, 45 clone groups and 450 reliability observations are retained.
- Operation coordinator complexity is bounded (`start_owned` 6/4, cancellation
  3/2 cyclomatic/cognitive). Its checked-owned-task `expect` is under one exclusive
  mutex with no intervening await/mutation. Three other changed-file warnings are
  existing offline serializer/object-fixture invariants; test unwraps stay visible.
- Type-health observes 186 rows, 130 resolved/four unresolved explicit impl joins
  and 1,854 cfg/attribute/macro limitation reasons. Unknown compiled totals stay
  null; seven unexpanded item macros and 861 unresolved semantic references remain.
- The configured architecture rule reports one error-level `protocol` → `daemon`
  observation. This offline canonical-fixture edge already existed at baseline
  (`3e0ecb4:src/protocol.rs:364`). It is retained, not suppressed or declared green.
  Coordinator decision is needed on the offline-fixture contract boundary or a
  separately scoped relocation; serializer duplication is not a fix.

**No additional code refactor is justified** by this bounded review. The existing
atomic claim/retention/publication and independent cancellation reaper are clearer
than new helper layers. Replacing a proven invariant with silent failure, changing
owner authorization, merging operation/OBEX cleanup, splitting the private journey
for metrics or deleting distinct tests would weaken clarity/protection. This
separate quality commit records actual evidence and rationale rather than code
churn. All non-document implementation bytes are preserved; comparable AFTER
retains observations rather than claiming a score reduction. A local comparison
initially rejected volatile per-record measurement timestamps after all functional
checks passed; raw artifacts/failure are retained and only timestamps/provenance
are normalized for comparison, never findings, metrics or policy.

Fresh affected producer/Qt/native/contracts/strict/language checks and sibling Nix
gates are rerun by the task-owned bounded verification driver before the quality
commit; final AFTER is repeated at clean quality heads. Exact outcomes, executable
hashes and retained limitations are sealed in the combined task/quality report.
Only this evidence document and the BT-specific frontend qualification document
change in quality. Pairing/scan/aggregate/OBEX, CQ-02/03, guarded-unknown retirement,
stalled reads, process/payload/load/startup/upgrade/release measurements and rollout
remain separate. Bridge default, retained D-Bus/client support, private authority,
admission, no replay and all editing/draft guards are unchanged.

## Consumer-authority repair quality continuation (20261026T232107Z)

This is a new exact-session checkpoint, not approval/reapplication of the earlier
seeds. Implementation BT `77f9027` / Shelllist `7e032ea` repairs aggregate local
consumer authority and same-device prompt/draft independence in QML, extending the
existing two-consumer native operation probe. Producer runtime, protocol/schema,
fixture serializers and checked fixture bytes stay unchanged from seeded HEAD.

Fresh review inventories the **entire original-baseline diff**: 13 producer and 16
frontend paths, including seeded runtime/tests, both shell generator/test files,
all JSON/generated companions and historical evidence. Pinned RQLens source
`d28f3d9`, CLI SHA-256
`622b2c793a574781cd40a190a457f58af4b8c222ee5efce04c4e77488c50929a`, and pinned
QML Lens `97feb1f` are unchanged. Analyzer-managed `cargo build --locked` provisioned
`rust_facts`/`ast_hasher` from that exact pinned `rust_helpers/Cargo.toml` into this
task's private cache; no main CLI rebuild/install or tool source/lock edit occurred.

Eight named Rust static producers and five QML producers run with identical owned
BEFORE/AFTER configs/scope; 18 normalized findings/metrics artifacts are unchanged.
Raw source/config/tool/helper fingerprints and timestamps are retained separately.
No compiler macro expansion, verify/audit/coverage/runtime or benchmark campaign
ran. Project adapters were inspected before using static-only configs.

Evidence remains **partial**: 78 authored Rust files parse with no fallback/read
errors, but seven item macros remain unexpanded. Auto semantic identity now runs
bounded offline rust-analyzer with build scripts/proc macros disabled: 838 of 861
collected references resolve; **23 unindexed project definitions remain unknown**.
This is not compiled graph completeness. Type-health has 186 rows, 130 resolved /
four unresolved explicit impl joins and null unknown compiled totals. All 991
hotspot, 45 clone and 450 reliability observations remain available. Four production
warning-level expects in baseline-changed paths are a same-mutex checked-owned-task
invariant and three existing offline serializer/object invariants; test unwraps
remain visible. Test-quality's 87 observed authored tests/two ignored fixtures are
syntax evidence, not per-test execution or sensitivity certification.

The error-level `data-contracts-independent-of-hardware` protocol→daemon edge
remains visible under the coordinator's **retained bounded offline-fixture debt**
disposition, not a rule pass. Its exact fixture constructors/rule are unchanged;
no policy tuning, relocation or serializer fork occurred. Map also discloses
unproduced correctness/locality/leverage evidence. QML remains partial with 27
unresolved imports, 78 unknown types and 548 skipped evaluations.

No additional code refactor is justified by this bounded review. The atomic
admission/claim/retention/publication and independent cancellation reaper stay
explicit; neither weakening invariants nor splitting the cohesive native phase
journey for heuristic scores improves the ownership proof. This distinct quality
commit records evidence/rationale only; implementation remains its ancestor.

Fresh functional reruns pass producer fmt, 83 library + two private process tests,
strict all-target/all-feature Clippy, matching build/offline fixture, generated and
language checks, QML lint, 31 focused Qt cases/hooks and two private native operation
runs (zero active/one retained terminal). Matching Nix BT contract/buildInputs pass.
**Full Qt/Nix Qt remain red:** 499 passed, one obsolete same-device-prompt expectation
in out-of-allowlist `tests/qml/tst_bluetooth_stream_recovery.qml`, one existing
renderer skip. Coordinator scope request is preserved, not silently fixed/skipped.
Overall and quality evidence remain partial. Full commands/hashes, raw BEFORE/AFTER,
final clean-head extraction and retained limitations are task `quality/` artifacts;
frontend `tests/bluetooth-consumer-authority.md` is the durable matching record.
No integration, lifecycle expansion, performance, startup or rollout is approved.
