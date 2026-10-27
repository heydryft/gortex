# Edit-loop timing observations

`mutation_status` adds `phases_ms`, `phase_states`, and `phase_timing` beside
the existing `publication_phases` timeline. The timeline keeps its existing
schema, origin and timestamp meanings. The new projection is read from the
same immutable snapshot, with no new graph/store queries or edit-path I/O.

This is partial instrumentation. Presence of every key does **not** mean every
lap has been measured, or that the phase-sum acceptance criterion is met.

## Duration evidence

`phases_ms` always lists these keys for a found mutation receipt:

- Acknowledgement: `dispatcher_queue`, `middleware`, `parse`, `disk_write`,
  `publish`, and the explicit additional `reindex_wait`.
- Patch: `debounce`, `lane_wait`, `gate_wait`, `patch_parse`, `apply`,
  `search_lap`, `resolve`, `mtimes`, and `derived`.

A duration is a monotonic millisecond number, or `null`. `phase_states` gives
the evidence for each key:

| State | Meaning |
| --- | --- |
| `measured` | Both boundaries were observed in order; zero is valid. |
| `pending` | The start was observed, but the end has not been observed and the receipt is not terminal. |
| `not_observed` | Boundaries are absent, including on paths without a publication timeline. |
| `not_applicable` | A graph-refresh/background record has no source-mutation acknowledgement. |
| `invalid_order` | Observed offsets contradict the interval; the duration stays null. |

The acknowledgement projection measures `disk_write` (`disk_write_started` to
`receipt_committed`) and `publish` (`receipt_committed` to `ticket_enqueued`)
for MCP-edit timelines. The latter includes post-commit capture and ticket
admission. It does not include waiting for the admitted publication to finish.
The projection uses nanosecond offsets before converting to milliseconds,
rather than subtracting rounded timeline values or parsing wall-clock strings.

Canonical single-file commits retain their own disk-start boundary and timing
record through the existing bounded commit ledger. Admission binds the original
watcher sequence before debounce can wake; only the existing ticket-result
consumer records completion. A newer coalesced application can serve that
original ticket, but does not become its ticket identity, a checkout generation,
or invented per-receipt patch laps. Native events have no MCP acknowledgement.

Immediate-success edits keep the existing empty public freshness-receipt field;
their commit receipt can still render its retained timing. Different files or
versions within one request own distinct disk intervals even though they share
the middleware origin. Their receipt walls overlap and must not be added as RPC
time. Timing pointers are cleared on ledger expiry/cap eviction even when source
evidence keeps a commit object alive; a late callback cannot resurrect them.
Rebinding a commit to a replacement freshness receipt does not borrow that
replacement's timing or retarget the original record. Atomic batch/journal
timing and response/transport timing remain separate pending work.

Canonical point execution additionally collects `cycle_started`,
`patch_execution_started`, and `patch_execution_finished` on a local attempt.
Only the exact successful winner at the existing guarded completion can absorb
those stamps, outside the watcher lock and before result delivery. Older
coalesced receipts receive completion without the winner's execution spans.
Retry, superseded and stopped attempts discard their collectors; native or
uninstrumented paths allocate no collector. Outstanding waiter ownership is
released by the existing completion/stop paths rather than a tracing registry.

The execution pair surrounds the common point executor, including its actual
standalone or MultiWatcher parse/apply/resolve/derived implementation. It excludes
before-image inspection and post-executor event work. `cycle_started` names the
accepted runnable attempt, which may follow retries. These are outer execution
events, not a pure debounce/lane/gate measurement, a full patch wall, or a split
of nested deep phases. No deep checkout-builder phase is relabelled onto the
contextless canonical pipeline, and internal storm/atomic batches remain outside
this observation. Debounce, repository-lane wait and the full patch wall remain unobserved;
no phase-sum or CPU attribution acceptance follows from these events.

The same optional attempt-local collector now follows the actual standalone or
MultiWatcher point executor to its precise derived-tail call. The raw pair
`patch_derived_started` to `patch_derived_finished` becomes `phases_ms.derived`
only with observed admission and enclosing executor boundaries in monotonic
order. Resolver, reference-fact, semantic and metadata work before that call
is excluded. A branch that bypasses the derived call has no pair and remains
null/`not_observed`; an actually reached zero-duration call is measured zero.
The existing exact-winner absorption remains the only publication point, so
failed/retried/stopped/coalesced sibling attempts cannot publish these spans.
There is no ambient Indexer tracing state or additional history.

## Canonical cumulative patch laps

A bound canonical attempt also collects a fixed set of scalar totals and
observation counts, without retaining per-file spans or a tracing registry.
Repeated chunks and same/cross-repository resolver calls contribute their actual
elapsed intervals. First-to-last timestamp envelopes are not used as a sum.

| Lap | Actual observed boundary |
| --- | --- |
| `gate_wait` | The call to `reach.BeginTopologyMutation`, until acquisition returns. |
| `patch_parse` | Each chunk's preparation: source read, parse admission, extraction and fingerprint preparation; excludes prior-graph loading. |
| `apply` | Deletion and staged graph application, explicitly split before/after the search refresh block. |
| `search_lap` | The explicit search/FTS refresh block; structural FTS deletion stays with graph replacement in `apply`. |
| `resolve` | Actual deferred same-repository/name and cross-repository resolver calls; intervening dataflow, reference-fact and semantic work is excluded. |
| `mtimes` | Source-version receipt validation/mtime persistence, deletion-mtime pruning and MultiIndexer metadata publication. |
| `derived` | The precise derived-tail call above. |

The collector rejects overlap, inverted clocks, incomplete begin/end counts,
and intervals outside the original admission/executor/terminal window. Its
scalar derived interval must match the existing raw derived pair, so they cannot
be counted twice or conceal an overlapping subtotal. The legacy
`publication_phases` JSON fields stay unchanged; its first-wins timeline alone
cannot reconstruct a repeated lap. `phases_ms` is projected from the same
immutable in-process receipt snapshot with its accepted scalar evidence.

A reached zero-duration operation is measured zero. A bypassed or unobserved
operation remains null. The legacy combined parse/apply fallback makes the full
`patch_parse`, `apply` and `search_lap` totals unavailable for that attempt,
including any earlier measured chunks; it is not assigned a fabricated split.
Other independently proven laps remain available. Uninstrumented transitions,
contract work, semantic/dataflow work and notification costs stay in the
publication residual. This does not establish complete 5% sum coverage.

For observed canonical attempts, the existing `graph patch complete` line adds
`attempt_<lap>_ms`, `attempt_<lap>_state`, `attempt_<lap>_count` and the original
`attempt_ticket`. Its `patch_lap_scope` is
`physical_attempt_before_receipt_validation`: the log happens before final
watcher winner validation and does not claim that a superseded attempt's timing
will be published into any receipt. The encoder formats an immutable copy and
never samples clocks or a live collector. Native/unobserved patches keep their
original log fields, with no new timing snapshot, phase clock or encoder.
No extra patch line is emitted. Total instrumentation overhead remains
unmeasured and needs the prescribed no-regression runs.

## Separate lifetimes

`phase_timing.clock` is `monotonic` when a timeline exists, otherwise
`unavailable`; `coverage` is currently `partial`.

- `ack_wall_ms` is null until an actual response boundary is recorded. The
  current publication origin is entry into MCP middleware, **after** daemon
  dispatcher admission. It cannot account for dispatcher queueing or client
  transport time.
- `patch_wall_ms` is null until the physical patch boundaries are recorded.
  One coalesced patch may serve multiple receipts.
- `publication_wall_ms`, when terminal boundaries exist, measures
  `received` to `ticket_completed` or `ticket_failed`. It is a receipt interval,
  not the acknowledgement time or the physical patch wall.
- `publication_unattributed_ms` is that publication wall minus the measured,
  disjoint acknowledgement and observed patch-lap intervals. Patch intervals
  must be wholly inside the admitted executor and publication window. Invalid ordering leaves
  it null. The residual contains
  uninstrumented work and waits; it does not classify them. In particular it
  does not measure the acknowledgement's default reindex wait.

Acknowledgement and asynchronous patch execution can overlap. Their walls
must not be added. Likewise `cycle_started` to `admitted` is not labelled pure
lane wait, and existing nested structural-apply spans are not summed as
disjoint patch laps. Complete sum accounting needs explicit boundaries at
those operations; it cannot be obtained by renaming the timestamp gaps.

Pending/unobserved durations must remain missing in benchmark inputs. They
must not become zero-valued successful observations. This initial projection
does not claim the 5% phase-sum, 100-edit, runtime-overhead, or real read-back
acceptance gates in #1005.

## Store-level WAL observations

The store's `WALReclaimStats` adds cancellation/refusal counts for the first
consumed writer-parked, announced-intent or original-operation cancellation
cause of a final non-reset reclaim attempt. A positively observed parked writer
has precedence when both demand signals are positive; this is observation
precedence, not a claim that only one signal caused contention. Multiple polls,
writer-credit stages and helper cleanup do not become multiple attempts.
Successful final resets, normal helper cleanup, cycle/bulk withdrawal and a
child budget expiration are not counted as other caller/shutdown cancellation.
The existing common writer-waiting error remains available through `errors.Is`.

`ObservedPeakWALBytes` is a sampled high-water mark since the shared store core
opened. `WALPeakObserved` distinguishes an actual size observation from missing
evidence; in-memory/unsupported stores do not fabricate a zero WAL observation.
It reuses existing successful size reads and adds no file/stat/SQL work to edits.
Generation handles share the same peak; independent stores do not. Truncate,
shrink and status reads never reset it. This is a lower bound on instantaneous
physical peak and cannot be subtracted into a warm-run peak after startup.
Daemon/MCP transport wiring and CPU/edit denominator coverage remain separate
pending work; these store observations do not establish those acceptance gates.

## Logical source-operation witness (transport pending)

The MCP Server privately counts actual completion of the five measured source
operations: `write_file`, `edit_file`, `edit_symbol`, `batch_edit`, and
`move_symbol`, including their `edit`/`refactor` facade spellings and selector
inference. These counters span that Server's sessions and views. The daemon's
SharedServer serves its clients through the same Server; the counters are not
per selected repository, per client or exclusive process-CPU attribution.

A fixed request-local witness runs inside the actual handler execution, including
when the deadline firewall has already answered the caller. A nested handler on
the same Server borrows its owner; a distinct Server has distinct counters.
Normal reads do not allocate a witness. Hooks only observe existing successful
forward source-write/removal seams and already-read before images: no additional
source reads, graph/store queries or process samples are introduced. A raw caller
without a witness also skips the added byte comparison.

| Private counter | Actual evidence |
| --- | --- |
| `completed` | At least one proven source change, the whole source operation committed, and the actual handler returned a nonnil, non-tool-error result without a Go error or typed committed-journal warning. This is source completion, not graph freshness or acknowledgement delivery. |
| `committedWithError` | The complete source effect landed, but the actual handler result was nil/error/`IsError`, or a typed `committed`/`committed` batch receipt carried a journal warning. Committed bytes are not labelled unapplied. |
| `failedAfterSourceEffect` | Forward source effects were observed, but the operation did not reach complete commit. This includes rollback and retained/conflicted partial effects. |
| `rolledBack` | The subset above with typed batch `DiskStatus=rolled_back`. Rollback writes are not counted as another edit. |
| `unknownSourceComparison` | An otherwise clean complete write lacked readable prior bytes, so a logical content change cannot be established. It is excluded from clean completed edits instead of guessing a no-op/change. |

The first four outcome classes above are exclusive except for the named rollback
subset. A successful filesystem call is positive effect evidence; absence of a
witness does not prove that an opaque I/O error left every byte unchanged. Existing
typed disk/transaction verdicts remain authoritative. Failed-source counts cover
observed forward effects, not a new filesystem failure-detection mechanism.

The original batch handler performs physical writes, directory durability and
journal publication synchronously. A caller can stop waiting while that handler
continues its deliberately non-cancellable commit. Only that original new
transaction owns source effects; duplicate/status/recovery replies own no
new source operation. The typed Status/DiskStatus distinguish aborted, rolled
back or conflicted outcomes from committed source plus a terminal-journal warning;
`err == nil` and a non-tool-error response alone are insufficient. Multi-file
batch and move writes contribute one logical operation, not one per file.

Idempotency replay, status-only calls, dry runs, unchanged rewrites and refusals
before source effects do not increment completed edits. Native editor stimuli
remain harness-owned: filesystem events/coalesced watcher patches cannot identify
a logical native edit and are never added to these MCP counters.

No CPU endpoint, transport field, reset API or CPU-per-edit average is added by
this slice. A later endpoint must pair raw process CPU/count baselines within
one Server epoch, expose a stable count bracket and unavailable/zero-denominator
states, and label background/other-session work honestly. Instrumentation cost,
real client read-back, complete phase sums and the full workload matrix remain
unmeasured acceptance work. Source tests are pending execution until an explicit
validation slot; protocol fixtures are not installed-client evidence.
