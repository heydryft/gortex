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
These store observations do not by themselves establish per-run peak or
cancellation/CPU attribution acceptance.

`graph_stats` and `gortex://stats` share a `wal_reclaim_observation` block. Its
scope is `physical_store_since_open`, independent of the selected graph/view or
repository. The optional store capability is queried once per payload through
its existing `WALReclaimStats` snapshot: a short counter mutex and atomic loads,
with no file stat, WAL-index read, SQL query or writer wait. Unsupported/masked
stores report `snapshot_available=false`, a reason and null counters/peak;
there is no unwrap or SQL fallback. A supported zero counter is numeric zero.

The fields `cancelled_writer_parked`, `cancelled_write_intent` and
`cancelled_other` preserve the store's final-attempt observation semantics.
`observed_peak_wal_bytes` is null while `wal_peak_observed=false`. An actually
observed empty WAL is `0` with `wal_peak_observed=true`; it is different from
missing evidence. `peak_scope=sampled_since_store_open_lower_bound` explicitly
excludes an exact instantaneous or warm-run peak. In-memory stores have no WAL
size observation even when their counter snapshot is supported.

`observation_epoch` reuses the Server's immutable observation binding and
`epoch_scope=server_binding`. It is **not** a durable store-open identity or a
per-run reset token. Production binds the Server's physical store at construction;
generation handles share the store core and its observations. A literal Server
without that binding has an empty epoch. Do not join samples across a changed
binding/reopen, or subtract two high-water marks into a run peak. Stats reads,
WAL resets and generation changes never clear these counters.

Daemon status JSON also exposes these new causes and nullable peak under
`storage.wal_reclaim`, extending its already-collected snapshot. This adds no
status collection call or I/O; the new MCP block does not call the daemon's
file-size/WAL-index collection path. Other daemon storage/status fields keep
their existing semantics.

## Private acknowledgement ownership (transport pending)

Measured source-tool calls now own one private, call-scoped acknowledgement
record at the `boundToolHandler` boundary. It reuses the existing five-tool
classification. Nested calls on the same Server borrow its identity, while a
different Server owns a different record. Reads allocate no acknowledgement
record or context value and take no acknowledgement clock sample.

The origin is `mcp_bound_call`, before the deadline firewall; it is neither
dispatcher entry nor the legacy `toolReceivedAt` middleware origin. The latter
and `publication_phases` remain unchanged. Caller response-ready freezes when
the outer bound call returns. Actual handler completion is observed separately,
including a detached physical commit that finishes later. Those walls overlap
and must never be added together or presented as client receipt/socket delivery.
Missing endpoints stay pending, missing origins stay unavailable and invalid
ordering stays invalid; none becomes measured zero. Snapshots own their scalar
values and never evaluate a live clock during formatting.

Physical file receipts written by one RPC now share that private observation
through the existing bounded mutation ledger. Both expiry and capacity eviction
clear the reference, even when pending-source evidence keeps the physical receipt
alive. A late handler can finish its call-scoped observation but cannot reinstall
it on an evicted record. Raw/unobserved writes have no MCP acknowledgement.

This ownership slice does not publish acknowledgement wire fields. It does not
attach pointers to `batchTransactions`, whose existing map has no bounded
eviction. Dispatcher queue/preamble are explicitly
unavailable pending the shared admission interface; actual parse/disk/enqueue
laps and additive-sum validation are partial work. It does not change the validated actual-completion counters or CPU/WAL
sampling. Read/native-path and mutation overhead still require measurement.

The private fixed acknowledgement accumulator now observes the actual pre-write
parse-gate call in `edit_file`/`write_file`, the `AtomicWriteFile` call (ending
before committed-hash/receipt bookkeeping), and canonical/checkout refresh
enqueue calls. Repeated chunks occupy fixed slots rather than a span history.
An unsupported parse gate is not applicable; an instrumented raw commit that
did not call a gate does not acquire a fabricated parse duration. Other tools'
actual parser and durability work is not inferred from these slots.

Each slot retains begin/completion counts, actual elapsed execution and interval
validity. Snapshot `ObservedMS` is completed execution; `AckMS` is an alternative
value present only when every applicable chunk is valid and contained in the
frozen reply window. A late interval keeps its full execution observation and
no acknowledgement duration. Overlap, missing ends and invalid containment
produce explicit states, not clipped or residual-derived timings. The
`LapsDisjoint` flag alone proves neither coverage nor the 5% sum requirement;
middleware, waits, reply preparation and unsupported paths remain unclassified.
No acknowledgement wire fields or additional patch log lines are emitted yet.

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

Process CPU sampling is separate from these operation hooks. Protocol fixtures
are not installed-client or performance evidence.

## Process CPU/count stats window

`graph_stats` and `gortex://stats` both add `process_edit_window` beside their
view-derived graph fields. Its `count_scope` is `server_all_sessions_views` and
its `cpu_scope` is `whole_process_including_background`. Process CPU includes
reads, indexing, other sessions, unfinished work and any other Server in the
process. It is not exclusive CPU attributed to the selected graph or an edit.

An immutable `epoch` identifies this Server's observation window. At the end of
construction, before handlers become callable, it captures process CPU between
two zero-count snapshots. A literal/uninitialized Server reports no baseline;
a failed baseline is retained rather than silently replaced on a later read.
No stats read resets counters or the epoch.

`baseline` and `sample` expose raw cumulative `cpu_seconds`, monotonic
`sample_started_seconds`/`sample_finished_seconds` within that epoch, and full
`counts_before`/`counts_after` brackets. A normal stats read takes exactly one
process CPU sample. Unavailable CPU is null with `cpu_available=false` and a
reason; a known zero CPU sample is a valid zero. Unix uses current-process
user+system Getrusage; Windows uses GetProcessTimes kernel+user durations;
unsupported platforms report unavailable.

`cpu_seconds_per_edit` divides the process CPU delta from that same baseline
by clean completed source operations from the matching zero-count baseline.
It is numeric only for available, ordered CPU/clocks, a stable complete count
bracket, at least one clean completed edit, and no committed-with-error,
partial-effect or unknown-comparison outcome in the window. Otherwise it is
null with `average_state=unavailable` and an explicit `average_reason`.
A measured value is labelled `measured_process_window`, not exclusive edit CPU.
The rollback subset is never added again as another operation.

Raw samples remain available even when that since-baseline average is null.
A harness can compare later warm-run endpoints only with the same nonempty
epoch, valid stable brackets and an eligible outcome delta. Native logical
operations still require the harness's own completed-stimulus denominator.
Sampled since-store-open WAL peaks cannot be subtracted into a warm-window peak.
No run-reset API, per-edit/process CPU syscall or tracing registry is introduced.
Instrumentation cost, real client read-back, complete phase sums and the full
workload matrix remain unmeasured acceptance work.
