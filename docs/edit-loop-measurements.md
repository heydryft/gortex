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
