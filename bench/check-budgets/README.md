# Performance budget checker

This command consumes repeated, pinned #996 report-v1 files and
[`../budgets.yaml`](../budgets.yaml). It does not run workloads, start daemons,
provision runners or infer missing observations. The wire mapping is pinned to
`95b7982bf1853b24bd963a0701aa163d9a72a2de`. The policy and input index reject
unknown fields. Metric and scenario names are closed tables in `specs.go`.

```sh
go test ./bench/check-budgets
go run ./bench/check-budgets --budgets bench/budgets.yaml \
  --inputs reviewed-inputs.json --lane smoke --out budget-results.json
```

The output is a before → after table and an optional new JSON evidence file.
Any breach, missing/null metric, wrong unit, omitted safety/censor count,
incomplete report, unavailable prerequisite or incomparable measurement exits
nonzero. Existing output files are never overwritten. No workflow is enabled by
this slice: unresolved wire/infrastructure prerequisites would otherwise make
a required PR check predictably red rather than provide useful validation.

## Repeated evidence and comparison identity

An input index has `schema_version: 1`, full `candidate_revision`,
`main_revision`, `tag_revision`, a default `repeats` of at least three, and
`hosts` mapping reviewed host classes to exact observed machine IDs. `runs`
contains `{role, host_class, scenario, repeat, report}` records; paths are
relative to the index or absolute. Roles are `candidate`, `main`, and `tag`.
The shipped tag is pinned to v0.64.10 / defa9f8e. `scenario_repeats` can declare
larger planned counts, notably `{"idle-stop-20":20}`. A row's `min_repeats`
cannot reduce the three-repeat minimum. Every planned repeat must be present;
missing or failed repetitions are not discarded. Reusing a report/run identity
does not constitute additional repetitions.

All roles within a matching repeat must have the same observed host/limits,
corpus commits and logical targets, actual normalized workload and request-plan
hash. Different owned source paths are expected and are excluded from corpus
identity. Repetition seeds may vary, but only the seed may vary across repeats;
it must agree across roles in that repeat. The load32/load8 denominator permits
only the intentional agent-count difference and still requires matching seeds.
Missing or non-hex plan hashes fail. No other load/scoping difference is silently
normalized. Paired native reports and saturation reports are rejected as formal
capacity evidence rather than relabeled as independent agent runs.

Reference Linux requires the actual validated cgroup/host observations: 8 CPU,
16 GiB memory and a 57–60 GiB usable filesystem. Native Mac class assignment
requires a separately reviewed runner inventory; the checker binds it to the
exact Darwin ARM64 machine and reported limits, without claiming to discover
whether a host is reference or large. Linux PSS and Mac physical footprint are
distinct fixed memory observations, compared only within the same host class
and measurement identity. Neither silently substitutes RSS when unavailable.

Each repeat contributes its own p95/p99/p50 or counter observation. The checker
uses the median of at least three repeats, not a pooled percentile. A ratio row
uses the ratio of repeated medians. Hard edit/stop maxima, zero-safety counters
and correctness floors apply to every repeat; a median cannot hide one bad
run. Complete reports must retain successful stop evidence, zero crashes and
uncensored/error-free observations. Busy attempts remain in the busy-fraction
denominator, including refused attempts whose eventual retry succeeded.

## Limits and named allowances

`block` uses its absolute target. In smoke, p95 observations also fail when their
median is more than 20% worse than main; `read_p95_vs_main` expresses the read
comparison without inventing an absolute one-repo p95 ceiling.

`guard` starts with the shipped median × 1.15. Only a disk/memory cell whose
pinned main median already exceeds shipped × 1.15 selects main instead. This
selection is independent of the candidate. A missing shipped observation cannot
trigger fallback. Named additive allowances then add their declared amounts.

The WAL-reclaim allowance is a **guard limit**, not an extra 2 GiB and not an
absolute cap: the resulting limit is `max(noise-adjusted baseline, 2048 MiB)`.
The table/JSON retain the effective added headroom. A baseline already above
2 GiB keeps its noise allowance; the WAL entry adds zero headroom in that case.
The pending-edit and dispatch-queue allowances add 64 MiB each only where
explicitly named/applicable, with their owning releases recorded.

A guard can retain a known future absolute `target`, ignored until its mode
changes to `block`. Guard allowances are not applied after that promotion.
An unknown future target is explicitly null. Unresolved `owning_release` is
explicitly `unassigned`, legal only for guards; those rows cannot be promoted
without resolving ownership. No measured baseline numbers live in the policy.

Known scheduling evidence: #1065 owns 15 MiB/edit at 1/4/8 agents in v0.64.15;
#1061 owns the 8/16-worktree peak in v0.64.14; #1076 owns distinct 60-second
idle/Go-Sys observations in v0.64.16; #1088 owns noncold WAL in v0.64.17; #1092
states the 1 MiB/min idle-write goal in v0.65.0. The conditional 6 MiB/edit
diff-on target from #1075 is **not** made the default: it depends on 14 clean
nightlies. #997 names v0.64.14 for cold writes while #1073's actual milestone is
v0.64.15; the policy uses the concrete owning issue's v0.64.15 milestone and
records that cross-reference conflict. Its future absolute target remains null
until the 20/40/45 GiB cells have matching measured topology definitions.
Generic peak/2-second-idle ownership
remains unassigned rather than borrowing stronger, different observations.

## Exact frozen-wire mapping and prerequisites

| Policy observation | Frozen source / limitation |
| --- | --- |
| Edit acknowledgement | `edit_latency_ms.p95/max`, ms converted to s; not indexed visibility |
| Read p95 | Successful workload `attempts[].logical_ms`, nearest-rank per repeat; includes backoff, excludes think time |
| Tool timing | `tools[search.symbols/read.source/explore.outline/analyze.dead_code].latency_ms`; completed count required |
| Busy fraction | Workload busy attempts / all workload attempts; probes use another denominator |
| CPU, WAL, stop | Phase metrics `daemon_cpu_avg_cores`, `peak_wal_bytes`, `stop_seconds`; actual `force_killed` required |
| Memory | Linux PSS / Mac physical footprint, with distinct peak and `idle_after_2s_` keys |
| Cold bytes | `startup_logical_written_bytes`; startup includes enrichment/analysis, not an isolated parser call |
| MiB/edit | Top-level inferred-contained patch WAL + main counters / acknowledged edits; reject missing, reset/lower-bound, boundary or overlapping evidence. This is amortized process-interval activity, not exclusive edit ownership. Tag cannot emit the new counters: an approved equivalent baseline strategy or explicit policy change is needed; whole-process bytes are not substituted. |
| Exact file readback/correctness | Missing. Indexed new-symbol visibility and mixed reading bursts are weaker observations. |
| Unflagged stale output / relations / derived lag | Missing structured output-mode correctness/publication observations; never inferred from visibility. |
| Reclaim writer hold / read-gate closures / cancellation release | Missing distributions/censuses, not inferred from WAL size or client deadline. |
| Primary stale refusals / restart refusal census / ready-to-first-OK | Missing structured reason and configured-root/worktree probes. Launch-to-queryable is not ready-to-first-OK. |
| Six-repo per-scope sweep | Frozen `sweep` loads only manifest[0]. `many`'s fast read mix is not the required sweep. |
| Linked-worktree edit probes | Frozen `worktrees` drives read bursts; its CPU/memory are not the combined edit scenario. |
| Idle/churn/schema21/freelist/lapsed/stop20 fixtures | Not distinct measured CLI scenarios yet. Stop20 requires 20 planned reports, not three zeros. |
| 60-second idle / cold Go Sys peak / idle bytes per minute | Missing. Two-second footprint and status snapshots cannot stand in for those intervals. |

Known unavailable metrics/scenarios stay declared and fail with their reason;
they are not silently omitted from the selected host/lane. The staged rows and
synthetic unit fixtures do not prove any owning product target passes.

## Remaining lane and acceptance work

Before workflow/release code or external setup, review a source plan consuming
the accepted #996 client, official baseline evidence and resolved metrics above.
No self-hosted runner is currently registered. A runner owner must provide the
reference cgroup/disk and reviewed Apple host inventory; this command does not
register services, alter Docker/Brew, install runners, set secrets or spend
model calls. A PR smoke lane must remain separate from CI test jobs and run on
`ubuntu-24.04-arm`; protected-main nightlies require available owned hosts.
Reports/table artifacts need effective retention of at least 90 days.

The PR plan must include all five steps, comparable main measurements, existing
micro-benchmark benchstat and a measured duration estimate. The frozen default
120-second workload windows repeated three times already consume substantial
time; do not assume the full lane fits 15 minutes. Any shorter smoke profile
needs matching baselines and enough observations, not reused 120-second data.
Nightly/release plans must preserve failed/censored runs, consecutive-night row
history, floor-host scope restrictions and every daemon's stop/lock receipts.
The floor class and extra lifecycle adapters require explicit reviewed support;
do not relabel floor runs as reference Linux.

Actual acceptance is still required: 20 real smoke runs for wall-time/flake
rates, linked seeded latency/edit-wait regressions, five actual nights on main,
a seeded nightly failure, a measured release subset under eight hours per host,
no forced daemons, and 20-PR CI wall-time comparison. None is simulated by unit
tests. #996 must land before #997; #998 is merged. #1000's deferred 400-task
harness is not recreated here. A small release scripted-agent fixture remains
separate future work, requiring its own reviewed plan.
