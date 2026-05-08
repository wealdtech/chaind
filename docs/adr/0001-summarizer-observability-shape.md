# 0001 — Use a single labelled lag gauge for summarizer stall observability

* Status: Accepted (2026-04-30); amended 2026-05-05 — see "Amendment 1" below.
* Date: 2026-04-30
* Scope: `services/summarizer/standard` in `wealdtech/chaind`. Companion alerts live in `attestantio/ops`.

## Context and Problem Statement

`services/summarizer/standard.summarizeEpoch` returns `(false, nil)` when validator balances for an epoch are unavailable. The outer `summarizeEpochs` exits the loop silently with only a `Debug` log. Because two downstream pipelines (`summarizeBlocks`, `summarizeValidators`) cap their target epoch at `md.LastEpoch`, the silent skip freezes the entire summarizer until the underlying data condition resolves. The 2026-04-07 Hoodi incident produced a multi-week Grafana gap before any human noticed.

We need stall observability that an operator can alert on within minutes. The shape of that observability is not obvious; several reasonable designs exist.

## Decision Drivers

- An alert must fire within minutes of any stall, not after a long-tail dashboard review.
- The signal should generalize across all stall shapes — including persistent errors in the `summarizeBlocks` and `summarizeValidators` pipelines, not only the `(false, nil)` site that triggered the original incident.
- Operators already reason about consumer-lag and replication-lag metrics; the new metric should fit that mental model so runbooks stay simple.
- Public Prometheus metric shape is hard to reverse — renaming or relabelling is a breaking change for downstream alerts, so the choice must be deliberate.

## Considered Options

1. **A single labelled lag gauge** — `chaind_summarizer_lag_epochs{pipeline}`.
2. **Original plan: `stalled_epoch` gauge + `stall_ticks_total{reason}` counter** — a paired stall-event metric pinned to the silent-skip site.
3. **Hybrid: lag gauge + `silent_skip_total{reason}` counter** — Option 1 plus an event counter for forensic detail.
4. **Pre-populate the gauge at service startup via `eth2Client.Finality()`** — variant of Option 1 that closes the cold-start blind spot.

## Decision Outcome

Chosen option: **Option 1 — a single labelled lag gauge**, because it surfaces every stall shape the summarizer can produce (not just the original `(false, nil)` site), collapses alerting to a single rule that matches operator intuition, and avoids coupling Prometheus cardinality to an internal enum that may evolve.

### Implementation notes

Stall state is exposed as a single Prometheus `GaugeVec`:

```
chaind_summarizer_lag_epochs{pipeline} = float64(targetEpoch) - float64(md.LastXEpoch)
```

where the `pipeline` label takes one of three values: `epoch`, `block`, or `validator`.

The gauge is updated by a private helper `updateLagGauges(ctx, finalizedEpoch)`, registered via `defer` at the top of `OnFinalityUpdated` so it fires on every handler invocation including all error-return paths. The pure-compute half (`setLagGauges`) is split out as a testable seam: internal-package unit tests drive the gauge math with synthetic metadata without standing up a database. Disabled pipelines (e.g. when `s.blockSummaries == false`) never call `WithLabelValues(...)` and so omit their series entirely; the alert query naturally ignores absent labels.

The `float64` cast is deliberate: a `uint64` subtraction would underflow to ~`1.8e19` if metadata is briefly ahead of finality.

Per-event diagnostic detail (which epoch is stuck, which silent-skip branch fired) lives in `Warn` logs at the two silent-skip sites — *not* in a counter, *not* in a `reason` label. The paging signal is the lag gauge alert; the logs are forensic context that becomes useful once the alert fires.

### Positive consequences

- One alert rule (`chaind_summarizer_lag_epochs > 2 for 15m`) covers every stall shape the summarizer can produce, including future ones.
- Operator mental model matches existing consumer-lag and replication-lag metrics elsewhere in the fleet.
- Metric maintenance is a single series with a small label set instead of two metrics with overlapping semantics.
- High-cardinality diagnostic detail lives in logs, where it belongs, without forcing Prometheus to track an enum that may grow.

### Negative consequences

- **The metric is part of the public contract** with operators. Renaming the metric or its labels is a breaking change for anyone running the alert rule. If we ever need to evolve the shape, we add a new metric alongside this one and deprecate the old.
- **Alert wording is coupled to log message text.** The runbook annotation references the literal `Warn` strings:
  - `"No validator balances available; cannot summarize epoch (will retry on next finality tick)"` (from `epoch.go`, the `len(balances) == 0` branch).
  - `"Not enough data to update summary; will retry on next finality tick"` (from `handler.go`, the `!updated` branch in `summarizeEpochs`).

  Changing those strings without updating the alert annotation breaks operator UX. The implementation includes an `Alert annotation contract:` code comment near each log line and on the `summarizerLagEpochs` variable declaration in `metrics.go`, so the contract is visible at the point of edit.
- **No per-event counter** means we cannot trivially answer "how often is the silent-skip path firing across the fleet?" That question, if it arises, will be answered from log-based metrics (e.g. Loki/Mimir) or from sampling logs in incidents. If the question becomes recurrent, that is itself a signal that we should re-introduce a counter — superseding this ADR rather than retrofitting one in silently.
- **A six-minute cold-start blind spot** exists between service startup and the first `OnFinalityUpdated` tick during which the gauge has no value. This is an accepted trade-off (see Option 4 below) covered by the existing flat-`chaind_summarizer_epochs_processed_total` alert.

## Pros and Cons of the Options

### Option 1 — Single labelled lag gauge (chosen)

- **Good** — generalizes across all summarizer stall shapes, including future ones.
- **Good** — alert rules collapse to a single `> 2 for 15m` query.
- **Good** — matches operator intuition for lag-style metrics.
- **Bad** — does not provide a per-event count of silent-skip occurrences; that question is deferred to logs.

### Option 2 — `stalled_epoch` gauge + `stall_ticks_total{reason}` counter (rejected)

- **Good** — explicit per-incident counter is convenient for "how many times did this fire" questions.
- **Bad** — only fires on the `(false, nil)` path inside `summarizeEpoch`. A persistent error inside `summarizeBlocksInEpoch` that causes `LastBlockEpoch` to fall behind `LastEpoch` would leave the gauge at zero and the alert silent — which is the original incident shape we are trying to prevent.
- **Bad** — the `reason` enum couples public metric cardinality to an internal classification we may want to evolve. Adding or renaming reasons becomes a breaking-change concern.

### Option 3 — Hybrid (lag gauge + `silent_skip_total{reason}` counter, rejected)

- **Good** — preserves the generalized alert from Option 1 while keeping a per-event counter.
- **Bad** — the forensic information the counter provides is already in the new `Warn` logs at the same sites. A per-event counter duplicates the log signal at higher cost (Prometheus cardinality, label maintenance, dashboard panels to ignore) without giving operators anything the logs do not.

### Option 4 — Pre-populate the gauge at service startup via `eth2Client.Finality()` (rejected)

- **Good** — closes the six-minute window after restart during which the gauge has no value.
- **Bad** — engineering for an unlikely failure shape ("chaind never receives a finality tick at all"). That shape is already covered by alerts on flat `chaind_summarizer_epochs_processed_total`.
- **Bad** — adds network I/O to the bootstrap path, creating new failure modes (slow or failing Beacon node at startup) for the small benefit of populating one gauge value six minutes earlier.

## Links

- Incident: 2026-04-07 chaindb-d04 outage (multi-week silent stall before detection)
- Implementation: chaind commit `066bb38` ("Add summarizer lag observability") — registers `chaind_summarizer_lag_epochs`, adds the two `Warn` log sites, and ships the `updateLagGauges` / `setLagGauges` split
- Companion alert rule: `attestantio/ops` work item — deploys the `chaind_summarizer_lag_epochs > 2 for 15m` alert against the strings above

## Amendment 1 — cursor-diff formula and zero-balance guard (2026-05-05)

* Status: Accepted, amends the Decision Outcome and Implementation Notes above.

### Motivation

The original formula `targetEpoch - md.LastXEpoch` was structurally incapable of detecting the silent-corruption mode the alert was meant to catch. End-to-end smoke verification on a seeded chaind instance — DELETE all `t_validator_balances` rows for one finalized epoch, observe behavior — established the failure shape empirically: the gauge stayed pinned at `0/0/0` throughout, neither alert-contract `Warn` log fired, the summarizer cursor advanced past the corrupt epoch, and `t_epoch_summaries.f_active_balance` was silently written as `0` for the affected epoch.

Root cause is in `services/chaindb/postgresql/validators.go` (`ValidatorBalancesByEpoch`): a `LEFT JOIN` of `t_validators` against the per-epoch balance subquery, with `COALESCE(f_balance, 0)`. When no balance rows exist for an epoch, the function returns ~1.3M rows of zeros instead of an empty slice — so the `len(balances) == 0` guard in `summarizeEpoch` never trips in any production state where `t_validators` is populated. The cursor advances unconditionally, the gauge stays at zero, the alert never fires, and corrupted summaries are written.

The `0` reading is fatally ambiguous for an alert metric — it can mean "summarizer is healthy" or "summarizer is silently corrupting data" with no way for a downstream consumer to distinguish them.

### Amended formula

The metric name (`chaind_summarizer_lag_epochs`), label key (`pipeline`), label values (`epoch`/`block`/`validator`), and gauge registration are **unchanged** — this is a formula amendment, not a metric replacement. Each pipeline now measures its lag against its **direct** upstream cursor:

| label | new formula | meaning |
|---|---|---|
| `pipeline=epoch` | `validators.latest_balances_epoch - summarizer.latest_epoch` | how far behind the upstream balances cursor the epoch-summary cursor is |
| `pipeline=block` | `summarizer.latest_epoch - summarizer.latest_block_epoch` | how far behind the epoch-summary cursor the block-summary sub-pipeline is |
| `pipeline=validator` | `summarizer.latest_epoch - summarizer.latest_validator_epoch` | how far behind the epoch-summary cursor the validator-summary sub-pipeline is |

The reading-side now fetches both `summarizer.standard` and `validators.standard` rows from `t_metadata` per finality tick. Diffs are clamped to `[0, +Inf)` via a small helper to absorb the benign race window where a downstream cursor is read ahead of its upstream across two metadata reads.

### Defensive zero-balance assertion

Independently of the gauge change, `summarizeEpoch` now refuses to write a corrupt summary. After the active-validator balance accumulation loop and before `BeginTx`, if `summary.ActiveValidators > 0 && summary.ActiveBalance == 0` the function emits the existing alert-contract `Warn` log (verbatim, single-sourced with the bootstrap-path `len(balances) == 0` branch above it) and returns `(false, nil)`. The cursor stays put, the cycle retries on the next finality tick, and the row is never written to `t_epoch_summaries`. This keeps the data-integrity guarantee even if the alert is missed.

### Why amend instead of supersede

The metric/label/registration contract is unchanged — operators running the existing alert rule do not need to migrate. The formula change is a defect fix that recovers the *originally intended* semantics ("detect when the balance pipeline falls behind the summarizer"), not a redesign. Per the original "Public Prometheus metric shape is hard to reverse" decision driver, supersession is reserved for breaking changes to the operator-facing contract.

### Why cursor-diff over the original formula

1. **Decoupled from cycle outcomes.** Whether a finality cycle errors, runs with garbage, succeeds, or is skipped, the gauge measures actual cursor-state divergence at read time. It cannot lie about lag because it isn't computed from the lossy cycle-completion signal.
2. **Disambiguates `0` readings.** A `0` now means exactly one thing: "downstream cursor matches its upstream right now."
3. **Per-pipeline labels carry distinct diagnostic value.**
   - `epoch` lag growing → upstream balances pipeline is stalled (network, API, validators-pipeline crash). Action: investigate `services/validators/standard/`.
   - `block` lag growing → block-summary sub-pipeline is internally lagging behind epoch summarization (DB write contention, slow query). Action: investigate `summarizeBlocksInEpoch`.
   - `validator` lag growing → same diagnosis pattern but for the validator sub-pipeline (`summarizeValidatorsInEpoch`).
4. **Alert thresholds become operationally direct.** "lag > N for M minutes" reads as "the upstream pipeline has been ≥N epochs ahead of the downstream pipeline for ≥M minutes" — a direct operational statement, not a proxy reading via cycle-completion.

### Considered options for the fix

- **Option A** — Add an "all-zero balances" guard inside `summarizeEpoch`. Adopted (the defensive assertion above).
- **Option B** — Change `ValidatorBalancesByEpoch` itself to return a `nil` slice when no rows. Rejected. The `LEFT JOIN` exists for a real reason: zero-balance validators don't have rows; the JOIN ensures every validator is represented in the result. Touching the SQL risks regressing the legitimate case. The defect is in the summarizer trusting the all-zero answer at face value, so the guard belongs at the summarizer layer where it can check the business invariant (`ActiveBalance > 0` when `ActiveValidators > 0`) rather than a structural property of the SQL.
- **Option C** — Re-instrument the lag gauge with cursor-diff math. Adopted (this amendment).

A and C ship together: C ensures the alert fires; A ensures no corrupt summary is written even if the alert is missed. Belt and suspenders.

### Operator-facing changes

Metric name, labels, and registration are unchanged — no migration required. **The threshold *meaning* has changed**: the gauge now reports data-cursor divergence rather than cycle-completion lag. For the existing `> 2 for 15m` rule the practical behavior is similar (both fire when the summarizer falls behind), but the new signal is sensitive to the previously-undetectable silent-corruption mode. Companion alert spec in `attestantio/ops` should reference the amended semantics; per-pipeline diagnosis hints become useful in the runbook because the labels now carry genuinely distinct failure surfaces.
