# 0002 — Validator-balance fetcher recovery model

* Status: Accepted (2026-05-07).
* Date: 2026-05-07
* Scope: `services/validators/standard` in `wealdtech/chaind`.  Adjacent
  consumer: `services/summarizer/standard.summarizeEpoch` (reads
  `t_validator_balances` via `ValidatorBalancesByEpoch`).

## Context and Problem Statement

The validator-balance fetcher in `services/validators/standard` is
responsible for keeping `t_validator_balances` in lock-step with finalised
epochs.  Its persistence-and-recovery contract was never written down.
The 2026-04-07 `chaindb-d04` outage and the subsequent root-cause
investigation revealed two distinct contract questions that future
maintainers will re-derive from code unless they are recorded:

1. **Persistence atomicity** — under what failure modes can the fetcher's
   own cursor (`md.LatestBalancesEpoch`) end up pointing past an epoch
   for which no balance rows are durable in `t_validator_balances`?
2. **Recovery on restart** — when chaind restarts, what guarantees does
   the fetcher provide about epochs that elapsed during the downtime?

The investigation surfaced that the answer to (1) depends on which
upstream failure mode triggers it: kill-9 crashes are safe, but a
"beacon returned empty 200 OK" path can advance the cursor without any
balance rows being persisted, producing 113 historical clusters of
silent corruption (828 epochs across 9 months on a production chaindb
instance).  The answer to (2) is "cursor-trusting, not gap-detecting"
— sound under crash recovery (because of (1)'s atomicity) but unsound
under any other gap source.

This ADR records the contract as it **should** be and names the existing
points where the implementation matches and where it does not.  The
implementation gap on the empty-response path is tracked as a follow-up
work item.

## Decision Drivers

- A future engineer modifying the fetcher must be able to reason about
  what is and is not promised, without re-running the root-cause
  investigation.
- The recorded contract must hold across the full set of upstream
  failure modes — not just kill-9 — because production data shows
  multiple shapes have occurred.
- The contract must compose with the summarizer's downstream invariants
  (post-`1edcb45` the summarizer assumes "if balances are present they
  are correct"; the fetcher must not produce missing-but-cursor-claims-
  present states).
- The contract should be expressible in a few sentences; if it cannot,
  the design is over-complex.

## Considered Options

1. **Cursor-trusting recovery with empty-response guarded** —
   atomic co-commit of cursor + balance rows; recovery on restart
   resumes from `LatestBalancesEpoch+1` with no row-count
   reconciliation; an `len(validatorsResponse.Data) == 0` guard rejects
   empty beacon responses by returning an error so the transaction
   rolls back and the cursor stays put.  *(The contract this ADR
   adopts.)*
2. **Cursor-trusting recovery without an empty-response guard** —
   atomic co-commit of cursor + balance rows; recovery resumes from
   `LatestBalancesEpoch+1`; empty beacon responses silently advance the
   cursor with zero rows persisted.  *(Today's behavior; this is the
   bug the investigation identified.)*
3. **Gap-detecting recovery** — on restart, scan
   `t_validator_balances` for missing epochs in the
   `[startup_floor, LatestBalancesEpoch]` range and re-fetch them from
   the beacon.  Replace cursor-trusting with row-count reconciliation.
4. **Eager re-fetch via a `MissedEpochs` queue** — populate
   `metadata.MissedEpochs` whenever a fetch fails, retry queued
   epochs on subsequent ticks until empty.  Apparently the original
   design intent of the field (the field is declared in
   `services/validators/standard/metadata.go:28`, JSON-migrated, but
   never populated).

## Decision Outcome

Chosen option: **Option 1 — cursor-trusting recovery with
empty-response guarded.**

### Rationale

- Option 1 is the smallest delta from current code that closes the
  observed silent-corruption pathway.  The atomic co-commit
  invariant from option 2 is preserved; only the empty-response path
  is hardened.
- Option 3 (gap-detecting recovery) is more robust but substantially
  more invasive: it requires per-epoch row-count semantics in
  `chaindb.ValidatorsProvider`, a new schema migration to record
  per-epoch row-count expectations, and a startup phase that scans a
  large table.  The cost is not justified by the production data —
  Option 1 catches every observed failure mode.
- Option 4 (eager re-fetch via `MissedEpochs`) is the apparent
  original design intent and is consistent with the field's presence
  in `metadata.go`.  However, the `MissedEpochs` consumer code in the
  *proposerduties* service (`services/proposerduties/standard/service.go:166-204`)
  reads from a permanently-empty list — the population logic was
  never written for any service.  Reviving this path would require
  designing the population logic from scratch, with no existing
  precedent to follow.  Option 1 is simpler and addresses the same
  failure modes.  The `MissedEpochs` field is documented as
  deprecated (in this slice) so that future maintainers do not
  accidentally activate dormant consumer code.

### The contract — "cursor-trusting recovery with empty-response guarded"

In one sentence: **`md.LatestBalancesEpoch` advances if and only if
`t_validator_balances` contains rows for the corresponding epoch in the
same database transaction**.

Decomposed into three claims:

1. **Atomic co-commit (already enforced)**: `SetValidatorBalances`,
   `setMetadata` (which writes the new cursor value), and
   `CommitTx` all run inside a single Postgres transaction in
   `onEpochTransitionValidatorBalancesForEpoch`
   (`services/validators/standard/handler.go:198-243`).  A crash at
   any point pre-commit rolls back both writes; a successful commit
   makes both visible together.  Verified empirically by a kill-9
   reproducer.

2. **No-empty-write (must be enforced; currently not)**: a beacon
   response with zero validators or with all validators having
   `Balance == 0` must be treated as a fetch failure, not a fetch
   success.  The fetcher must return an error and not advance the
   cursor.  The next finality tick will retry.  *This is the
   investigation-identified defect; a follow-up brings the
   implementation into compliance.*

3. **Cursor-trusting recovery (already enforced)**: on chaind restart,
   `updateAfterRestart` (`services/validators/standard/service.go:84-137`)
   calls `onEpochTransitionValidatorBalances(ctx, md, currentEpoch)`,
   which loops from `md.LatestBalancesEpoch+1` to the current epoch
   and re-fetches each.  No row-count reconciliation; the cursor is
   the authoritative record of "what has been persisted."  With (1)
   and (2) holding, this is sound.

### What this contract does NOT promise

- **Recovery from "beacon-state retention exceeded"**: if chaind is
  off-air for longer than the beacon's state-retention horizon
  (typically 8 epochs for Prysm with default flags, ~2 weeks for
  Lighthouse), the beacon may return empty responses for the missed
  epochs.  Under this contract, the fetcher will stall (refusing to
  advance the cursor) and the lag gauge from ADR 0001 will fire the
  alert.  The operator must then either repair the beacon (e.g.,
  point at a different endpoint with longer retention) or explicitly
  acknowledge the data loss with a manual cursor advance.  This is
  the *correct* behaviour for a chronic upstream condition; turning
  it into automated data loss is a separate decision and is not part
  of this contract.
- **Per-validator row-count completeness**: the fetcher writes one
  row per validator with non-zero balance.  Validators with
  `Balance == 0` are intentionally not stored (see handler.go:206-208).
  Downstream consumers must not assume every validator index has a
  row in `t_validator_balances` for every epoch.  The summarizer's
  zero-balance assertion (commit `1edcb45` and `8d9e6f2`) handles
  this correctly.

### Implementation notes

The bug-confirming test
`services/validators/standard/handler_internal_test.go` exercises all
three contract claims:

- **Claim 1 (atomic co-commit)**: covered by the *beacon error* case,
  which asserts the cursor is not advanced and no transaction is
  committed when `Validators()` returns an error.
- **Claim 2 (no-empty-write)**: covered by the *empty validators map*
  and *all-zero balances* cases, both of which currently document
  the bug (test passes against current code where the cursor *does*
  advance).  When the follow-up fix lands, the assertions in those
  cases invert from "cursor advanced" to "cursor unchanged, error
  returned" and the test serves as the regression boundary for the
  contract.
- **Claim 3 (cursor-trusting recovery)** is exercised end-to-end by
  the existing integration smoke tests on the local Hoodi playbook
  and the kill-9 reproducer.  No further unit-level coverage is
  added here.

### The retention-pruning interaction (related but separate)

The pruner consults the *day-summary* cursor, not the
*epoch-summarizer* cursor (`services/summarizer/standard/prune.go:64-67`).
The proxy holds because day summaries entail epoch summaries, but the
contract is implicit.  The investigation's code-reading audit flagged
this as a residual risk; the kill-9 reproducer found no exploit; the
production forensic data shows no pattern consistent with retention-
race triggering.  No contract change is filed here.  If the empty-
response amplifier had been refuted as a load-bearing cause, the
retention-ordering question would have been the next target — but it
was not, so this ADR does not expand its scope.

### Positive consequences

- The fetcher's promised behaviour matches the summarizer's
  assumptions on `t_validator_balances` content.  The two services
  compose without unstated invariants.
- A future maintainer reading
  `services/validators/standard/handler.go` can read this ADR and
  understand the contract without re-running the investigation.
- The contract scopes the fetcher's responsibility narrowly: it
  guarantees consistency between cursor and rows, but does not
  guarantee freshness (the summarizer's lag gauge from ADR 0001 is
  the freshness signal).

### Negative consequences / accepted trade-offs

- The contract requires the fetcher to **stall** (cursor not
  advancing) on chronic upstream failures.  Operators who would
  prefer "skip the bad epochs and keep going" must take that
  decision explicitly via a manual cursor advance — chaind will not
  silently absorb data loss.  This is a deliberate UX choice.
- Contract claim (2) is tested only against synthetic stubs in this
  slice; the post-fix end-to-end behaviour (production beacon flap →
  fetcher stall → lag gauge alert → operator-driven recovery) is
  not exercised by automated tests.  It can be by extending the
  Hoodi playbook with a "beacon empties for 2 minutes" mode in a
  future slice.

### Rejected alternatives — why not

- **Option 2 (no empty-response guard)** is the current (buggy)
  state.  Production data showed it is responsible for the silent-
  corruption pathway across 113 historical clusters.  Cannot stand
  as the documented contract.
- **Option 3 (gap-detecting recovery)** is more invasive than the
  data justifies.  Disk and CPU cost of the startup scan, plus a new
  schema-migration burden, plus operator-visible latency on chaind
  startup.  Reconsider only if a future verdict identifies a failure
  mode that Option 1 does not catch.
- **Option 4 (`MissedEpochs` queue)** had a chance to be the
  intended design but the consumer code never had a population path,
  meaning re-deriving the design from scratch.  No precedent
  cheapens this option below Option 1.  See the field's deprecation
  note in `services/validators/standard/metadata.go`.

## Open questions for upstream review

When this ADR is reviewed by `wealdtech/chaind` maintainers (alongside
the no-empty-write fix PR that brings the implementation into
compliance):

- Confirm the MADR-template style choice from ADR 0001 is acceptable
  for ADR 0002 as well.  No prior style precedent existed at ADR
  0001's authorship; this ADR follows it.
- Confirm Option 1 is the preferred contract.  If the maintainers
  prefer the gap-detecting recovery (Option 3) for some reason this
  ADR did not anticipate, the contract changes and the fix scope
  expands.
