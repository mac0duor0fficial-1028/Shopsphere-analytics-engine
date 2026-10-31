# ADR 001: Airbyte "Incremental | Append" with dedupe in dbt

**Status:** accepted

**Context.** Append+Deduped gives a ready current-state table but applies deletes and keeps no change history.

**Decision.** Use Append. The raw layer is an immutable change log; staging derives current state
(`row_number() ... order by _ab_cdc_lsn desc`).

**Consequences.** (+) full audit trail, delete tracking, replayable, SCD2 for free. (-) staging does more work
(mitigated by incremental facts and views over a compact log); storage grows (apply retention/cleanup on RAW).
