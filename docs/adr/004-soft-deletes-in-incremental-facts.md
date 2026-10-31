# ADR 004: Soft-delete in incremental facts

**Status:** accepted

**Context.** `merge` cannot remove rows that disappear from the source; CDC delivers deletes as events.

**Decision.** Facts keep deleted rows with `is_deleted = true`; metric marts filter `where not is_deleted`.
Dimensions (full tables) exclude deleted rows.

**Consequences.** Deletes propagate correctly without full refreshes; consumers of facts must filter. GDPR erasure
additionally requires physical purge of RAW/fact rows (documented as a roadmap item).
