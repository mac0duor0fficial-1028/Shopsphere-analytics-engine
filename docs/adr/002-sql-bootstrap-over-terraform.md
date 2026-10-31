# ADR 002: Idempotent SQL bootstrap for Snowflake instead of Terraform

**Status:** accepted for the portfolio edition

**Context.** The Snowflake Terraform provider has had breaking resource renames between major versions, and
Airbyte's Terraform provider is similarly version-sensitive.

**Decision.** Snowflake objects are created by versioned, idempotent SQL (`infra/snowflake/sql`) executed with
`scripts/snowflake_sql.py`; the Airbyte pipeline is applied through its public API (`airbyte/apply_airbyte.py`).

**Consequences.** Simple to read and review; no provider pinning problems. State is not tracked (no drift detection).
For a multi-environment production estate, port these files to Terraform with a pinned provider version.
