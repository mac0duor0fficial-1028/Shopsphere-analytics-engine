"""Pipeline health checks: CDC replication-slot lag and Postgres <-> Snowflake reconciliation."""

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import dagster as dg

from .resources import PostgresResource, SnowflakeResource

SLOT_NAME = "airbyte_slot"
RECON_TABLES = {
    # postgres table -> (snowflake relation, where-clause for live rows, timestamp column)
    "orders": ("SHOPSPHERE_ANALYTICS.MARTS.FCT_ORDERS", "not is_deleted", "order_ts"),
    "payments": ("SHOPSPHERE_ANALYTICS.MARTS.FCT_PAYMENTS", "not is_deleted", "created_at"),
}


@dataclass(frozen=True)
class SlotHealth:
    exists: bool
    wal_status: str | None
    retained_mb: float
    confirmed_lag_mb: float


def evaluate_slot(h: SlotHealth, max_retained_mb: float) -> list[str]:
    """Return human-readable problems (empty list == healthy).

    NOTE: `active = false` is NORMAL between Airbyte micro-batches, so it is deliberately not alerted on.
    What matters is how much WAL the slot is pinning, and whether it has been invalidated.
    """
    if not h.exists:
        return [f"replication slot '{SLOT_NAME}' does not exist"]
    problems = []
    if h.wal_status in ("unreserved", "lost"):
        problems.append(f"slot wal_status={h.wal_status} (data loss risk; plan a re-sync)")
    if h.retained_mb > max_retained_mb:
        problems.append(f"slot retains {h.retained_mb:.0f} MB of WAL (> {max_retained_mb:.0f} MB)")
    return problems


def evaluate_recon(
    pg: tuple[int, Decimal], sf: tuple[int, Decimal], tolerance_pct: float
) -> list[str]:
    problems = []
    for label, a, b in (
        ("row count", Decimal(pg[0]), Decimal(sf[0])),
        ("sum(amount)", pg[1], sf[1]),
    ):
        base = max(abs(a), Decimal(1))
        drift = abs(a - b) / base * 100
        if drift > Decimal(str(tolerance_pct)):
            problems.append(
                f"{label}: postgres={a} snowflake={b} drift={drift:.3f}% > {tolerance_pct}%"
            )
    return problems


@dg.op(required_resource_keys={"postgres"})
def check_replication_slot(context: dg.OpExecutionContext) -> None:
    pg: PostgresResource = context.resources.postgres
    max_mb = float(os.environ.get("CDC_MAX_RETAINED_WAL_MB", "2048"))
    with pg.connect() as conn:
        row = conn.execute(
            "SELECT wal_status, "
            "  COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0) / 1048576.0, "
            "  COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0) / 1048576.0 "
            "FROM pg_replication_slots WHERE slot_name = %s",
            (SLOT_NAME,),
        ).fetchone()
    health = (
        SlotHealth(False, None, 0.0, 0.0)
        if row is None
        else SlotHealth(True, row[0], float(row[1]), float(row[2]))
    )
    context.log.info("slot health: %s", health)
    problems = evaluate_slot(health, max_mb)
    if problems:
        raise dg.Failure(
            "CDC slot unhealthy: " + "; ".join(problems),
            metadata={"retained_mb": health.retained_mb, "wal_status": str(health.wal_status)},
        )


@dg.op(required_resource_keys={"postgres", "snowflake"})
def reconcile_postgres_vs_snowflake(context: dg.OpExecutionContext) -> None:
    pgres: PostgresResource = context.resources.postgres
    sfres: SnowflakeResource = context.resources.snowflake
    tol = float(os.environ.get("RECON_TOLERANCE_PCT", "0.5"))
    # Ignore the in-flight window so normal sync latency is not reported as drift.
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=45)
    all_problems: list[str] = []
    with pgres.connect() as pg, sfres.connect() as sf:
        for table, (relation, live, ts_col) in RECON_TABLES.items():
            amount_col = "total_amount" if table == "orders" else "amount"
            pg_ts = "created_at"
            p_cnt, p_sum = pg.execute(
                f"SELECT count(*), COALESCE(sum({amount_col}), 0) FROM shop.{table} WHERE {pg_ts} < %s",
                (cutoff,),
            ).fetchone()
            cur = sf.cursor()
            cur.execute(
                f"SELECT count(*), COALESCE(sum({amount_col}), 0) FROM {relation} "
                f"WHERE {live} AND {ts_col} < %(cutoff)s",
                {"cutoff": cutoff},
            )
            s_cnt, s_sum = cur.fetchone()
            context.log.info(
                "%s postgres=(%s, %s) snowflake=(%s, %s)", table, p_cnt, p_sum, s_cnt, s_sum
            )
            all_problems += [
                f"{table} {p}"
                for p in evaluate_recon((p_cnt, Decimal(p_sum)), (s_cnt, Decimal(s_sum)), tol)
            ]
    if all_problems:
        raise dg.Failure("Reconciliation drift: " + " | ".join(all_problems))


@dg.job
def cdc_health_job():
    check_replication_slot()


@dg.job
def reconciliation_job():
    reconcile_postgres_vs_snowflake()
