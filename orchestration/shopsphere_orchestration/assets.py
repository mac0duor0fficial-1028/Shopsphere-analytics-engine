"""Assets: Airbyte CDC sync -> dbt (sources freshness, models, snapshots, tests)."""

from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from .resources import AirbyteResource

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt"
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()

SOURCE_TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "inventory",
    "events",
]
RAW_KEYS = [
    dg.AssetKey(["shopsphere_raw", t]) for t in SOURCE_TABLES
]  # == dbt's default source keys


@dg.multi_asset(
    name="airbyte_cdc_sync",
    specs=[
        dg.AssetSpec(key=k, group_name="ingestion", kinds={"airbyte", "snowflake"})
        for k in RAW_KEYS
    ],
)
def airbyte_cdc_sync(context: dg.AssetExecutionContext, airbyte: AirbyteResource):
    """One Airbyte job syncs all seven tables from the shared WAL slot."""
    job = airbyte.run_sync(context.log)
    for key in RAW_KEYS:
        yield dg.MaterializeResult(
            asset_key=key,
            metadata={
                "airbyte_job_id": str(job.get("jobId")),
                "rows_synced": job.get("rowsSynced"),
                "bytes_synced": job.get("bytesSynced"),
            },
        )


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def shopsphere_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    # Fail fast (error-level freshness) before spending warehouse credits on stale data.
    dbt.cli(["source", "freshness"], context=context).wait()
    yield from dbt.cli(["build"], context=context).stream()
