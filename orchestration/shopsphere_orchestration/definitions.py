"""Dagster code location: ingestion -> transformation -> monitoring, with Slack alerting."""

from __future__ import annotations

import os

import dagster as dg
import requests
from dagster_dbt import DbtCliResource, build_dbt_asset_selection

from .assets import airbyte_cdc_sync, dbt_project, shopsphere_dbt_assets
from .monitoring import cdc_health_job, reconciliation_job
from .resources import AirbyteResource, PostgresResource, SnowflakeResource

# Hot path: sync CDC, then build the tagged "hot" models (staging, intermediate, core facts/marts).
hot_selection = dg.AssetSelection.assets(airbyte_cdc_sync) | build_dbt_asset_selection(
    [shopsphere_dbt_assets], dbt_select="tag:hot"
)
hot_pipeline_job = dg.define_asset_job("hot_pipeline_job", selection=hot_selection)
# Daily: everything (all marts, snapshots, full test suite).
daily_full_job = dg.define_asset_job("daily_full_job", selection=dg.AssetSelection.all())

schedules = [
    dg.ScheduleDefinition(
        job=hot_pipeline_job, cron_schedule="*/15 * * * *", execution_timezone="UTC"
    ),
    dg.ScheduleDefinition(job=daily_full_job, cron_schedule="0 3 * * *", execution_timezone="UTC"),
    dg.ScheduleDefinition(
        job=cdc_health_job, cron_schedule="*/5 * * * *", execution_timezone="UTC"
    ),
    dg.ScheduleDefinition(
        job=reconciliation_job, cron_schedule="30 4 * * *", execution_timezone="UTC"
    ),
]


@dg.run_failure_sensor(name="slack_on_failure", default_status=dg.DefaultSensorStatus.RUNNING)
def slack_on_failure(context: dg.RunFailureSensorContext):
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        context.log.warning("SLACK_WEBHOOK_URL not set; failure not forwarded")
        return
    run = context.dagster_run
    text = f":rotating_light: *{run.job_name}* failed (run `{run.run_id[:8]}`): {context.failure_event.message}"
    requests.post(url, json={"text": text[:2900]}, timeout=10).raise_for_status()


defs = dg.Definitions(
    assets=[airbyte_cdc_sync, shopsphere_dbt_assets],
    jobs=[hot_pipeline_job, daily_full_job, cdc_health_job, reconciliation_job],
    schedules=schedules,
    sensors=[slack_on_failure],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
        "airbyte": AirbyteResource(
            api_url=dg.EnvVar("AIRBYTE_API_URL"),
            client_id=dg.EnvVar("AIRBYTE_CLIENT_ID"),
            client_secret=dg.EnvVar("AIRBYTE_CLIENT_SECRET"),
            connection_id=dg.EnvVar("AIRBYTE_CONNECTION_ID"),
        ),
        "postgres": PostgresResource(dsn=dg.EnvVar("PG_DSN")),
        "snowflake": SnowflakeResource(
            account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
            user=dg.EnvVar("DBT_SNOWFLAKE_USER"),
            private_key_path=dg.EnvVar("SNOWFLAKE_PRIVATE_KEY_PATH"),
            private_key_passphrase=dg.EnvVar("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        ),
    },
)
