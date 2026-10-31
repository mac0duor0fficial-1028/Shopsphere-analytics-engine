import os
from pathlib import Path

import pytest

os.environ["DAGSTER_IS_DEV_CLI"] = "1"  # lets DbtProject.prepare_if_dev() run `dbt parse`
os.environ.setdefault("SNOWFLAKE_ACCOUNT", "x")
os.environ.setdefault("DBT_SNOWFLAKE_USER", "x")
os.environ.setdefault("SNOWFLAKE_PRIVATE_KEY_PATH", "/tmp/none.p8")
os.environ.setdefault("DBT_PROFILES_DIR", str(Path(__file__).resolve().parents[2] / "dbt"))

dbt_deps = Path(__file__).resolve().parents[2] / "dbt" / "dbt_packages"
pytestmark = pytest.mark.skipif(not dbt_deps.exists(), reason="run `make dbt-deps` first")


def test_definitions_load_and_wire_up():
    from dagster import AssetKey

    from shopsphere_orchestration.definitions import defs

    repo = defs.get_repository_def()
    assert {"hot_pipeline_job", "daily_full_job", "cdc_health_job", "reconciliation_job"} <= set(
        repo.job_names
    )

    keys = {k.to_user_string() for k in repo.assets_defs_by_key}
    assert "shopsphere_raw/orders" in keys  # produced by the Airbyte multi-asset
    assert any(k.endswith("fct_orders") for k in keys)

    # The dbt staging model must depend on the Airbyte-produced raw asset.
    raw_orders = AssetKey(["shopsphere_raw", "orders"])
    deps = {
        d for ad in repo.assets_defs_by_key.values() for ds in ad.asset_deps.values() for d in ds
    }
    assert raw_orders in deps
