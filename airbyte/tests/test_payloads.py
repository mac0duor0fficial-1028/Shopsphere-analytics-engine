import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import apply_airbyte as a  # noqa: E402


def test_source_is_cdc_pgoutput():
    cfg = a.source_configuration({})
    rm = cfg["replication_method"]
    assert rm["method"] == "CDC" and rm["plugin"] == "pgoutput"
    assert rm["replication_slot"] == "airbyte_slot" and rm["publication"] == "shopsphere_pub"


def test_connection_uses_append_for_every_table():
    p = a.connection_payload("s", "d", {})
    streams = p["configurations"]["streams"]
    assert {s["name"] for s in streams} == set(a.TABLES)
    assert all(s["syncMode"] == "incremental_append" for s in streams)


def test_schedule_manual_vs_cron():
    assert a.schedule({"AIRBYTE_SCHEDULE": "manual"}) == {"scheduleType": "manual"}
    assert a.schedule({"AIRBYTE_SCHEDULE": "0 */10 * * * ?"})["scheduleType"] == "cron"


def test_redact_hides_secrets():
    out = a.redact({"password": "x", "nested": {"private_key": "k", "host": "h"}})
    assert out["password"] == "<redacted>" and out["nested"]["private_key"] == "<redacted>"
    assert out["nested"]["host"] == "h"
