"""Idempotently create/update the Airbyte Postgres(CDC) -> Snowflake pipeline via the public API.

Works against Airbyte Cloud (https://api.airbyte.com/v1) and self-hosted abctl
(http://localhost:8000/api/public/v1). Objects are matched by name, so re-running is safe.

    python airbyte/apply_airbyte.py --dry-run   # print redacted payloads only
    python airbyte/apply_airbyte.py             # apply

IMPORTANT: field names follow the Airbyte public API / connector specs current at the time of
writing. Connector specs evolve; if the API rejects a field, compare with the spec shown in the
Airbyte UI ("Setup guide" / API reference) for your connector version.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
from pathlib import Path

import requests

TABLES = ["customers", "products", "orders", "order_items", "payments", "inventory", "events"]
SOURCE_NAME = "shopsphere-postgres-cdc"
DEST_NAME = "shopsphere-snowflake-raw"
CONNECTION_NAME = "shopsphere-postgres-to-snowflake"
SECRET_KEYS = {"password", "private_key", "private_key_password", "client_secret"}


def env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        raise SystemExit(f"Missing required environment variable: {name}")
    return v


def source_configuration(e=os.environ) -> dict:
    return {
        "sourceType": "postgres",
        "host": e.get("AIRBYTE_SOURCE_PG_HOST", "host.docker.internal"),
        "port": int(e.get("POSTGRES_PORT", "5432")),
        "database": e.get("POSTGRES_DB", "shopsphere"),
        "schemas": ["shop"],
        "username": "airbyte_cdc",
        "password": e.get("AIRBYTE_CDC_PASSWORD", ""),
        "ssl_mode": {"mode": e.get("AIRBYTE_SOURCE_PG_SSL_MODE", "prefer")},
        "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
        "replication_method": {
            "method": "CDC",
            "plugin": "pgoutput",
            "replication_slot": "airbyte_slot",
            "publication": "shopsphere_pub",
            "initial_waiting_seconds": int(e.get("AIRBYTE_INITIAL_WAITING_SECONDS", "1200")),
            "queue_size": 10000,
            # Only advance the slot after data is safely in Snowflake => no loss on failed syncs.
            "lsn_commit_behaviour": "After loading Data in the destination",
        },
    }


def destination_configuration(e=os.environ) -> dict:
    key_path = Path(e.get("AIRBYTE_SNOWFLAKE_PRIVATE_KEY_PATH", "./secrets/svc_airbyte.p8"))
    credentials = {
        "auth_type": "Key Pair Authentication",
        "private_key": key_path.read_text() if key_path.exists() else "",
    }
    if e.get("AIRBYTE_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
        credentials["private_key_password"] = e["AIRBYTE_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]
    return {
        "destinationType": "snowflake",
        "host": f"{e.get('SNOWFLAKE_ACCOUNT', '')}.snowflakecomputing.com",
        "role": "LOADER",
        "warehouse": "LOADING_WH",
        "database": "SHOPSPHERE_RAW",
        "schema": "AIRBYTE",
        "username": "SVC_AIRBYTE",
        "credentials": credentials,
        "raw_data_schema": "AIRBYTE_INTERNAL",
        "disable_type_dedupe": False,
    }


def schedule(e=os.environ) -> dict:
    s = e.get("AIRBYTE_SCHEDULE", "manual")
    return (
        {"scheduleType": "manual"}
        if s == "manual"
        else {"scheduleType": "cron", "cronExpression": s}
    )


def connection_payload(source_id: str, destination_id: str, e=os.environ) -> dict:
    return {
        "name": CONNECTION_NAME,
        "sourceId": source_id,
        "destinationId": destination_id,
        "status": "active",
        "schedule": schedule(e),
        "namespaceDefinition": "destination",
        "nonBreakingSchemaUpdatesBehavior": "propagate_columns",
        # Append (not append+dedupe): keep the full CDC change log; dbt dedupes and tracks deletes.
        "configurations": {
            "streams": [{"name": t, "syncMode": "incremental_append"} for t in TABLES]
        },
    }


def redact(obj):
    if isinstance(obj, dict):
        return {k: ("<redacted>" if k in SECRET_KEYS and v else redact(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact(v) for v in obj]
    return obj


class Airbyte:
    def __init__(self, base: str, client_id: str, client_secret: str, workspace_id: str):
        self.base, self.workspace_id = base.rstrip("/"), workspace_id
        r = requests.post(
            f"{self.base}/applications/token",
            json={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant-type": "client_credentials",
            },
            timeout=30,
        )
        self._check(r)
        self.s = requests.Session()
        self.s.headers.update({"Authorization": f"Bearer {r.json()['access_token']}"})

    @staticmethod
    def _check(r: requests.Response) -> None:
        if not r.ok:
            raise SystemExit(
                f"Airbyte API error {r.status_code} {r.request.method} {r.url}: {r.text[:800]}"
            )

    def find(self, kind: str, name: str) -> dict | None:
        r = self.s.get(
            f"{self.base}/{kind}",
            params={"workspaceIds": self.workspace_id, "limit": 100},
            timeout=30,
        )
        self._check(r)
        return next((x for x in r.json().get("data", []) if x["name"] == name), None)

    def upsert(self, kind: str, id_key: str, name: str, payload: dict) -> str:
        existing = self.find(kind, name)
        if existing:
            body = {
                k: v
                for k, v in payload.items()
                if k not in ("workspaceId", "sourceId", "destinationId")
            }
            r = self.s.patch(f"{self.base}/{kind}/{existing[id_key]}", json=body, timeout=60)
            self._check(r)
            print(f"updated {kind[:-1]} {name} ({existing[id_key]})")
            return existing[id_key]
        r = self.s.post(f"{self.base}/{kind}", json=payload, timeout=60)
        self._check(r)
        print(f"created {kind[:-1]} {name} ({r.json()[id_key]})")
        return r.json()[id_key]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    src = {
        "name": SOURCE_NAME,
        "workspaceId": os.environ.get("AIRBYTE_WORKSPACE_ID", "<workspace>"),
        "configuration": source_configuration(),
    }
    dst = {
        "name": DEST_NAME,
        "workspaceId": os.environ.get("AIRBYTE_WORKSPACE_ID", "<workspace>"),
        "configuration": destination_configuration(),
    }
    if args.dry_run:
        print(
            json.dumps(
                redact(
                    {
                        "source": src,
                        "destination": dst,
                        "connection": connection_payload("<sourceId>", "<destinationId>"),
                    }
                ),
                indent=2,
            )
        )
        return 0

    ab = Airbyte(
        env("AIRBYTE_API_URL", "http://localhost:8000/api/public/v1"),
        env("AIRBYTE_CLIENT_ID"),
        env("AIRBYTE_CLIENT_SECRET"),
        env("AIRBYTE_WORKSPACE_ID"),
    )
    source_id = ab.upsert("sources", "sourceId", SOURCE_NAME, copy.deepcopy(src))
    dest_id = ab.upsert("destinations", "destinationId", DEST_NAME, copy.deepcopy(dst))
    conn_id = ab.upsert(
        "connections", "connectionId", CONNECTION_NAME, connection_payload(source_id, dest_id)
    )
    print(f"\nSet AIRBYTE_CONNECTION_ID={conn_id} in your .env (used by Dagster).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
