"""Dagster resources: Airbyte public-API client and database connections."""

from __future__ import annotations

import time
from pathlib import Path

import dagster as dg
import requests

TERMINAL_OK = {"succeeded"}
TERMINAL_BAD = {"failed", "cancelled"}


class AirbyteResource(dg.ConfigurableResource):
    """Triggers a sync on one Airbyte connection and blocks until it finishes."""

    api_url: str = "http://localhost:8000/api/public/v1"
    client_id: str
    client_secret: str
    connection_id: str
    poll_interval_seconds: int = 15
    timeout_seconds: int = 3600

    def _headers(self) -> dict[str, str]:
        r = requests.post(
            f"{self.api_url.rstrip('/')}/applications/token",
            json={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant-type": "client_credentials",
            },
            timeout=30,
        )
        r.raise_for_status()
        return {"Authorization": f"Bearer {r.json()['access_token']}"}

    def run_sync(self, log: dg.DagsterLogManager) -> dict:
        base = self.api_url.rstrip("/")
        h = self._headers()
        r = requests.post(
            f"{base}/jobs",
            headers=h,
            timeout=60,
            json={"connectionId": self.connection_id, "jobType": "sync"},
        )
        if r.status_code == 409:  # a sync is already running: wait for it instead of failing
            running = requests.get(
                f"{base}/jobs",
                headers=h,
                timeout=30,
                params={
                    "connectionId": self.connection_id,
                    "jobType": "sync",
                    "status": "running",
                    "limit": 1,
                },
            )
            running.raise_for_status()
            data = running.json().get("data", [])
            if not data:
                raise dg.Failure("Airbyte reported a conflicting job but none is running")
            job = data[0]
            log.info("Attaching to already-running Airbyte job %s", job["jobId"])
        else:
            r.raise_for_status()
            job = r.json()
        job_id = job["jobId"]
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            s = requests.get(f"{base}/jobs/{job_id}", headers=h, timeout=30)
            s.raise_for_status()
            job = s.json()
            status = job.get("status")
            log.info("Airbyte job %s status=%s rows=%s", job_id, status, job.get("rowsSynced"))
            if status in TERMINAL_OK:
                return job
            if status in TERMINAL_BAD:
                raise dg.Failure(
                    f"Airbyte job {job_id} ended with status '{status}'",
                    metadata={"job_id": str(job_id), "status": str(status)},
                )
            if time.monotonic() > deadline:
                raise dg.Failure(f"Airbyte job {job_id} exceeded {self.timeout_seconds}s")
            time.sleep(self.poll_interval_seconds)


class PostgresResource(dg.ConfigurableResource):
    dsn: str

    def connect(self):
        import psycopg

        return psycopg.connect(self.dsn, autocommit=True)


class SnowflakeResource(dg.ConfigurableResource):
    """Key-pair connection used by monitoring jobs (read-only role)."""

    account: str
    user: str
    private_key_path: str
    private_key_passphrase: str = ""
    role: str = "TRANSFORMER"
    warehouse: str = "TRANSFORM_WH"
    database: str = "SHOPSPHERE_ANALYTICS"

    def connect(self):
        import snowflake.connector
        from cryptography.hazmat.primitives import serialization

        key = serialization.load_pem_private_key(
            Path(self.private_key_path).read_bytes(),
            self.private_key_passphrase.encode() if self.private_key_passphrase else None,
        )
        der = key.private_bytes(
            serialization.Encoding.DER,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            private_key=der,
            role=self.role,
            warehouse=self.warehouse,
            database=self.database,
        )
