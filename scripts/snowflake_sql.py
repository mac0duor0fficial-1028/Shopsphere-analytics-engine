"""Run idempotent SQL files against Snowflake with ${ENV_VAR} substitution.

Auth (admin bootstrap only): SNOWFLAKE_ACCOUNT + SNOWFLAKE_ADMIN_USER and either
SNOWFLAKE_ADMIN_PASSWORD or SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH.

    python scripts/snowflake_sql.py --role ACCOUNTADMIN file1.sql file2.sql
    python scripts/snowflake_sql.py --dry-run file1.sql        # print statements, no connection
"""

from __future__ import annotations

import argparse
import io
import os
import re
import sys
from pathlib import Path
from string import Template

from snowflake.connector.util_text import split_statements

_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def render(sql: str, env: dict[str, str]) -> str:
    """Substitute ${VAR}; fail loudly on missing variables instead of emitting broken SQL."""
    missing = sorted({m for m in _VAR.findall(sql) if not env.get(m)})
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    return Template(sql).substitute(env)


def statements(sql: str) -> list[str]:
    return [
        s.strip().rstrip(";")
        for s, _ in split_statements(io.StringIO(sql), remove_comments=True)
        if s.strip()
    ]


def connect(role: str):
    import snowflake.connector

    kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_ADMIN_USER"], role=role
    )
    key_path = os.environ.get("SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH")
    if key_path:
        from cryptography.hazmat.primitives import serialization

        pw = os.environ.get("SNOWFLAKE_ADMIN_PRIVATE_KEY_PASSPHRASE")
        key = serialization.load_pem_private_key(
            Path(key_path).read_bytes(), pw.encode() if pw else None
        )
        kwargs["private_key"] = key.private_bytes(
            serialization.Encoding.DER,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    else:
        kwargs["password"] = os.environ["SNOWFLAKE_ADMIN_PASSWORD"]
    return snowflake.connector.connect(**kwargs)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("files", nargs="+", type=Path)
    ap.add_argument("--role", default="ACCOUNTADMIN")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    plan = [(f, statements(render(f.read_text(), dict(os.environ)))) for f in args.files]
    if args.dry_run:
        for f, stmts in plan:
            print(f"-- {f} ({len(stmts)} statements)")
            for s in stmts:
                print(re.sub(r"RSA_PUBLIC_KEY = '[^']*'", "RSA_PUBLIC_KEY = '<redacted>'", s) + ";")
        return 0

    conn = connect(args.role)
    try:
        cur = conn.cursor()
        for f, stmts in plan:
            print(f"==> {f} ({len(stmts)} statements)")
            for s in stmts:
                label = re.sub(r"\s+", " ", s)[:110]
                try:
                    cur.execute(s)
                except Exception as exc:  # noqa: BLE001
                    print(f"FAILED: {label}\n  {exc}", file=sys.stderr)
                    return 1
                print(f"  ok  {label}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
