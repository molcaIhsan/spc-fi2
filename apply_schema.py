"""
One-off helper: apply sql/schema.sql to the real database. Not part of the
regular poc_consumer.py flow -- run this once before poc_consumer.py can
successfully write anything.

Uses the same PostgresConfig as everything else (spc/config.py), so host/port/
database defaults stay consistent -- only credentials need supplying via env.
"""

import sys
from pathlib import Path

import psycopg2

from spc.config import DEFAULT_CONFIG


def main():
    pg = DEFAULT_CONFIG.postgres
    if not pg.username or not pg.password:
        print("SPC_PG_USERNAME/SPC_PG_PASSWORD not set -- refusing to run without them.", file=sys.stderr)
        sys.exit(1)

    schema_path = Path(__file__).parent / "sql" / "schema.sql"
    sql_text = schema_path.read_text()

    conn = psycopg2.connect(
        host=pg.host, port=pg.port, dbname=pg.database,
        user=pg.username, password=pg.password,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql_text)
    finally:
        conn.close()

    print(f"Applied {schema_path} to {pg.host}:{pg.port}/{pg.database}")


if __name__ == "__main__":
    main()
