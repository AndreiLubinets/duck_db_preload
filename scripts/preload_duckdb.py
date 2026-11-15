#!/usr/bin/env python3
"""Stream the Axum endpoint's Arrow IPC response into DuckDB via ADBC."""

import re
import sys
import urllib.request


DEFAULT_URL = "http://localhost:3000/objects/bench-bucket/dataset/events.parquet"
DEFAULT_DATABASE = "./benchmark.duckdb"
DEFAULT_TABLE = "preloaded_data"


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
        print(
            "Usage: scripts/preload_duckdb.sh [URL] [DATABASE_FILE] [TABLE]\n"
            "\nFetch an Arrow IPC stream from the Axum endpoint and ingest it into DuckDB via ADBC.\n"
            "Set PYTHON_BIN to select the Python interpreter.\n"
            "Requires: duckdb, adbc_driver_manager, pyarrow"
        )
        return 0

    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    database = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DATABASE
    table = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_TABLE
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        print(
            "Invalid table name: use letters, digits, and underscores; "
            "the first character must be a letter or underscore.",
            file=sys.stderr,
        )
        return 2

    try:
        import adbc_driver_duckdb.dbapi
        import pyarrow.ipc
    except ImportError as error:
        print(
            f"Missing Python dependency: {error.name}. Install with:\n"
            "  python3 -m pip install duckdb adbc_driver_manager pyarrow",
            file=sys.stderr,
        )
        return 1

    print(f"Loading {url} into {database}.{table}", file=sys.stderr)
    with urllib.request.urlopen(url) as response:
        reader = pyarrow.ipc.open_stream(response)
        with adbc_driver_duckdb.dbapi.connect(database) as connection:
            with connection.cursor() as cursor:
                cursor.adbc_ingest(table, reader, mode="replace")
            connection.commit()

    print(f"Preload complete: {table}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Preload failed: {error}", file=sys.stderr)
        raise SystemExit(1)
