#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
exec "${PYTHON_BIN:-python3}" "$script_dir/preload_duckdb.py" "$@"
