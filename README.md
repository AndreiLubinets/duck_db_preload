# Arrow S3 mock benchmark

A small Axum HTTP gateway and a DataFusion Flight SQL server. The gateway forwards object reads to a custom gRPC service and streams its Arrow IPC response to HTTP clients. Separately, Flight SQL accepts SQL queries, executes them with DataFusion, and streams Arrow results. The `events` table currently contains deterministic mock rows; the S3 step remains simulated.

## Run

```sh
nix develop
cargo run --release
```

The [Nix development shell](flake.nix) provides the Rust toolchain and `protoc` required by the build. Without Nix, install Rust/Cargo and Protocol Buffers compiler (`protoc`) separately.

The HTTP gateway listens on `0.0.0.0:3000`, Flight SQL on `0.0.0.0:50051`, and the HTTP gateway's internal custom gRPC service on `0.0.0.0:50052` by default. Configuration is available through environment variables:

| Variable | Default | Meaning |
| --- | ---: | --- |
| `PORT` | `3000` | HTTP listen port |
| `GRPC_PORT` | `50052` | Internal custom gRPC service port |
| `FLIGHT_PORT` | `50051` | DataFusion Flight SQL port |
| `MOCK_S3_DELAY_MS` | `25` | Simulated S3 request latency |
| `MOCK_ROWS` | `100000` | Rows returned unless overridden in the request |
| `BATCH_SIZE` | `8192` | Arrow record batch size unless overridden |

## Request

```sh
curl -v 'http://localhost:3000/objects/bench-bucket/dataset/events.parquet?rows=50000&batch_size=4096' \
  -o events.arrow
```

The response body is an Arrow IPC stream (`application/vnd.apache.arrow.stream`) with `row_id` (`Int64`), `source_object` (`Utf8`), and `value` (`Utf8`) columns. The gRPC method is declared in [arrow_bench.proto](/mnt/hdd/Files/Git/duck_db_preload/proto/arrow_bench.proto). It sends bounded chunks of the IPC stream; Axum forwards them as the HTTP body with backpressure.

`GET /health` returns `ok`. In a real S3 integration, replace `mock_s3_get_object` in the gRPC service with the S3 client call and map its result to `RecordBatch` values; the Axum HTTP gateway can stay the same.

## Query with Flight SQL

Flight SQL exposes the in-memory `events` table to clients. For example, connect with a Flight SQL JDBC/ADBC client to `grpc://localhost:50051` and run:

```sql
SELECT source_object, count(*) AS rows
FROM events
GROUP BY source_object
```

The server registers mock batches once at startup. To query real Parquet objects, replace that registration with a DataFusion table provider that scans the desired S3 paths (using DataFusion's object-store integration or a custom provider); then Flight SQL can run normal SQL over that provider. Avoid loading an unbounded dataset into the startup `MemTable` when moving beyond the mock.

## Preload into DuckDB with ADBC

Install the Python DuckDB ADBC driver and Arrow reader:

```sh
python3 -m pip install duckdb adbc_driver_manager pyarrow
scripts/preload_duckdb.sh \
  'http://localhost:3000/objects/bench-bucket/dataset/events.parquet?rows=50000&batch_size=4096' \
  ./benchmark.duckdb events
```

The shell script delegates to [preload_duckdb.py](/mnt/hdd/Files/Git/duck_db_preload/scripts/preload_duckdb.py), which reads the Axum HTTP response as an Arrow IPC stream and ingests its batches with DuckDB's ADBC API. It creates or replaces the requested table without writing an intermediate file or materializing the full response in Python. `PYTHON_BIN` can select another Python interpreter. See the [DuckDB ADBC guide](https://duckdb.org/docs/current/clients/adbc) for the driver API and [Arrow IPC guide](https://duckdb.org/2025/05/23/arrow-ipc-support-in-duckdb) for stream support.
