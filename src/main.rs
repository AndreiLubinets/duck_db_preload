use std::{io, net::SocketAddr, pin::Pin, time::Duration};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response as HttpResponse},
    routing::get,
    Router,
};
use bytes::Bytes;
use datafusion::{datasource::MemTable, prelude::SessionContext};
use datafusion_flight_sql_server::service::FlightSqlService;
use futures_util::{Stream, TryStreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request, Response as GrpcResponse, Status,
};
use tracing::{info, warn};

pub mod arrowbench {
    tonic::include_proto!("arrowbench.v1");
}

use arrowbench::{
    arrow_benchmark_client::ArrowBenchmarkClient,
    arrow_benchmark_server::{ArrowBenchmark, ArrowBenchmarkServer},
    ArrowChunk, StreamObjectRequest,
};

#[derive(Clone)]
struct AppState {
    mock_fetch_delay: Duration,
    rows_per_object: usize,
    batch_size: usize,
}

#[derive(Clone)]
struct ArrowBenchmarkService {
    state: AppState,
}

#[derive(Clone)]
struct GatewayState {
    grpc_channel: Channel,
    rows_per_object: usize,
    batch_size: usize,
}

#[derive(Deserialize)]
struct ReadOptions {
    rows: Option<u64>,
    batch_size: Option<u64>,
}

type ArrowStream = Pin<Box<dyn Stream<Item = Result<ArrowChunk, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl ArrowBenchmark for ArrowBenchmarkService {
    type StreamObjectStream = ArrowStream;

    /// Handles one object-read request and returns an Arrow IPC stream in gRPC chunks.
    ///
    /// The mock fetch delay happens before encoding. The bounded channel between the
    /// blocking Arrow writer and async gRPC stream provides backpressure while batches
    /// are serialized; replace `mock_s3_get_object` with the real S3-to-Arrow reader
    /// when connecting this benchmark to S3.
    async fn stream_object(
        &self,
        request: Request<StreamObjectRequest>,
    ) -> Result<GrpcResponse<Self::StreamObjectStream>, Status> {
        let request = request.into_inner();
        if request.bucket.is_empty() || request.key.is_empty() {
            return Err(Status::invalid_argument("bucket and key must be provided"));
        }
        let rows = if request.rows == 0 {
            self.state.rows_per_object
        } else {
            usize::try_from(request.rows).unwrap_or(usize::MAX)
        };
        let batch_size = if request.batch_size == 0 {
            self.state.batch_size
        } else {
            usize::try_from(request.batch_size)
                .unwrap_or(usize::MAX)
                .max(1)
        };
        let bucket = request.bucket;
        let key = request.key;

        // This is where a real S3 client would issue GetObject. Keep the simulated
        // latency outside the Arrow encoder, just as remote I/O happens before decoding.
        info!(%bucket, %key, rows, batch_size, "mock S3 GetObject");
        tokio::time::sleep(self.state.mock_fetch_delay).await;

        let (tx, rx) = mpsc::channel::<Result<ArrowChunk, Status>>(4);
        tokio::task::spawn_blocking(move || {
            let result = (|| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let output = ChannelWriter { tx: tx.clone() };
                let schema = schema();
                let mut writer = StreamWriter::try_new(output, &schema)?;
                for start in (0..rows).step_by(batch_size) {
                    let batch_rows = (rows - start).min(batch_size);
                    let batch = mock_s3_get_object(&bucket, &key, start, batch_rows, batch_size)?;
                    writer.write(&batch)?;
                }
                writer.finish()?;
                Ok(())
            })();

            if let Err(error) = result {
                warn!(%error, %bucket, %key, "Arrow stream encoding failed");
                let _ = tx.blocking_send(Err(Status::internal("Arrow stream encoding failed")));
            }
        });

        let output = Box::pin(futures_util::stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|item| (item, rx))
        }));
        Ok(GrpcResponse::new(output))
    }
}

/// Starts both benchmark paths: HTTP → custom gRPC → Arrow IPC, and SQL → DataFusion → Flight SQL.
///
/// The Flight SQL `events` table is populated once at startup from mock batches. The
/// HTTP route instead generates a fresh stream per request. These are independent paths.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let state = AppState {
        mock_fetch_delay: Duration::from_millis(env_usize("MOCK_S3_DELAY_MS", 25) as u64),
        rows_per_object: env_usize("MOCK_ROWS", 100_000),
        batch_size: env_usize("BATCH_SIZE", 8_192).max(1),
    };
    let http_addr: SocketAddr = format!("0.0.0.0:{}", env_usize("PORT", 3000)).parse()?;
    let grpc_port = env_usize("GRPC_PORT", 50052);
    let flight_port = env_usize("FLIGHT_PORT", 50051);
    let grpc_addr: SocketAddr = format!("0.0.0.0:{grpc_port}").parse()?;
    let grpc_listener = tokio::net::TcpListener::bind(grpc_addr).await?;
    let grpc_addr = grpc_listener.local_addr()?;
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    let grpc_service = ArrowBenchmarkServer::new(ArrowBenchmarkService {
        state: state.clone(),
    });

    // Flight SQL path: register a DataFusion table in memory, then expose the context
    // through the standard Flight SQL protocol. SQL queries run in this process.
    let flight_context = SessionContext::new();
    let mut batches = Vec::new();
    for start in (0..state.rows_per_object).step_by(state.batch_size) {
        let batch_rows = (state.rows_per_object - start).min(state.batch_size);
        batches.push(mock_s3_get_object(
            "bench-bucket",
            "dataset/events.parquet",
            start,
            batch_rows,
            state.batch_size,
        )?);
    }
    let table = MemTable::try_new(std::sync::Arc::new(schema()), vec![batches])?;
    flight_context.register_table("events", std::sync::Arc::new(table))?;
    let flight_addr = format!("0.0.0.0:{flight_port}");
    tokio::spawn(async move {
        info!(%flight_addr, "DataFusion Flight SQL service listening; query table: events");
        if let Err(error) = FlightSqlService::new(flight_context.state())
            .serve(flight_addr)
            .await
        {
            warn!(%error, "Flight SQL service stopped");
        }
    });

    // HTTP preload path: the Axum handler calls this custom gRPC service, which emits
    // Arrow IPC. It is separate from Flight SQL and does not execute SQL.
    tokio::spawn(async move {
        info!(%grpc_addr, "mock S3 gRPC service listening");
        if let Err(error) = Server::builder()
            .add_service(grpc_service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                grpc_listener,
            ))
            .await
        {
            warn!(%error, "gRPC service stopped");
        }
    });

    let grpc_endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{grpc_port}"))?;
    let gateway_state = GatewayState {
        grpc_channel: grpc_endpoint.connect_lazy(),
        rows_per_object: state.rows_per_object,
        batch_size: state.batch_size,
    };
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/objects/{bucket}/{*key}", get(read_object))
        .with_state(gateway_state);

    info!(%http_addr, "Axum Arrow HTTP gateway listening");
    axum::serve(http_listener, app).await?;
    Ok(())
}

/// Serves `GET /objects/{bucket}/{key}` by forwarding the request to custom gRPC.
///
/// The returned gRPC Arrow IPC chunks become the HTTP response body without being
/// collected into one large buffer. Query parameters `rows` and `batch_size` override
/// the configured defaults. This route feeds the existing DuckDB preload script.
async fn read_object(
    State(state): State<GatewayState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(options): Query<ReadOptions>,
) -> HttpResponse {
    let mut client = ArrowBenchmarkClient::new(state.grpc_channel);
    let request = StreamObjectRequest {
        bucket,
        key,
        rows: options.rows.unwrap_or(state.rows_per_object as u64),
        batch_size: options.batch_size.unwrap_or(state.batch_size as u64),
    };
    let grpc_response = match client.stream_object(request).await {
        Ok(response) => response,
        Err(error) => {
            warn!(%error, "gRPC object request failed");
            return (StatusCode::BAD_GATEWAY, "gRPC object request failed").into_response();
        }
    };

    let body_stream = grpc_response
        .into_inner()
        .map_ok(|chunk| Bytes::from(chunk.data))
        .map_err(|error| io::Error::other(error.to_string()));
    let mut response = HttpResponse::new(Body::from_stream(body_stream));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// Builds one deterministic mock batch for the requested range of row numbers.
///
/// `row_id` is the row number, `source_object` identifies a simulated Parquet part,
/// and `value` makes each row distinguishable. `split_size` controls how many rows
/// are assigned to each simulated part; no S3 request or Parquet decoding happens here.
fn mock_s3_get_object(
    bucket: &str,
    key: &str,
    start: usize,
    rows: usize,
    split_size: usize,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let seed = format!("{bucket}/{key}");
    let ids: Int64Array = (start..start + rows).map(|row| row as i64).collect();
    let object_keys: StringArray = (start..start + rows)
        .map(|row| Some(format!("{seed}/part-{:05}.parquet", row / split_size)))
        .collect();
    let values: StringArray = (start..start + rows)
        .map(|row| Some(format!("mock-value-{row}")))
        .collect();
    let columns: Vec<ArrayRef> = vec![
        std::sync::Arc::new(ids),
        std::sync::Arc::new(object_keys),
        std::sync::Arc::new(values),
    ];
    RecordBatch::try_new(std::sync::Arc::new(schema()), columns)
}

/// Defines the shared three-column schema used by mock batches and the IPC stream.
fn schema() -> Schema {
    Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("source_object", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ])
}

/// A synchronous Arrow writer that forwards IPC bytes to the gRPC response stream.
struct ChannelWriter {
    tx: mpsc::Sender<Result<ArrowChunk, Status>>,
}

impl io::Write for ChannelWriter {
    /// Sends each Arrow IPC write to gRPC in chunks up to 1 MiB.
    ///
    /// `blocking_send` is used because Arrow's `StreamWriter` writes synchronously on
    /// a blocking worker. If the client disconnects, the closed channel stops encoding.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        for chunk in buf.chunks(1024 * 1024) {
            self.tx
                .blocking_send(Ok(ArrowChunk {
                    data: Bytes::copy_from_slice(chunk).to_vec(),
                }))
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "gRPC client disconnected")
                })?;
        }
        Ok(buf.len())
    }

    /// The bounded Tokio channel does not need an additional flush operation.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Reads an unsigned integer setting from the environment, using `default` if absent or invalid.
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
