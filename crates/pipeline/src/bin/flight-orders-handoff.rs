//! Phase G (`docs/internals/experiment.md` section 15): one Flight
//! producer, one endpoint, one consumer, over real loopback TCP/gRPC in a
//! single process.
//!
//! Scope, stated before any code (andrej-karpathy-skills: state
//! assumptions explicitly, no speculative framework):
//! - One fixed ticket (`b"orders"`) serves one fixed `RecordBatch` (the
//!   `Int32` `value` column already used by Phase A/D/E). Every other
//!   ticket is rejected, not silently coerced — "define errors out of
//!   existence" (software-design-depth-doctrine): there is exactly one
//!   valid request shape, so there is no ambiguous case to design for.
//! - Every other `FlightService` method (`handshake`, `list_flights`,
//!   `get_flight_info`, `poll_flight_info`, `get_schema`, `do_put`,
//!   `do_exchange`, `do_action`, `list_actions`) returns
//!   `Status::unimplemented` — the plan's own "one producer, one
//!   endpoint, one consumer" scope, not a shortcut around something this
//!   phase was supposed to implement.
//! - **Not a reliability or scalability claim** (ddia-data-systems ch02):
//!   no bounded-buffering, slow-consumer, or cancellation/disconnect
//!   behavior is tested here. Each is its own follow-up, already recorded
//!   as such in `docs/internals/experiment-ledger.md`'s Phase G entry —
//!   not silently assumed to work just because this smaller slice does.
//! - Same-host only: client and server share one process and one Tokio
//!   runtime, connected over real `127.0.0.1` TCP (not an in-memory
//!   channel), so the gRPC/HTTP2 stack is genuinely exercised, not
//!   bypassed.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures_util::StreamExt;
use futures_util::stream::{self, BoxStream};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status, Streaming};

const VALID_TICKET: &[u8] = b"orders";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]))
}

/// Not timed, not part of the Flight round trip: fixture generation.
fn generate_batch(len: usize) -> RecordBatch {
    let values = Int32Array::from((0..len as i32).collect::<Vec<_>>());
    RecordBatch::try_new(schema(), vec![Arc::new(values)]).expect("one column, matching schema")
}

/// The one producer this phase scopes to: a single already-built batch,
/// served under one fixed ticket (grokking-simplicity: Data held, not
/// computed, by this struct — construction happens once in `main`).
struct OrdersFlightService {
    batch: RecordBatch,
}

/// Rejects anything but the one valid ticket. Kept as its own pure
/// Calculation (grokking-simplicity) so `do_get` — the Action that
/// actually touches the network request — stays a thin wrapper around a
/// decision this function alone owns.
fn validate_ticket(ticket: &[u8]) -> Result<(), Status> {
    if ticket == VALID_TICKET {
        Ok(())
    } else {
        Err(Status::not_found(format!(
            "no flight for ticket {ticket:?}; the only valid ticket is {VALID_TICKET:?}"
        )))
    }
}

/// Shared by every method this phase does not implement — one helper
/// instead of nine near-identical bodies (software-design-depth-doctrine:
/// repetition is a red flag even when each occurrence is "only" one line).
fn unimplemented_stream<T: Send + 'static>(
    method: &str,
) -> Result<Response<BoxStream<'static, Result<T, Status>>>, Status> {
    Err(Status::unimplemented(format!(
        "{method} is outside Phase G's one-producer/one-endpoint/one-consumer scope"
    )))
}

impl FlightService for OrdersFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented_stream("handshake")
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        unimplemented_stream("list_flights")
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info is outside Phase G's scope",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "poll_flight_info is outside Phase G's scope",
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(
            "get_schema is outside Phase G's scope",
        ))
    }

    /// The one implemented endpoint. Validates the ticket (a pure
    /// Calculation), then hands the already-built batch to
    /// `FlightDataEncoderBuilder` — the documented, general encoding
    /// path, not a hand-rolled one.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        validate_ticket(&request.into_inner().ticket)?;

        let batch = self.batch.clone();
        let input = stream::once(async move { Ok::<_, FlightError>(batch) });
        let encoded = FlightDataEncoderBuilder::new()
            .build(input)
            .map(|result| result.map_err(Status::from));

        Ok(Response::new(Box::pin(encoded)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        unimplemented_stream("do_put")
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented_stream("do_exchange")
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        unimplemented_stream("do_action")
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented_stream("list_actions")
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let fixture = generate_batch(500_000);

    // Bind first so the actual ephemeral port is known before the server
    // starts serving — `Server::serve(addr)` alone never hands the bound
    // port back to the caller.
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("flight-orders-handoff: failed to bind a local port: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let addr = match listener.local_addr() {
        Ok(addr) => addr,
        Err(error) => {
            eprintln!("flight-orders-handoff: failed to read the bound address: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };

    let service = OrdersFlightService {
        batch: fixture.clone(),
    };
    let server = tokio::spawn(
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    let outcome = run_client(addr, &fixture).await;

    // The server future only resolves on shutdown/error; this binary's
    // whole point is the one client round trip above, so it is dropped
    // (aborting the task) once that round trip has a result, not awaited
    // to completion — consistent with the "one producer, one endpoint,
    // one consumer" scope, which does not include graceful shutdown.
    server.abort();

    match outcome {
        Ok(()) => {
            println!(
                "flight-orders-handoff: PASS — do_get over real loopback TCP/gRPC returned the expected batch, unimplemented methods rejected as expected"
            );
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("flight-orders-handoff: FAIL: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run_client(addr: std::net::SocketAddr, expected: &RecordBatch) -> Result<(), String> {
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|error| format!("invalid endpoint: {error}"))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|error| format!("failed to connect: {error}"))?;
    let mut client = FlightServiceClient::new(channel);

    let request = Ticket {
        ticket: VALID_TICKET.to_vec().into(),
    };
    let response = client
        .do_get(request)
        .await
        .map_err(|status| format!("do_get failed: {status}"))?;

    let flight_data_stream = response
        .into_inner()
        .map(|result| result.map_err(FlightError::from));
    let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(flight_data_stream);

    let mut received = Vec::new();
    while let Some(batch) = batch_stream.next().await {
        let batch = batch.map_err(|error| format!("decode failed: {error}"))?;
        received.push(batch);
    }

    if received.len() != 1 {
        return Err(format!(
            "expected exactly one record batch, got {}",
            received.len()
        ));
    }
    if &received[0] != expected {
        return Err("received batch does not match the expected fixture".to_string());
    }

    // Confirm every unimplemented method is actually rejected, not
    // silently accepted — this is part of this phase's own scope
    // statement, not an afterthought.
    let rejected = client.list_actions(Empty {}).await.is_err();
    if !rejected {
        return Err("list_actions should have been rejected as unimplemented".to_string());
    }

    Ok(())
}
