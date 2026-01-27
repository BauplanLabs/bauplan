//! Support for fetching query results via Arrow Flight.

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time,
};

use arrow::{array::RecordBatch, datatypes::Schema};
use arrow_flight::{
    FlightClient, FlightEndpoint,
    error::{FlightError, Result as FlightResult},
};
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use http::Uri;
use serde_json::json;
use tonic::transport::{Channel, ClientTlsConfig};

/// Connects to a given flight server and streams all the batches from all the
/// endpoints. This is bauplan-specific and not generically useful.
pub async fn fetch_flight_results(
    endpoint: Uri,
    auth_token: String,
    client_timeout: time::Duration,
    row_limit: Option<u64>,
) -> FlightResult<(Schema, impl Stream<Item = FlightResult<RecordBatch>>)> {
    let channel = Channel::builder(endpoint)
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .unwrap()
        .timeout(client_timeout)
        .connect_lazy();

    // TODO: this is only supported by the legacy infra, and should be removed.
    let criteria = json!({"max_rows": row_limit}).to_string();
    let (schema, batches) = fetch(channel, auth_token, criteria).await?;

    // We'll enforce the row limit on the client side. To do that, we allocate
    // rows from the total to each RecordBatch, using an atomic int to track it.
    let remaining = row_limit.map(|v| Arc::new(AtomicI64::new(v as _)));
    let batches = batches
        .map_ok(move |b| {
            let Some(remaining) = remaining.as_ref() else {
                return b;
            };

            let n = b.num_rows() as i64;

            // Subtract our rows from the total. The return value is the
            // value before the subtraction.
            let r = remaining.fetch_sub(n, Ordering::SeqCst).max(0);

            let limit = std::cmp::min(r, n) as usize;
            b.slice(0, limit)
        })
        .try_take_while(|b| {
            // Short circuit the stream if we run out. `try_take_while` has a
            // slightly odd signature.
            let take = b.num_rows() > 0;
            futures::future::ready(Ok(take))
        });

    Ok((schema, batches))
}

async fn fetch(
    channel: Channel,
    auth_token: String,
    serialized_criteria: String,
) -> FlightResult<(Schema, impl Stream<Item = FlightResult<RecordBatch>>)> {
    let mut client = FlightClient::new(channel.clone());
    client.add_header("authorization", &format!("Bearer {auth_token}"))?;

    // This returns a list of "flights", each of which have 0..N endpoints.
    let mut flights_stream = client.list_flights(serialized_criteria).await?.peekable();

    // Read the first flight to get the schema. We have to do this because it's
    // the only way to get the schema if there are zero record batches (meaning
    // the final stream will be empty). This also means it's an error if there
    // are no flights.
    let Some(info) = flights_stream.try_next().await? else {
        return Err(FlightError::Tonic(Box::new(tonic::Status::cancelled(
            "ListFlights returned no flights",
        ))));
    };

    let schema = info.clone().try_decode_schema()?;

    // Rejoin the info with the rest of the stream.
    let flights_stream = stream::once(async { Ok(info) }).chain(flights_stream);

    // This is a stream of Result<FlightEndpoint, _>.
    let all_endpoints = flights_stream
        .map_ok(|flight| {
            stream::iter(flight.endpoint.into_iter().filter(|e| e.ticket.is_some()))
                .map(FlightResult::Ok) // try_flatten wants Stream<Result<Stream<Result>>>.
        })
        .try_flatten();

    // For up to two endpoints at once, fetch results. Flatten all the results
    // into one stream.
    let stream = all_endpoints
        .map_ok(move |ep| fetch_batches(channel.clone(), auth_token.clone(), ep))
        .try_buffer_unordered(2)
        .try_flatten();

    Ok((schema, stream))
}

async fn fetch_batches(
    channel: Channel,
    auth_token: String,
    endpoint: FlightEndpoint,
) -> FlightResult<impl Stream<Item = FlightResult<RecordBatch>>> {
    let mut client = FlightClient::new(channel);
    client.add_header("authorization", &format!("Bearer {auth_token}"))?;

    let stream = client.do_get(endpoint.ticket.unwrap()).await?;
    Ok(stream)
}
