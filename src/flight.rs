//! Support for fetching query results via Arrow Flight.

use std::{pin::Pin, time};

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
    traceparent: Option<&str>,
) -> FlightResult<(
    Schema,
    impl Stream<Item = FlightResult<RecordBatch>> + use<>,
)> {
    let channel = Channel::builder(endpoint)
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .unwrap()
        .timeout(client_timeout)
        .connect_lazy();

    // TODO: this is only supported by the legacy infra, and should be removed.
    let criteria = json!({"max_rows": row_limit}).to_string();
    let (schema, batches) =
        fetch(channel.clone(), auth_token.clone(), criteria, traceparent).await?;

    let batches = limit_rows(batches, row_limit);

    // After all batches are consumed, tell the flight server to shut down.
    //
    // Note that this doesn't fire if the batches/limit aren't consumed,
    // but we're moving away from this architecture anyway and the lack of async
    // drop makes a more robust solution very difficult.
    let shutdown = stream::unfold(Some((channel, auth_token)), |state| async {
        if let Some((channel, token)) = state {
            let _ = shutdown(channel, token).await;
        }
        None
    });

    Ok((schema, batches.chain(shutdown)))
}

async fn fetch(
    channel: Channel,
    auth_token: String,
    serialized_criteria: String,
    traceparent: Option<&str>,
) -> FlightResult<(
    Schema,
    impl Stream<Item = FlightResult<RecordBatch>> + use<>,
)> {
    let mut client = FlightClient::new(channel.clone());
    client.add_header("authorization", &format!("Bearer {auth_token}"))?;
    if let Some(tp) = traceparent {
        client.add_header("traceparent", tp)?;
    }

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

/// Truncates a stream of record batches to at most `row_limit` rows total.
pub fn limit_rows<E>(
    stream: impl Stream<Item = Result<RecordBatch, E>>,
    row_limit: Option<u64>,
) -> impl Stream<Item = Result<RecordBatch, E>> {
    let remaining = row_limit.unwrap_or(u64::MAX);
    stream::try_unfold(
        (Pin::from(Box::new(stream)), remaining),
        |(mut stream, remaining)| async move {
            if remaining == 0 {
                return Ok(None);
            }

            let Some(batch) = stream.try_next().await? else {
                return Ok(None);
            };

            let limit = remaining.min(batch.num_rows() as u64);
            let batch = batch.slice(0, limit as usize);
            Ok(Some((batch, (stream, remaining - limit))))
        },
    )
}

async fn shutdown(channel: Channel, auth_token: String) -> FlightResult<()> {
    let mut client = FlightClient::new(channel);
    client.add_header("authorization", &format!("Bearer {auth_token}"))?;

    let mut stream = client
        .do_action(arrow_flight::Action::new("shutdown", ""))
        .await?;
    while stream.next().await.is_some() {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_enforce_row_limit() -> anyhow::Result<()> {
        let make_batch = |values: &[i32]| {
            let array = arrow::array::Int32Array::from(values.to_vec());
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
            RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
        };

        let input = stream::iter(vec![make_batch(&[1, 2, 3]), make_batch(&[4, 5, 6])]).map(FlightResult::Ok);
        let batches: Vec<RecordBatch> = limit_rows(input, Some(4)).try_collect().await?;

        let row_counts: Vec<_> = batches.iter().map(|b| b.num_rows()).collect();
        assert_eq!(row_counts, vec![3, 1]);
        Ok(())
    }
}
