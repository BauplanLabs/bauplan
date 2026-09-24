//! Bindings for fetching query results from a browser.

use bytes::Buf;
use futures::stream::{self, TryStreamExt};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

use crate::{BauplanPreset, Error};

/// Closes the endpoint once the stream is finished/canceled/dropped.
struct EndpointGuard(Endpoint);

impl Drop for EndpointGuard {
    fn drop(&mut self) {
        let endpoint = self.0.clone();
        n0_future::task::spawn(async move { endpoint.close().await });
    }
}

/// Fetch the results of a query from the runner, as a stream of Arrow IPC
/// bytes. Cancelling the stream closes the connection.
///
/// This uses iroh, but only over the relay via websocket (iroh doesn't support
/// path migration to QUIC in the browser).
///
/// The key should be in z32.
#[wasm_bindgen(js_name = fetchQueryResults, unchecked_return_type = "ReadableStream<Uint8Array>")]
pub async fn fetch_query_results(
    endpoint_id: &str,
    artifact_id: &str,
    auth_token: &str,
    limit: Option<u32>,
) -> Result<web_sys::ReadableStream, JsError> {
    let endpoint_id = EndpointId::from_z32(endpoint_id).map_err(|_| Error::InvalidKey)?;

    let preset = BauplanPreset::default();
    let addr = preset.add_relay_urls(EndpointAddr::new(endpoint_id));
    let endpoint = Endpoint::bind(preset).await.map_err(Error::from)?;
    let guard = EndpointGuard(endpoint.clone());

    let limit = limit.map(|x| x as u64);
    let (client, stream) =
        crate::client::read_runner_artifact(&endpoint, addr, artifact_id, auth_token, limit)
            .await?;
    let stream = stream::try_unfold(
        (stream, client, guard),
        |(mut stream, client, guard)| async move {
            match stream.recv_data().await? {
                Some(mut chunk) => {
                    let bytes = chunk.copy_to_bytes(chunk.remaining());
                    Ok(Some((bytes, (stream, client, guard))))
                }
                None => Ok::<_, Error>(None),
            }
        },
    )
    .map_ok(|chunk| JsValue::from(Uint8Array::from(chunk.as_ref())))
    .map_err(|err| JsValue::from(JsError::from(&err)));

    Ok(wasm_streams::ReadableStream::from_stream(stream).into_raw())
}
