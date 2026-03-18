use arrow::{array::RecordBatch, buffer::Buffer, datatypes::SchemaRef, ipc::reader::StreamDecoder};
use futures::stream::{self, Stream};
use iroh::{
    Endpoint, EndpointAddr, RelayMode,
    endpoint::{ConnectError, Connection, RecvStream},
};
use n0_error::StdResultExt;
use tracing::debug;

use crate::{ALPN, Error, StreamToken};

struct ArrowStreamState {
    conn: Connection,
    recv: RecvStream,
    decoder: StreamDecoder,
    remaining: Buffer,
    batch: Option<RecordBatch>,
    endpoint: Endpoint,
}

impl Drop for ArrowStreamState {
    fn drop(&mut self) {
        // Iroh says to use Endpoint::close, which waits for the
        // CONNECTION_CLOSE to reach the server. But we don't care, because
        // we're the only ones receiving data, and the extra latency is stupid.
        self.conn.close(0_u8.into(), b"done");
    }
}

/// Connect, sending the desired stream token with the initial handshake
/// data to shave latency in some scenarios.
///
/// If this optimization was successful, the opened stream is returned along
/// with the connection. Otherwise, it should be opened manually.
async fn connect(
    server: EndpointAddr,
    initial_token: StreamToken,
) -> Result<(Connection, Endpoint, Option<RecvStream>), Error> {
    // let key_data = public_key
    //     .as_ref()
    //     .try_into()
    //     .map_err(|_| Error::InvalidKey)?;
    // let public_key = iroh::PublicKey::from_bytes(key_data).map_err(|_| Error::InvalidKey)?;

    // let relay_url = RelayUrl::from(relay);
    // let addr = iroh::EndpointAddr::new(public_key).with_relay_url(relay_url.clone());

    // TODO: we should maybe disable ipv6 proactively, since our servers don't
    // support it and it can cause extra startup latency. :(
    let endpoint = iroh::Endpoint::empty_builder()
        .relay_mode(RelayMode::custom(server.relay_urls().cloned()))
        .bind()
        .await?;

    let connecting = endpoint
        .connect_with_opts(server, ALPN, Default::default())
        .await
        .map_err(ConnectError::from)?;

    // TODO: since the client is always ephemeral, we have no way to cache
    // sessions and therefore 0rtt will never work. We could fix this by
    // providing our own (filesystem-based) cache, but iroh doesn't expose that
    // as an option (yet).
    let (conn, recv) = match connecting.into_0rtt() {
        Ok(outgoing) => {
            let (mut send, recv) = outgoing.open_bi().await?;
            send.write_all(initial_token.as_bytes()).await.anyerr()?;
            send.finish().map_err(|_| Error::StreamClosed)?;

            match outgoing
                .handshake_completed()
                .await
                .map_err(ConnectError::from)?
            {
                iroh::endpoint::ZeroRttStatus::Accepted(conn) => (conn, Some(recv)),
                iroh::endpoint::ZeroRttStatus::Rejected(conn) => {
                    // The stream doesn't exist from the perspective of
                    // the server.
                    debug!("0rtt rejected");
                    (conn, None)
                }
            }
        }
        Err(conn) => {
            debug!("0rtt not possible");
            (conn.await.map_err(ConnectError::from)?, None)
        }
    };

    Ok((conn, endpoint, recv))
}

/// Connect to an endpoint with the intention to download query results.
///
/// Returns the stream schema and a stream of record batches.
pub async fn fetch_query_results(
    server: EndpointAddr,
) -> Result<
    (
        SchemaRef,
        impl Stream<Item = Result<RecordBatch, Error>> + Unpin,
    ),
    Error,
> {
    let (conn, endpoint, recv) = connect(server, StreamToken::QueryResults).await?;

    let mut recv = match recv {
        Some(r) => r,
        None => {
            let (mut send, recv) = conn.open_bi().await?;
            send.write_all(StreamToken::QueryResults.as_bytes()).await?;
            send.finish().map_err(|_| Error::StreamClosed)?;
            recv
        }
    };

    // Read the schema first.
    let mut first_batch = None;
    let mut decoder = StreamDecoder::new();
    let mut remaining;

    let schema = loop {
        match recv.read_chunk(usize::MAX).await? {
            Some(chunk) => {
                remaining = Buffer::from(chunk.bytes);
                if let Some(batch) = decoder.decode(&mut remaining)? {
                    first_batch = Some(batch);
                    break decoder.schema().unwrap();
                }

                if let Some(schema) = decoder.schema() {
                    break schema;
                }
            }
            None => {
                decoder.finish()?;
                return Err(Error::StreamClosed);
            }
        }
    };

    let state = ArrowStreamState {
        endpoint,
        conn,
        recv,
        decoder,
        remaining,
        batch: first_batch,
    };

    let stream = Box::pin(stream::try_unfold(state, |mut state| async move {
        if let Some(batch) = state.batch.take() {
            return Ok(Some((batch, state)));
        }

        loop {
            if let Some(batch) = state.decoder.decode(&mut state.remaining)? {
                return Ok(Some((batch, state)));
            }

            match state.recv.read_chunk(usize::MAX).await? {
                Some(chunk) => {
                    state.remaining = Buffer::from(chunk.bytes);
                    if let Some(batch) = state.decoder.decode(&mut state.remaining)? {
                        return Ok(Some((batch, state)));
                    }
                }
                None => {
                    state.endpoint.close().await;
                    return Ok(None);
                }
            }
        }
    }));

    Ok((schema, stream))
}
