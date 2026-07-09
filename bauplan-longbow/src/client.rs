use arrow::{array::RecordBatch, buffer::Buffer, datatypes::SchemaRef, ipc::reader::StreamDecoder};
use bytes::Buf;
use futures::stream::{self, Stream};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{ConnectError, Connection, ConnectionState, RecvStream},
};
use n0_error::StdResultExt;
use tracing::debug;

use crate::{ALPN, Error, StreamToken};

/// An attached user code task. Dropping closes the connection.
#[derive(Debug)]
pub struct AttachedTask {
    /// Stdout from the task.
    pub stdout: RecvStream,
    /// Stderr from the task.
    pub stderr: RecvStream,

    conn: Connection,
}

impl Drop for AttachedTask {
    fn drop(&mut self) {
        self.conn.close(0_u8.into(), b"done");
    }
}

/// Connect to an endpoint with the intention to stream user code stdout/stderr.
///
/// Returns a handle with a separate stream for stdout and stderr, respectively.
/// Dropping the handle closes the connection.
pub async fn attach_task(endpoint: &Endpoint, server: EndpointAddr) -> Result<AttachedTask, Error> {
    let connecting = endpoint
        .connect_with_opts(server, ALPN, Default::default())
        .await
        .map_err(ConnectError::from)?;

    let conn = match connecting.into_0rtt() {
        Ok(outgoing) => {
            let stdout_fut = request_stream(&outgoing, StreamToken::UserCodeStdout);
            let stderr_fut = request_stream(&outgoing, StreamToken::UserCodeStderr);
            let (stdout_recv, stderr_recv) =
                futures::future::try_join(stdout_fut, stderr_fut).await?;

            match outgoing
                .handshake_completed()
                .await
                .map_err(ConnectError::from)?
            {
                iroh::endpoint::ZeroRttStatus::Accepted(conn) => {
                    return Ok(AttachedTask {
                        stdout: stdout_recv,
                        stderr: stderr_recv,
                        conn,
                    });
                }
                iroh::endpoint::ZeroRttStatus::Rejected(conn) => {
                    // The streams don't exist from the perspective of
                    // the server.
                    debug!("0rtt rejected");
                    conn
                }
            }
        }
        Err(conn) => {
            debug!("0rtt not possible");
            conn.await.map_err(ConnectError::from)?
        }
    };

    let stdout_fut = request_stream(&conn, StreamToken::UserCodeStdout);
    let stderr_fut = request_stream(&conn, StreamToken::UserCodeStderr);
    let (stdout_recv, stderr_recv) = futures::future::try_join(stdout_fut, stderr_fut).await?;

    Ok(AttachedTask {
        stdout: stdout_recv,
        stderr: stderr_recv,
        conn,
    })
}

struct ArrowStreamState<C, S> {
    stream: S,
    decoder: StreamDecoder,
    remaining: Buffer,
    batch: Option<RecordBatch>,

    _client: C,
}

/// Connect to an endpoint with the intention to download query results.
///
/// Returns the stream schema and a stream of record batches.
pub async fn fetch_query_results(
    endpoint: &Endpoint,
    addr: EndpointAddr,
    artifact_id: &str,
    auth_token: &str,
    limit: Option<u64>,
) -> Result<
    (
        SchemaRef,
        impl Stream<Item = Result<RecordBatch, Error>> + Unpin + use<>,
    ),
    Error,
> {
    let conn = endpoint
        .connect_with_opts(addr, b"h3", Default::default())
        .await
        .map_err(ConnectError::from)?
        .await
        .map_err(ConnectError::from)?;

    let h3_conn = iroh_h3::Connection::new(conn);
    let (mut driver, mut client) = h3::client::new(h3_conn).await?;

    // Drive the h3 connection in the background.
    tokio::spawn(async move {
        let err = driver.wait_idle().await;
        debug!(%err, "h3 connection closed");
    });

    let mut uri = format!("https://longbow/artifacts/{artifact_id}?arrow=true");
    if let Some(limit) = limit {
        uri = format!("{}&head={}", uri, limit);
    }

    let req = http::Request::builder()
        .method(http::Method::GET)
        .uri(uri)
        .header(http::header::AUTHORIZATION, format!("Bearer {auth_token}"))
        .body(())
        .unwrap();

    let mut stream = client.send_request(req).await?;
    stream.finish().await?;

    let resp = stream.recv_response().await?;
    if !resp.status().is_success() {
        return Err(Error::UnexpectedStatus(resp.status()));
    }

    // Read the schema first.
    let mut first_batch = None;
    let mut decoder = StreamDecoder::new();
    let mut remaining;

    let schema = loop {
        match stream.recv_data().await? {
            Some(mut chunk) => {
                // This should be zero-copy.
                remaining = chunk.copy_to_bytes(chunk.remaining()).into();
                if let Some(batch) = decoder.decode(&mut remaining)? {
                    let schema = batch.schema().clone();
                    first_batch = Some(batch);
                    break schema;
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
        stream,
        decoder,
        remaining,
        batch: first_batch,

        _client: client,
    };

    let stream = Box::pin(stream::try_unfold(state, |mut state| async move {
        if let Some(batch) = state.batch.take() {
            return Ok(Some((batch, state)));
        }

        loop {
            if let Some(batch) = state.decoder.decode(&mut state.remaining)? {
                return Ok(Some((batch, state)));
            }

            match state.stream.recv_data().await? {
                Some(mut chunk) => {
                    // This should be zero-copy.
                    state.remaining = chunk.copy_to_bytes(chunk.remaining()).into();
                    if let Some(batch) = state.decoder.decode(&mut state.remaining)? {
                        return Ok(Some((batch, state)));
                    }
                }
                None => return Ok(None),
            }
        }
    }));

    Ok((schema, stream))
}

async fn request_stream(
    conn: &Connection<impl ConnectionState>,
    token: StreamToken,
) -> Result<RecvStream, Error> {
    let (mut send, recv) = conn.open_bi().await?;
    send.write_all(token.as_bytes()).await.anyerr()?;
    send.finish().map_err(|_| Error::StreamClosed)?;
    Ok(recv)
}
