use arrow::{array::RecordBatch, buffer::Buffer, datatypes::SchemaRef, ipc::reader::StreamDecoder};
use futures::stream::{self, Stream};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{ConnectError, Connection, ConnectionState, RecvStream},
};
use n0_error::StdResultExt;
use tracing::debug;

use crate::{ALPN, Error, StreamToken};

struct ArrowStreamState {
    recv: RecvStream,
    decoder: StreamDecoder,
    remaining: Buffer,
    batch: Option<RecordBatch>,

    conn: Connection,
}

impl Drop for ArrowStreamState {
    fn drop(&mut self) {
        self.conn.close(0_u8.into(), b"done");
    }
}

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

/// Connect, sending the desired stream token with the initial handshake
/// data to shave latency in some scenarios.
///
/// If this optimization was successful, the opened stream is returned along
/// with the connection. Otherwise, it should be opened manually.
async fn connect(
    endpoint: &Endpoint,
    server: EndpointAddr,
    initial_token: StreamToken,
) -> Result<(Connection, Option<RecvStream>), Error> {
    // TODO: we should maybe disable ipv6 proactively, since our servers don't
    // support it and it can cause extra startup latency. :(
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

    Ok((conn, recv))
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

/// Connect to an endpoint with the intention to download query results.
///
/// Returns the stream schema and a stream of record batches.
pub async fn fetch_query_results(
    endpoint: &Endpoint,
    server: EndpointAddr,
) -> Result<
    (
        SchemaRef,
        impl Stream<Item = Result<RecordBatch, Error>> + Unpin + use<>,
    ),
    Error,
> {
    let (conn, recv) = connect(endpoint, server, StreamToken::QueryResults).await?;

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
                remaining = Buffer::from(chunk);
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
        recv,
        decoder,
        remaining,
        batch: first_batch,

        conn,
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
                    state.remaining = Buffer::from(chunk);
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
