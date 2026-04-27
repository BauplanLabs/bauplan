use arrow::{array::RecordBatch, buffer::Buffer, datatypes::SchemaRef, ipc::reader::StreamDecoder};
use futures::stream::{self, Stream};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{ConnectError, Connection, ConnectionState, RecvStream, presets::Preset},
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
    _endpoint: Endpoint,
}

impl Drop for ArrowStreamState {
    fn drop(&mut self) {
        // Iroh says to use Endpoint::close, which waits for the
        // CONNECTION_CLOSE to reach the server. But we don't care, because
        // we're the only ones receiving data, and the extra latency is stupid.
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
    _endpoint: Endpoint,
}

/// Connect to an endpoint with the intention to stream user code stdout/stderr.
///
/// Returns a stream for stdout and stderr, respectively.
pub async fn attach_task(preset: impl Preset, server: EndpointAddr) -> Result<AttachedTask, Error> {
    let endpoint = Endpoint::builder(preset).bind().await?;

    let connecting = endpoint
        .connect_with_opts(server, ALPN, Default::default())
        .await
        .map_err(ConnectError::from)?;

    // TODO: since the client is always ephemeral, we have no way to cache
    // sessions and therefore 0rtt will never work. We could fix this by
    // providing our own (filesystem-based) cache, but iroh doesn't expose that
    // as an option (yet).
    let conn = match connecting.into_0rtt() {
        Ok(outgoing) => {
            let stdout_fut = open_stream(&outgoing, StreamToken::UserCodeStdout);
            let stderr_fut = open_stream(&outgoing, StreamToken::UserCodeStderr);
            let (stdout_recv, stderr_recv) =
                futures::future::try_join(stdout_fut, stderr_fut).await?;

            match outgoing
                .handshake_completed()
                .await
                .map_err(ConnectError::from)?
            {
                iroh::endpoint::ZeroRttStatus::Accepted(_conn) => {
                    return Ok(AttachedTask {
                        stdout: stdout_recv,
                        stderr: stderr_recv,
                        _endpoint: endpoint,
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

    let stdout_fut = open_stream(&conn, StreamToken::UserCodeStdout);
    let stderr_fut = open_stream(&conn, StreamToken::UserCodeStderr);
    let (stdout_recv, stderr_recv) = futures::future::try_join(stdout_fut, stderr_fut).await?;

    Ok(AttachedTask {
        stdout: stdout_recv,
        stderr: stderr_recv,
        _endpoint: endpoint,
    })
}

/// Connect to an endpoint with the intention to download query results.
///
/// Returns the stream schema and a stream of record batches.
pub async fn fetch_query_results(
    preset: impl Preset,
    server: EndpointAddr,
) -> Result<
    (
        SchemaRef,
        impl Stream<Item = Result<RecordBatch, Error>> + Unpin,
    ),
    Error,
> {
    let endpoint = Endpoint::builder(preset).bind().await?;

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
            let recv = open_stream(&outgoing, StreamToken::QueryResults).await?;

            match outgoing
                .handshake_completed()
                .await
                .map_err(ConnectError::from)?
            {
                iroh::endpoint::ZeroRttStatus::Accepted(conn) => (conn, Some(recv)),
                iroh::endpoint::ZeroRttStatus::Rejected(conn) => {
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

    let mut recv = match recv {
        Some(r) => r,
        None => open_stream(&conn, StreamToken::QueryResults).await?,
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
        conn,
        recv,
        decoder,
        remaining,
        batch: first_batch,
        _endpoint: endpoint,
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
                None => return Ok(None),
            }
        }
    }));

    Ok((schema, stream))
}

async fn open_stream(
    conn: &Connection<impl ConnectionState>,
    token: StreamToken,
) -> Result<RecvStream, Error> {
    let (mut send, recv) = conn.open_bi().await?;
    send.write_all(token.as_bytes()).await.anyerr()?;
    send.finish().map_err(|_| Error::StreamClosed)?;
    Ok(recv)
}
