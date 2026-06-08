use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    ipc::{
        CompressionType,
        writer::{IpcWriteOptions, StreamWriter},
    },
};
use bytes::{BufMut, BytesMut};
use iroh::{
    Endpoint,
    endpoint::{Connection, ConnectionState, IncomingZeroRtt, SendStream},
};
use tracing::{debug, error};

use crate::{Error, StreamToken};

/// Binds an endpoint and waits for the first connection, then returns it. This
/// will run forever and has no inherent timeout.
pub async fn accept_connection(endpoint: &Endpoint) -> Result<Connection<IncomingZeroRtt>, Error> {
    loop {
        let Some(incoming) = endpoint.accept().await else {
            // The endpoint was dropped.
            return Err(Error::NoPeer);
        };

        match incoming.accept() {
            Ok(acc) => return Ok(acc.into_0rtt()),
            Err(err) => {
                error!(?err, "handshake failed, listening again");
                continue;
            }
        }
    }
}

/// Accept the next bidirectional stream on a connection, reading the
/// one-byte [`StreamToken`] the client sends to identify the stream type.
pub async fn accept_stream<C: ConnectionState>(
    conn: &Connection<C>,
) -> Result<(StreamToken, SendStream), Error> {
    let (send, mut recv) = conn.accept_bi().await?;

    let mut token_buf = [0u8; 1];
    recv.read_exact(&mut token_buf).await.map_err(|e| match e {
        iroh::endpoint::ReadExactError::ReadError(e) => e,
        iroh::endpoint::ReadExactError::FinishedEarly(_) => iroh::endpoint::ReadError::ClosedStream,
    })?;

    let token = StreamToken::try_from(token_buf[0])?;
    debug!(?token, "accepted stream");

    Ok((token, send))
}

/// A server capable of pushing arrow record batches to a single client.
pub struct ArrowIPCServer {
    send: SendStream,
    writer: StreamWriter<bytes::buf::Writer<BytesMut>>,
}

impl std::fmt::Debug for ArrowIPCServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowIPCServer").finish()
    }
}

impl ArrowIPCServer {
    /// Sets up a server and waits for the client. This has no internal timeout.
    pub async fn accept(endpoint: &Endpoint, schema: SchemaRef) -> Result<Self, Error> {
        let conn = accept_connection(endpoint).await?;
        let (token, send) = accept_stream(&conn).await?;
        if token != StreamToken::QueryResults {
            return Err(Error::InvalidStreamToken);
        }

        let write_options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .unwrap();

        let buf = BytesMut::new().writer();
        let writer = StreamWriter::try_new_with_options(buf, &schema, write_options)?;

        Ok(Self { send, writer })
    }

    /// Sends a record batch over the wire.
    pub async fn send_record_batch(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        self.writer.write(batch)?;
        self.flush().await?;
        Ok(())
    }

    /// Writes the buffer out to the wire.
    async fn flush(&mut self) -> Result<(), Error> {
        let chunk = self.writer.get_mut().get_mut().split().freeze();
        self.send.write_chunk(chunk).await?;
        Ok(())
    }

    /// Tears down the connection. Must be called so that the client knows when
    /// to stop receiving data.
    pub async fn finish(mut self) -> Result<(), Error> {
        self.writer.finish()?;
        self.flush().await?;

        // Signal that no more data will be sent.
        let _ = self.send.finish();
        self.send.stopped().await.map_err(|_| Error::StreamClosed)?;
        Ok(())
    }
}
