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
    Endpoint, RelayMode, SecretKey,
    endpoint::{Connection, ConnectionState, IncomingZeroRtt, SendStream},
};
use tracing::{debug, error};

use crate::{ALPN, Error, StreamToken};

/// Binds an endpoint and waits for the first connection, then returns it.
async fn accept_connection(
    relay: url::Url,
    secret_key: SecretKey,
) -> Result<(Endpoint, Connection<IncomingZeroRtt>), Error> {
    let builder = Endpoint::empty_builder()
        .relay_mode(RelayMode::custom([relay.into()]))
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()]);

    let endpoint = builder.bind().await?;

    loop {
        let Some(incoming) = endpoint.accept().await else {
            // The endpoint was dropped.
            return Err(Error::NoPeer);
        };

        match incoming.accept() {
            Ok(acc) => return Ok((endpoint, acc.into_0rtt())),
            Err(err) => {
                error!(?err, "handshake failed, listening again");
                continue;
            }
        }
    }
}

/// Accept the next bidirectional stream on a connection, reading the
/// one-byte [`StreamToken`] the client sends to identify the stream type.
async fn accept_stream<C: ConnectionState>(
    conn: &Connection<C>,
) -> Result<(StreamToken, SendStream), Error> {
    let (send, mut recv) = conn.accept_bi().await?;

    let mut token_buf = [0u8; 1];
    recv.read_exact(&mut token_buf).await.map_err(|e| match e {
        iroh::endpoint::ReadExactError::ReadError(e) => e,
        iroh::endpoint::ReadExactError::FinishedEarly(_) => iroh::endpoint::ReadError::ClosedStream,
    })?;

    let token = StreamToken::from_byte(token_buf[0]).ok_or(Error::InvalidStreamToken)?;
    debug!(?token, "accepted stream");

    Ok((token, send))
}

/// A server capable of pushing arrow record batches to a single client.
pub struct ArrowIPCServer {
    endpoint: Endpoint,
    send: SendStream,
    writer: StreamWriter<bytes::buf::Writer<BytesMut>>,
}

impl ArrowIPCServer {
    /// Sets up a server and waits for the client. This has no internal timeout.
    pub async fn accept(
        relay: url::Url,
        secret_key: SecretKey,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        let (endpoint, conn) = accept_connection(relay, secret_key).await?;
        let (token, send) = accept_stream(&conn).await?;
        if token != StreamToken::QueryResults {
            return Err(Error::InvalidStreamToken);
        }

        let write_options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .unwrap();

        let buf = BytesMut::new().writer();
        let writer = StreamWriter::try_new_with_options(buf, &schema, write_options)?;

        Ok(Self {
            endpoint,
            send,
            writer,
        })
    }

    /// Buffers a record batch to be sent. Data can be flushed with [flush].
    pub async fn send_record_batch(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        self.writer.write(batch)?;
        Ok(())
    }

    /// Sends any buffered data over the wire.
    pub async fn flush(&mut self) -> Result<(), Error> {
        let chunk = self.writer.get_mut().get_mut().split().freeze();
        self.send.write_chunk(chunk).await?;
        Ok(())
    }

    /// Tears down the connection. Must be called so that the client knows when
    /// to stop receiving data.
    pub async fn finish(mut self) -> Result<(), Error> {
        self.writer.finish()?;
        self.flush().await?;

        // This waits for the client to ack everything.
        let _ = self.send.finish();
        let _ = self.send.stopped().await;

        self.endpoint.close().await;
        Ok(())
    }
}
