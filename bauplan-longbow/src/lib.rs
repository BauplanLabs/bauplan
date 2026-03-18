pub const ALPN: &[u8] = b"bpln/longbow/0";

/// The types of streams that can be opened over a longbow connection. Generally
/// a server will support either query results or stdout/stderr.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamToken {
    /// Stdout from user code run in a DAG.
    UserCodeStdout,
    /// Stderr from user code run in a DAG.
    UserCodeStderr,
    /// A flight IPC stream with results for a query.
    QueryResults,
}

impl StreamToken {
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            StreamToken::UserCodeStdout => &[0x01],
            StreamToken::UserCodeStderr => &[0x02],
            StreamToken::QueryResults => &[0x03],
        }
    }

    pub fn from_byte(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(StreamToken::UserCodeStdout),
            0x02 => Some(StreamToken::UserCodeStderr),
            0x03 => Some(StreamToken::QueryResults),
            _ => None,
        }
    }
}

/// An error from a longbow server or client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("No one home")]
    NoPeer,
    #[error("Failed to bind endpoint")]
    Bind(#[from] iroh::endpoint::BindError),
    #[error("Connection failed")]
    Connect(#[from] iroh::endpoint::ConnectError),
    #[error("Connection closed")]
    ConnectionClosed(#[from] iroh::endpoint::ConnectionError),
    #[error("Read failed")]
    Read(#[from] iroh::endpoint::ReadError),
    #[error("Write failed")]
    Write(#[from] iroh::endpoint::WriteError),
    #[error("Arrow decode error")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Invalid key data")]
    InvalidKey,
    #[error("Invalid stream token")]
    InvalidStreamToken,
    #[error("Stream closed")]
    StreamClosed,
    #[error("An internal error occurred")]
    Internal(#[from] n0_error::AnyError),
}

#[cfg(feature = "server")]
mod server;

#[cfg(feature = "server")]
pub use server::*;

#[cfg(feature = "client")]
mod client;

#[cfg(feature = "client")]
pub use client::*;

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time};

    use super::*;
    use anyhow::Context as _;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures::StreamExt;

    fn test_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![4, 5])),
                    Arc::new(StringArray::from(vec!["dave", "eve"])),
                ],
            )
            .unwrap(),
        ]
    }

    #[cfg(all(feature = "server", feature = "client"))]
    #[test_log::test(tokio::test)]
    async fn round_trip_arrow_stream() -> anyhow::Result<()> {
        use iroh::EndpointAddr;

        let relay: url::Url = iroh::defaults::prod::default_eu_relay().url.into();
        let secret_key = iroh::SecretKey::generate(&mut rand::rng());
        let public_key = secret_key.public();

        let batches = test_batches();
        let schema = batches[0].schema();
        let server_task = tokio::spawn({
            let relay = relay.clone();
            let batches = batches.clone();
            async move {
                let mut server = ArrowIPCServer::accept(relay, secret_key, schema).await?;

                for batch in batches {
                    server.send_record_batch(&batch).await?;
                }

                server.finish().await?;
                Ok::<_, anyhow::Error>(())
            }
        });

        let server_addr = EndpointAddr::new(public_key).with_relay_url(relay.clone().into());
        let (_schema, mut stream) = fetch_query_results(server_addr)
            .await
            .context("fetch_query_results failed")?;

        let mut received = Vec::new();
        while let Some(batch) = stream.next().await {
            received.push(batch.context("batch")?);
        }

        assert_eq!(received.len(), batches.len());
        for (got, expected) in received.iter().zip(&batches) {
            assert_eq!(got, expected);
        }

        server_task.await??;
        Ok(())
    }
}
