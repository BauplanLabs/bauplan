//! Longbow is the p2p protocol Bauplan uses for direct client-to-runtime
//! communication, for example for fetching the results of queries.
//! Longbow is built on [iroh](https://iroh.computer).

#![warn(
    anonymous_parameters,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    rust_2018_idioms,
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unused_extern_crates,
    unused_qualifications,
    variant_size_differences
)]

pub use iroh;

const ALPN: &[u8] = b"bpln/longbow/0";

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
    /// Return the token as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            StreamToken::UserCodeStdout => &[0x01],
            StreamToken::UserCodeStderr => &[0x02],
            StreamToken::QueryResults => &[0x03],
        }
    }
}

impl TryFrom<u8> for StreamToken {
    type Error = Error;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0x01 => Ok(StreamToken::UserCodeStdout),
            0x02 => Ok(StreamToken::UserCodeStderr),
            0x03 => Ok(StreamToken::QueryResults),
            _ => Err(Error::InvalidStreamToken),
        }
    }
}

impl From<StreamToken> for u8 {
    fn from(v: StreamToken) -> Self {
        match v {
            StreamToken::UserCodeStdout => 0x01,
            StreamToken::UserCodeStderr => 0x02,
            StreamToken::QueryResults => 0x03,
        }
    }
}

/// Configures iroh to use the Bauplan relays.
#[derive(Default, Debug, Clone)]
pub struct BauplanPreset {
    relay_override: Option<RelayMap>,
}

impl BauplanPreset {
    /// Overrides the default production relays.
    pub fn with_relays(map: impl IntoIterator<Item = iroh::RelayUrl>) -> Self {
        Self {
            relay_override: Some(RelayMap::from_iter(map)),
        }
    }

    /// Construct an EndpointAddr with the Bauplan relays attached as hints.
    /// This will allow an Endpoint to connect to the address without address
    /// lookup enabled.
    pub fn add_relay_urls(&self, mut addr: EndpointAddr) -> EndpointAddr {
        for url in self.relay_map().urls::<Vec<_>>() {
            addr = addr.with_relay_url(url);
        }
        addr
    }

    /// The relays to use.
    pub fn relay_map(&self) -> RelayMap {
        self.relay_override.clone().unwrap_or_else(|| {
            RelayMap::from_iter([
                "https://relay.use1.computing.bauplanlabs.com"
                    .parse::<iroh::RelayUrl>()
                    .unwrap(),
                "https://relay.euw1.computing.bauplanlabs.com"
                    .parse::<iroh::RelayUrl>()
                    .unwrap(),
            ])
        })
    }
}

impl iroh::endpoint::presets::Preset for BauplanPreset {
    fn apply(self, builder: iroh::endpoint::Builder) -> iroh::endpoint::Builder {
        // We don't use address lookup; instead we just try all relays.
        builder
            .clear_address_lookup()
            .alpns(vec![ALPN.to_owned()])
            .relay_mode(RelayMode::Custom(self.relay_map()))
    }
}

/// An error from a longbow server or client.
#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
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

use iroh::{EndpointAddr, RelayMap, RelayMode};
#[cfg(feature = "server")]
pub use server::*;

#[cfg(feature = "client")]
mod client;

#[cfg(feature = "client")]
pub use client::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
    async fn query_results() -> anyhow::Result<()> {
        use iroh::EndpointAddr;

        let secret_key = iroh::SecretKey::generate(&mut rand::rng());
        let public_key = secret_key.public();

        let batches = test_batches();
        let schema = batches[0].schema();
        let server_task = tokio::spawn({
            let batches = batches.clone();
            async move {
                let mut server =
                    ArrowIPCServer::accept(BauplanPreset::default(), secret_key, schema).await?;

                for batch in batches {
                    server.send_record_batch(&batch).await?;
                }

                server.finish().await?;
                Ok::<_, anyhow::Error>(())
            }
        });

        let preset = BauplanPreset::default();
        let server_addr = preset.add_relay_urls(EndpointAddr::new(public_key));

        let (_schema, mut stream) = fetch_query_results(preset, server_addr)
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
