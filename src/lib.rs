//! A client for [Bauplan](https://bauplanlabs.com).
//!
//! This crate provides a Rust SDK for interacting with the Bauplan data platform.
//!
//! # HTTP Requests and Responses
//!
//! The API types are designed to work with any HTTP client that uses the [`http`]
//! crate. Use [`ApiRequest::into_request`] to create a request, and
//! [`ApiResponse::from_response`] to parse the response.
//!
//! # Example with ureq
//!
//! ```
//! use bauplan::{ApiRequest, ApiResponse, Profile, table::GetTable};
//!
//! # fn main() -> anyhow::Result<()> {
//! let profile = Profile::from_default_env()?;
//!
//! let req = GetTable {
//!     name: "taxi_fhvhv",
//!     at_ref: "main",
//!     namespace: Some("bauplan"),
//! };
//!
//! let http_req = req.into_request(&profile)?;
//! let resp = ureq::run(http_req)?;
//!
//! // You can use the associated type to read the response, or use e.g.
//! // TableWithMetadata directly.
//! let table = <GetTable as ApiRequest>::Response::from_response(
//!     resp.map(ureq::Body::into_reader),
//! )?;
//!
//! println!("Table: {} ({} records)", table.name, table.records.unwrap_or(0));
//! # Ok(())
//! # }
//! ```
//!
//! # Example with reqwest
//!
//! ```
//! use bauplan::{ApiRequest, ApiResponse, Profile, table::GetTable};
//! use http_body_util::BodyExt;
//! use std::io::Cursor;
//!
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! let profile = Profile::from_default_env()?;
//! let client = reqwest::Client::new();
//!
//! let req = GetTable {
//!     name: "bauplan.taxi_fhvhv",
//!     at_ref: "main",
//!     namespace: None,
//! };
//!
//! let http_req = req.into_request(&profile)?;
//! let reqwest_req: reqwest::Request = http_req.try_into()?;
//!
//! let resp = client.execute(reqwest_req).await?;
//! let http_resp: http::Response<_> = resp.into();
//! let (parts, body) = http_resp.into_parts();
//! let bytes = body.collect().await?.to_bytes();
//!
//! // You can use the associated type to read the response, or use e.g.
//! // TableWithMetadata directly
//! let table = <GetTable as ApiRequest>::Response::from_response_parts(parts, Cursor::new(bytes))?;
//! println!("Table: {} ({} records)", table.name, table.records.unwrap_or(0));
//! # Ok(())
//! # }
//! ```

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

mod api;
mod config;

#[doc(hidden)]
pub mod grpc;

pub use api::*;
pub use config::Profile;

#[cfg(feature = "python")]
mod python;
