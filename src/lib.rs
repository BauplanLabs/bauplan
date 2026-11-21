//! A client for [Bauplan](https://bauplanlabs.com).
//!
//! This crate provides a Rust SDK for interacting with the Bauplan data platform.
//!

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

mod config;

pub mod flight;
pub mod grpc;
pub mod project;

pub use config::Profile;

#[cfg(feature = "python")]
mod python;
