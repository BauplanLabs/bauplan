//! The public Bauplan API, as gropius traits.
//!
//! The `client-reqwest` or the `client-ureq` feature generates the clients,
//! on top of `reqwest` or `ureq`.

pub mod arrow_schema;
pub mod branch;
pub mod catalog;
pub mod catalog_ref;
pub mod error;
pub mod model;
pub mod namespace;
pub mod pagination;
pub mod table;
pub mod tag;

pub use gropius;
