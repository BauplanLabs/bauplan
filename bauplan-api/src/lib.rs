//! The public Bauplan API, as gropius traits.
//!
//! The `client` feature generates the clients.

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
