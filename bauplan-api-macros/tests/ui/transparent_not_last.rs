use bauplan_api_macros::ErrorEnvelope;

#[derive(Debug, displaydoc::Display, ErrorEnvelope)]
pub enum InnerError {
    /// '{name}' not found
    NotFound { name: String },
}

#[derive(Debug, displaydoc::Display, ErrorEnvelope)]
pub enum ChairError {
    /// An inner error.
    #[api_error(transparent)]
    Inner(InnerError),
    /// '{name}' not found
    NotFound { name: String },
}

fn main() {}
