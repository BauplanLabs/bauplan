use bauplan_api_macros::ErrorEnvelope;

#[derive(Debug, displaydoc::Display, ErrorEnvelope)]
pub enum ChairError {
    /// '{name}' not found
    #[api_error(transparent)]
    NotFound { name: String },
}

fn main() {}
