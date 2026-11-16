use bauplan_api_macros::ErrorEnvelope;

#[derive(Debug, displaydoc::Display, gropius::ApiError, ErrorEnvelope)]
pub enum ChairError {
    /// chair '{name}' not found
    #[api_error(404, "chair '{name}' not found")]
    NotFound { name: String },
}

fn main() {}
