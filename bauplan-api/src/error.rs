//! API error types.
//!
//! Each endpoint declares its own error enum, composing `displaydoc` for the
//! message, `thiserror` for the `From` conversions, [`gropius::ApiError`]
//! for the status, and [`ErrorEnvelope`] for the wire shape.

pub use bauplan_api_macros::ErrorEnvelope;

/// All endpoints can produce these errors.
#[derive(Debug, displaydoc::Display, thiserror::Error, gropius::ApiError, ErrorEnvelope)]
pub enum GenericError {
    /// Internal error.
    #[api_error(500)]
    Internal,
    /// Invalid or missing bearer token.
    #[api_error(401)]
    Unauthorized,
    /// Your role does not allow this action.
    #[api_error(403)]
    Forbidden,
    /// Temporarily unavailable. Retry later.
    #[api_error(503)]
    ServiceUnavailable,
    /// The request was invalid.
    #[displaydoc("{message}")]
    #[api_error(400)]
    BadRequest {
        /// Why the request was rejected.
        #[serde(skip_serializing)]
        #[schemars(skip)]
        message: String,
    },
}

#[cfg(test)]
mod test {
    use std::error::Error as _;

    use super::*;
    use crate::branch::ListBranchesError;

    #[test]
    fn generic_error_envelope_shapes() -> anyhow::Result<()> {
        let v = serde_json::to_value(GenericError::Unauthorized)?;
        assert_eq!(v["type"], "UNAUTHORIZED");
        assert_eq!(v["message"], "Invalid or missing bearer token.");

        let v = serde_json::to_value(ListBranchesError::Generic(GenericError::Internal))?;
        assert_eq!(v["type"], "INTERNAL");
        assert_eq!(v["message"], "Internal error.");

        let err = ListBranchesError::Generic(GenericError::BadRequest {
            message: "name: nope".into(),
        });
        let source = err.source().map(ToString::to_string);
        assert_eq!(source.as_deref(), Some("name: nope"));
        let v = serde_json::to_value(err)?;
        assert_eq!(v["type"], "BAD_REQUEST");
        assert_eq!(v["message"], "name: nope");

        // A client reads the message back from the envelope.
        let err: ListBranchesError = serde_json::from_value(v)?;
        let source = err.source().map(ToString::to_string);
        assert_eq!(source.as_deref(), Some("name: nope"));

        Ok(())
    }
}
