use proc_macro::TokenStream;

mod error_envelope;

/// Derives the Bauplan wire envelope for an error type, adding `Deserialize`,
/// `Serialize` and `JsonSchema` derives, with the latter two seeing the
/// following shape:
///
/// ```json
/// {
///     "type": "FOO_BAR_BAZ",
///     "message": "...",
///     <fields>
/// }
/// ```
///
/// The `type` field is the serde tag, and `message` is derived from `Display`.
///
/// The macro respects the `#[api_error(transparent)]` attribute from gropius,
/// and/or `#[error(transparent)]` from thiserror. Field and container
/// `#[serde(...)]` and `#[schemars(...)]` attributes are also respected.
#[proc_macro_derive(ErrorEnvelope, attributes(api_error, error, serde, schemars))]
pub fn derive_error_envelope(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::DeriveInput);

    error_envelope::expand(input).into()
}
