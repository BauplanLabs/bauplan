use proc_macro2::TokenStream;
use proc_macro2_diagnostics::SpanDiagnosticExt;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Fields, parse_quote};

// The strategy here is to define a "mirror" version of the type using
// `serde(remote)`:
//
// https://serde.rs/remote-derive.html#invoking-the-remote-impl-directly
//
// Then we can use that implementation for the original type, but add `message`
// based on `Display`.

pub(crate) fn expand(input: DeriveInput) -> TokenStream {
    if !input.generics.params.is_empty() {
        return input
            .generics
            .span()
            .error("ErrorEnvelope cannot be derived for generic types")
            .emit_as_item_tokens();
    }

    match &input.data {
        Data::Enum(_) => expand_enum(&input),
        Data::Struct(_) => expand_struct(&input),
        Data::Union(_) => input
            .ident
            .span()
            .error("ErrorEnvelope cannot be derived for unions")
            .emit_as_item_tokens(),
    }
}

fn expand_enum(input: &DeriveInput) -> TokenStream {
    let mut errors = Vec::new();

    let name = &input.ident;
    let name_str = name.to_string();
    let mirror = format_ident!("__{}Mirror", name);
    let mirror_str = mirror.to_string();
    let envelope = format_ident!("__{}Envelope", name);

    let mut mirror_body = input.clone();
    mirror_body.ident = mirror.clone();
    mirror_body.vis = syn::Visibility::Inherited;
    mirror_body.attrs.retain(keep_attribute);

    let Data::Enum(data) = &mut mirror_body.data else {
        unreachable!();
    };

    let mut transparent_arms = Vec::new();
    let mut seen_transparent = false;

    for variant in data.variants.iter_mut() {
        let ident = variant.ident.clone();
        let transparent = is_transparent(&variant.attrs);

        variant.attrs.retain(keep_attribute);
        for field in variant.fields.iter_mut() {
            field.attrs.retain(keep_attribute);
        }

        if transparent {
            if !matches!(&variant.fields, Fields::Unnamed(fields) if fields.unnamed.len() == 1) {
                errors.push(
                    ident
                        .span()
                        .error("`transparent` requires a newtype variant"),
                );
                continue;
            }

            // serde(untagged) makes serialization pass through correctly, but
            // if the underlying type also derives ErrorEnvelope, then we'll
            // add the "message" field twice.
            variant.attrs.push(parse_quote!(#[serde(untagged)]));
            transparent_arms.push(quote! {
                #name::#ident(inner) => ::serde::Serialize::serialize(inner, serializer),
            });

            seen_transparent = true;
        } else {
            if seen_transparent {
                errors.push(ident.span().error("`transparent` variants must come last"));
            }
        }
    }

    if !errors.is_empty() {
        let error_tokens = errors.into_iter().map(|d| d.emit_as_item_tokens());
        return quote! { #(#error_tokens)* };
    }

    let serialize_body = quote! {
        ::serde::Serialize::serialize(
            &#envelope {
                message: ::std::string::ToString::to_string(self),
                inner: self,
            },
            serializer,
        )
    };

    let serialize_body = if transparent_arms.is_empty() {
        serialize_body
    } else {
        quote! {
            match self {
                #(#transparent_arms)*
                _ => #serialize_body,
            }
        }
    };

    let docs = doc_attrs(&input.attrs);

    quote! {
        const _: () = {
            #[derive(::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
            #[serde(remote = #name_str, tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
            #mirror_body

            #(#docs)*
            #[derive(::serde::Serialize, ::schemars::JsonSchema)]
            #[schemars(rename = #name_str)]
            struct #envelope<'a> {
                /// Human-readable error message.
                message: ::std::string::String,
                #[serde(flatten, with = #mirror_str)]
                #[schemars(with = #mirror_str)]
                inner: &'a #name,
            }

            impl ::serde::Serialize for #name {
                fn serialize<S: ::serde::Serializer>(
                    &self,
                    serializer: S,
                ) -> ::core::result::Result<S::Ok, S::Error> {
                    #serialize_body
                }
            }

            impl<'de> ::serde::Deserialize<'de> for #name {
                fn deserialize<D: ::serde::Deserializer<'de>>(
                    deserializer: D,
                ) -> ::core::result::Result<Self, D::Error> {
                    #mirror::deserialize(deserializer)
                }
            }

            impl ::schemars::JsonSchema for #name {
                fn schema_name() -> ::std::borrow::Cow<'static, str> {
                    <#envelope as ::schemars::JsonSchema>::schema_name()
                }

                fn schema_id() -> ::std::borrow::Cow<'static, str> {
                    <#envelope as ::schemars::JsonSchema>::schema_id()
                }

                fn json_schema(generator: &mut ::schemars::SchemaGenerator) -> ::schemars::Schema {
                    <#envelope as ::schemars::JsonSchema>::json_schema(generator)
                }
            }
        };
    }
}

fn expand_struct(input: &DeriveInput) -> TokenStream {
    let name = &input.ident;
    let name_str = name.to_string();
    let mirror = format_ident!("__{}Mirror", name);
    let mirror_str = mirror.to_string();
    let envelope = format_ident!("__{}Envelope", name);
    let tag = screaming_snake_case(&name_str);

    let mut mirror_body = input.clone();
    mirror_body.ident = mirror.clone();
    mirror_body.vis = syn::Visibility::Inherited;
    mirror_body.attrs.retain(keep_attribute);

    let Data::Struct(data) = &mut mirror_body.data else {
        unreachable!();
    };

    let Fields::Named(fields) = &mut data.fields else {
        return name
            .span()
            .error("ErrorEnvelope requires named fields")
            .emit_as_item_tokens();
    };

    for field in fields.named.iter_mut() {
        field.attrs.retain(keep_attribute);
    }

    let docs = doc_attrs(&input.attrs);

    quote! {
        const _: () = {
            fn type_schema(_: &mut ::schemars::SchemaGenerator) -> ::schemars::Schema {
                ::schemars::json_schema!({
                    "type": "string",
                    "const": #tag
                })
            }

            #[derive(::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
            #[serde(remote = #name_str)]
            #mirror_body

            #(#docs)*
            #[derive(::serde::Serialize, ::schemars::JsonSchema)]
            #[schemars(rename = #name_str)]
            struct #envelope<'a> {
                #[schemars(schema_with = "type_schema")]
                r#type: &'static str,
                /// Human-readable error message.
                message: ::std::string::String,
                #[serde(flatten, with = #mirror_str)]
                #[schemars(with = #mirror_str)]
                inner: &'a #name,
            }

            impl ::serde::Serialize for #name {
                fn serialize<S: ::serde::Serializer>(
                    &self,
                    serializer: S,
                ) -> ::core::result::Result<S::Ok, S::Error> {
                    ::serde::Serialize::serialize(
                        &#envelope {
                            r#type: #tag,
                            message: ::std::string::ToString::to_string(self),
                            inner: self,
                        },
                        serializer,
                    )
                }
            }

            impl<'de> ::serde::Deserialize<'de> for #name {
                fn deserialize<D: ::serde::Deserializer<'de>>(
                    deserializer: D,
                ) -> ::core::result::Result<Self, D::Error> {
                    // If this type is the *inner* type of a *different* enum,
                    // then we need to make sure we validate the tag. That's
                    // because, in a shape like this:
                    //
                    // ```
                    // #[serde(tag = "type")]
                    // enum FooBarBaz {
                    //     Foo { foo: String },
                    //     Bar(InnerBar),
                    //     Baz(InnerBaz),
                    // }
                    // ```
                    //
                    // If the tag doesn't match `Foo`, serde will try to match
                    // the other two variants by shape alone.
                    //
                    // On the other hand, if we throw an error when
                    // deserializing `Bar`, then serde will move on and try
                    // `Baz` instead.
                    #[derive(::serde::Deserialize)]
                    struct WithTag {
                        r#type: ::std::string::String,
                        #[serde(flatten, with = #mirror_str)]
                        inner: #name,
                    }

                    let envelope = WithTag::deserialize(deserializer)?;
                    if envelope.r#type != #tag {
                        return ::core::result::Result::Err(::serde::de::Error::custom(
                            ::std::format!(
                                "invalid `type` tag: expected `{}`, found `{}`",
                                #tag,
                                envelope.r#type,
                            ),
                        ));
                    }

                    ::core::result::Result::Ok(envelope.inner)
                }
            }

            impl ::schemars::JsonSchema for #name {
                fn schema_name() -> ::std::borrow::Cow<'static, str> {
                    <#envelope as ::schemars::JsonSchema>::schema_name()
                }

                fn schema_id() -> ::std::borrow::Cow<'static, str> {
                    <#envelope as ::schemars::JsonSchema>::schema_id()
                }

                fn json_schema(generator: &mut ::schemars::SchemaGenerator) -> ::schemars::Schema {
                    <#envelope as ::schemars::JsonSchema>::json_schema(generator)
                }
            }
        };
    }
}

fn keep_attribute(attr: &Attribute) -> bool {
    // These are explicitly passed through the macro, but we don't want them
    // on the mirror.
    for ident in ["error", "api_error", "displaydoc", "from", "source"] {
        if attr.path().is_ident(ident) {
            return false;
        }
    }

    true
}

/// Return true if a variant is marked `#[api_error(transparent)]`, or
/// `#[error(transparent)]` from thiserror.
fn is_transparent(attrs: &[Attribute]) -> bool {
    for attr in attrs {
        if !attr.path().is_ident("api_error") && !attr.path().is_ident("error") {
            continue;
        }

        let Ok(ident) = attr.parse_args::<syn::Ident>() else {
            continue;
        };

        if ident == "transparent" {
            return true;
        }
    }

    false
}

fn doc_attrs(attrs: &[Attribute]) -> impl Iterator<Item = &Attribute> {
    attrs.iter().filter(|a| a.path().is_ident("doc"))
}

/// Serde's `SCREAMING_SNAKE_CASE` conversion.
fn screaming_snake_case(s: &str) -> String {
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}
