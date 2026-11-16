use bauplan_api_macros::ErrorEnvelope;
use gropius::ApiError as _;
use serde_json::json;

/// API error returned by `create_chair`.
#[derive(Debug, PartialEq, displaydoc::Display, gropius::ApiError, ErrorEnvelope)]
enum CreateChairError {
    /// Chair '{chair_name}' already exists.
    #[api_error(409)]
    ChairExists {
        /// The conflicting chair name.
        chair_name: String,
    },
    /// Not implemented.
    #[api_error(501)]
    NotImplemented,
}

#[test]
fn simple() -> anyhow::Result<()> {
    let err = CreateChairError::ChairExists {
        chair_name: "throne".into(),
    };
    assert_eq!(err.status_code().as_u16(), 409);

    let serialized = serde_json::to_value(&err)?;
    assert_eq!(
        serialized,
        json!({
            "type": "CHAIR_EXISTS",
            "message": "Chair 'throne' already exists.",
            "chair_name": "throne",
        })
    );

    let de: CreateChairError = serde_json::from_value(serialized)?;
    assert_eq!(de, err);

    Ok(())
}

#[test]
fn unit_variant() -> anyhow::Result<()> {
    let err = CreateChairError::NotImplemented;
    assert_eq!(err.status_code().as_u16(), 501);
    let serialized = serde_json::to_value(&err)?;
    assert_eq!(
        serialized,
        json!({ "type": "NOT_IMPLEMENTED", "message": "Not implemented." })
    );

    let de: CreateChairError = serde_json::from_value(serialized)?;
    assert_eq!(de, err);

    Ok(())
}

#[test]
fn transparent() -> anyhow::Result<()> {
    #[derive(Debug, PartialEq, displaydoc::Display, gropius::ApiError, ErrorEnvelope)]
    enum InnerError {
        /// Widget '{name}' not found.
        #[api_error(404)]
        WidgetNotFound { name: String },
    }

    #[derive(Debug, PartialEq, displaydoc::Display, gropius::ApiError, ErrorEnvelope)]
    enum OuterError {
        /// Not implemented.
        #[api_error(501)]
        NotImplemented,
        /// An error resolving the widget.
        #[api_error(transparent)]
        Inner(InnerError),
    }

    let err = OuterError::Inner(InnerError::WidgetNotFound { name: "w1".into() });
    assert_eq!(err.status_code().as_u16(), 404);

    let serialized = serde_json::to_value(&err)?;
    assert_eq!(
        serialized,
        json!({
            "type": "WIDGET_NOT_FOUND",
            "message": "Widget 'w1' not found.",
            "name": "w1",
        })
    );
    let _ = OuterError::NotImplemented;

    let de: OuterError = serde_json::from_value(serialized)?;
    assert_eq!(de, err);

    Ok(())
}

#[test]
fn error_transparent() -> anyhow::Result<()> {
    #[derive(Debug, PartialEq, displaydoc::Display, ErrorEnvelope)]
    enum InnerError {
        /// Widget '{name}' not found.
        WidgetNotFound { name: String },
    }

    #[derive(Debug, PartialEq, displaydoc::Display, ErrorEnvelope)]
    enum OuterError {
        /// An error resolving the widget.
        #[error(transparent)]
        Inner(InnerError),
    }

    let err = OuterError::Inner(InnerError::WidgetNotFound { name: "w1".into() });
    let serialized = serde_json::to_value(&err)?;

    assert_eq!(
        serialized,
        json!({
            "type": "WIDGET_NOT_FOUND",
            "message": "Widget 'w1' not found.",
            "name": "w1",
        })
    );

    let de: OuterError = serde_json::from_value(serialized)?;
    assert_eq!(de, err);

    Ok(())
}

#[test]
fn transparent_structs_disambiguate() -> anyhow::Result<()> {
    /// Widget '{name}' not found.
    #[derive(Debug, PartialEq, displaydoc::Display, ErrorEnvelope)]
    struct WidgetNotFound {
        name: String,
    }

    /// Gadget '{name}' not found.
    #[derive(Debug, PartialEq, displaydoc::Display, ErrorEnvelope)]
    struct GadgetNotFound {
        name: String,
    }

    // Both structs have the same shape, so only the `type` tag distinguishes
    // them among the untagged transparent variants.
    #[derive(Debug, PartialEq, displaydoc::Display, ErrorEnvelope)]
    enum OuterError {
        /// A widget error.
        #[error(transparent)]
        Widget(WidgetNotFound),
        /// A gadget error.
        #[error(transparent)]
        Gadget(GadgetNotFound),
    }

    let widget = OuterError::Widget(WidgetNotFound { name: "w1".into() });
    let gadget = OuterError::Gadget(GadgetNotFound { name: "g1".into() });

    for err in [widget, gadget] {
        let serialized = serde_json::to_value(&err)?;
        let de: OuterError = serde_json::from_value(serialized)?;
        assert_eq!(de, err);
    }

    Ok(())
}

#[test]
fn plain_struct() -> anyhow::Result<()> {
    /// Widget '{name}' not found.
    #[derive(Debug, PartialEq, displaydoc::Display, gropius::ApiError, ErrorEnvelope)]
    #[api_error(404)]
    struct WidgetNotFound {
        name: String,
    }

    let err = WidgetNotFound { name: "w1".into() };
    assert_eq!(err.status_code().as_u16(), 404);

    let serialized = serde_json::to_value(&err)?;
    assert_eq!(
        serialized,
        json!({
            "type": "WIDGET_NOT_FOUND",
            "message": "Widget 'w1' not found.",
            "name": "w1",
        })
    );

    let de: WidgetNotFound = serde_json::from_value(serialized)?;
    assert_eq!(de, err);

    Ok(())
}

#[test]
fn forward_attrs() -> anyhow::Result<()> {
    /// A widget error.
    #[derive(Debug, PartialEq, displaydoc::Display, ErrorEnvelope)]
    enum WidgetError {
        /// Widget '{name}' not found.
        NotFound {
            #[serde(rename = "widget_name")]
            #[schemars(length(min = 1))]
            name: String,
        },
    }

    let err = WidgetError::NotFound { name: "w1".into() };
    let serialized = serde_json::to_value(&err)?;
    assert_eq!(
        serialized,
        json!({
            "type": "NOT_FOUND",
            "message": "Widget 'w1' not found.",
            "widget_name": "w1",
        })
    );

    let schema = serde_json::to_value(schemars::schema_for!(WidgetError))?;
    let prop = &schema["oneOf"][0]["properties"]["widget_name"];
    assert_eq!(prop["type"], "string");
    assert_eq!(prop["minLength"], 1);

    let de: WidgetError = serde_json::from_value(serialized)?;
    assert_eq!(de, err);

    Ok(())
}

#[test]
fn schema() -> anyhow::Result<()> {
    let schema = serde_json::to_value(schemars::schema_for!(CreateChairError))?;
    assert_eq!(schema["title"], "CreateChairError");
    assert_eq!(
        schema["description"],
        "API error returned by `create_chair`."
    );
    assert_eq!(schema["properties"]["message"]["type"], "string");

    Ok(())
}
