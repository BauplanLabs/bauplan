//! API operations concerning tags.

use serde::{Deserialize, Serialize};

use crate::{
    PaginatedResponse,
    api::{ApiRequest, DataResponse},
};

/// A tag in the catalog.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Tag {
    /// The tag name.
    pub name: String,
    /// The commit hash the tag points to.
    pub hash: String,
}

impl DataResponse for Tag {}

/// Get a single tag by name.
#[derive(Debug, Clone)]
pub struct GetTag<'a> {
    /// The name of the tag to fetch.
    pub name: &'a str,
}

impl ApiRequest for GetTag<'_> {
    type Response = Tag;

    fn path(&self) -> String {
        format!("/catalog/v0/tags/{}", self.name)
    }
}

/// List tags.
#[derive(Debug, Clone)]
pub struct GetTags<'a> {
    /// Filter tags by name pattern.
    pub filter_by_name: Option<&'a str>,
}

#[derive(Serialize)]
struct GetTagsQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_name: Option<&'a str>,
}

impl ApiRequest for GetTags<'_> {
    type Response = PaginatedResponse<Tag>;

    fn path(&self) -> String {
        "/catalog/v0/tags".to_string()
    }

    fn query(&self) -> Option<impl Serialize> {
        Some(GetTagsQuery {
            filter_by_name: self.filter_by_name,
        })
    }
}

/// Create a new tag.
#[derive(Debug, Clone)]
pub struct CreateTag<'a> {
    /// The name of the tag to create.
    pub name: &'a str,

    /// The ref to create the tag from (e.g., "main" or "main@abc123").
    pub from_ref: &'a str,
}

#[derive(Debug, Clone, Serialize)]
struct CreateTagBody<'a> {
    tag_name: &'a str,
    from_ref: &'a str,
}

impl ApiRequest for CreateTag<'_> {
    type Response = Tag;

    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        "/catalog/v0/tags".to_string()
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(CreateTagBody {
            tag_name: self.name,
            from_ref: self.from_ref,
        })
    }
}

/// Delete a tag.
#[derive(Debug, Clone)]
pub struct DeleteTag<'a> {
    /// The name of the tag to delete.
    pub name: &'a str,
}

impl ApiRequest for DeleteTag<'_> {
    type Response = Tag;

    fn method(&self) -> http::Method {
        http::Method::DELETE
    }

    fn path(&self) -> String {
        format!("/catalog/v0/tags/{}", self.name)
    }
}

/// Rename a tag.
#[derive(Debug, Clone)]
pub struct RenameTag<'a> {
    /// The current name of the tag.
    pub name: &'a str,

    /// The new name for the tag.
    pub new_name: &'a str,
}

#[derive(Debug, Clone, Serialize)]
struct RenameTagBody<'a> {
    tag_name: &'a str,
}

impl ApiRequest for RenameTag<'_> {
    type Response = Tag;

    fn method(&self) -> http::Method {
        http::Method::PATCH
    }

    fn path(&self) -> String {
        format!("/catalog/v0/tags/{}", self.name)
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(RenameTagBody {
            tag_name: self.new_name,
        })
    }
}

#[cfg(all(test, feature = "_integration_tests"))]
mod test {
    use assert_matches::assert_matches;

    use super::*;
    use crate::{ApiError, ApiErrorKind, api::testutil::roundtrip};

    #[test]
    fn get_tags() -> anyhow::Result<()> {
        let req = GetTags {
            filter_by_name: None,
        };

        let tags = crate::paginate(req, Some(5), |r| roundtrip(r))?
            .collect::<Result<Vec<Tag>, ApiError>>()?;

        // Tags may or may not exist, just verify the request works.
        assert!(tags.len() <= 5);

        Ok(())
    }

    #[test]
    fn get_tags_with_filter() -> anyhow::Result<()> {
        use crate::api::testutil::test_name;

        let tag_name = test_name("filter_test_tag");
        let req = CreateTag {
            name: &tag_name,
            from_ref: "main",
        };
        roundtrip(req)?;

        let req = GetTags {
            filter_by_name: Some(&tag_name),
        };
        let tags = crate::paginate(req, Some(10), |r| roundtrip(r))?
            .collect::<Result<Vec<Tag>, ApiError>>()?;

        assert!(!tags.is_empty());
        assert!(tags.iter().all(|t| t.name.contains(&tag_name)));
        Ok(())
    }

    #[test]
    fn create_and_delete_tag() -> anyhow::Result<()> {
        use crate::api::testutil::test_name;

        let tag_name = test_name("test_tag");

        // Create the tag.
        let req = CreateTag {
            name: &tag_name,
            from_ref: "main",
        };
        let created = roundtrip(req)?;
        assert_eq!(created.name, tag_name);

        // Verify it exists.
        let req = GetTag { name: &tag_name };
        let fetched = roundtrip(req)?;
        assert_eq!(fetched.name, tag_name);
        assert_eq!(fetched.hash, created.hash);

        // Delete it.
        let req = DeleteTag { name: &tag_name };
        let deleted = roundtrip(req)?;
        assert_eq!(deleted.name, tag_name);

        // Verify it's gone.
        let req = GetTag { name: &tag_name };
        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::TagNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn get_tag_not_found() -> anyhow::Result<()> {
        let req = GetTag {
            name: "nonexistent_tag_12345",
        };

        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::TagNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn create_tag_already_exists() -> anyhow::Result<()> {
        use crate::api::testutil::test_name;

        let tag_name = test_name("test_tag_exists");

        // Create the tag first.
        let req = CreateTag {
            name: &tag_name,
            from_ref: "main",
        };
        roundtrip(req)?;

        // Try to create it again.
        let req = CreateTag {
            name: &tag_name,
            from_ref: "main",
        };
        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::TagExists,
                ..
            })
        );

        // Clean up.
        let req = DeleteTag { name: &tag_name };
        roundtrip(req)?;

        Ok(())
    }

    #[test]
    fn rename_tag() -> anyhow::Result<()> {
        use crate::api::testutil::test_name;

        let old_name = test_name("test_rename_tag_old");
        let new_name = test_name("test_rename_tag_new");

        // Create the tag.
        let req = CreateTag {
            name: &old_name,
            from_ref: "main",
        };
        roundtrip(req)?;

        // Rename it.
        let req = RenameTag {
            name: &old_name,
            new_name: &new_name,
        };
        let renamed = roundtrip(req)?;
        assert_eq!(renamed.name, new_name);

        // Verify old name is gone.
        let req = GetTag { name: &old_name };
        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::TagNotFound,
                ..
            })
        );

        // Clean up.
        let req = DeleteTag { name: &new_name };
        roundtrip(req)?;

        Ok(())
    }
}
