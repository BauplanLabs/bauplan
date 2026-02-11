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

#[cfg(all(test, feature = "_integration-tests"))]
mod test {
    use super::*;
    use crate::api::testutil::{TestTag, roundtrip, test_name};
    use crate::{ApiError, ApiErrorKind};

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
        let tag = TestTag::new("filter_test_tag")?;

        let req = GetTags {
            filter_by_name: Some(&tag.name),
        };
        let tags = crate::paginate(req, Some(10), |r| roundtrip(r))?
            .collect::<Result<Vec<Tag>, ApiError>>()?;

        assert!(!tags.is_empty());
        assert!(tags.iter().all(|t| t.name.contains(&tag.name)));
        Ok(())
    }

    #[test]
    fn create_and_delete_tag() -> anyhow::Result<()> {
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

        let Err(ApiError::ErrorResponse {
            kind: ApiErrorKind::TagNotFound { tag_name: name },
            ..
        }) = roundtrip(req)
        else {
            panic!("expected TAG_NOT_FOUND");
        };

        assert_eq!(name, tag_name);

        Ok(())
    }

    #[test]
    fn get_tag_not_found() -> anyhow::Result<()> {
        let req = GetTag {
            name: "nonexistent_tag_12345",
        };

        let Err(ApiError::ErrorResponse {
            kind: ApiErrorKind::TagNotFound { tag_name },
            ..
        }) = roundtrip(req)
        else {
            panic!("expected TAG_NOT_FOUND");
        };

        assert_eq!(tag_name, "nonexistent_tag_12345");

        Ok(())
    }

    #[test]
    fn create_tag_already_exists() -> anyhow::Result<()> {
        let tag = TestTag::new("test_tag_exists")?;

        // Try to create it again.
        let req = CreateTag {
            name: &tag.name,
            from_ref: "main",
        };
        let Err(ApiError::ErrorResponse {
            kind: ApiErrorKind::TagExists { tag_name, .. },
            ..
        }) = roundtrip(req)
        else {
            panic!("expected TAG_EXISTS");
        };

        assert_eq!(tag_name, tag.name);

        Ok(())
    }

    #[test]
    fn rename_tag_not_a_tag() -> anyhow::Result<()> {
        // Try to rename a branch as if it were a tag.
        let req = RenameTag {
            name: "main",
            new_name: &test_name("not_a_tag"),
        };

        let Err(ApiError::ErrorResponse {
            kind: ApiErrorKind::NotATagRef { input_ref },
            ..
        }) = roundtrip(req)
        else {
            panic!("expected NOT_A_TAG_REF");
        };

        assert_eq!(input_ref, "main");

        Ok(())
    }

    #[test]
    fn rename_tag() -> anyhow::Result<()> {
        let mut tag = TestTag::new("test_rename_tag_old")?;
        let new_name = test_name("test_rename_tag_new");

        let req = RenameTag {
            name: &tag.name,
            new_name: &new_name,
        };
        let renamed = roundtrip(req)?;
        assert_eq!(renamed.name, new_name);

        // Verify old name is gone.
        let Err(ApiError::ErrorResponse {
            kind: ApiErrorKind::TagNotFound { tag_name },
            ..
        }) = roundtrip(GetTag { name: &tag.name })
        else {
            panic!("expected TAG_NOT_FOUND");
        };

        assert_eq!(tag_name, tag.name);

        // Update so Drop cleans up the right name.
        tag.name = new_name;

        Ok(())
    }
}
