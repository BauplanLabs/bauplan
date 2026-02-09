//! API operations concerning branches.

use serde::{Deserialize, Serialize};

use crate::{
    CatalogRef, PaginatedResponse,
    api::{ApiRequest, DataResponse},
};

/// A branch in the catalog.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Branch {
    /// The branch name.
    pub name: String,
    /// The commit hash at the head of the branch.
    pub hash: String,
}

impl DataResponse for Branch {}

/// Get a single branch by name.
#[derive(Debug, Clone)]
pub struct GetBranch<'a> {
    /// The name of the branch to fetch.
    pub name: &'a str,
}

impl ApiRequest for GetBranch<'_> {
    type Response = Branch;

    fn path(&self) -> String {
        format!("/catalog/v0/branches/{}", self.name)
    }
}

/// List branches.
#[derive(Debug, Clone)]
pub struct GetBranches<'a> {
    /// Filter branches by name pattern.
    pub filter_by_name: Option<&'a str>,

    /// Filter branches by user.
    pub filter_by_user: Option<&'a str>,
}

#[derive(Serialize)]
struct GetBranchesQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_user: Option<&'a str>,
}

impl ApiRequest for GetBranches<'_> {
    type Response = PaginatedResponse<Branch>;

    fn path(&self) -> String {
        "/catalog/v0/branches".to_string()
    }

    fn query(&self) -> Option<impl Serialize> {
        Some(GetBranchesQuery {
            filter_by_name: self.filter_by_name,
            filter_by_user: self.filter_by_user,
        })
    }
}

/// Create a new branch.
#[derive(Debug, Clone)]
pub struct CreateBranch<'a> {
    /// The name of the branch to create.
    pub name: &'a str,

    /// The ref to create the branch from (e.g., "main" or "main@abc123").
    pub from_ref: &'a str,
}

#[derive(Debug, Clone, Serialize)]
struct CreateBranchBody<'a> {
    branch_name: &'a str,
    from_ref: &'a str,
}

impl ApiRequest for CreateBranch<'_> {
    type Response = Branch;

    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        "/catalog/v0/branches".to_string()
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(CreateBranchBody {
            branch_name: self.name,
            from_ref: self.from_ref,
        })
    }
}

/// Delete a branch.
#[derive(Debug, Clone)]
pub struct DeleteBranch<'a> {
    /// The name of the branch to delete.
    pub name: &'a str,
}

impl ApiRequest for DeleteBranch<'_> {
    type Response = Branch;

    fn method(&self) -> http::Method {
        http::Method::DELETE
    }

    fn path(&self) -> String {
        format!("/catalog/v0/branches/{}", self.name)
    }
}

/// Rename a branch.
#[derive(Debug, Clone)]
pub struct RenameBranch<'a> {
    /// The current name of the branch.
    pub name: &'a str,

    /// The new name for the branch.
    pub new_name: &'a str,
}

#[derive(Debug, Clone, Serialize)]
struct RenameBranchBody<'a> {
    branch_name: &'a str,
}

impl ApiRequest for RenameBranch<'_> {
    type Response = Branch;

    fn method(&self) -> http::Method {
        http::Method::PATCH
    }

    fn path(&self) -> String {
        format!("/catalog/v0/branches/{}", self.name)
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(RenameBranchBody {
            branch_name: self.new_name,
        })
    }
}

/// Merge a ref into a branch.
#[derive(Debug, Clone)]
pub struct MergeBranch<'a> {
    /// The source ref to merge from (e.g., "feature-branch" or "main@abc123").
    pub source_ref: &'a str,

    /// The target branch to merge into.
    pub into_branch: &'a str,

    /// Override the commit message or add custom properties.
    pub commit: MergeCommitOptions<'a>,
}

/// Options for a merge commit.
#[derive(Default, Debug, Clone, Serialize)]
pub struct MergeCommitOptions<'a> {
    /// The commit message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_message: Option<&'a str>,

    /// The commit body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_body: Option<&'a str>,

    /// Additional commit properties.
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub commit_properties: std::collections::BTreeMap<&'a str, &'a str>,
}

impl ApiRequest for MergeBranch<'_> {
    type Response = CatalogRef;

    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        format!(
            "/catalog/v0/refs/{}/merge/{}",
            self.source_ref, self.into_branch
        )
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(self.commit.clone())
    }
}

#[cfg(all(test, feature = "_integration-tests"))]
mod test {
    use assert_matches::assert_matches;

    use super::*;
    use crate::{ApiError, ApiErrorKind, api::testutil::roundtrip};

    #[test]
    fn get_branch() -> anyhow::Result<()> {
        let req = GetBranch { name: "main" };

        let branch: Branch = roundtrip(req)?;
        assert_eq!(branch.name, "main");
        assert!(!branch.hash.is_empty());

        Ok(())
    }

    #[test]
    fn get_branch_not_found() -> anyhow::Result<()> {
        let req = GetBranch {
            name: "nonexistent_branch_12345",
        };

        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::BranchNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn get_branches() -> anyhow::Result<()> {
        let req = GetBranches {
            filter_by_name: None,
            filter_by_user: None,
        };

        let branches = crate::paginate(req, Some(5), |r| roundtrip(r))?
            .collect::<Result<Vec<Branch>, ApiError>>()?;

        assert!(!branches.is_empty());

        Ok(())
    }

    #[test]
    fn get_branches_with_filter() -> anyhow::Result<()> {
        let req = GetBranches {
            filter_by_name: Some("main"),
            filter_by_user: None,
        };

        let branches = crate::paginate(req, Some(10), |r| roundtrip(r))?
            .collect::<Result<Vec<Branch>, ApiError>>()?;

        assert!(!branches.is_empty());
        assert!(branches.iter().all(|b| b.name.contains("main")));

        Ok(())
    }

    #[test]
    fn create_and_delete_branch() -> anyhow::Result<()> {
        use crate::api::testutil::test_name;

        let branch_name = format!("colinmarc.{}", test_name("test_branch"));

        // Create the branch.
        let req = CreateBranch {
            name: &branch_name,
            from_ref: "main",
        };
        let created = roundtrip(req)?;
        assert_eq!(created.name, branch_name);

        // Verify it exists.
        let req = GetBranch { name: &branch_name };
        let fetched = roundtrip(req)?;
        assert_eq!(fetched.name, branch_name);
        assert_eq!(fetched.hash, created.hash);

        // Delete it.
        let req = DeleteBranch { name: &branch_name };
        let deleted = roundtrip(req)?;
        assert_eq!(deleted.name, branch_name);

        // Verify it's gone.
        let req = GetBranch { name: &branch_name };
        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::BranchNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn create_branch_already_exists() -> anyhow::Result<()> {
        use crate::api::testutil::TestBranch;

        let branch = TestBranch::new("test_exists")?;

        // Try to create it again.
        let req = CreateBranch {
            name: &branch.name,
            from_ref: "main",
        };
        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::BranchExists,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn rename_branch() -> anyhow::Result<()> {
        use crate::api::testutil::test_name;

        let old_name = format!("colinmarc.{}", test_name("test_rename_old"));
        let new_name = format!("colinmarc.{}", test_name("test_rename_new"));

        // Create the branch.
        let req = CreateBranch {
            name: &old_name,
            from_ref: "main",
        };
        roundtrip(req)?;

        // Rename it.
        let req = RenameBranch {
            name: &old_name,
            new_name: &new_name,
        };
        let renamed = roundtrip(req)?;
        assert_eq!(renamed.name, new_name);

        // Verify old name is gone.
        let req = GetBranch { name: &old_name };
        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::BranchNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn merge_branch() -> anyhow::Result<()> {
        use crate::api::testutil::TestBranch;
        use crate::namespace::CreateNamespace;

        let source = TestBranch::new("test_merge_src")?;
        let target = TestBranch::new("test_merge_dst")?;

        // Make a change on the source branch by creating a namespace.
        let req = CreateNamespace {
            name: "test_merge_ns",
            branch: &source.name,
            commit: Default::default(),
        };
        roundtrip(req)?;

        // Merge source into target.
        let req = MergeBranch {
            source_ref: &source.name,
            into_branch: &target.name,
            commit: Default::default(),
        };
        let result = roundtrip(req)?;

        // The result should be a ref pointing to the target branch.
        match result {
            CatalogRef::Branch { name, .. } => assert_eq!(name, target.name),
            other => panic!("expected Branch ref, got {:?}", other),
        }

        // Verify the namespace exists on the target.
        let req = crate::namespace::GetNamespace {
            name: "test_merge_ns",
            at_ref: &target.name,
        };
        let ns = roundtrip(req)?;
        assert_eq!(ns.name, "test_merge_ns");

        Ok(())
    }

    #[test]
    fn merge_branch_not_found() -> anyhow::Result<()> {
        let req = MergeBranch {
            source_ref: "main",
            into_branch: "nonexistent_branch_12345",
            commit: Default::default(),
        };

        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::BranchNotFound,
                ..
            })
        );

        Ok(())
    }
}
