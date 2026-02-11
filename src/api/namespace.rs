//! API operations concerning table namespaces.

use serde::{Deserialize, Serialize};

use crate::{
    CatalogRef, PaginatedResponse,
    api::{ApiRequest, DataResponse, commit::CommitOptions},
};

/// A table namespace.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(name = "Namespace", module = "bauplan", get_all)
)]
pub struct Namespace {
    /// The namespace name.
    pub name: String,
}

impl DataResponse for Namespace {}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl Namespace {
    fn __repr__(&self) -> String {
        format!("Namespace(name={:?})", self.name)
    }
}

/// Load a single namespace.
#[derive(Debug, Clone)]
pub struct GetNamespace<'a> {
    /// The name of the namespace to fetch.
    pub name: &'a str,

    /// The ref (branch, tag, etc) at which to read the namespace.
    pub at_ref: &'a str,
}

impl ApiRequest for GetNamespace<'_> {
    type Response = Namespace;

    fn path(&self) -> String {
        format!("/catalog/v0/refs/{}/namespaces/{}", self.at_ref, self.name)
    }
}

/// List namespaces in a ref.
#[derive(Debug, Clone)]
pub struct GetNamespaces<'a> {
    /// The ref (branch, tag, etc) at which to list namespaces.
    pub at_ref: &'a str,

    /// Filter namespaces by name pattern.
    pub filter_by_name: Option<&'a str>,
}

#[derive(Serialize)]
struct GetNamespacesQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_by_name: Option<&'a str>,
}

impl ApiRequest for GetNamespaces<'_> {
    type Response = PaginatedResponse<Namespace>;

    fn path(&self) -> String {
        format!("/catalog/v0/refs/{}/namespaces", self.at_ref)
    }

    fn query(&self) -> Option<impl Serialize> {
        Some(GetNamespacesQuery {
            filter_by_name: self.filter_by_name,
        })
    }
}

/// Create a namespace on a branch.
#[derive(Debug, Clone)]
pub struct CreateNamespace<'a> {
    /// The name of the namespace to create.
    pub name: &'a str,

    /// The branch to create the namespace on.
    pub branch: &'a str,

    /// Override the commit body or add custom properties.
    pub commit: CommitOptions<'a>,
}

#[derive(Debug, Clone, Serialize)]
struct CreateNamespaceBody<'a> {
    namespace_name: &'a str,
    #[serde(flatten)]
    commit: CommitOptions<'a>,
}

impl ApiRequest for CreateNamespace<'_> {
    type Response = Namespace;

    fn method(&self) -> http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        format!("/catalog/v0/branches/{}/namespaces", self.branch)
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(CreateNamespaceBody {
            namespace_name: self.name,
            commit: self.commit.clone(),
        })
    }
}

/// Delete a namespace from a branch.
#[derive(Debug, Clone)]
pub struct DeleteNamespace<'a> {
    /// The name of the namespace to delete.
    pub name: &'a str,

    /// The branch to delete the namespace from.
    pub branch: &'a str,

    /// Override the commit body or add custom properties.
    pub commit: CommitOptions<'a>,
}

#[derive(Debug, Clone, Serialize)]
struct DeleteNamespaceBody<'a> {
    #[serde(flatten)]
    commit: CommitOptions<'a>,
}

impl ApiRequest for DeleteNamespace<'_> {
    type Response = CatalogRef;

    fn method(&self) -> http::Method {
        http::Method::DELETE
    }

    fn path(&self) -> String {
        format!(
            "/catalog/v0/branches/{}/namespaces/{}",
            self.branch, self.name
        )
    }

    fn body(&self) -> Option<impl Serialize> {
        Some(DeleteNamespaceBody {
            commit: self.commit.clone(),
        })
    }
}

#[cfg(all(test, feature = "_integration-tests"))]
mod test {
    use assert_matches::assert_matches;

    use super::*;
    use crate::{ApiError, ApiErrorKind, api::testutil::roundtrip};

    #[test]
    fn get_namespace() -> anyhow::Result<()> {
        let req = GetNamespace {
            name: "bauplan",
            at_ref: "main",
        };

        let ns: Namespace = roundtrip(req)?;
        assert_eq!(ns.name, "bauplan");

        Ok(())
    }

    #[test]
    fn get_namespace_not_found() -> anyhow::Result<()> {
        let req = GetNamespace {
            name: "nonexistent_namespace_12345",
            at_ref: "main",
        };

        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::NamespaceNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn get_namespace_ref_not_found() -> anyhow::Result<()> {
        let req = GetNamespace {
            name: "bauplan",
            at_ref: "nonexistent_branch_12345",
        };

        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::RefNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn get_namespaces() -> anyhow::Result<()> {
        let req = GetNamespaces {
            at_ref: "main",
            filter_by_name: None,
        };

        let namespaces = crate::paginate(req, None, |r| roundtrip(r))?
            .collect::<Result<Vec<Namespace>, ApiError>>()?;

        let bauplan = namespaces.iter().find(|ns| ns.name == "bauplan");
        assert!(bauplan.is_some());

        Ok(())
    }

    #[test]
    fn get_namespaces_with_filter() -> anyhow::Result<()> {
        let req = GetNamespaces {
            at_ref: "main",
            filter_by_name: Some("bauplan"),
        };

        let namespaces = crate::paginate(req, Some(10), |r| roundtrip(r))?
            .collect::<Result<Vec<Namespace>, ApiError>>()?;

        assert!(!namespaces.is_empty());
        assert!(namespaces.iter().all(|ns| ns.name.contains("bauplan")));

        Ok(())
    }

    #[test]
    fn get_namespaces_ref_not_found() -> anyhow::Result<()> {
        let req = GetNamespaces {
            at_ref: "nonexistent_branch_12345",
            filter_by_name: None,
        };

        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::RefNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn create_and_delete_namespace() -> anyhow::Result<()> {
        use crate::api::testutil::{TestBranch, test_name};

        let branch = TestBranch::new("test_ns")?;
        let ns_name = test_name("test_namespace");

        // Create the namespace.
        let req = CreateNamespace {
            name: &ns_name,
            branch: &branch.name,
            commit: Default::default(),
        };
        let created = roundtrip(req)?;
        assert_eq!(created.name, ns_name);

        // Verify it exists.
        let req = GetNamespace {
            name: &ns_name,
            at_ref: &branch.name,
        };
        let fetched = roundtrip(req)?;
        assert_eq!(fetched.name, ns_name);

        // Delete it.
        let req = DeleteNamespace {
            name: &ns_name,
            branch: &branch.name,
            commit: Default::default(),
        };
        roundtrip(req)?;

        // Verify it's gone.
        let req = GetNamespace {
            name: &ns_name,
            at_ref: &branch.name,
        };
        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::NamespaceNotFound,
                ..
            })
        );

        Ok(())
    }

    #[test]
    fn create_namespace_already_exists() -> anyhow::Result<()> {
        use crate::api::testutil::{TestBranch, test_name};

        let branch = TestBranch::new("test_ns_exists")?;
        let ns_name = test_name("test_namespace");

        // Create the namespace.
        let req = CreateNamespace {
            name: &ns_name,
            branch: &branch.name,
            commit: Default::default(),
        };
        roundtrip(req)?;

        // Try to create it again.
        let req = CreateNamespace {
            name: &ns_name,
            branch: &branch.name,
            commit: Default::default(),
        };
        let result = roundtrip(req);
        assert_matches!(
            result,
            Err(ApiError::ErrorResponse {
                kind: ApiErrorKind::NamespaceExists,
                ..
            })
        );

        Ok(())
    }
}
