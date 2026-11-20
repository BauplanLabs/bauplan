use anstyle::{AnsiColor, Style};
use bauplan_api::branch::{BranchApiClient, CreateBranchBody};
use bstr::{BStr, BString, ByteSlice as _};
use predicates::reflection::{Case, Product};
use similar::{ChangeTag, TextDiff};
use std::fmt::Write as _;

mod cli {
    mod auth;
    mod branch;
    mod config;
    mod import;
    mod init;
    mod job;
    mod parameter;
    mod query;
    mod run;
    mod table;
    mod tpch;
    mod types;

    use super::*;
}

pub fn bauplan() -> assert_cmd::Command {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bauplan");
    cmd.env("NO_COLOR", "1");
    cmd
}

pub fn username() -> &'static str {
    use std::sync::OnceLock;

    static USERNAME: OnceLock<String> = OnceLock::new();
    USERNAME.get_or_init(|| {
        let profile = bauplan::Profile::from_default_env()
            .expect("Failed to load test profile. Did you forget to set BAUPLAN_PROFILE?");

        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        rt.block_on(async {
            let mut client =
                bauplan::grpc::Client::new_lazy(&profile, std::time::Duration::from_secs(30))
                    .expect("Failed to create gRPC client");
            client.username().await.expect("Failed to get username")
        })
    })
}

pub fn test_branch(suffix: &str) -> TestBranch {
    let profile = bauplan::Profile::from_default_env()
        .expect("Failed to load test profile. Did you forget to set BAUPLAN_PROFILE?");
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let name = format!("{}.{suffix}.{timestamp}", username());
    let branch = TestBranch { name, profile };

    // Clean up any leftover branch from a previous run.
    let _ = branch.delete();

    let body = CreateBranchBody {
        name: branch.name.clone(),
        from_ref: "main".to_owned(),
        ttl_seconds: None,
    };
    branch
        .client()
        .create_branch(branch.profile.catalog.clone(), body)
        .expect("Failed to create test branch");

    branch
}

/// A temporary branch that is deleted when dropped.
pub struct TestBranch {
    pub name: String,
    profile: bauplan::Profile,
}

impl TestBranch {
    fn client(&self) -> BranchApiClient {
        let authorization = self
            .profile
            .api_key
            .as_ref()
            .map(|key| format!("Bearer {key}"));
        let middleware =
            move |mut req: http::Request<ureq::SendBody<'_>>,
                  next: ureq::middleware::MiddlewareNext<'_>| {
                if let Some(authorization) = &authorization {
                    req.headers_mut()
                        .insert(http::header::AUTHORIZATION, authorization.parse().unwrap());
                }
                next.handle(req)
            };
        let agent = ureq::Agent::new_with_config(
            ureq::config::Config::builder()
                .http_status_as_error(false)
                .middleware(middleware)
                .build(),
        );
        BranchApiClient::with_transport(self.profile.new_api_endpoint.to_string(), agent)
    }

    fn delete(&self) -> anyhow::Result<()> {
        let path = (self.profile.catalog.clone(), self.name.clone());
        self.client().delete_branch(path, None)?;
        Ok(())
    }
}

impl Drop for TestBranch {
    fn drop(&mut self) {
        if let Err(e) = self.delete() {
            eprintln!("Warning: failed to delete test branch {}: {e}", self.name);
        }
    }
}

/// A predicate that checks strings appear consecutively in order.
pub fn lines(expected: &[&str]) -> Lines {
    Lines {
        expected: expected.iter().map(|&s| s.into()).collect(),
    }
}

pub struct Lines {
    expected: Vec<BString>,
}

impl Lines {
    fn matches(&self, output_lines: &[&[u8]]) -> bool {
        let Some(first_expected) = self.expected.first() else {
            return true;
        };

        // Find the first line that matches.
        let Some(start) = output_lines.iter().position(|line| line == first_expected) else {
            return false;
        };

        if output_lines.len() - start < self.expected.len() {
            return false;
        }

        output_lines[start + 1..start + self.expected.len()] == self.expected[1..]
    }
}

// We implement for [u8] because otherwise predicates adds some noisy output.

impl predicates::Predicate<[u8]> for Lines {
    fn eval(&self, variable: &[u8]) -> bool {
        self.find_case(true, variable).is_some()
    }

    fn find_case<'a>(
        &'a self,
        expected: bool,
        output: &[u8],
    ) -> Option<predicates::reflection::Case<'a>> {
        let output_lines = BStr::new(output).lines().collect::<Vec<_>>();
        let matches = self.matches(&output_lines);
        if matches == expected {
            let expected_lines: Vec<_> = self.expected.iter().map(|b| b.as_bytes()).collect();
            let changes = &TextDiff::from_slices(&expected_lines, &output_lines)
                .iter_all_changes()
                .collect::<Vec<_>>();

            const RED: Style = AnsiColor::BrightRed.on_default();
            const GREEN: Style = AnsiColor::BrightGreen.on_default();
            const DIM: Style = Style::new().dimmed();

            let mut diff = "\n".to_string();
            for change in changes {
                match change.tag() {
                    ChangeTag::Delete => {
                        write!(&mut diff, "{RED}- {change}{RED:#}")
                    }
                    ChangeTag::Insert => {
                        write!(&mut diff, "{GREEN}+ {change}{GREEN:#}")
                    }
                    ChangeTag::Equal => write!(&mut diff, "{DIM}  {change}{DIM:#}"),
                }
                .unwrap();
            }

            Some(Case::new(Some(self), matches).add_product(Product::new("diff", diff)))
        } else {
            None
        }
    }
}

impl predicates::reflection::PredicateReflection for Lines {}

impl std::fmt::Display for Lines {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lines")
    }
}
