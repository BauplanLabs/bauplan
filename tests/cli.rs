use anstyle::{AnsiColor, Style};
use anyhow::Context as _;
use bauplan::ApiResponse as _;
use bstr::{BStr, BString, ByteSlice as _};
use predicates::reflection::{Case, Product};
use similar::{ChangeTag, TextDiff};
use std::fmt::Write as _;

mod cli {
    mod auth;
    mod branch;
    mod import;
    mod init;
    mod job;
    mod parameter;
    mod query;
    mod run;
    mod table;
    mod tpch;

    use super::*;
}

pub fn bauplan() -> assert_cmd::Command {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bauplan");
    cmd.env("NO_COLOR", "1");
    cmd
}

pub fn username() -> String {
    std::env::var("BPLN_USERNAME").unwrap_or_else(|_| "bauplan-e2e-check".to_string())
}

pub fn test_branch(suffix: &str) -> TestBranch {
    let profile = bauplan::Profile::from_default_env()
        .expect("Failed to load test profile. Did you forget to set BAUPLAN_PROFILE?");
    let name = format!("{}.{suffix}", username());

    // Clean up any leftover branch from a previous run.
    let _ = roundtrip(&profile, bauplan::branch::DeleteBranch { name: &name });

    roundtrip(
        &profile,
        bauplan::branch::CreateBranch {
            name: &name,
            from_ref: "main",
        },
    )
    .expect("Failed to create test branch");

    TestBranch { name, profile }
}

/// A temporary branch that is deleted when dropped.
pub struct TestBranch {
    pub name: String,
    profile: bauplan::Profile,
}

fn roundtrip<T: bauplan::ApiRequest>(
    profile: &bauplan::Profile,
    req: T,
) -> Result<T::Response, anyhow::Error> {
    let req = req
        .into_request(profile)
        .context("Failed to create request")?;
    let resp = ureq::run(req).context("HTTP error")?;
    let resp = T::Response::from_response(resp.map(ureq::Body::into_reader))?;
    Ok(resp)
}

impl Drop for TestBranch {
    fn drop(&mut self) {
        if let Err(e) = roundtrip(
            &self.profile,
            bauplan::branch::DeleteBranch { name: &self.name },
        ) {
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
