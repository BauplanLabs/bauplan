use bstr::{BStr, BString, ByteSlice as _};
use predicates::reflection::{Case, Product};
use similar::{ChangeTag, TextDiff};
use std::fmt::Write as _;
use yansi::Paint as _;

mod cli {
    mod branch;
    mod import;
    mod query;
    mod run;
    mod table;
    mod tpch;

    use super::*;
}

pub fn bauplan() -> assert_cmd::Command {
    assert_cmd::cargo::cargo_bin_cmd!("bauplan")
}

pub fn username() -> String {
    std::env::var("BPLN_USERNAME").unwrap_or_else(|_| "bauplan-e2e-check".to_string())
}

/// A predicate that checks strings appear consecutively in order.
/// Mimics Python's ValidatorStringSequence.
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

            let mut diff = "\n".to_string();
            for change in changes {
                match change.tag() {
                    ChangeTag::Delete => {
                        write!(&mut diff, "{}", format!("- {change}").bright_red())
                    }
                    ChangeTag::Insert => {
                        write!(&mut diff, "{}", format!("+ {change}").bright_green())
                    }
                    ChangeTag::Equal => write!(&mut diff, "{}", format!("  {change}").dim()),
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
