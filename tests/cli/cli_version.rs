use crate::cli::bauplan;
use predicates::prelude::*;

const BAUPLAN_VERSION_PREFIX: &str = "0.0.3a";

#[test]
fn cli_version() {
    bauplan()
        .args(["version"])
        .assert()
        .success()
        .stdout(predicate::str::contains(BAUPLAN_VERSION_PREFIX));
}
