use crate::cli::bauplan;
use predicates::prelude::PredicateBooleanExt as _;
use predicates::str::contains;

#[test]
fn invalid_api_key() {
    bauplan()
        .env("BAUPLAN_API_KEY", "invalid")
        .args(["branch", "ls", "--limit", "10"])
        .assert()
        .failure()
        .stderr(contains("Invalid or missing bearer token."))
        .stderr(contains("A generic error.").not())
        .stderr(contains("Failed to parse").not());
}
