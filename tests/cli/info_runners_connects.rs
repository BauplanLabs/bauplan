use crate::cli::{bauplan, username};
use predicates::prelude::*;

#[test]
fn info_runners_connects() {
    bauplan()
        .args(["info"])
        .assert()
        .success()
        .stdout(predicate::str::contains("API Endpoint"))
        .stdout(predicate::str::contains("Catalog Endpoint"))
        .stdout(predicate::str::contains("Profile"))
        .stdout(predicate::str::contains("User"))
        .stdout(predicate::str::contains("Client Version"))
        .stdout(predicate::str::contains("Server Version"))
        .stdout(predicate::str::contains("Runners"))
        .stdout(predicate::str::contains("╰ bauplan"))
        .stdout(predicate::str::contains(username()));

    bauplan()
        .args(["--debug", "info"])
        .assert()
        .success()
        .stdout(predicate::str::contains("API Endpoint"))
        .stdout(predicate::str::contains("Catalog Endpoint"))
        .stdout(predicate::str::contains("Profile"))
        .stdout(predicate::str::contains("User"))
        .stdout(predicate::str::contains("Client Version"))
        .stdout(predicate::str::contains("Server Version"))
        .stdout(predicate::str::contains("Runners"))
        .stdout(predicate::str::contains("╰ bauplan"))
        .stdout(predicate::str::contains(&username()));
}
