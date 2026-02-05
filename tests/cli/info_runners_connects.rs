use crate::cli::{bauplan, username};
use predicates::prelude::*;

#[test]
fn info_runners_connects() {
    bauplan()
        .args(["info"])
        .assert()
        .success()
        .stdout(contains("API Endpoint"))
        .stdout(contains("Catalog Endpoint"))
        .stdout(contains("Profile"))
        .stdout(contains("User"))
        .stdout(contains("Client Version"))
        .stdout(contains("Server Version"))
        .stdout(contains("Runners"))
        .stdout(contains("╰ bauplan"))
        .stdout(contains(username()));

    bauplan()
        .args(["--debug", "info"])
        .assert()
        .success()
        .stdout(contains("API Endpoint"))
        .stdout(contains("Catalog Endpoint"))
        .stdout(contains("Profile"))
        .stdout(contains("User"))
        .stdout(contains("Client Version"))
        .stdout(contains("Server Version"))
        .stdout(contains("Runners"))
        .stdout(contains("╰ bauplan"))
        .stdout(contains(&username()));
}
