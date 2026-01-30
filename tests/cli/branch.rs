use crate::cli::bauplan;
use predicates::prelude::*;

#[test]
fn ls_json_output() {
    bauplan()
        .args(["-O", "json", "branch", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("["));
}

#[test]
fn ls() {
    bauplan()
        .args(["branch", "ls"])
        .assert()
        .success()
        .stdout(predicate::str::contains("main"));
}

#[test]
fn get_main() {
    bauplan().args(["branch", "get", "main"]).assert().success();
}
