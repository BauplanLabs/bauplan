use crate::cli::{bauplan, username};
use predicates::prelude::PredicateBooleanExt as _;
use predicates::str::{contains, starts_with};

#[test]
fn ls_json_output() {
    bauplan()
        .args(["-O", "json", "branch", "ls"])
        .assert()
        .success()
        .stdout(starts_with("["));
}

#[test]
fn ls() {
    bauplan()
        .args(["branch", "ls"])
        .assert()
        .success()
        .stdout(contains("main"));
}

#[test]
fn get_main() {
    bauplan().args(["branch", "get", "main"]).assert().success();
}

#[test]
fn create_and_delete() {
    let branch = format!("{}.cli_create_delete", username());
    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success()
        .stderr(contains(format!("Created branch \"{branch}\"")));

    bauplan()
        .args(["branch", "ls", "--all-zones"])
        .assert()
        .success()
        .stdout(contains(&branch));

    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .success()
        .stderr(contains(format!("Deleted branch \"{branch}\"")));

    bauplan()
        .args(["branch", "ls", "--all-zones"])
        .assert()
        .success()
        .stdout(contains(&branch).not());
}

#[test]
fn create_if_not_exists() {
    let branch = format!("{}.cli_create_idempotent", username());
    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success();

    // With --if-not-exists, should succeed.
    bauplan()
        .args(["branch", "create", "--if-not-exists", &branch])
        .assert()
        .success()
        .stderr(contains("already exists"));

    // Without the flag, should fail.
    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .failure();

    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .success();
}

#[test]
fn delete_if_exists() {
    let branch = format!("{}.cli_delete_idempotent", username());

    // Make sure it doesn't exist.
    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    // With --if-exists, should succeed.
    bauplan()
        .args(["branch", "delete", "--if-exists", &branch])
        .assert()
        .success()
        .stderr(contains("does not exist"));

    // Without the flag, should fail.
    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .failure();
}

#[test]
fn rename() {
    let old_name = format!("{}.cli_rename_old", username());
    let new_name = format!("{}.cli_rename_new", username());
    let _ = bauplan().args(["branch", "delete", &old_name]).ok();
    let _ = bauplan().args(["branch", "delete", &new_name]).ok();

    bauplan()
        .args(["branch", "create", &old_name])
        .assert()
        .success();

    bauplan()
        .args(["branch", "rename", &old_name, &new_name])
        .assert()
        .success()
        .stderr(contains(format!(
            "Renamed branch \"{old_name}\" to \"{new_name}\""
        )));

    // Old name should be gone, new name should exist.
    bauplan()
        .args(["branch", "ls", "--all-zones"])
        .assert()
        .success()
        .stdout(contains(&old_name).not())
        .stdout(contains(&new_name));

    bauplan()
        .args(["branch", "delete", &new_name])
        .assert()
        .success();
}
