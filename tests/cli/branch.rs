use crate::cli::{bauplan, username, test_branch};
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
    let branch = test_branch("cli_create_delete");

    bauplan()
        .args(["branch", "ls", "--all-zones"])
        .assert()
        .success()
        .stdout(contains(&branch.name));

    // Explicitly delete to test the delete command's output.
    bauplan()
        .args(["branch", "delete", &branch.name])
        .assert()
        .success()
        .stderr(contains(format!("Deleted branch \"{}\"", branch.name)));

    bauplan()
        .args(["branch", "ls", "--all-zones"])
        .assert()
        .success()
        .stdout(contains(&branch.name).not());

    // Drop will try to delete again, but that's harmless.
}

#[test]
fn create_if_not_exists() {
    let branch = test_branch("cli_create_idempotent");

    // With --if-not-exists, should succeed.
    bauplan()
        .args(["branch", "create", "--if-not-exists", &branch.name])
        .assert()
        .success()
        .stderr(contains("already exists"));

    // Without the flag, should fail.
    bauplan()
        .args(["branch", "create", &branch.name])
        .assert()
        .failure();
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
    let mut branch = test_branch("cli_rename_old");
    let new_name = format!("{}.cli_rename_new", username());
    let _ = bauplan().args(["branch", "delete", &new_name]).ok();

    bauplan()
        .args(["branch", "rename", &branch.name, &new_name])
        .assert()
        .success()
        .stderr(contains(format!(
            "Renamed branch \"{}\" to \"{new_name}\"",
            branch.name
        )));

    // Update the name so Drop cleans up the renamed branch.
    branch.name = new_name.clone();

    // Old name should be gone, new name should exist.
    bauplan()
        .args(["branch", "ls", "--all-zones"])
        .assert()
        .success()
        .stdout(contains(&branch.name))
        .stdout(contains("cli_rename_old").not());
}
