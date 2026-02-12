use crate::cli::{bauplan, username};
use predicates::prelude::PredicateBooleanExt as _;
use predicates::str::contains;

#[test]
fn namespace() {
    bauplan()
        .args(["namespace", "ls"])
        .assert()
        .success()
        .stdout(contains("bauplan"));
}

#[test]
fn register_table_metadata() {
    let branch = format!("{}.externalclimetadata", username());

    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "create-external",
            "external_table_metadata",
            "--branch",
            &branch,
            "--metadata-json-uri",
            "s3://bauplan-openlake-db87a23/iceberg/tpch_1/customer_e53c682c-36c4-4e3d-9ded-1214d0ee157f/metadata/00000-b6f502e1-5140-499e-bf83-22f943067e36.metadata.json",
            "--namespace",
            "bauplan",
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "query",
            "--ref",
            &branch,
            "SELECT COUNT(*) FROM external_table_metadata",
        ])
        .assert()
        .success()
        .stdout(contains("150000"));

    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .success();
}

#[test]
fn register_table_parquet() {
    let branch = format!("{}.externalcliparquet", username());

    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "create-external",
            "external_table_parquet",
            "--branch",
            &branch,
            "--search-pattern",
            "s3://bauplan-openlake-db87a23/stage/taxi_fhvhv/*2023*",
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "query",
            "--ref",
            &branch,
            "SELECT COUNT(*) FROM external_table_parquet",
        ])
        .assert()
        .success()
        .stdout(contains("134344870"));

    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .success();
}

#[test]
fn get_json_output() {
    bauplan()
        .args(["-O", "json", "table", "get", "bauplan.taxi_fhvhv"])
        .assert()
        .success()
        .stdout(contains(r#""name":"taxi_fhvhv","#))
        .stdout(contains(r#""namespace":"bauplan","#));
}

#[test]
fn main_taxi_fhvhv() {
    bauplan()
        .args(["table", "get", "--ref", "main", "bauplan.taxi_fhvhv"])
        .assert()
        .success();
}

#[test]
fn delete_table() {
    let branch = format!("{}.cli_delete_table", username());
    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success();

    // Create a table via external metadata registration.
    bauplan()
        .args([
            "table",
            "create-external",
            "delete_me",
            "--branch",
            &branch,
            "--metadata-json-uri",
            "s3://bauplan-openlake-db87a23/iceberg/tpch_1/customer_e53c682c-36c4-4e3d-9ded-1214d0ee157f/metadata/00000-b6f502e1-5140-499e-bf83-22f943067e36.metadata.json",
            "--namespace",
            "bauplan",
        ])
        .assert()
        .success();

    // Verify it exists.
    bauplan()
        .args(["table", "ls", "--ref", &branch])
        .assert()
        .success()
        .stdout(contains("delete_me"));

    // Delete it.
    bauplan()
        .args(["table", "rm", "bauplan.delete_me", "--branch", &branch])
        .assert()
        .success()
        .stderr(contains("Deleted table"));

    // Verify it's gone.
    bauplan()
        .args(["table", "ls", "--ref", &branch])
        .assert()
        .success()
        .stdout(contains("delete_me").not());

    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .success();
}

#[test]
fn delete_table_if_exists() {
    let branch = format!("{}.cli_delete_table_exists", username());
    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success();

    // With --if-exists, should succeed even though the table doesn't exist.
    bauplan()
        .args([
            "table",
            "rm",
            "bauplan.nonexistent_xyz",
            "--branch",
            &branch,
            "--if-exists",
        ])
        .assert()
        .success()
        .stderr(contains("does not exist"));

    // Without the flag, should fail.
    bauplan()
        .args([
            "table",
            "rm",
            "bauplan.nonexistent_xyz",
            "--branch",
            &branch,
        ])
        .assert()
        .failure();

    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .success();
}
