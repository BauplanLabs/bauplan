use crate::cli::{bauplan, username};
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
            "--name",
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
            "--name",
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
