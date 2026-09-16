use crate::cli::{bauplan, test_branch};
use crate::lines;
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
    let branch = test_branch("externalclimetadata");

    bauplan()
        .args([
            "table",
            "create-external",
            "external_table_metadata",
            "--branch",
            &branch.name,
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
            &branch.name,
            "SELECT COUNT(*) FROM external_table_metadata",
        ])
        .assert()
        .success()
        .stdout(contains("150000"));
}

#[test]
fn register_table_parquet() {
    let branch = test_branch("externalcliparquet");

    bauplan()
        .args([
            "table",
            "create-external",
            "external_table_parquet",
            "--branch",
            &branch.name,
            "--search-pattern",
            "s3://bauplan-openlake-db87a23/stage/taxi_fhvhv/*2023*",
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "query",
            "--ref",
            &branch.name,
            "SELECT COUNT(*) FROM external_table_parquet",
        ])
        .assert()
        .success()
        .stdout(contains("134344870"));
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
fn get_column_documentation() {
    let branch = test_branch("cli_table_get_docs");

    bauplan()
        .args([
            "run",
            "--ref",
            &branch.name,
            "--no-cache",
            "-p",
            "tests/fixtures/simple_taxi_dag",
        ])
        .assert()
        .success();

    // We expect to cover the following cases:
    // - doc string fits neatly (short single-line)
    // - doc string has a newline (short multi-line)
    // - doc string has a multi-byte character (character counting)
    //   - The `trip_miles` doc string is 40 characters but 41 bytes, `è` covers
    //     bytes 20 and 21, so counting bytes would truncate at `espress...`
    //     instead of `espressa...`.
    // - doc string truncation occurs after whitespace (long multi-line)
    // - doc string contains a tab (tab expansion in a DAG)
    // - empty doc (`None`)
    bauplan()
        .args(["table", "get", "--ref", &branch.name, "normalize_data"])
        .assert()
        .success()
        .stdout(lines(&[
            "A \"no-op\" normalization of taxi data from `bauplan.query_model` that doesn't do",
            "anything and materializes the data as-is into `bauplan.normalize_data` (results",
            "should be identical to `bauplan.query_model`).",
            "",
            "The output schema, `NormalizedTripsSchema`, is also used as a projection schema on",
            "`query_model`.",
        ]))
        .stdout(lines(&[
            "COLUMN               TYPE         NULLABLE  DOC",
            "trip_time            long         true      Trip duration, in seconds.",
            "pickup_datetime      timestamptz  true      Pickup time....",
            "trip_miles           double       true      Distanza percorsa: è espressa...",
            "dropoff_datetime     timestamptz  true      The time the passengers left...",
            "base_passenger_fare  double       true      Fare    before tolls and tax.",
            "tolls                double       true      -",
        ]))
        .stderr(contains("some documentation was truncated"));

    // We also check that SQL models have documentation persisted.
    bauplan()
        .args(["table", "get", "--ref", &branch.name, "query_model"])
        .assert()
        .success()
        .stdout(lines(&[
            "Output schema for `query_model` applied as a projection on `bauplan.taxi_fhvhv`.",
            "",
            "+ ---------- +   QueryModelSchema    + ----------- +",
            "| taxi_fhvhv | --------------------> | query_model |",
            "+ ---------- +                       + ----------- +",
        ]))
        .stdout(lines(&[
            "COLUMN               TYPE         NULLABLE  DOC",
            "pickup_datetime      timestamptz  true      Pickup time, UTC timezone, fi...",
            "dropoff_datetime     timestamptz  true      -",
            "PULocationID         long         true      -",
            "DOLocationID         long         true      -",
            "trip_miles           double       true      -",
            "trip_time            long         true      -",
            "base_passenger_fare  double       true      -",
            "tolls                double       true      -",
            "sales_tax            double       true      -",
            "tips                 double       true      -",
        ]))
        .stderr(contains("some documentation was truncated"));
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
    let branch = test_branch("cli_delete_table");

    // Create a table via external metadata registration.
    bauplan()
        .args([
            "table",
            "create-external",
            "delete_me",
            "--branch",
            &branch.name,
            "--metadata-json-uri",
            "s3://bauplan-openlake-db87a23/iceberg/tpch_1/customer_e53c682c-36c4-4e3d-9ded-1214d0ee157f/metadata/00000-b6f502e1-5140-499e-bf83-22f943067e36.metadata.json",
            "--namespace",
            "bauplan",
        ])
        .assert()
        .success();

    // Verify it exists.
    bauplan()
        .args(["table", "ls", "--ref", &branch.name])
        .assert()
        .success()
        .stdout(contains("delete_me"));

    // Delete it.
    bauplan()
        .args(["table", "rm", "bauplan.delete_me", "--branch", &branch.name])
        .assert()
        .success()
        .stderr(contains("Deleted table"));

    // Verify it's gone.
    bauplan()
        .args(["table", "ls", "--ref", &branch.name])
        .assert()
        .success()
        .stdout(contains("delete_me").not());
}

#[test]
fn delete_table_if_exists() {
    let branch = test_branch("cli_delete_table_exists");

    // With --if-exists, should succeed even though the table doesn't exist.
    bauplan()
        .args([
            "table",
            "rm",
            "bauplan.nonexistent_xyz",
            "--branch",
            &branch.name,
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
            &branch.name,
        ])
        .assert()
        .failure();
}
