use predicates::str::contains;

use crate::{cli::bauplan, lines};

#[test]
fn ambiguous_column_name() {
    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "-f",
            "tests/fixtures/queries/ambiguous_column_name.sql",
        ])
        .assert()
        .code(1);
}

#[test]
fn expected_zero_results() {
    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "SELECT trip_time, trip_miles FROM taxi_fhvhv WHERE pickup_datetime >= '2046-01-01T00:00:00-05:00'",
        ])
        .assert()
        .success();
}

#[test]
fn nested_queries() {
    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "-f",
            "tests/fixtures/queries/nested_queries_check.sql",
        ])
        .assert()
        .success();
}

#[test]
fn order_by() {
    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "SELECT PULocationID AS id_1, PULocationID, trip_miles AS miles_1 FROM taxi_fhvhv WHERE pickup_datetime >= '2023-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-02T00:00:00-05:00' ORDER BY 1 LIMIT 5",
        ])
        .assert()
        .success()
        .stdout(lines(&[
            "COLUMN        TYPE     NULLABLE",
            "id_1          Int64    true",
            "PULocationID  Int64    true",
            "miles_1       Float64  true",
        ]));
}

#[test]
fn reshaped_scan() {
    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "SELECT PULocationID AS id_1, PULocationID, trip_miles AS miles_1 FROM taxi_fhvhv WHERE pickup_datetime >= '2023-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-02T00:00:00-05:00' LIMIT 5",
        ])
        .assert()
        .success()
        .stdout(lines(&[
            "COLUMN        TYPE     NULLABLE",
            "id_1          Int64    true",
            "PULocationID  Int64    true",
            "miles_1       Float64  true",
        ]));
}

#[test]
fn string_view() {
    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "SELECT CONCAT(hvfhs_license_num, 'foobar') as license, FROM taxi_fhvhv LIMIT 5",
        ])
        .assert()
        .success()
        .stdout(lines(&[
            "COLUMN   TYPE      NULLABLE",
            "license  Utf8View  true",
        ]));
}

#[test]
fn subquery_optimized_scan() {
    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "WITH a AS (SELECT PULocationID AS id_1, trip_miles, pickup_datetime FROM taxi_fhvhv WHERE pickup_datetime >= '2023-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-02T00:00:00-05:00') SELECT MAX(id_1) AS max_id_1, 'ciao' AS ciao_1 FROM a",
        ])
        .assert()
        .success()
        .stdout(lines(&[
            "COLUMN    TYPE   NULLABLE",
            "max_id_1  Int64  true",
            "ciao_1    Utf8   false",
            "",
            "max_id_1  ciao_1",
            "265       ciao",
        ]));
}

#[test]
fn with_results_json_output() {
    bauplan()
        .args([
            "-O", "json",
            "query",
            "--cache", "off",
            "SELECT PULocationID, COUNT(*) FROM taxi_fhvhv WHERE pickup_datetime >= '2023-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T01:00:00-05:00' GROUP BY 1 ORDER BY PULocationID",
        ])
        .assert()
        .success()
        .stdout(contains(r#""results":"#));
}

#[test]
fn run_twice() {
    bauplan()
        .args([
            "query",
            "--cache", "on",
            "SELECT PULocationID, COUNT(*) FROM taxi_fhvhv WHERE pickup_datetime >= '2023-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-02T00:00:00-05:00' GROUP BY 1 ORDER BY PULocationID",
        ])
        .assert()
        .success()
        .stdout(lines(&[
            "3             973",
            "4             1314",
            "5             132",
            "6             207"
        ]));

    bauplan()
        .args([
            "query",
            "--cache", "on",
            "SELECT PULocationID, COUNT(*) FROM taxi_fhvhv WHERE pickup_datetime >= '2023-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-02T00:00:00-05:00' GROUP BY 1 ORDER BY PULocationID",
        ])
        .assert()
        .success()
        .stdout(lines(&[
            "3             973",
            "4             1314",
            "5             132",
            "6             207"
        ]));
}
