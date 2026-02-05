use crate::cli::bauplan;
use predicates::str::contains;

#[test]
fn query_tpch_1_01() {
    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "--file",
            "tests/fixtures/queries/tpch_1.q01.sql",
        ])
        .assert()
        .success()
        .stdout(contains("37734107"))
        .stdout(contains("56586554400."))
        .stdout(contains("53758257134."))
        .stdout(contains("55909065222."));
}

#[test]
fn query_tpch_1_q15() {
    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "-f",
            "tests/fixtures/queries/tpch_1.q13.sql",
        ])
        .assert()
        .success()
        .stdout(contains("50004"))
        .stdout(contains("6668"))
        .stdout(contains("6563"));
}
