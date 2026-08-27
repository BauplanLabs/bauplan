use crate::{bauplan, test_branch};
use predicates::str::contains;

#[test]
fn multi_field_schema() {
    let branch = test_branch("sdk_types_happy_path");

    bauplan()
        .args([
            "run",
            "--ref",
            &branch.name,
            "--no-cache",
            "-p",
            "tests/fixtures/multi-field-schema",
        ])
        .assert()
        .success()
        .stderr(contains("Golden Ratio: 1.666"))
        .stderr(contains("Start Datetime: 2023-01-01T00:00:00+00:00"));
}

#[test]
fn missing_schema() {
    bauplan()
        .args([
            "run",
            "--ref",
            "main",
            "--dry-run",
            "--no-cache",
            "-p",
            "tests/fixtures/missing-schema",
        ])
        .assert()
        .code(1)
        .stderr(contains("while parsing your code"))
        .stderr(contains(r#"returns unknown schema "TestSchemaMisnamed""#));
}

#[test]
fn schema_wrong_base() {
    bauplan()
        .args([
            "run",
            "--ref",
            "main",
            "--dry-run",
            "--no-cache",
            "-p",
            "tests/fixtures/schema-wrong-base",
        ])
        .assert()
        .code(1)
        .stderr(contains("while parsing your code"))
        .stderr(contains(r#"returns unknown schema "BadBaseSchema""#));
}
