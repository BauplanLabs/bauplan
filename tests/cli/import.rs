use predicates::prelude::predicate;

use crate::cli::{bauplan, username};

const NAMESPACE_NAME: &str = "e2e-create-table";
const TABLE_NAME: &str = "my-table-name";

#[test]
fn import_create_table_and_query() {
    let branch = format!("{}.e2e_create_table", username());

    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success();

    bauplan()
        .args(["namespace", "create", "--branch", &branch, NAMESPACE_NAME])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "create",
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--namespace",
            NAMESPACE_NAME,
            "--name",
            TABLE_NAME,
            "--branch",
            &branch,
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "import",
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--namespace",
            NAMESPACE_NAME,
            "--name",
            TABLE_NAME,
            "--branch",
            &branch,
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "--ref",
            &branch,
            "--namespace",
            NAMESPACE_NAME,
            &format!("SELECT COUNT(*) FROM \"{}\"", TABLE_NAME),
        ])
        .assert()
        .success();

    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .success();
}

#[test]
fn import_manually() {
    let branch = format!("{}.e2e_import_manual", username());

    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "create-plan",
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--name",
            "table_with_partitions",
            "--branch",
            &branch,
            "--save-plan",
            "/tmp/planabcd.yaml",
        ])
        .assert()
        .success();

    bauplan()
        .args(["table", "create-plan-apply", "--plan", "/tmp/planabcd.yaml"])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "import",
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--name",
            "table_with_partitions",
            "--branch",
            &branch,
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "import",
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--name",
            "table_with_partitions",
            "--branch",
            &branch,
            "--import-duplicate-files",
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "import",
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--name",
            "table_with_partitions",
            "--branch",
            &branch,
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "query",
            "--cache",
            "off",
            "--ref",
            &branch,
            "SELECT COUNT(*) FROM table_with_partitions",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("400"));

    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .success();
}
