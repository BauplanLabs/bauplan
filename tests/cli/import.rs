use predicates::str::contains;

use crate::cli::{bauplan, test_branch};

const NAMESPACE_NAME: &str = "e2e-create-table";
const TABLE_NAME: &str = "my-table-name";

#[test]
fn import_create_table_and_query() {
    let branch = test_branch("e2e_create_table");

    bauplan()
        .args(["namespace", "create", "--branch", &branch.name, NAMESPACE_NAME])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "create",
            TABLE_NAME,
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--namespace",
            NAMESPACE_NAME,
            "--branch",
            &branch.name,
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "import",
            TABLE_NAME,
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--namespace",
            NAMESPACE_NAME,
            "--branch",
            &branch.name,
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "query",
            "--no-cache",
            "--ref",
            &branch.name,
            "--namespace",
            NAMESPACE_NAME,
            &format!("SELECT COUNT(*) FROM \"{}\"", TABLE_NAME),
        ])
        .assert()
        .success();
}

#[test]
fn import_manually() {
    let branch = test_branch("e2e_import_manual");

    bauplan()
        .args([
            "table",
            "create-plan",
            "table_with_partitions",
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--branch",
            &branch.name,
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
            "table_with_partitions",
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--branch",
            &branch.name,
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "import",
            "table_with_partitions",
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--branch",
            &branch.name,
            "--import-duplicate-files",
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "table",
            "import",
            "table_with_partitions",
            "--search-uri",
            "s3://bpln-e2e-test-tables/test_tables/two_columns_two_dates/*",
            "--branch",
            &branch.name,
        ])
        .assert()
        .success();

    bauplan()
        .args([
            "query",
            "--no-cache",
            "--ref",
            &branch.name,
            "SELECT COUNT(*) FROM table_with_partitions",
        ])
        .assert()
        .success()
        .stdout(contains("400"));
}
