use crate::{bauplan, username};
use predicates::{
    prelude::*,
    str::{contains, starts_with},
};

#[test]
fn dry_run() {
    bauplan()
        .args([
            "run",
            "--ref",
            "main",
            "--dry-run",
            "--cache",
            "off",
            "-p",
            "tests/fixtures/simple_taxi_dag",
        ])
        .assert()
        .success()
        .stderr(contains("num_rows= 430488"));
}

#[test]
fn run_json_output() {
    bauplan()
        .args([
            "-O",
            "json",
            "run",
            "--dry-run",
            "--cache",
            "off",
            "-p",
            "tests/fixtures/simple_taxi_dag",
        ])
        .assert()
        .success()
        .stdout(starts_with("{"));
}

#[test]
fn executor_pip_install_error() {
    bauplan()
        .args([
            "run",
            "--ref",
            "main",
            "--dry-run",
            "--cache",
            "off",
            "-p",
            "tests/fixtures/simple_taxi_dag",
            "--arg",
            "executor.pip-install-error=true",
        ])
        .assert()
        .code(1)
        .stderr(contains("an internal error occurred"));
}

#[test]
fn expectations_returns_int() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "--strict",
            "on",
            "-p",
            "tests/fixtures/expectation_returns_int",
        ])
        .assert()
        .code(1)
        .stderr(
            contains("Expectation must return a boolean!")
                .or(contains("expectation returned with unsupported type")),
        );
}

#[test]
fn run_failing_expectation() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "--strict",
            "on",
            "-p",
            "tests/fixtures/failing_expectation",
        ])
        .assert()
        .code(1)
        .stderr(contains("expectation returned false"));
}

#[test]
fn run_failing_expectation_strict_on() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "--strict",
            "on",
            "-p",
            "tests/fixtures/assert_in_expectation",
        ])
        .assert()
        .code(1)
        .stderr(contains("assert False"));
}

#[test]
fn failing_expectation() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "--strict",
            "off",
            "-p",
            "tests/fixtures/failing_expectation",
        ])
        .assert()
        .success();
}

#[test]
fn run_assert_in_expectation() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "--strict",
            "off",
            "-p",
            "tests/fixtures/assert_in_expectation",
        ])
        .assert()
        .success();
}

#[test]
fn invalid_package_ppandas() {
    bauplan()
        .args([
            "run",
            "--cache",
            "off",
            "--dry-run",
            "--project-dir",
            "tests/fixtures/invalid_package_pppandas",
        ])
        .assert()
        .code(1)
        .stderr(
            contains(
                "depends on pppandas (2.1.0) which doesn't match any versions, version solving",
            )
            .or(contains("pppandas was not found in the package registry")),
        );
}

#[test]
fn materialize_partitioned_by_year() {
    let branch = format!("{}.e2e_test_for_materialization", username());

    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success();

    bauplan()
        .args([
            "run",
            "--ref",
            &branch,
            "--cache",
            "off",
            "-p",
            "tests/fixtures/materialize_partitioned_by_year",
        ])
        .assert()
        .success();

    let _ = bauplan().args(["branch", "delete", &branch]).ok();
}

#[test]
fn multiparent() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "-p",
            "tests/fixtures/multiparent",
        ])
        .assert()
        .success()
        .stderr(contains("val_0,val_1,val_0,val_1,val_0,val_1"));
}

// TODO: add this back once we have explicit fixtures. For now this depends
// on the environment being set up correctly, but we want people to be able to
// run tests themselves.
// #[test]
// fn no_write_access() {
//     // Supposed to fail, because it's supposed to be run as a non-admin user.
//     bauplan()
//         .args(["run", "--ref", "main", "-p", "tests/fixtures/prophet"])
//         .assert()
//         .code(1)
//         .stderr(contains("you don't have write access to"));
// }

// Disabled - this depends on careful timing to make sure a node fails during a
// model write, and producing that timing is easier in unit tests.
// See: service-runner-poc-go/service/bauplan_runner_poc/test/integration/physical_plan/physical_plan_test.go

// use crate::cli::bauplan;
// use predicates::prelude::*;
//
// const BRANCH_NAME: &str = "e2e_cli_parallelized";
//
// #[test]
// fn run() {
//     let _ = bauplan().args(["branch", "delete", BRANCH_NAME]).ok();
//
//     bauplan()
//         .args(["branch", "create", BRANCH_NAME])
//         .assert()
//         .success();
//
//     bauplan()
//         .args([
//             "run",
//             "--cache", "off",
//             "--branch", BRANCH_NAME,
//             "--param", "child_1_should_fail=false",
//             "--param", "child_2_should_fail=false",
//             "--param", "child_3_should_fail=false",
//             "--param", "grand_child_1_should_fail=true",
//             "--param", "child_1_should_sleep=false",
//             "--param", "child_2_should_sleep=false",
//             "--param", "child_3_should_sleep=false",
//             "--param", "grand_child_1_should_sleep=false",
//             "-p", "tests/fixtures/parallel_models",
//         ])
//         .assert()
//         .code(1)
//         .stdout(contains("timed").not())
//         .stdout(contains("Task failed"))
//         .stdout(contains("s3write parallel_child_1"));
// }

#[test]
fn parameters_project() {
    bauplan()
        .args([
            "run",
            "--cache",
            "off",
            "--dry-run",
            "-p",
            "tests/fixtures/parameters",
            "--param",
            "location_id=123",
            "--param",
            "golden_ratio=4.2",
            "--param",
            "use_random_forest=false",
            "--param",
            "start_datetime=2023-01-01T00:00:00+00:00",
            "--param",
            "end_datetime=2023-01-02T00:00:00+00:00",
        ])
        .assert()
        .success()
        .stderr(contains("golden_ratio=4.2"))
        .stderr(contains("use_random_forest=False"))
        .stderr(contains("start_datetime=2023-01-01T00:00:00+00:00"))
        .stderr(contains("end_datetime=2023-01-02T00:00:00+00:00"))
        .stderr(contains("yayparams.num_rows=629154"))
        .stderr(contains("yayparams.num_columns=3"));
}

#[test]
fn parameters_project_default_values() {
    bauplan()
        .args([
            "run",
            "--cache",
            "off",
            "--dry-run",
            "-p",
            "tests/fixtures/parameters",
        ])
        .assert()
        .success()
        .stderr(contains("golden_ratio=1.666"))
        .stderr(contains("use_random_forest=True"))
        .stderr(contains("start_datetime=2023-01-01T00:00:00+00:00"))
        .stderr(contains("end_datetime=2023-01-03T00:00:00+00:00"))
        .stderr(contains("yayparams.num_rows=1037238"))
        .stderr(contains("yayparams.num_columns=3"));
}

#[test]
fn parameters_project_kms_ssm() {
    const REDACTED_MESSAGE: &str = "<secret-***>";

    fn reverse(s: &str) -> String {
        s.chars().rev().collect()
    }

    let my_secret_key_reversed_1 = reverse("this is my secret");
    let my_secret_key_reversed_2 = reverse("this is another secret");
    let my_vault_secure_string_us_reversed = reverse("This is the US encrypted string value");
    let my_vault_override_us_with_eu_reversed = reverse("This is the EU encrypted string value");
    let my_vault_secure_string_eu_reversed = reverse("This is the EU encrypted string value");
    let my_vault_override_eu_with_us_reversed = reverse("This is the US encrypted string value");

    bauplan()
        .args([
            "run",
            "--cache", "off",
            "--dry-run",
            "-p", "tests/fixtures/parameters_kms_ssm",
            "--param", "my_secret_key_2=this is another secret",
            "--param", "my_vault_override_us_with_eu=awsssm:///arn:aws:ssm:eu-west-1:381492128837:parameter/e2e/secure-string-parameter",
            "--param", "my_vault_override_eu_with_us=awsssm:///arn:aws:ssm:us-east-1:381492128837:parameter/e2e/secure-string-parameter",
        ])
        .assert()
        .success()
        .stderr(contains(format!("my_secret_key_1={REDACTED_MESSAGE}")))
        .stderr(contains(format!("my_secret_key_2={REDACTED_MESSAGE}")))
        .stderr(contains(format!("my_secret_key_1_reversed={my_secret_key_reversed_1}")))
        .stderr(contains(format!("my_secret_key_2_reversed={my_secret_key_reversed_2}")))
        .stderr(contains("my_vault_string_us=This is the US string value"))
        .stderr(contains("my_vault_string_list_us=this,is,the,us,string,list,value"))
        .stderr(contains(format!("my_vault_secure_string_us={REDACTED_MESSAGE}")))
        .stderr(contains(format!("my_vault_secure_string_us_reversed={my_vault_secure_string_us_reversed}")))
        .stderr(contains(format!("my_vault_override_us_with_eu={REDACTED_MESSAGE}")))
        .stderr(contains(format!("my_vault_override_us_with_eu_reversed={my_vault_override_us_with_eu_reversed}")))
        .stderr(contains("my_vault_string_eu=This is the EU string value"))
        .stderr(contains("my_vault_string_list_eu=this,is,the,eu,string,list,value"))
        .stderr(contains(format!("my_vault_secure_string_eu={REDACTED_MESSAGE}")))
        .stderr(contains(format!("my_vault_secure_string_eu_reversed={my_vault_secure_string_eu_reversed}")))
        .stderr(contains(format!("my_vault_override_eu_with_us={REDACTED_MESSAGE}")))
        .stderr(contains(format!("my_vault_override_eu_with_us_reversed={my_vault_override_eu_with_us_reversed}")));
}

#[test]
fn parquet_field_ids() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "-p",
            "tests/fixtures/parquet_field_ids",
        ])
        .assert()
        .success();
}

#[test]
fn prophet_with_materialization() {
    let branch = format!("{}.prophet_with_materialization", username());

    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success();

    bauplan()
        .args([
            "run",
            "--ref",
            &branch,
            "--cache",
            "off",
            "-p",
            "tests/fixtures/prophet",
        ])
        .assert()
        .success()
        .stderr(contains("ciao gianx"));

    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .success();
}

#[test]
fn prophet() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "-p",
            "tests/fixtures/prophet",
        ])
        .assert()
        .success()
        .stderr(contains("ciao gianx"));
}

#[test]
fn pyspark() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "-p",
            "tests/fixtures/pyspark",
        ])
        .assert()
        .success()
        .stderr(contains("I'm in the spark model now!"));
}

#[test]
fn python_3_10() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "-p",
            "tests/fixtures/python_3_10",
        ])
        .assert()
        .success()
        .stderr(contains("Running on python 3.10."));
}

#[test]
fn python_3_12() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "-p",
            "tests/fixtures/python_3_12",
        ])
        .assert()
        .success()
        .stderr(contains("Running on python 3.12."));
}

#[test]
fn sdk_expectations_project() {
    bauplan()
        .args([
            "run",
            "--dry-run",
            "--cache",
            "off",
            "-p",
            "tests/fixtures/python_3_12",
        ])
        .assert()
        .success()
        .stderr(contains("Running on python 3.12."));
}

#[test]
fn with_transaction() {
    let branch = format!("{}.run_with_transaction", username());

    let _ = bauplan().args(["branch", "delete", &branch]).ok();

    bauplan()
        .args(["branch", "create", &branch])
        .assert()
        .success();

    bauplan()
        .args([
            "run",
            "--transaction",
            "on",
            "--cache",
            "off",
            "--ref",
            &branch,
            "-p",
            "tests/fixtures/simple_taxi_dag",
        ])
        .assert()
        .success()
        .stderr(contains("num_rows= 430488"));

    bauplan()
        .args(["branch", "delete", &branch])
        .assert()
        .success();
}
