use crate::cli::bauplan;

#[test]
fn ls() {
    bauplan().args(["job", "ls"]).assert().success();
}

#[test]
fn filter_by_kind_pascal_case() {
    bauplan()
        .args(["job", "ls", "--kind", "Query"])
        .assert()
        .success();
}

#[test]
fn filter_by_status_screaming_case() {
    bauplan()
        .args(["job", "ls", "--status", "COMPLETE"])
        .assert()
        .success();
}
