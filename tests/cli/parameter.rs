use crate::bauplan;
use predicates::str::contains;

#[test]
fn set_secret_parameter() {
    let tmp = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir("tests/fixtures/parameters").unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), tmp.path().join(entry.file_name())).unwrap();
    }

    let p = tmp.path().to_str().unwrap();

    bauplan()
        .args([
            "parameter",
            "set",
            "--type",
            "secret",
            "-p",
            p,
            "test_secret",
            "some-value",
        ])
        .assert()
        .success();

    bauplan()
        .args(["parameter", "ls", "-p", p])
        .assert()
        .success()
        .stdout(contains("test_secret"));
}
