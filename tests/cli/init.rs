use std::process::Command;

use anyhow::{Result, bail};
use bstr::ByteSlice as _;

#[test]
fn init_generates_valid_dag() -> Result<()> {
    let dir = tempfile::tempdir()?;

    crate::bauplan()
        .args(["init", "--name", "test_project"])
        .arg(dir.path())
        .assert()
        .success();

    let models = dir.path().join("models.py");

    let output = Command::new("uvx")
        .args(["--isolated", "ruff", "check", "--isolated"])
        .arg(&models)
        .output()?;
    if !output.status.success() {
        bail!(
            "ruff check failed:\n{}",
            [&output.stdout[..], &output.stderr[..]]
                .concat()
                .to_str_lossy()
        );
    }

    crate::bauplan()
        .args(["run", "--dry-run", "--no-cache", "--strict", "-p"])
        .arg(dir.path())
        .assert()
        .success();

    Ok(())
}
