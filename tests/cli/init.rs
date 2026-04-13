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

    let output = Command::new("uv")
        .arg("--directory")
        .arg(dir.path())
        .args(["run", "ruff", "check"])
        .output()?;
    if !output.status.success() {
        bail!(
            "ruff check failed:\n{}",
            [&output.stdout[..], &output.stderr[..]]
                .concat()
                .to_str_lossy()
        );
    }

    let output = Command::new("uv")
        .arg("--directory")
        .arg(dir.path())
        .args(["run", "ty", "check"])
        .output()?;
    if !output.status.success() {
        bail!(
            "ty check failed:\n{}",
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
