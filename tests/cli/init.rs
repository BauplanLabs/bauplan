use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Result, bail};
use bstr::ByteSlice as _;

#[test]
fn init_generates_valid_python() -> Result<()> {
    let dir = tempfile::tempdir()?;

    crate::bauplan()
        .args(["init", "--name", "test_project"])
        .arg(dir.path())
        .assert()
        .success();

    let models = dir.path().join("models.py");

    let output = Command::new("uv")
        .args(["run", "ruff", "check", "--isolated"])
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

    Ok(())
}
