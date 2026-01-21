use std::env::consts::{ARCH, OS};
use std::process::Command;

fn main() -> anyhow::Result<()> {
    tonic_prost_build::compile_protos("src/proto/bpln_proto/commander/service/v2/service.proto")?;

    // Build a version string to use in the user-agent and `--version` flag for the CLI.
    #[cfg(debug_assertions)]
    const BUILD_TYPE: &str = "debug";
    #[cfg(not(debug_assertions))]
    const BUILD_TYPE: &str = "release";

    let version_string = format!(
        "{} ({}:{}{}, {}, {}/{})",
        env!("CARGO_PKG_VERSION"),
        get_branch_name()?,
        get_commit_hash()?,
        if is_working_tree_clean()? { "" } else { "+" },
        BUILD_TYPE,
        OS,
        ARCH
    );

    println!("cargo:rustc-env=BPLN_VERSION={}", version_string);
    Ok(())
}

fn get_commit_hash() -> anyhow::Result<String> {
    let output = Command::new("git")
        .arg("log")
        .arg("-1")
        .arg("--pretty=format:%h") // Abbreviated commit hash
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()?;

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn get_branch_name() -> anyhow::Result<String> {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--abbrev-ref")
        .arg("HEAD")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()?;

    Ok(String::from_utf8_lossy(&output.stdout)
        .trim_end()
        .to_string())
}

fn is_working_tree_clean() -> anyhow::Result<bool> {
    let status = Command::new("git")
        .arg("diff")
        .arg("--quiet")
        .arg("--exit-code")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()?;

    Ok(status.code().unwrap() == 0)
}
