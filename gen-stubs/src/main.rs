//! Generates Python type stubs from the compiled bauplan extension module.

use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use pyo3_introspection::{introspect_cdylib, module_stub_files};

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let target_dir = root.join("target/release");

    let lib_path = if cfg!(target_os = "macos") {
        target_dir.join("libbauplan.dylib")
    } else if cfg!(target_os = "linux") {
        target_dir.join("libbauplan.so")
    } else if cfg!(target_os = "windows") {
        target_dir.join("bauplan.dll")
    } else {
        bail!("unsupported platform");
    };

    if !lib_path.exists() {
        bail!(
            "library not found at {}\nrun `cargo build --release --features python` first",
            lib_path.display()
        );
    }

    let module = introspect_cdylib(&lib_path, "_internal")
        .with_context(|| format!("failed to introspect {}", lib_path.display()))?;

    let stubs = module_stub_files(&module);
    let out_dir = root.join("python/bauplan/_internal");
    fs::create_dir_all(&out_dir)?;

    for (name, content) in stubs {
        let path = out_dir.join(&name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, content)?;
        eprintln!("wrote {}", path.display());
    }

    Ok(())
}
