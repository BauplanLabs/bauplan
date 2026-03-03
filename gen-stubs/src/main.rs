//! Generates Python type stubs from the compiled bauplan extension module.

use std::{
    env,
    io::{Write as _, stdout},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use pyo3_introspection::{introspect_cdylib, module_stub_files};

fn remap_path(path: &Path) -> PathBuf {
    if path.file_name().is_some_and(|f| f != "__init__.pyi") {
        if let Ok(stripped) = path.strip_prefix("_internal") {
            return stripped.to_path_buf();
        }
    }
    path.to_path_buf()
}

fn rewrite_imports(content: &str) -> String {
    content.replace("from _internal.", "from bauplan.")
}

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

    let mut out = stdout().lock();
    for (name, content) in &stubs {
        let path = remap_path(name);
        let content = rewrite_imports(content);
        writeln!(&mut out, "# {}", path.display())?;
        writeln!(&mut out, "{content}")?;
    }

    Ok(())
}
