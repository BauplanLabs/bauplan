use std::fmt;
use std::fs;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};
use bstr::ByteSlice as _;
use regex::Regex;
use tree_sitter::{Parser, Query, QueryCursor, StreamingIterator};
use walkdir::WalkDir;

const PY_DOCSTRINGS: &str = "(expression_statement (string (string_content) @doc))";
const MD_CODE_BLOCKS: &str = "(fenced_code_block (info_string) @info (code_fence_content) @code)";

// Whitelist the code snippet languages, so that snippets like ```pyfon or
// ```bash don't sneak by.
const ALLOWED_LANGUAGES: &[&str] = &["python", "sh", "sql", "json", "yaml", "mermaid", "text", "shell-session"];

struct Snippet {
    code: String,
    path: PathBuf,
    line: usize,
    /// From a `since:0.2.0` tag on the fence. The snippet is skipped when we
    /// check against an SDK older than this.
    since: Option<Version>,
}

impl fmt::Display for Snippet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.path.display(), self.line)
    }
}

type Version = (u64, u64, u64);

/// The leading `major.minor.patch` of a version, ignoring any prerelease
/// suffix. PyPI writes release candidates as `0.2.0rc1` and cargo writes them
/// as `0.2.0-rc.1`; an API added in 0.2.0 is present in both, so for our
/// purposes they compare equal to the release.
fn parse_version(raw: &str) -> anyhow::Result<Version> {
    let re = Regex::new(r"^(\d+)\.(\d+)\.(\d+)").unwrap();
    let caps = re
        .captures(raw)
        .with_context(|| format!("can't read a version out of {raw:?}"))?;

    Ok((caps[1].parse()?, caps[2].parse()?, caps[3].parse()?))
}

/// The version of bauplan installed in `env`. This doubles as a check that the
/// environment exists and has the SDK in it, so a bad BAUPLAN_SNIPPET_ENV
/// fails loudly rather than quietly checking nothing.
fn installed_version(env: &Path) -> anyhow::Result<Version> {
    let python = env.join("bin/python");
    let output = Command::new(&python)
        .args(["-c", "import bauplan; print(bauplan.__version__)"])
        .output()
        .with_context(|| format!("failed to run {}", python.display()))?;

    if !output.status.success() {
        bail!(
            "no bauplan installed in {}:\n{}",
            env.display(),
            output.stderr.to_str_lossy()
        );
    }

    parse_version(output.stdout.to_str_lossy().trim())
}

fn include_entry(entry: &walkdir::DirEntry) -> bool {
    entry
        .path()
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| !n.starts_with('.'))
}

/// Extract fenced code blocks from a markdown string.
fn extract_md_snippets(lang: &str, src: &str, path: &Path) -> anyhow::Result<Vec<Snippet>> {
    let md_lang = tree_sitter_md::LANGUAGE.into();

    let mut md_parser = Parser::new();
    md_parser.set_language(&md_lang)?;

    let md_query = Query::new(&md_lang, MD_CODE_BLOCKS)?;
    let info_idx = md_query.capture_index_for_name("info").unwrap();
    let code_idx = md_query.capture_index_for_name("code").unwrap();

    let md_tree = md_parser
        .parse(src, None)
        .context("markdown parse failed")?;

    let mut snippets = Vec::new();
    let mut md_cursor = QueryCursor::new();
    let mut md_matches = md_cursor.matches(&md_query, md_tree.root_node(), src.as_bytes());

    // We use hidden lines, like in rust's docstrings. This regex adds them
    // as normal lines for typechecking.
    let re = Regex::new(r"#! ?").unwrap();

    while let Some(mm) = md_matches.next() {
        let info_node = mm.captures.iter().find(|c| c.index == info_idx).unwrap();
        let code_node = mm.captures.iter().find(|c| c.index == code_idx).unwrap();

        let info = info_node.node.utf8_text(src.as_bytes())?;
        let line = info_node.node.start_position().row;

        if !info.starts_with(lang) {
            let lang = info.split_whitespace().next();

            let Some(lang) = lang else {
                bail!("{}:{line}: code snippet without language", path.display(),);
            };

            if !ALLOWED_LANGUAGES.contains(&lang) {
                bail!(
                    "{}:{line}: unexpected language {lang}, expected {ALLOWED_LANGUAGES:?}",
                    path.display(),
                );
            }

            continue;
        }

        let tag = info.strip_prefix(lang).unwrap().trim();
        if tag.split_whitespace().any(|t| t == "type:ignore") {
            continue;
        }

        let mut since = None;
        for t in tag.split_whitespace() {
            if let Some(v) = t.strip_prefix("since:") {
                since = Some(parse_version(v).with_context(|| {
                    format!("{}:{line}: bad since: tag", path.display())
                })?);
            }
        }

        let code = code_node.node.utf8_text(src.as_bytes())?;

        // In rare cases (bulleted lists), the entire block might be indented.
        let indent = code_node.node.start_position().column;
        let code = code
            .lines()
            .map(|line| {
                let spaces = line.len() - line.trim_start().len();
                &line[spaces.min(indent)..]
            })
            .collect::<Vec<_>>()
            .join("\n");

        snippets.push(Snippet {
            code: re.replace_all(&code, "").into_owned(),
            path: path.to_owned(),
            line: line + 1, // Editors show files 1-indexed.
            since,
        });
    }

    Ok(snippets)
}

fn extract_pyi_snippets(path: &Path, src: &str, snippets: &mut Vec<Snippet>) -> anyhow::Result<()> {
    let py_lang = tree_sitter_python::LANGUAGE.into();

    let mut py_parser = Parser::new();
    py_parser.set_language(&py_lang)?;
    let py_tree = py_parser.parse(src, None).context("python parse failed")?;

    let py_query = Query::new(&py_lang, PY_DOCSTRINGS)?;
    let mut py_cursor = QueryCursor::new();

    let mut py_matches = py_cursor.matches(&py_query, py_tree.root_node(), src.as_bytes());
    while let Some(m) = py_matches.next() {
        for cap in m.captures {
            let raw = cap.node.utf8_text(src.as_bytes())?;
            let base_line = cap.node.start_position().row;
            let docstring = textwrap::dedent(raw);

            for mut snippet in extract_md_snippets("python", &docstring, path)? {
                snippet.line += base_line;
                snippets.push(snippet);
            }
        }
    }

    Ok(())
}

fn typecheck_snippets(project_dir: &Path, snippets: &[Snippet]) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;

    // Set by the `check-docs-release` recipe to an environment with a released
    // bauplan in it. Unset means check against this checkout.
    let env = std::env::var_os("BAUPLAN_SNIPPET_ENV").map(PathBuf::from);
    let target = env.as_deref().map(installed_version).transpose()?;

    let mut checked = Vec::new();
    let mut skipped = 0;

    for (i, snippet) in snippets.iter().enumerate() {
        // The snippet documents an API the target SDK doesn't have yet.
        if let (Some(target), Some(since)) = (target, snippet.since)
            && since > target
        {
            skipped += 1;
            continue;
        }

        let path = dir.path().join(format!("snippet_{i}.py"));
        let mut file = fs::File::create(&path)?;
        writeln!(file, "import bauplan\nimport pyarrow\n")?;
        file.write_all(snippet.code.as_bytes())?;
        checked.push((path, snippet));
    }

    let paths: Vec<&Path> = checked.iter().map(|(p, _)| p.as_path()).collect();

    let color = if std::io::stderr().is_terminal() {
        "always"
    } else {
        "never"
    };

    let mut cmd = Command::new("uv");
    cmd.arg("run")
        .args(["--project".as_ref(), project_dir.as_os_str()])
        .args(["ty", "check", "--color", color]);

    match &env {
        // ty searches the directory it runs from before it searches
        // site-packages, so we run from the snippet directory. From the repo it
        // would find python/bauplan first and never look at the installed copy,
        // which would check the working tree while claiming otherwise.
        Some(env) => cmd.current_dir(dir.path()).arg("--python").arg(env),
        None => cmd.arg("--project").arg(project_dir),
    };

    let output = cmd
        .args(&paths)
        .output()
        .context("failed to run ty")?;

    if output.status.success() {
        match target {
            Some((x, y, z)) => eprintln!(
                "{} snippets checked against bauplan {x}.{y}.{z}, all passed \
                 ({skipped} skipped as newer)",
                checked.len(),
            ),
            None => eprintln!("{} snippets checked, all passed", checked.len()),
        }

        return Ok(());
    }

    // Map temp filenames back to original locations.
    let mut msg = [&output.stdout[..], &output.stderr[..]].concat();
    for (path, snippet) in &checked {
        msg = msg.replace(path.to_str().unwrap(), snippet.to_string());
    }

    if let Some((x, y, z)) = target {
        msg.extend_from_slice(
            format!(
                "\nnote: checked against bauplan {x}.{y}.{z}. Tag a snippet \
                 ```python since:<version> if it documents a newer API.\n"
            )
            .as_bytes(),
        );
    }

    bail!("errors in snippets:\n{}", msg.to_str_lossy());
}

/// Checking against a released SDK only means anything if ty resolves `bauplan`
/// from the environment we point it at. If this checkout's copy leaks onto the
/// search path it shadows that, and every snippet passes while checking the
/// working tree instead. That failure is silent, so pin it down here: against
/// an empty environment, `bauplan` has to come up missing.
#[test]
fn snippet_env_is_not_shadowed() -> anyhow::Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = tempfile::tempdir()?;
    let venv = dir.path().join("venv");

    let status = Command::new("uv")
        .args(["venv", "-q"])
        .arg(&venv)
        .status()
        .context("failed to run uv venv")?;
    if !status.success() {
        bail!("failed to create an empty venv");
    }

    let probe = dir.path().join("probe.py");
    fs::write(&probe, "import bauplan\n")?;

    let output = Command::new("uv")
        .arg("run")
        .args(["--project".as_ref(), root.as_os_str()])
        .args(["ty", "check", "--python"])
        .arg(&venv)
        .arg(&probe)
        .current_dir(dir.path())
        .output()
        .context("failed to run ty")?;

    let msg = [&output.stdout[..], &output.stderr[..]].concat();
    if !msg.contains_str("Cannot resolve imported module `bauplan`") {
        bail!(
            "ty resolved `bauplan` from an empty environment, so the snippet \
             check is not isolated from this checkout:\n{}",
            msg.to_str_lossy()
        );
    }

    Ok(())
}

#[test]
fn docstrings() -> anyhow::Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));

    let mut snippets = Vec::new();
    for entry in WalkDir::new(root.join("python/bauplan"))
        .into_iter()
        .filter_entry(include_entry)
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "pyi") {
            let rel = path.strip_prefix(root).unwrap_or(path);
            let src = fs::read_to_string(path)?;
            extract_pyi_snippets(rel, &src, &mut snippets)?;
        }
    }

    typecheck_snippets(root, &snippets)
}

#[test]
fn python_examples() -> anyhow::Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));

    let mut snippets = Vec::new();

    let entries = ["docs/pages", "examples"]
        .iter()
        .flat_map(|p| WalkDir::new(p).into_iter().filter_entry(include_entry));
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "mdx" && e != "md") {
            continue;
        }

        let rel = path.strip_prefix(root).unwrap_or(path);
        let src = fs::read_to_string(path)?;
        for snippet in extract_md_snippets("python", &src, rel)? {
            snippets.push(snippet);
        }
    }

    typecheck_snippets(root, &snippets)
}

/// Look for and validate `bauplan` invocations in the docs.
#[test]
fn cli_examples() -> anyhow::Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let bin = escargot::CargoBuild::new()
        .bin("bauplan")
        .manifest_path(root.join("Cargo.toml"))
        // This feature disables actually running the CLI.
        .features("_check-parse")
        .run()
        .context("failed to build bauplan binary")?;

    let placeholder_re = Regex::new(r"<[A-Za-z_]+>").unwrap();
    let continuation_re = Regex::new(r"\\\s*\n\s*").unwrap();

    let blue = anstyle::AnsiColor::Blue.on_default();
    let dim = anstyle::Style::new().dimmed();

    let mut failures = Vec::new();
    let mut successes = 0;

    let entries = ["docs/pages", "examples"]
        .iter()
        .flat_map(|p| WalkDir::new(p).into_iter().filter_entry(include_entry));
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "mdx" && e != "md") {
            continue;
        }

        let rel = path.strip_prefix(root).unwrap_or(path);
        let src = fs::read_to_string(path)?;
        for snippet in extract_md_snippets("sh", &src, rel)? {
            // Join backslash-continuation lines, then split into
            // individual commands.
            let joined = continuation_re.replace_all(&snippet.code, " ");
            for (i, line) in joined.lines().enumerate() {
                let line = line.trim();
                if !line.starts_with("bauplan ") && line != "bauplan" {
                    continue;
                }

                // Skip synopsis lines like `bauplan run [flags]`.
                if line.contains("[flags]") {
                    continue;
                }

                // Replace <PLACEHOLDER> with a dummy value so clap can parse it.
                let line = placeholder_re.replace_all(line, "PLACEHOLDER");

                let loc = format!("{}:{}", snippet.path.display(), snippet.line + i);
                let Some(args) = shlex::split(&line) else {
                    failures.push(format!("{loc}: failed to shell-split: {line}"));
                    continue;
                };

                let output = bin
                    .command()
                    .args(&args[1..])
                    .output()
                    .context("failed to run bauplan")?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    failures.push(format!(
                        "{loc}:\n{blue}% {line}{blue:#}\n{dim}{stderr}{dim:#}"
                    ));
                } else {
                    successes += 1;
                }
            }
        }
    }

    if failures.is_empty() {
        let green = anstyle::AnsiColor::Green.on_default();
        anstream::eprintln!("{green}{successes} CLI invocations checked, all passed{green:#}");
        Ok(())
    } else {
        bail!(
            "{} invocation(s) failed to parse:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}
