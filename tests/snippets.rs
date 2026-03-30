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
const ALLOWED_LANGUAGES: &[&str] = &["python", "sh", "sql", "json", "yaml", "mermaid", "text"];

struct Snippet {
    code: String,
    path: PathBuf,
    line: usize,
}

impl fmt::Display for Snippet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.path.display(), self.line)
    }
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
    let mut paths = Vec::new();

    for (i, snippet) in snippets.iter().enumerate() {
        let path = dir.path().join(format!("snippet_{i}.py"));
        let mut file = fs::File::create(&path)?;
        writeln!(file, "import bauplan\nimport pyarrow\n")?;
        file.write_all(snippet.code.as_bytes())?;
        paths.push(path);
    }

    let color = if std::io::stderr().is_terminal() {
        "always"
    } else {
        "never"
    };

    let output = Command::new("uv")
        .args(["run", "ty", "check", "--color", color, "--project"])
        .arg(project_dir)
        .args(&paths)
        .output()
        .context("failed to run ty")?;

    if output.status.success() {
        eprintln!("{} snippets checked, all passed", snippets.len());
        return Ok(());
    }

    // Map temp filenames back to original locations.
    let mut msg = [&output.stdout[..], &output.stderr[..]].concat();
    for (i, snippet) in snippets.iter().enumerate() {
        let tmp_path = dir.path().join(format!("snippet_{i}.py"));
        msg = msg.replace(tmp_path.to_str().unwrap(), snippet.to_string());
    }

    bail!("errors in snippets:\n{}", msg.to_str_lossy());
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
