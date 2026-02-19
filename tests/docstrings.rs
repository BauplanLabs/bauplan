use std::fs;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use bstr::ByteSlice as _;
use tree_sitter::{Parser, Query, QueryCursor, StreamingIterator};

const PY_DOCSTRINGS: &str = "(expression_statement (string (string_content) @doc))";
const MD_CODE_BLOCKS: &str = "(fenced_code_block (info_string) @info (code_fence_content) @code)";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

struct Snippet {
    tag: String,
    code: String,
    location: String,
}

/// Extract fenced ```python blocks from docstrings in a .pyi file.
///
/// Uses tree-sitter-python to find docstrings, then tree-sitter-markdown
/// to find fenced code blocks within each docstring.
fn extract_snippets(path: &Path, snippets: &mut Vec<Snippet>) -> Result<()> {
    let src = fs::read_to_string(path)?;
    let py_lang = tree_sitter_python::LANGUAGE.into();
    let md_lang = tree_sitter_md::LANGUAGE.into();

    let mut py_parser = Parser::new();
    py_parser.set_language(&py_lang)?;
    let py_tree = py_parser.parse(&src, None).context("python parse failed")?;

    let mut md_parser = Parser::new();
    md_parser.set_language(&md_lang)?;

    let py_query = Query::new(&py_lang, PY_DOCSTRINGS)?;
    let md_query = Query::new(&md_lang, MD_CODE_BLOCKS)?;
    let info_idx = md_query.capture_index_for_name("info").unwrap();
    let code_idx = md_query.capture_index_for_name("code").unwrap();

    let mut py_cursor = QueryCursor::new();
    let mut md_cursor = QueryCursor::new();

    let root = repo_root();
    let rel = path.strip_prefix(&root).unwrap_or(path);

    let mut py_matches = py_cursor.matches(&py_query, py_tree.root_node(), src.as_bytes());
    while let Some(m) = py_matches.next() {
        for cap in m.captures {
            let raw = cap.node.utf8_text(src.as_bytes())?;
            let base_line = cap.node.start_position().row + 1;
            let docstring = textwrap::dedent(raw);

            let md_tree = md_parser
                .parse(&docstring, None)
                .context("markdown parse failed")?;

            let mut md_matches =
                md_cursor.matches(&md_query, md_tree.root_node(), docstring.as_bytes());
            while let Some(mm) = md_matches.next() {
                let info_node = mm.captures.iter().find(|c| c.index == info_idx).unwrap();
                let code_node = mm.captures.iter().find(|c| c.index == code_idx).unwrap();

                let info = info_node.node.utf8_text(docstring.as_bytes())?;
                if !info.starts_with("python") {
                    continue;
                }

                let tag = info.strip_prefix("python").unwrap().trim();
                let fence_line = base_line + info_node.node.start_position().row;

                let code = code_node.node.utf8_text(docstring.as_bytes())?;

                snippets.push(Snippet {
                    tag: tag.to_string(),
                    code: code.to_string(),
                    location: format!("{}:{fence_line}", rel.display()),
                });
            }
        }
    }

    Ok(())
}

fn write_fixtures(out: &mut impl Write, tag: &str) -> Result<()> {
    // Handle "fixtures" — directives for doctests elsewhere that refer to
    // pytest fixtures. We handle the common cases.
    for part in tag.split_whitespace() {
        match part.strip_prefix("fixture:") {
            None => continue,
            Some("bauplan") => writeln!(out, "import bauplan\n")?,
            Some(name) => writeln!(out, "import typing\n{name} = typing.cast(typing.Any, None)")?,
        }
    }
    Ok(())
}

#[test]
fn docstrings() -> Result<()> {
    let root = repo_root();
    let dir = tempfile::tempdir()?;

    let mut snippets = Vec::new();
    for entry in fs::read_dir(root.join("python/bauplan/_internal"))? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "pyi") {
            extract_snippets(&path, &mut snippets)?;
        }
    }

    let mut paths = Vec::new();
    for (i, snippet) in snippets.iter().enumerate() {
        let path = dir.path().join(format!("snippet_{i}.py"));
        let mut file = fs::File::create(&path)?;
        write_fixtures(&mut file, &snippet.tag)?;
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
        .arg(&root)
        .args(&paths)
        .output()
        .context("failed to run ty")?;

    if output.status.success() {
        eprintln!("{} snippets checked, all passed", snippets.len());
        return Ok(());
    }

    // Map temp filenames back to .pyi locations.
    let mut msg = [&output.stdout[..], &output.stderr[..]].concat();
    for (i, snippet) in snippets.iter().enumerate() {
        let tmp_path = dir.path().join(format!("snippet_{i}.py"));
        msg = msg.replace(tmp_path.to_str().unwrap(), &snippet.location);
    }

    bail!("type errors in docstring snippets:\n{}", msg.to_str_lossy());
}
