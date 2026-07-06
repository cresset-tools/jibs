//! Corpus test: every .jibs file shipped in the repository must parse.
//!
//! This catches the docs/examples drifting behind the language (the shipped
//! example once used a removed keyword for months without anything noticing).

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    // crates/parser -> repo root
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

fn collect_jibs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_jibs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "jibs") {
            out.push(path);
        }
    }
}

/// Extract ```jibs fenced code blocks from a markdown file.
/// Returns (starting line number, block content) pairs.
fn extract_jibs_blocks(markdown: &str) -> Vec<(usize, String)> {
    let mut blocks = Vec::new();
    let mut current: Option<(usize, String)> = None;
    for (i, line) in markdown.lines().enumerate() {
        match &mut current {
            None => {
                if line.trim() == "```jibs" {
                    current = Some((i + 2, String::new()));
                }
            }
            Some((_, content)) => {
                if line.trim() == "```" {
                    blocks.push(current.take().unwrap());
                } else {
                    content.push_str(line);
                    content.push('\n');
                }
            }
        }
    }
    blocks
}

/// Every ```jibs example in the documentation must parse — this is what
/// keeps SPEC.md and GRAMMAR.md from drifting behind the language.
#[test]
fn all_doc_examples_parse() {
    let root = repo_root();
    let mut failures = Vec::new();
    let mut total = 0;

    for doc in ["SPEC.md", "GRAMMAR.md"] {
        let path = root.join(doc);
        let content = std::fs::read_to_string(&path).expect("read doc");
        for (line, block) in extract_jibs_blocks(&content) {
            total += 1;
            if let Err(errors) = jibs_parser::parse(&block) {
                let rendered: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
                failures.push(format!(
                    "{}:{} (jibs block):\n  {}",
                    doc,
                    line,
                    rendered.join("\n  ")
                ));
            }
        }
    }

    assert!(
        total >= 20,
        "expected to find the docs' jibs examples, found only {}",
        total
    );
    assert!(
        failures.is_empty(),
        "{} documentation example(s) failed to parse:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn all_repo_jibs_files_parse() {
    let root = repo_root();
    let mut files = Vec::new();
    for dir in ["test", "examples"] {
        collect_jibs_files(&root.join(dir), &mut files);
    }
    // Top-level .jibs files (e.g. the generated Magento config)
    if let Ok(entries) = std::fs::read_dir(&root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|e| e == "jibs") {
                files.push(path);
            }
        }
    }

    assert!(
        files.len() >= 10,
        "expected to find the repo's .jibs corpus, found only {} files",
        files.len()
    );

    let mut failures = Vec::new();
    for path in &files {
        let source = std::fs::read_to_string(path).expect("read .jibs file");
        if let Err(errors) = jibs_parser::parse(&source) {
            let rendered: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
            failures.push(format!("{}:\n  {}", path.display(), rendered.join("\n  ")));
        }
    }

    assert!(
        failures.is_empty(),
        "{} corpus file(s) failed to parse:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
