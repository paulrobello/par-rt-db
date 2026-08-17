//! ENH-025: regenerate the `<!-- cli-reference -->` region of `cli/README.md`
//! from the CLI's own clap definition ([`args::cli`] — the same command the
//! shipped binary parses with), so the documented command reference cannot
//! drift from the argument definitions.
//!
//! Usage: `cargo run --bin gen-cli-docs -- <README-path>` (default
//! `cli/README.md`). Invoked by `make cli-docs` (regenerate in place) and
//! `make cli-docs-check` (regenerate a copy and diff — the `make checkall`
//! drift gate). Only the text between the begin/end markers is rewritten;
//! hand-written prose outside them is preserved byte-for-byte.

// Shares the single clap definition with the shipped binary without widening
// the crate's `pub(crate)` items into a library API. dead_code is allowed
// here because only `cli()` is reachable from this bin — the `rtdb` binary
// exercises the rest.
#[path = "../args.rs"]
#[allow(dead_code)]
mod args;

use anyhow::{Context, Result};
use std::fmt::Write as _;
use std::path::PathBuf;

const BEGIN: &str = "<!-- cli-reference:begin -->";
const END: &str = "<!-- cli-reference:end -->";

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("cli/README.md"));
    let readme =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let updated = replace_region(&readme)?;
    std::fs::write(&path, updated).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Replace the marker-bounded region of `readme` with freshly generated
/// reference markdown, leaving everything outside the markers untouched.
fn replace_region(readme: &str) -> Result<String> {
    let start = readme
        .find(BEGIN)
        .with_context(|| format!("{BEGIN} marker not found — add it to cli/README.md first"))?;
    let end = readme
        .find(END)
        .with_context(|| format!("{END} marker not found — add it to cli/README.md first"))?;
    anyhow::ensure!(start < end, "{BEGIN} marker must precede the {END} marker");
    Ok(format!(
        "{}\n{}\n{}",
        &readme[..start + BEGIN.len()],
        render().trim_end(),
        &readme[end..]
    ))
}

/// Render the full command reference: the root `--help` block, the global
/// flag/env-var table, then one section per (nested) subcommand.
fn render() -> String {
    let mut root = args::cli();
    let mut out = String::new();

    let root_help = root.render_help().to_string();
    let _ = write!(
        out,
        "Full `rtdb --help` output:\n\n```text\n{}\n```\n",
        root_help.trim_end()
    );

    let _ = writeln!(
        out,
        "\n### Global flags and environment variables\n\
         \n\
         | Flag | Env var | Description |\n\
         | --- | --- | --- |"
    );
    for arg in root.get_arguments() {
        let Some(long) = arg.get_long() else { continue };
        if long == "help" || long == "version" {
            continue;
        }
        let env = arg
            .get_env()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut description = arg.get_help().map(|h| h.to_string()).unwrap_or_default();
        if arg.is_required_set() {
            description.push_str(" **(required)**");
        }
        let _ = writeln!(
            out,
            "| `{}` | `{}` | {} |",
            flag_label(arg),
            env,
            cell(&description)
        );
    }

    // Clone the subcommands out before rendering: render_help needs &mut and
    // each section gets its own copy with an explicit bin_name so the usage
    // line reads `rtdb <sub> ...`.
    let top: Vec<clap::Command> = root
        .get_subcommands()
        .filter(|sub| sub.get_name() != "help")
        .cloned()
        .collect();
    for sub in top {
        let bin = format!("rtdb {}", sub.get_name());
        let nested: Vec<clap::Command> = sub
            .get_subcommands()
            .filter(|nested| nested.get_name() != "help")
            .cloned()
            .collect();
        render_command(&mut out, &bin, sub, "###");
        for nested in nested {
            let nested_bin = format!("{bin} {}", nested.get_name());
            render_command(&mut out, &nested_bin, nested, "####");
        }
    }
    out
}

/// One heading + fenced `--help` block for a single command.
fn render_command(out: &mut String, bin: &str, cmd: clap::Command, heading: &str) {
    let mut cmd = cmd.bin_name(bin);
    let help = cmd.render_help().to_string();
    let _ = writeln!(
        out,
        "\n{heading} `{bin}`\n\n```text\n{}\n```",
        help.trim_end()
    );
    let note = match bin {
        "rtdb migrate" => Some(
            "Directive reference and examples: \
             [Schema migration (`rtdb migrate`)](#schema-migration-rtdb-migrate).",
        ),
        "rtdb workflows" => Some(
            "Spec format and semantics: \
             [Workflow runs (`rtdb workflows`)](#workflow-runs-rtdb-workflows).",
        ),
        _ => None,
    };
    if let Some(note) = note {
        let _ = writeln!(out, "\n{note}");
    }
}

/// Render an Arg as it appears in help, e.g. `--url <URL>`. Value names are
/// only appended for args that take a value — clap reports a name even for
/// boolean SetTrue flags.
fn flag_label(arg: &clap::Arg) -> String {
    let mut label = format!("--{}", arg.get_long().unwrap_or_default());
    let takes_values = arg.get_num_args().is_none_or(|range| range.takes_values());
    if takes_values && let Some(names) = arg.get_value_names() {
        for name in names {
            label.push_str(&format!(" <{name}>"));
        }
    }
    label
}

/// Make help text safe for a markdown table cell: pipes would break the row.
fn cell(text: &str) -> String {
    text.replace('|', "\\|").replace('\n', " ")
}
