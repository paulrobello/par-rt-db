//! ENH-052 family 4: regenerates `wire-corpus/spec/pagination.json` —
//! the btree keyset-pagination decision table — from the server's reference
//! walk (see `spec_corpus_pagination.rs`). Run via
//! `make gen-spec-pagination`;
//! `make gen-spec-pagination-check` fails the gate when the committed fixture
//! disagrees with a fresh generation.

use std::path::PathBuf;
use std::process::ExitCode;

struct Args {
    out: PathBuf,
}

fn parse_args() -> Result<Args, String> {
    let mut out = PathBuf::from("../wire-corpus/spec/pagination.json");
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--out" => {
                out = PathBuf::from(
                    args.next()
                        .ok_or_else(|| "--out needs a path".to_string())?,
                )
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(Args { out })
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    match rtdb_server::spec_corpus_pagination::render_pagination_spec() {
        Ok(rendered) => {
            if let Some(parent) = args.out.parent()
                && let Err(e) = std::fs::create_dir_all(parent)
            {
                eprintln!("create {}: {e}", parent.display());
                return ExitCode::FAILURE;
            }
            match std::fs::write(&args.out, rendered) {
                Ok(()) => {
                    println!("wrote {} (spec table)", args.out.display());
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("write {}: {e}", args.out.display());
                    ExitCode::FAILURE
                }
            }
        }
        Err(e) => {
            eprintln!("render failed: {e}");
            ExitCode::FAILURE
        }
    }
}
