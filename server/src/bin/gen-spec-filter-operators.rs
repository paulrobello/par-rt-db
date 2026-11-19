//! Regenerates the filter operator spec table using production query compiler.

use std::path::PathBuf;
use std::process::ExitCode;

struct Args {
    out: PathBuf,
}

fn parse_args() -> Result<Args, String> {
    let mut out = PathBuf::from("../wire-corpus/spec/filter-operators.json");
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
    match rtdb_server::spec_corpus_filter_operators::render_filter_operators_spec() {
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
