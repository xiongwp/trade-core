//! `journal-inspect` — offline reader for durable journal and execution-outbox
//! files. A thin CLI over `trade_core::journal_inspect`.
//!
//! Usage:
//!   journal-inspect dump   --path FILE [--from-seq N] [--to-seq N]
//!                                      [--from-ts NS] [--to-ts NS]
//!   journal-inspect dump   --outbox --path FILE [--from-seq IDX] [--to-seq IDX]
//!   journal-inspect verify --path FILE
//!   journal-inspect diff   --path FILE --path2 FILE
//!
//! `--path` is the journal (or outbox, with `--outbox`) file. For `dump
//! --outbox`, `--from-seq`/`--to-seq` are an inclusive `raft_index` range and
//! the timestamp bounds are ignored. Output is one human-readable record per
//! line on stdout; errors and summaries go to stderr via the structured logger.

use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use trade_core::journal_inspect::{
    diff_journals, dump_journal, dump_outbox, verify_journal, DumpFilter, JournalDiff,
};
use trade_core::{log_error, log_info};

struct Args {
    command: String,
    path: Option<PathBuf>,
    path2: Option<PathBuf>,
    outbox: bool,
    filter: DumpFilter,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let command = args
        .next()
        .ok_or("missing sub-command (dump|verify|diff)")?;
    let mut path = None;
    let mut path2 = None;
    let mut outbox = false;
    let mut filter = DumpFilter::default();
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag.as_str() {
            "--outbox" => outbox = true,
            "--path" => path = Some(PathBuf::from(value()?)),
            "--path2" => path2 = Some(PathBuf::from(value()?)),
            "--from-seq" => filter.from_seq = Some(parse_u64(&value()?, &flag)?),
            "--to-seq" => filter.to_seq = Some(parse_u64(&value()?, &flag)?),
            "--from-ts" => filter.from_ts = Some(parse_u64(&value()?, &flag)?),
            "--to-ts" => filter.to_ts = Some(parse_u64(&value()?, &flag)?),
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(Args {
        command,
        path,
        path2,
        outbox,
        filter,
    })
}

fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{flag} expects a non-negative integer, got {value:?}"))
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    let path = args.path.ok_or("--path is required")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    match args.command.as_str() {
        "dump" => {
            let written = if args.outbox {
                dump_outbox(&path, &args.filter, &mut out)
            } else {
                dump_journal(&path, &args.filter, &mut out)
            }
            .map_err(|error| format!("dump {}: {error}", path.display()))?;
            out.flush().ok();
            log_info!(
                "journal-inspect",
                "event=dump file={} outbox={} records={written}",
                path.display(),
                args.outbox
            );
        }
        "verify" => {
            let report = verify_journal(&path)
                .map_err(|error| format!("verify {}: {error}", path.display()))?;
            writeln!(
                out,
                "records={} first_seq={:?} last_valid_seq={:?} contiguous={} first_gap_after={:?}",
                report.records,
                report.first_seq,
                report.last_valid_seq,
                report.contiguous,
                report.first_gap_after
            )
            .ok();
            out.flush().ok();
            if !report.contiguous {
                log_error!(
                    "journal-inspect",
                    "event=verify_gap file={} first_gap_after={:?} last_valid_seq={:?}",
                    path.display(),
                    report.first_gap_after,
                    report.last_valid_seq
                );
                return Err("journal has a sequence gap".into());
            }
        }
        "diff" => {
            let path2 = args.path2.ok_or("diff requires --path and --path2")?;
            let diff = diff_journals(&path, &path2).map_err(|error| {
                format!("diff {} vs {}: {error}", path.display(), path2.display())
            })?;
            match &diff {
                JournalDiff::Identical => {
                    writeln!(out, "identical").ok();
                }
                JournalDiff::FrameDiffers { seq } => {
                    writeln!(out, "diverges: frame differs at seq={seq}").ok();
                }
                JournalDiff::SeqMismatch { a_seq, b_seq } => {
                    writeln!(out, "diverges: seq mismatch a={a_seq} b={b_seq}").ok();
                }
                JournalDiff::LengthMismatch {
                    at_seq,
                    longer_is_a,
                } => {
                    let longer = if *longer_is_a { "path" } else { "path2" };
                    writeln!(
                        out,
                        "diverges: length mismatch, {longer} has extra record at seq={at_seq}"
                    )
                    .ok();
                }
            }
            out.flush().ok();
            if diff != JournalDiff::Identical {
                return Err("journals diverge".into());
            }
        }
        other => return Err(format!("unknown sub-command {other} (dump|verify|diff)")),
    }
    Ok(())
}

fn main() -> ExitCode {
    trade_core::oblog::init_from_env();
    trade_core::oblog::set_panic_hook("journal-inspect");
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            log_error!("journal-inspect", "{error}");
            ExitCode::from(1)
        }
    }
}
