//! Structured, dependency-free operational logging.
//!
//! The codebase logged with bare `eprintln!`: no level, no timestamp, no
//! component tag, and no way to silence debug chatter in production. This module
//! replaces that with a tiny leveled logger that keeps the house rule of *no new
//! crates* (no `log`, no `tracing`, no `chrono`) — everything is built on
//! `std`.
//!
//! # Format
//!
//! Every line is:
//!
//! ```text
//! 2026-07-18T09:00:00.123Z WARN [raft-node g3] peer 2 unreachable, retrying
//! ```
//!
//! i.e. RFC-3339-ish UTC timestamp with millisecond precision, a fixed-width
//! level word, `[component]` or `[component instance]`, then the message.
//!
//! # Design tradeoffs
//!
//! * **Level filtering is read once, cached atomically.** `TC_LOG`
//!   (`error`/`warn`/`info`/`debug`, default `info`) is parsed on first use and
//!   cached in a plain atomic (sentinel-guarded), so the steady-state check is a
//!   single relaxed load. The `log_*!` macros gate on [`enabled`] *before* evaluating
//!   their format arguments, so a disabled level costs one relaxed load and a
//!   compare — the message is never formatted.
//! * **Line atomicity without a mutex on the hot path.** Each record is
//!   formatted into a single `String` and written with one `write_all` under a
//!   short-lived `stderr` lock, so concurrent components never interleave within
//!   a line.
//! * **UTC without `chrono`.** The civil date is derived from the Unix day count
//!   with Howard Hinnant's `civil_from_days` algorithm; no time zone handling is
//!   needed because we always emit UTC (`Z`).
//! * **`panic = "abort"` aware.** [`set_panic_hook`] logs a formatted ERROR line
//!   (payload + location) *before* the runtime aborts, so a crashing thread
//!   leaves a structured breadcrumb rather than the default unformatted message.

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Severity level. Ordinal doubles as the filter threshold: a record is emitted
/// when `level as u8 <= configured_threshold`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Level {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

impl Level {
    /// Fixed-width, upper-case name used in the log line.
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
        }
    }

    fn parse(s: &str) -> Option<Level> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Some(Level::Error),
            "warn" | "warning" => Some(Level::Warn),
            "info" => Some(Level::Info),
            "debug" => Some(Level::Debug),
            _ => None,
        }
    }
}

/// Default threshold when `TC_LOG` is unset or unparseable: `info`.
const DEFAULT_THRESHOLD: u8 = Level::Info as u8;

/// Cached threshold. Sentinel `u8::MAX` means "not yet read from the
/// environment". Kept as a plain atomic so the hot-path check is a single
/// relaxed load.
static THRESHOLD: AtomicU8 = AtomicU8::new(u8::MAX);

fn read_threshold_from_env() -> u8 {
    std::env::var("TC_LOG")
        .ok()
        .and_then(|v| Level::parse(&v))
        .map(|l| l as u8)
        .unwrap_or(DEFAULT_THRESHOLD)
}

#[inline]
fn threshold() -> u8 {
    let cached = THRESHOLD.load(Ordering::Relaxed);
    if cached != u8::MAX {
        return cached;
    }
    // First use: read the env once and publish. Racing initializers all compute
    // the same value, so a plain store is fine.
    let t = read_threshold_from_env();
    THRESHOLD.store(t, Ordering::Relaxed);
    t
}

/// Re-read `TC_LOG` and reset the cache. Intended for process startup (call once
/// after the environment is set) and for tests.
pub fn init_from_env() {
    THRESHOLD.store(read_threshold_from_env(), Ordering::Relaxed);
}

/// Force the threshold to a specific level, bypassing the environment. Mainly
/// for tests.
pub fn set_level(level: Level) {
    THRESHOLD.store(level as u8, Ordering::Relaxed);
}

/// The active threshold level.
pub fn level() -> Level {
    match threshold() {
        0 => Level::Error,
        1 => Level::Warn,
        2 => Level::Info,
        _ => Level::Debug,
    }
}

/// True if a record at `level` would be emitted. The `log_*!` macros call this
/// before formatting so disabled levels are free.
#[inline]
pub fn enabled(level: Level) -> bool {
    (level as u8) <= threshold()
}

// --- UTC timestamp formatting (no chrono) --------------------------------

/// Civil (year, month, day) from a count of days since 1970-01-01, per Howard
/// Hinnant's `civil_from_days`. Valid for the full range we will ever see.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    // Shift epoch to 0000-03-01 for a regular 400-year cycle.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as i64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Format `now` as `YYYY-MM-DDTHH:MM:SS.mmmZ` (UTC).
fn format_timestamp(now: SystemTime) -> String {
    let dur = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs();
    let millis = dur.subsec_millis();
    let days = (secs / 86_400) as i64;
    let tod = secs % 86_400;
    let (h, m, s) = (tod / 3_600, (tod % 3_600) / 60, tod % 60);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}.{millis:03}Z"
    )
}

// --- Emission -------------------------------------------------------------

/// Format and write one record. Callers should gate on [`enabled`] first (the
/// macros do); this function does not re-check the level so it can also serve
/// the panic hook, which must always print.
pub fn emit(level: Level, component: &str, instance: Option<fmt::Arguments>, message: fmt::Arguments) {
    let mut line = String::with_capacity(128);
    line.push_str(&format_timestamp(SystemTime::now()));
    line.push(' ');
    line.push_str(level.as_str());
    match instance {
        Some(inst) => {
            let _ = std::fmt::write(&mut line, format_args!(" [{component} {inst}] "));
        }
        None => {
            let _ = std::fmt::write(&mut line, format_args!(" [{component}] "));
        }
    }
    let _ = std::fmt::write(&mut line, message);
    line.push('\n');
    // Single locked write keeps the whole line atomic against other threads.
    let stderr = std::io::stderr();
    let mut handle = stderr.lock();
    let _ = handle.write_all(line.as_bytes());
}

/// Time `f`, and if it takes at least `threshold`, emit a WARN naming the
/// operation and its duration. The timing is always taken (an `Instant` pair is
/// nearly free); only the WARN is gated. Handy for fsync / commit slow-path
/// alerting.
pub fn warn_if_slow<T>(
    component: &str,
    op_name: &str,
    threshold: Duration,
    f: impl FnOnce() -> T,
) -> T {
    let start = Instant::now();
    let out = f();
    let elapsed = start.elapsed();
    if elapsed >= threshold && enabled(Level::Warn) {
        emit(
            Level::Warn,
            component,
            None,
            format_args!(
                "slow {op_name}: {:.3}ms >= {:.3}ms threshold",
                elapsed.as_secs_f64() * 1e3,
                threshold.as_secs_f64() * 1e3
            ),
        );
    }
    out
}

/// Install a panic hook that logs a structured ERROR (payload + location)
/// tagged with `component` before the process unwinds/aborts. Safe to call once
/// per binary from `main`; under `panic = "abort"` this is the last log line a
/// crashing thread produces.
pub fn set_panic_hook(component: &'static str) {
    std::panic::set_hook(Box::new(move |info| {
        let payload = if let Some(s) = info.payload().downcast_ref::<&str>() {
            *s
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.as_str()
        } else {
            "<non-string panic payload>"
        };
        match info.location() {
            Some(loc) => emit(
                Level::Error,
                component,
                None,
                format_args!(
                    "panic: {payload} at {}:{}:{}",
                    loc.file(),
                    loc.line(),
                    loc.column()
                ),
            ),
            None => emit(
                Level::Error,
                component,
                None,
                format_args!("panic: {payload} at <unknown location>"),
            ),
        }
    }));
}

// --- Macros ---------------------------------------------------------------
//
// Each macro has two arms: `component` alone, or `component, instance;` where
// the `;` separates the optional instance label from the format string. The
// level is checked before the format arguments are evaluated.

/// Internal: shared expansion. Not part of the public API.
#[macro_export]
#[doc(hidden)]
macro_rules! __tc_log {
    ($level:expr, $comp:expr, $inst:expr; $($arg:tt)*) => {{
        if $crate::oblog::enabled($level) {
            $crate::oblog::emit(
                $level,
                $comp,
                ::core::option::Option::Some(::core::format_args!("{}", $inst)),
                ::core::format_args!($($arg)*),
            );
        }
    }};
    ($level:expr, $comp:expr, $($arg:tt)*) => {{
        if $crate::oblog::enabled($level) {
            $crate::oblog::emit(
                $level,
                $comp,
                ::core::option::Option::None,
                ::core::format_args!($($arg)*),
            );
        }
    }};
}

/// Log at ERROR. `log_error!(component, "msg {}", x)` or
/// `log_error!(component, instance; "msg {}", x)`.
#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => { $crate::__tc_log!($crate::oblog::Level::Error, $($arg)*) };
}

/// Log at WARN. See [`log_error!`] for the argument forms.
#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => { $crate::__tc_log!($crate::oblog::Level::Warn, $($arg)*) };
}

/// Log at INFO. See [`log_error!`] for the argument forms.
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => { $crate::__tc_log!($crate::oblog::Level::Info, $($arg)*) };
}

/// Log at DEBUG. See [`log_error!`] for the argument forms.
#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => { $crate::__tc_log!($crate::oblog::Level::Debug, $($arg)*) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parse_and_filter() {
        assert_eq!(Level::parse("WARN"), Some(Level::Warn));
        assert_eq!(Level::parse(" debug "), Some(Level::Debug));
        assert_eq!(Level::parse("warning"), Some(Level::Warn));
        assert_eq!(Level::parse("nonsense"), None);

        set_level(Level::Warn);
        assert!(enabled(Level::Error));
        assert!(enabled(Level::Warn));
        assert!(!enabled(Level::Info));
        assert!(!enabled(Level::Debug));

        set_level(Level::Debug);
        assert!(enabled(Level::Debug));

        // Restore default so other tests aren't affected.
        set_level(Level::Info);
    }

    #[test]
    fn timestamp_format_is_rfc3339ish_utc() {
        // 2026-07-18T09:00:00.123Z  →  1_784_365_200 s since epoch + 123 ms.
        let ts = format_timestamp(UNIX_EPOCH + Duration::new(1_784_365_200, 123_000_000));
        assert_eq!(ts, "2026-07-18T09:00:00.123Z");
        // Epoch itself.
        assert_eq!(format_timestamp(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        // A leap-year day boundary: 2024-02-29.
        // 2024-02-29T23:59:59.000Z
        let leap = format_timestamp(UNIX_EPOCH + Duration::new(1_709_251_199, 0));
        assert_eq!(leap, "2024-02-29T23:59:59.000Z");
    }

    #[test]
    fn civil_from_days_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(31 + 28), (1970, 3, 1)); // 1970 not a leap year
        assert_eq!(civil_from_days(365), (1971, 1, 1));
    }

    #[test]
    fn warn_if_slow_returns_value() {
        set_level(Level::Info);
        // Fast op: below threshold, still returns the closure value.
        let v = warn_if_slow("test", "noop", Duration::from_secs(3600), || 42);
        assert_eq!(v, 42);
        // Slow op relative to a zero threshold: exercises the WARN branch
        // (output goes to stderr; we only assert the return value here).
        let v = warn_if_slow("test", "instant", Duration::from_nanos(0), || 7);
        assert_eq!(v, 7);
    }

    #[test]
    fn macros_compile_both_arms() {
        // Silence everything so the test does not spam stderr, then exercise
        // both the component-only and component+instance arms of each macro.
        set_level(Level::Error);
        log_error!("comp", "e {}", 1);
        log_warn!("comp", "w");
        log_info!("comp", "i {}", 2);
        log_debug!("comp", "d");
        log_error!("comp", 3; "with instance {}", "x");
        log_warn!("raft-node", format_args!("g{}", 3); "peer {} down", 2);
        set_level(Level::Info);
    }
}
