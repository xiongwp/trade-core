//! K-line (candlestick / OHLCV) aggregation for the market-data service.
//!
//! Trades stream in as `(timestamp, price, qty)`; the aggregator maintains a
//! rolling window of candles per instrument per interval. Supported intervals
//! (the standard stock-chart set): 1s, 1m, 3m, 5m, 10m, 15m, 30m, 1d, 1w, 1mo.
//!
//! Bucketing rules:
//! * second/minute/day intervals are fixed-length UTC buckets (`ts - ts % n`);
//! * **weeks start on Monday** (exchange convention);
//! * **months are calendar months**, computed with Howard Hinnant's civil-date
//!   algorithm — no time-zone tables needed for UTC bucketing.

use std::collections::{HashMap, VecDeque};

use crate::types::{InstrumentId, Price, Qty};

/// One OHLCV candle. `start` is the bucket's opening time (Unix seconds).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Candle {
    pub start: u64,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Qty,
    pub trades: u32,
}

/// A candle interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interval {
    /// Fixed length in seconds (1s .. 1d).
    Fixed(u64),
    /// Calendar week, starting Monday 00:00 UTC.
    Week,
    /// Calendar month, starting on the 1st, 00:00 UTC.
    Month,
}

/// The supported intervals, name → rule. Order defines storage layout.
pub const INTERVALS: [(&str, Interval); 10] = [
    ("1s", Interval::Fixed(1)),
    ("1m", Interval::Fixed(60)),
    ("3m", Interval::Fixed(180)),
    ("5m", Interval::Fixed(300)),
    ("10m", Interval::Fixed(600)),
    ("15m", Interval::Fixed(900)),
    ("30m", Interval::Fixed(1800)),
    ("1d", Interval::Fixed(86_400)),
    ("1w", Interval::Week),
    ("1mo", Interval::Month),
];

/// Look up an interval by its API name (`"5m"`, `"1mo"`, ...).
pub fn interval_by_name(name: &str) -> Option<(usize, Interval)> {
    INTERVALS
        .iter()
        .position(|(n, _)| *n == name)
        .map(|i| (i, INTERVALS[i].1))
}

// --- civil-date arithmetic (Hinnant's algorithms, proleptic Gregorian) ------

/// Days since 1970-01-01 → (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// (year, month, day) → days since 1970-01-01.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

/// The opening second of the bucket containing `ts_sec`.
pub fn bucket_start(interval: Interval, ts_sec: u64) -> u64 {
    match interval {
        Interval::Fixed(n) => ts_sec - ts_sec % n,
        Interval::Week => {
            let days = ts_sec / 86_400;
            // 1970-01-01 was a Thursday; with Monday = 0 its weekday index is 3.
            let dow = (days + 3) % 7;
            // The week containing the epoch began 1969-12-29: clamp to 0.
            days.saturating_sub(dow) * 86_400
        }
        Interval::Month => {
            let days = (ts_sec / 86_400) as i64;
            let (y, m, _) = civil_from_days(days);
            days_from_civil(y, m, 1) as u64 * 86_400
        }
    }
}

/// Rolling multi-interval OHLCV aggregation across instruments.
pub struct KlineAggregator {
    /// Per instrument: one candle deque per entry in [`INTERVALS`].
    series: HashMap<InstrumentId, Vec<VecDeque<Candle>>>,
    /// Maximum retained candles per series.
    cap: usize,
}

impl KlineAggregator {
    pub fn new(cap: usize) -> Self {
        KlineAggregator { series: HashMap::new(), cap }
    }

    /// Ingest one trade print.
    pub fn on_trade(&mut self, instrument: InstrumentId, ts_sec: u64, price: Price, qty: Qty) {
        let cap = self.cap;
        let series = self
            .series
            .entry(instrument)
            .or_insert_with(|| vec![VecDeque::new(); INTERVALS.len()]);

        for (idx, (_, interval)) in INTERVALS.iter().enumerate() {
            let start = bucket_start(*interval, ts_sec);
            let dq = &mut series[idx];
            match dq.back_mut() {
                Some(c) if c.start == start => {
                    c.high = c.high.max(price);
                    c.low = c.low.min(price);
                    c.close = price;
                    c.volume += qty;
                    c.trades += 1;
                }
                _ => {
                    dq.push_back(Candle {
                        start,
                        open: price,
                        high: price,
                        low: price,
                        close: price,
                        volume: qty,
                        trades: 1,
                    });
                    if dq.len() > cap {
                        dq.pop_front();
                    }
                }
            }
        }
    }

    /// The most recent `limit` candles for `(instrument, interval_name)`,
    /// oldest first. Empty if unknown instrument/interval.
    pub fn candles(&self, instrument: InstrumentId, interval: &str, limit: usize) -> Vec<Candle> {
        let Some((idx, _)) = interval_by_name(interval) else {
            return Vec::new();
        };
        self.series
            .get(&instrument)
            .map(|s| {
                let dq = &s[idx];
                dq.iter().skip(dq.len().saturating_sub(limit)).copied().collect()
            })
            .unwrap_or_default()
    }

    /// Instruments that have printed at least one trade.
    pub fn instruments(&self) -> Vec<InstrumentId> {
        let mut v: Vec<InstrumentId> = self.series.keys().copied().collect();
        v.sort();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_buckets_align() {
        assert_eq!(bucket_start(Interval::Fixed(60), 3_723), 3_720); // 01:02:03 -> 01:02
        assert_eq!(bucket_start(Interval::Fixed(300), 3_723), 3_600);
        assert_eq!(bucket_start(Interval::Fixed(86_400), 90_000), 86_400);
    }

    #[test]
    fn weeks_start_monday() {
        // 1970-01-05 (Monday) 00:00 = 345600.
        let monday = 4 * 86_400;
        // Any moment that week maps to that Monday…
        assert_eq!(bucket_start(Interval::Week, monday), monday);
        assert_eq!(bucket_start(Interval::Week, monday + 6 * 86_400 + 3600), monday);
        // …and the Sunday before belongs to the previous week (which began
        // 1969-12-29, before the epoch — clamps into that week's Thursday-start
        // epoch segment; verify the *next* Monday rolls over instead).
        assert_eq!(bucket_start(Interval::Week, monday + 7 * 86_400), monday + 7 * 86_400);
    }

    #[test]
    fn months_are_calendar_months() {
        // 2024-01-31 23:59:59 UTC = 1706745599; 2024-02-01 00:00:00 = 1706745600.
        let jan31 = 1_706_745_599;
        let feb1 = 1_706_745_600;
        let jan_start = bucket_start(Interval::Month, jan31);
        let feb_start = bucket_start(Interval::Month, feb1);
        assert_eq!(jan_start, 1_704_067_200); // 2024-01-01 00:00 UTC
        assert_eq!(feb_start, feb1); // February begins exactly there
        assert_ne!(jan_start, feb_start);
    }

    #[test]
    fn ohlcv_aggregation_is_correct() {
        let sym = InstrumentId(1);
        let mut agg = KlineAggregator::new(100);
        // Three trades in one minute bucket, one in the next.
        agg.on_trade(sym, 60, 100, 5); // open
        agg.on_trade(sym, 90, 130, 2); // high
        agg.on_trade(sym, 119, 95, 3); // low + close
        agg.on_trade(sym, 120, 105, 1); // next bucket

        let m = agg.candles(sym, "1m", 10);
        assert_eq!(m.len(), 2);
        assert_eq!(
            m[0],
            Candle { start: 60, open: 100, high: 130, low: 95, close: 95, volume: 10, trades: 3 }
        );
        assert_eq!(m[1].open, 105);

        // The 1s series split them into four separate candles.
        assert_eq!(agg.candles(sym, "1s", 10).len(), 4);
        // The daily series merged them into one.
        let d = agg.candles(sym, "1d", 10);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].volume, 11);
        assert_eq!(d[0].trades, 4);
    }

    #[test]
    fn window_is_bounded() {
        let sym = InstrumentId(1);
        let mut agg = KlineAggregator::new(3);
        for i in 0..10 {
            agg.on_trade(sym, i, 100 + i, 1); // one 1s candle each
        }
        let s = agg.candles(sym, "1s", 100);
        assert_eq!(s.len(), 3, "window must stay bounded");
        assert_eq!(s[0].start, 7);
    }
}
