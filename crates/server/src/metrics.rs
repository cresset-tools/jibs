//! Performance metrics collection for the server
//!
//! Provides thread-safe timing accumulation for measuring where time is spent
//! during the import process.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use jibs_protocol::ServerMetrics;

/// Thread-safe metrics accumulator for server-side timing
#[derive(Default)]
pub struct MetricsCollector {
    /// Time spent executing MySQL queries
    query_time_ns: AtomicU64,
    /// Time spent iterating over result rows
    iterate_time_ns: AtomicU64,
    /// Time spent serializing rows to TSV
    serialize_time_ns: AtomicU64,
    /// Time spent writing to stdout (indicates backpressure)
    write_time_ns: AtomicU64,
    /// Total rows sent
    rows_sent: AtomicU64,
    /// Total bytes sent (before compression)
    bytes_sent: AtomicU64,
    /// Whether metrics collection is enabled
    enabled: bool,
}

impl MetricsCollector {
    /// Create a new disabled metrics collector (no-op)
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Create a new enabled metrics collector
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Check if metrics collection is enabled
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Add query execution time
    #[inline]
    pub fn add_query_time(&self, duration: Duration) {
        if self.enabled {
            self.query_time_ns
                .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Add row iteration time
    #[inline]
    pub fn add_iterate_time(&self, duration: Duration) {
        if self.enabled {
            self.iterate_time_ns
                .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Add TSV serialization time
    #[inline]
    pub fn add_serialize_time(&self, duration: Duration) {
        if self.enabled {
            self.serialize_time_ns
                .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Add stdout write time
    #[inline]
    pub fn add_write_time(&self, duration: Duration) {
        if self.enabled {
            self.write_time_ns
                .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Add rows sent count
    #[inline]
    pub fn add_rows_sent(&self, count: u64) {
        if self.enabled {
            self.rows_sent.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Add bytes sent count
    #[inline]
    pub fn add_bytes_sent(&self, count: u64) {
        if self.enabled {
            self.bytes_sent.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Convert to protocol ServerMetrics
    pub fn to_server_metrics(&self) -> Option<ServerMetrics> {
        if !self.enabled {
            return None;
        }

        Some(ServerMetrics {
            query_time_ms: self.query_time_ns.load(Ordering::Relaxed) / 1_000_000,
            iterate_time_ms: self.iterate_time_ns.load(Ordering::Relaxed) / 1_000_000,
            serialize_time_ms: self.serialize_time_ns.load(Ordering::Relaxed) / 1_000_000,
            write_time_ms: self.write_time_ns.load(Ordering::Relaxed) / 1_000_000,
            rows_sent: self.rows_sent.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
        })
    }
}

/// RAII timer that records elapsed time when dropped
pub struct Timer<'a, F>
where
    F: Fn(&MetricsCollector, Duration),
{
    collector: &'a MetricsCollector,
    start: Instant,
    record_fn: F,
}

impl<'a, F> Timer<'a, F>
where
    F: Fn(&MetricsCollector, Duration),
{
    pub fn new(collector: &'a MetricsCollector, record_fn: F) -> Self {
        Self {
            collector,
            start: Instant::now(),
            record_fn,
        }
    }
}

impl<'a, F> Drop for Timer<'a, F>
where
    F: Fn(&MetricsCollector, Duration),
{
    fn drop(&mut self) {
        if self.collector.is_enabled() {
            (self.record_fn)(self.collector, self.start.elapsed());
        }
    }
}

/// Helper macros for timing code sections
impl MetricsCollector {
    /// Create a timer for query execution
    #[inline]
    pub fn time_query(&self) -> Timer<impl Fn(&MetricsCollector, Duration) + '_> {
        Timer::new(self, |c, d| c.add_query_time(d))
    }

    /// Create a timer for row iteration
    #[inline]
    pub fn time_iterate(&self) -> Timer<impl Fn(&MetricsCollector, Duration) + '_> {
        Timer::new(self, |c, d| c.add_iterate_time(d))
    }

    /// Create a timer for serialization
    #[inline]
    pub fn time_serialize(&self) -> Timer<impl Fn(&MetricsCollector, Duration) + '_> {
        Timer::new(self, |c, d| c.add_serialize_time(d))
    }

    /// Create a timer for writing
    #[inline]
    pub fn time_write(&self) -> Timer<impl Fn(&MetricsCollector, Duration) + '_> {
        Timer::new(self, |c, d| c.add_write_time(d))
    }
}
