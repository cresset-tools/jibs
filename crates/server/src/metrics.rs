//! Performance metrics collection for the server
//!
//! Provides thread-safe timing accumulation for measuring where time is spent
//! during the import process.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use jibs_protocol::{QueryTiming, ServerMetrics};

/// Thread-safe metrics accumulator for server-side timing
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
    /// Time spent on dedup and FK extraction during aggregate BFS
    dedup_time_ns: AtomicU64,
    /// Wall-clock time for aggregate BFS (Phase 1)
    aggregate_wall_ns: AtomicU64,
    /// Wall-clock time for full table streaming (Phase 2)
    full_tables_wall_ns: AtomicU64,
    /// Serialize time during aggregate phase only
    aggregate_serialize_ns: AtomicU64,
    /// Write time during aggregate phase only
    aggregate_write_ns: AtomicU64,
    /// Time spent on zstd compression
    compress_time_ns: AtomicU64,
    /// Compression time during aggregate phase only
    aggregate_compress_ns: AtomicU64,
    /// Time spent pre-caching table schemas
    schema_cache_time_ns: AtomicU64,
    /// Number of data messages sent
    message_count: AtomicU64,
    /// Total compressed bytes in data messages
    total_compressed_bytes: AtomicU64,
    /// Time spent on inter-level dedupe_values() calls
    interlevel_dedup_time_ns: AtomicU64,
    /// Per-query timing records for aggregate BFS queries
    query_timings: Mutex<Vec<QueryTiming>>,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self {
            query_time_ns: AtomicU64::new(0),
            iterate_time_ns: AtomicU64::new(0),
            serialize_time_ns: AtomicU64::new(0),
            write_time_ns: AtomicU64::new(0),
            rows_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            enabled: false,
            dedup_time_ns: AtomicU64::new(0),
            aggregate_wall_ns: AtomicU64::new(0),
            full_tables_wall_ns: AtomicU64::new(0),
            aggregate_serialize_ns: AtomicU64::new(0),
            aggregate_write_ns: AtomicU64::new(0),
            compress_time_ns: AtomicU64::new(0),
            aggregate_compress_ns: AtomicU64::new(0),
            schema_cache_time_ns: AtomicU64::new(0),
            message_count: AtomicU64::new(0),
            total_compressed_bytes: AtomicU64::new(0),
            interlevel_dedup_time_ns: AtomicU64::new(0),
            query_timings: Mutex::new(Vec::new()),
        }
    }
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

    /// Add dedup/FK extraction time
    #[inline]
    pub fn add_dedup_time(&self, duration: Duration) {
        if self.enabled {
            self.dedup_time_ns
                .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Add compression time
    #[inline]
    pub fn add_compress_time(&self, duration: Duration) {
        if self.enabled {
            self.compress_time_ns
                .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Set schema cache time
    pub fn set_schema_cache_time(&self, duration: Duration) {
        if self.enabled {
            self.schema_cache_time_ns
                .store(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Track a data message being sent (count + compressed size)
    #[inline]
    pub fn add_message(&self, compressed_bytes: u64) {
        if self.enabled {
            self.message_count.fetch_add(1, Ordering::Relaxed);
            self.total_compressed_bytes
                .fetch_add(compressed_bytes, Ordering::Relaxed);
        }
    }

    /// Add inter-level dedup time (dedupe_values between BFS levels)
    #[inline]
    pub fn add_interlevel_dedup_time(&self, duration: Duration) {
        if self.enabled {
            self.interlevel_dedup_time_ns
                .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Set wall-clock time for aggregate BFS traversal (Phase 1)
    pub fn set_aggregate_wall_time(&self, duration: Duration) {
        if self.enabled {
            self.aggregate_wall_ns
                .store(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Set wall-clock time for full table streaming (Phase 2).
    /// Uses fetch_max so concurrent workers report correctly (longest wins).
    pub fn set_full_tables_wall_time(&self, duration: Duration) {
        if self.enabled {
            self.full_tables_wall_ns
                .fetch_max(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Snapshot current serialize/write totals as the aggregate phase totals.
    /// Call this at the end of Phase 1, before Phase 2 starts adding to the same counters.
    pub fn snapshot_aggregate_phase(&self) {
        if self.enabled {
            self.aggregate_serialize_ns.store(
                self.serialize_time_ns.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.aggregate_write_ns.store(
                self.write_time_ns.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.aggregate_compress_ns.store(
                self.compress_time_ns.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
        }
    }

    /// Record timing for a single aggregate BFS query
    pub fn record_query(&self, timing: QueryTiming) {
        if self.enabled {
            self.query_timings.lock().unwrap().push(timing);
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
            dedup_time_ms: self.dedup_time_ns.load(Ordering::Relaxed) / 1_000_000,
            aggregate_wall_ms: self.aggregate_wall_ns.load(Ordering::Relaxed) / 1_000_000,
            full_tables_wall_ms: self.full_tables_wall_ns.load(Ordering::Relaxed) / 1_000_000,
            aggregate_serialize_ms: self.aggregate_serialize_ns.load(Ordering::Relaxed) / 1_000_000,
            aggregate_write_ms: self.aggregate_write_ns.load(Ordering::Relaxed) / 1_000_000,
            compress_time_ms: self.compress_time_ns.load(Ordering::Relaxed) / 1_000_000,
            aggregate_compress_ms: self.aggregate_compress_ns.load(Ordering::Relaxed) / 1_000_000,
            schema_cache_time_ms: self.schema_cache_time_ns.load(Ordering::Relaxed) / 1_000_000,
            message_count: self.message_count.load(Ordering::Relaxed),
            total_compressed_bytes: self.total_compressed_bytes.load(Ordering::Relaxed),
            aggregate_interlevel_dedup_ms: self.interlevel_dedup_time_ns.load(Ordering::Relaxed)
                / 1_000_000,
            query_timings: std::mem::take(&mut *self.query_timings.lock().unwrap()),
        })
    }

    /// Lightweight snapshot without per-query timings (for TableDone messages)
    pub fn snapshot(&self) -> Option<ServerMetrics> {
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
            dedup_time_ms: self.dedup_time_ns.load(Ordering::Relaxed) / 1_000_000,
            aggregate_wall_ms: self.aggregate_wall_ns.load(Ordering::Relaxed) / 1_000_000,
            full_tables_wall_ms: self.full_tables_wall_ns.load(Ordering::Relaxed) / 1_000_000,
            aggregate_serialize_ms: self.aggregate_serialize_ns.load(Ordering::Relaxed) / 1_000_000,
            aggregate_write_ms: self.aggregate_write_ns.load(Ordering::Relaxed) / 1_000_000,
            compress_time_ms: self.compress_time_ns.load(Ordering::Relaxed) / 1_000_000,
            aggregate_compress_ms: self.aggregate_compress_ns.load(Ordering::Relaxed) / 1_000_000,
            schema_cache_time_ms: self.schema_cache_time_ns.load(Ordering::Relaxed) / 1_000_000,
            message_count: self.message_count.load(Ordering::Relaxed),
            total_compressed_bytes: self.total_compressed_bytes.load(Ordering::Relaxed),
            aggregate_interlevel_dedup_ms: self.interlevel_dedup_time_ns.load(Ordering::Relaxed)
                / 1_000_000,
            query_timings: Vec::new(),
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
