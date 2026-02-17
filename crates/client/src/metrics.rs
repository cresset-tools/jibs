//! Performance metrics collection and display for the client
//!
//! Provides timing measurements for identifying bottlenecks in the import pipeline.

use std::time::{Duration, Instant};

use jibs_protocol::ServerMetrics;

/// Client-side performance metrics
#[derive(Debug, Default)]
pub struct ClientMetrics {
    /// Time spent receiving messages from server (network/SSH)
    pub recv_time: Duration,
    /// Time spent decompressing data
    pub decompress_time: Duration,
    /// Time spent executing LOAD DATA (sequential mode only; parallel mode loads in background)
    pub load_time: Duration,
    /// Total rows loaded
    pub rows_loaded: u64,
    /// Total bytes received (after decompression)
    pub bytes_received: u64,
    /// Wall clock time for entire import
    pub wall_time: Duration,
    /// Start time for wall clock measurement
    start_time: Option<Instant>,
}

impl ClientMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start wall clock timing
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
    }

    /// Stop wall clock timing
    pub fn stop(&mut self) {
        if let Some(start) = self.start_time.take() {
            self.wall_time = start.elapsed();
        }
    }

    /// Add time spent receiving data
    pub fn add_recv_time(&mut self, duration: Duration) {
        self.recv_time += duration;
    }

    /// Add time spent decompressing
    pub fn add_decompress_time(&mut self, duration: Duration) {
        self.decompress_time += duration;
    }

    /// Add time spent loading data
    pub fn add_load_time(&mut self, duration: Duration) {
        self.load_time += duration;
    }

    /// Add rows loaded
    pub fn add_rows_loaded(&mut self, count: u64) {
        self.rows_loaded += count;
    }

    /// Add bytes received
    pub fn add_bytes_received(&mut self, count: u64) {
        self.bytes_received += count;
    }

    /// Display metrics summary
    pub fn display(&self, server_metrics: Option<&ServerMetrics>) {
        println!();
        println!("=== Performance Metrics ===");
        println!();

        // Server metrics
        if let Some(sm) = server_metrics {
            let server_total_ms = sm.query_time_ms + sm.iterate_time_ms + sm.serialize_time_ms + sm.write_time_ms;
            println!("Server (remote):");
            println!(
                "  Query execution:     {:>8} ({:>3}%)",
                format_duration_ms(sm.query_time_ms),
                percent(sm.query_time_ms, server_total_ms)
            );
            println!(
                "  Row iteration:       {:>8} ({:>3}%)",
                format_duration_ms(sm.iterate_time_ms),
                percent(sm.iterate_time_ms, server_total_ms)
            );
            println!(
                "  TSV serialization:   {:>8} ({:>3}%)",
                format_duration_ms(sm.serialize_time_ms),
                percent(sm.serialize_time_ms, server_total_ms)
            );
            let write_pct = percent(sm.write_time_ms, server_total_ms);
            let backpressure_note = if write_pct > 30 { " <- backpressure" } else { "" };
            println!(
                "  Stdout write:        {:>8} ({:>3}%){}",
                format_duration_ms(sm.write_time_ms),
                write_pct,
                backpressure_note
            );
            println!();
        }

        // Client metrics
        let client_total = self.recv_time + self.decompress_time + self.load_time;
        let client_total_ms = client_total.as_millis() as u64;

        println!("Client (local):");
        println!(
            "  Message receive:     {:>8} ({:>3}%)",
            format_duration(self.recv_time),
            percent(self.recv_time.as_millis() as u64, client_total_ms)
        );
        println!(
            "  Decompression:       {:>8} ({:>3}%)",
            format_duration(self.decompress_time),
            percent(self.decompress_time.as_millis() as u64, client_total_ms)
        );
        let load_pct = percent(self.load_time.as_millis() as u64, client_total_ms);
        let load_note = if load_pct > 40 { " <- bottleneck" } else { "" };
        println!(
            "  LOAD DATA:           {:>8} ({:>3}%){}",
            format_duration(self.load_time),
            load_pct,
            load_note
        );
        println!();

        // Transfer stats
        let wall_secs = self.wall_time.as_secs_f64();
        let mb_received = self.bytes_received as f64 / (1024.0 * 1024.0);
        let throughput = if wall_secs > 0.0 {
            mb_received / wall_secs
        } else {
            0.0
        };
        println!(
            "Transfer: {:.1} MB at {:.1} MB/s",
            mb_received,
            throughput
        );
        println!("Rows: {}", format_rows(self.rows_loaded));
        println!("Wall time: {}", format_duration(self.wall_time));
    }
}

/// Format milliseconds as duration string
fn format_duration_ms(ms: u64) -> String {
    let secs = ms as f64 / 1000.0;
    if secs >= 60.0 {
        let mins = (secs / 60.0).floor();
        let rem_secs = secs - mins * 60.0;
        format!("{:.0}m {:.1}s", mins, rem_secs)
    } else {
        format!("{:.1}s", secs)
    }
}

/// Format Duration as string
fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 60.0 {
        let mins = (secs / 60.0).floor();
        let rem_secs = secs - mins * 60.0;
        format!("{:.0}m {:.1}s", mins, rem_secs)
    } else {
        format!("{:.1}s", secs)
    }
}

/// Calculate percentage, handling zero division
fn percent(value: u64, total: u64) -> u64 {
    if total == 0 {
        0
    } else {
        (value * 100) / total
    }
}

/// Format row count with thousands separators
fn format_rows(count: u64) -> String {
    let s = count.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}
