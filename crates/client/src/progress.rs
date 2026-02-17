//! Progress tracking for import operations

use std::collections::{HashMap, VecDeque};
use std::io::IsTerminal;
use std::time::Instant;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use jibs_protocol::TableInfo;

/// Statistics tracked during import
#[derive(Default)]
struct ProgressStats {
    #[allow(dead_code)]
    tables_total: usize,
    tables_completed: usize,
    tables_skipped: usize,
    #[allow(dead_code)]
    rows_total: u64,
    bytes_total: u64,
}

/// Progress tracking for import operations
pub struct ImportProgress {
    /// Multi-progress container
    multi: MultiProgress,
    /// Overall tables progress bar
    overall_bar: ProgressBar,
    /// Active table progress bars (multiple tables can be in-flight)
    table_bars: HashMap<String, ProgressBar>,
    /// Whether we're running in a TTY
    is_tty: bool,
    /// Start time of import
    start_time: Instant,
    /// Current statistics
    stats: ProgressStats,
    /// Rolling window of (timestamp, cumulative_bytes) for throughput calculation
    throughput_samples: VecDeque<(Instant, u64)>,
}

impl ImportProgress {
    /// Create a new progress tracker
    ///
    /// `tables` - list of tables to import (with estimates)
    /// `skipped` - number of tables already completed (resume scenario)
    pub fn new(tables: &[TableInfo], skipped: usize) -> Self {
        let is_tty = std::io::stderr().is_terminal();
        let multi = MultiProgress::new();

        let total_tables = tables.len();
        let total_rows: u64 = tables.iter().map(|t| t.estimated_rows).sum();

        // Create overall progress bar
        let overall_bar = if is_tty {
            let bar = multi.add(ProgressBar::new(total_tables as u64));
            bar.set_style(
                ProgressStyle::default_bar()
                    .template(" {spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} tables ({msg})")
                    .expect("valid template")
                    .progress_chars("=>-"),
            );
            bar.set_position(skipped as u64);
            bar.set_message("starting...");
            bar
        } else {
            ProgressBar::hidden()
        };

        Self {
            multi,
            overall_bar,
            table_bars: HashMap::new(),
            is_tty,
            start_time: Instant::now(),
            stats: ProgressStats {
                tables_total: total_tables,
                tables_completed: skipped,
                tables_skipped: skipped,
                rows_total: total_rows,
                bytes_total: 0,
            },
            throughput_samples: VecDeque::new(),
        }
    }

    /// Called when starting to import a table
    pub fn start_table(&mut self, name: &str, estimated_rows: u64) {
        if self.is_tty {
            // Create new table progress bar
            let bar = self.multi.add(ProgressBar::new(estimated_rows));
            bar.set_style(
                ProgressStyle::default_bar()
                    .template("   {prefix:.bold} [{bar:30.yellow/white}] {pos}/{len} rows ({per_sec})")
                    .expect("valid template")
                    .progress_chars("=>-"),
            );
            bar.set_prefix(name.to_string());
            self.table_bars.insert(name.to_string(), bar);
        }
    }

    /// Called when a data chunk is received
    pub fn update_table(&mut self, name: &str, rows: u32, bytes: usize) {
        self.stats.bytes_total += bytes as u64;

        if let Some(bar) = self.table_bars.get(name) {
            bar.inc(rows as u64);
        }

        // Update overall bar message with throughput
        self.update_overall_message();
    }

    /// Called when a table is fully completed
    pub fn finish_table(&mut self, name: &str, final_rows: u64) {
        self.stats.tables_completed += 1;

        if let Some(bar) = self.table_bars.remove(name) {
            bar.set_position(final_rows);
            bar.finish_and_clear();
        }

        if self.is_tty {
            self.overall_bar.inc(1);
            self.overall_bar.set_message(format!("{} done", name));
        } else {
            // Plain log output for non-TTY
            tracing::info!("Imported table {} ({} rows)", name, final_rows);
        }
    }

    /// Called when skipping an already-completed table (resume)
    pub fn skip_table(&mut self, name: &str) {
        if self.is_tty {
            self.overall_bar.set_message(format!("{} (skipped)", name));
        } else {
            tracing::info!("Skipping table {} (already completed)", name);
        }
    }

    /// Called when import is fully complete
    pub fn finish(&self) {
        for bar in self.table_bars.values() {
            bar.finish_and_clear();
        }
        self.overall_bar.finish_and_clear();

        let elapsed = self.start_time.elapsed();
        let tables_imported = self.stats.tables_completed - self.stats.tables_skipped;

        tracing::info!(
            "Import complete: {} tables imported in {:.1}s ({} total, {} MB transferred)",
            tables_imported,
            elapsed.as_secs_f64(),
            self.stats.tables_completed,
            self.stats.bytes_total / (1024 * 1024)
        );
    }

    /// Update the overall progress bar message with rolling 5s throughput
    fn update_overall_message(&mut self) {
        if !self.is_tty {
            return;
        }

        let now = Instant::now();
        self.throughput_samples
            .push_back((now, self.stats.bytes_total));

        // Remove samples older than 5 seconds
        let cutoff = now - std::time::Duration::from_secs(5);
        while self
            .throughput_samples
            .front()
            .is_some_and(|(t, _)| *t < cutoff)
        {
            self.throughput_samples.pop_front();
        }

        if let Some((oldest_time, oldest_bytes)) = self.throughput_samples.front() {
            let dt = now.duration_since(*oldest_time).as_secs_f64();
            let db = self.stats.bytes_total - oldest_bytes;
            if dt > 0.1 {
                let mb_per_sec = (db as f64 / (1024.0 * 1024.0)) / dt;
                self.overall_bar
                    .set_message(format!("{:.1} MB/s", mb_per_sec));
            }
        }
    }

    /// Suspend progress bars for logging
    ///
    /// Use this when you need to print a log message that shouldn't be
    /// overwritten by progress bars.
    pub fn suspend<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.multi.suspend(f)
    }
}

impl Drop for ImportProgress {
    fn drop(&mut self) {
        for bar in self.table_bars.values() {
            bar.finish_and_clear();
        }
        self.overall_bar.finish_and_clear();
    }
}
