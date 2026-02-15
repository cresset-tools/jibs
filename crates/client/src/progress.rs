//! Progress tracking for import operations

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
    /// Current table progress bar
    table_bar: Option<ProgressBar>,
    /// Whether we're running in a TTY
    is_tty: bool,
    /// Start time of import
    start_time: Instant,
    /// Current statistics
    stats: ProgressStats,
    /// Estimated rows for current table
    current_table_rows: u64,
    /// Current table name
    current_table_name: Option<String>,
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
            table_bar: None,
            is_tty,
            start_time: Instant::now(),
            stats: ProgressStats {
                tables_total: total_tables,
                tables_completed: skipped,
                tables_skipped: skipped,
                rows_total: total_rows,
                bytes_total: 0,
            },
            current_table_rows: 0,
            current_table_name: None,
        }
    }

    /// Called when starting to import a table
    pub fn start_table(&mut self, name: &str, estimated_rows: u64) {
        self.current_table_name = Some(name.to_string());
        self.current_table_rows = 0;

        if self.is_tty {
            // Remove old table bar if exists
            if let Some(bar) = self.table_bar.take() {
                bar.finish_and_clear();
            }

            // Create new table progress bar
            let bar = self.multi.add(ProgressBar::new(estimated_rows));
            bar.set_style(
                ProgressStyle::default_bar()
                    .template("   {prefix:.bold} [{bar:30.yellow/white}] {pos}/{len} rows ({per_sec})")
                    .expect("valid template")
                    .progress_chars("=>-"),
            );
            bar.set_prefix(name.to_string());
            self.table_bar = Some(bar);
        }
    }

    /// Called when a data chunk is received
    pub fn update_table(&mut self, rows: u32, bytes: usize) {
        self.current_table_rows += rows as u64;
        self.stats.bytes_total += bytes as u64;

        if let Some(bar) = &self.table_bar {
            bar.inc(rows as u64);
        }

        // Update overall bar message with throughput
        self.update_overall_message();
    }

    /// Called when a table is fully completed
    pub fn finish_table(&mut self, name: &str, final_rows: u64) {
        self.stats.tables_completed += 1;

        if let Some(bar) = self.table_bar.take() {
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

        self.current_table_name = None;
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
        if let Some(bar) = &self.table_bar {
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

    /// Update the overall progress bar message with throughput info
    fn update_overall_message(&mut self) {
        if !self.is_tty {
            return;
        }

        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            let mb_per_sec = (self.stats.bytes_total as f64 / (1024.0 * 1024.0)) / elapsed;
            let msg = format!("{:.1} MB/s", mb_per_sec);
            self.overall_bar.set_message(msg);
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
        if let Some(bar) = &self.table_bar {
            bar.finish_and_clear();
        }
        self.overall_bar.finish_and_clear();
    }
}
