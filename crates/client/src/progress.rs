//! Progress tracking for import operations

use std::collections::{HashMap, VecDeque};
use std::io::IsTerminal;
use std::time::Instant;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use jibs_protocol::TableInfo;

/// Maximum number of table progress bars to show at once
const MAX_VISIBLE_TABLES: usize = 5;

/// Statistics tracked during import
#[derive(Default)]
struct ProgressStats {
    tables_total: usize,
    tables_completed: usize,
    tables_skipped: usize,
    #[allow(dead_code)]
    rows_total: u64,
    bytes_total: u64,
}

/// Track state for each active table
struct TableState {
    /// Progress bar (only Some if this table is visible)
    bar: Option<ProgressBar>,
    /// Rows received so far
    rows_received: u64,
    /// Estimated total rows (may be adjusted)
    estimated_rows: u64,
}

/// Progress tracking for import operations
pub struct ImportProgress {
    /// Multi-progress container
    multi: MultiProgress,
    /// Overall tables progress bar
    overall_bar: ProgressBar,
    /// State for all active tables (keyed by name)
    table_states: HashMap<String, TableState>,
    /// Order in which tables started (for visibility priority)
    table_order: VecDeque<String>,
    /// Number of currently visible table bars
    visible_count: usize,
    /// Overflow indicator bar ("+ N more tables...")
    overflow_bar: Option<ProgressBar>,
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
            table_states: HashMap::new(),
            table_order: VecDeque::new(),
            visible_count: 0,
            overflow_bar: None,
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
        // Track this table in our order queue
        self.table_order.push_back(name.to_string());

        let bar = if self.is_tty && self.visible_count < MAX_VISIBLE_TABLES {
            // Create visible progress bar
            let bar = self.multi.add(ProgressBar::new(estimated_rows.max(1)));
            bar.set_style(
                ProgressStyle::default_bar()
                    .template("   {prefix:.bold} [{bar:30.yellow/white}] {msg}")
                    .expect("valid template")
                    .progress_chars("=>-"),
            );
            bar.set_prefix(name.to_string());
            bar.set_message(format_row_progress(0, estimated_rows));
            self.visible_count += 1;
            Some(bar)
        } else {
            None
        };

        self.table_states.insert(
            name.to_string(),
            TableState {
                bar,
                rows_received: 0,
                estimated_rows,
            },
        );

        // Update overflow indicator
        self.update_overflow_bar();
    }

    /// Called when a data chunk is received
    pub fn update_table(&mut self, name: &str, rows: u32, bytes: usize) {
        self.stats.bytes_total += bytes as u64;

        if let Some(state) = self.table_states.get_mut(name) {
            state.rows_received += rows as u64;

            // If we've exceeded the estimate, adjust it upward
            // Use a heuristic: assume we're ~halfway done if we hit the estimate
            if state.rows_received > state.estimated_rows {
                state.estimated_rows = state.rows_received * 2;
                if let Some(ref bar) = state.bar {
                    bar.set_length(state.estimated_rows);
                }
            }

            if let Some(ref bar) = state.bar {
                bar.set_position(state.rows_received);
                bar.set_message(format_row_progress(
                    state.rows_received,
                    state.estimated_rows,
                ));
            }
        }

        // Update overall bar message with throughput
        self.update_overall_message();
    }

    /// Called when a table is fully completed
    pub fn finish_table(&mut self, name: &str, final_rows: u64) {
        self.stats.tables_completed += 1;

        // Remove from order queue
        self.table_order.retain(|n| n != name);

        // Clean up the table state
        if let Some(state) = self.table_states.remove(name) {
            if let Some(bar) = state.bar {
                bar.set_position(final_rows);
                bar.set_message(format!("{} rows", format_number(final_rows)));
                bar.finish_and_clear();
                self.visible_count -= 1;
            }
        }

        // Maybe promote a hidden table to visible
        self.promote_hidden_table();

        // Update overflow indicator
        self.update_overflow_bar();

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
        // Clear any remaining bars
        for state in self.table_states.values() {
            if let Some(ref bar) = state.bar {
                bar.finish_and_clear();
            }
        }
        if let Some(ref bar) = self.overflow_bar {
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

    /// Promote a hidden table to become visible
    fn promote_hidden_table(&mut self) {
        if !self.is_tty || self.visible_count >= MAX_VISIBLE_TABLES {
            return;
        }

        // Find first table in order that doesn't have a visible bar
        for table_name in &self.table_order {
            if let Some(state) = self.table_states.get_mut(table_name) {
                if state.bar.is_none() {
                    // Create a bar for this table
                    let bar = self.multi.add(ProgressBar::new(state.estimated_rows.max(1)));
                    bar.set_style(
                        ProgressStyle::default_bar()
                            .template("   {prefix:.bold} [{bar:30.yellow/white}] {msg}")
                            .expect("valid template")
                            .progress_chars("=>-"),
                    );
                    bar.set_prefix(table_name.clone());
                    bar.set_position(state.rows_received);
                    bar.set_message(format_row_progress(
                        state.rows_received,
                        state.estimated_rows,
                    ));
                    state.bar = Some(bar);
                    self.visible_count += 1;
                    return;
                }
            }
        }
    }

    /// Update or create/remove the overflow indicator bar
    fn update_overflow_bar(&mut self) {
        if !self.is_tty {
            return;
        }

        let hidden_count = self.table_states.len().saturating_sub(self.visible_count);

        if hidden_count > 0 {
            // Need overflow bar
            if self.overflow_bar.is_none() {
                let bar = self.multi.add(ProgressBar::new_spinner());
                bar.set_style(
                    ProgressStyle::default_spinner()
                        .template("   {spinner:.dim} {msg}")
                        .expect("valid template"),
                );
                self.overflow_bar = Some(bar);
            }

            if let Some(ref bar) = self.overflow_bar {
                bar.set_message(format!(
                    "+ {} more table{} in progress...",
                    hidden_count,
                    if hidden_count == 1 { "" } else { "s" }
                ));
                bar.tick();
            }
        } else {
            // Remove overflow bar
            if let Some(bar) = self.overflow_bar.take() {
                bar.finish_and_clear();
            }
        }
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
        for state in self.table_states.values() {
            if let Some(ref bar) = state.bar {
                bar.finish_and_clear();
            }
        }
        if let Some(ref bar) = self.overflow_bar {
            bar.finish_and_clear();
        }
        self.overall_bar.finish_and_clear();
    }
}

/// Format row progress with nice numbers and percentage
fn format_row_progress(current: u64, total: u64) -> String {
    if total == 0 {
        return format!("{} rows", format_number(current));
    }

    let pct = (current as f64 / total as f64 * 100.0).min(999.0);
    format!(
        "{}/{} rows ({:.0}%)",
        format_number(current),
        format_number(total),
        pct
    )
}

/// Format a number with thousand separators
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}
