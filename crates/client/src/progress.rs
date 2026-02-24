//! Progress tracking for import operations
//!
//! Progress bars update independently on a background ticker thread,
//! decoupled from the import message loop.
//!
//! All tracing output is automatically routed through `MultiProgress::println()`
//! when progress bars are active, preventing log lines from corrupting the
//! terminal cursor tracking.

use std::collections::{HashMap, VecDeque};
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tracing_subscriber::fmt::MakeWriter;

use jibs_protocol::TableInfo;

/// Maximum number of table progress bars to show at once
const MAX_VISIBLE_TABLES: usize = 5;

/// How often the background ticker thread updates (spinner animation, throughput)
const TICK_INTERVAL: Duration = Duration::from_millis(150);

// ---------------------------------------------------------------------------
// Tracing writer that routes through MultiProgress when active
// ---------------------------------------------------------------------------

/// Global handle to the active MultiProgress (set while progress bars are shown)
static ACTIVE_MULTI: OnceLock<Mutex<Option<MultiProgress>>> = OnceLock::new();

fn active_multi() -> &'static Mutex<Option<MultiProgress>> {
    ACTIVE_MULTI.get_or_init(|| Mutex::new(None))
}

/// A [`MakeWriter`] implementation that routes tracing output through
/// [`MultiProgress::println`] when progress bars are active, falling back
/// to stderr otherwise. This prevents log lines from corrupting the
/// progress bar cursor tracking.
///
/// Use this as the writer for `tracing_subscriber::fmt()`:
/// ```ignore
/// tracing_subscriber::fmt()
///     .with_writer(ProgressWriter)
///     .init();
/// ```
pub struct ProgressWriter;

/// Writer returned by [`ProgressWriter`] for a single log event.
/// Buffers the formatted log line and writes it on flush/drop.
pub struct ProgressWriterGuard {
    multi: Option<MultiProgress>,
    buf: Vec<u8>,
}

impl<'a> MakeWriter<'a> for ProgressWriter {
    type Writer = ProgressWriterGuard;

    fn make_writer(&'a self) -> Self::Writer {
        ProgressWriterGuard {
            multi: active_multi().lock().unwrap().clone(),
            buf: Vec::with_capacity(256),
        }
    }
}

impl std::io::Write for ProgressWriterGuard {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }

        if let Some(ref multi) = self.multi {
            // Route through MultiProgress so cursor tracking stays correct
            let msg = String::from_utf8_lossy(&self.buf);
            let msg = msg.trim_end_matches('\n');
            if !msg.is_empty() {
                let _ = multi.println(msg);
            }
        } else {
            // No active progress bars — write directly to stderr
            let mut stderr = std::io::stderr().lock();
            stderr.write_all(&self.buf)?;
            stderr.flush()?;
        }

        self.buf.clear();
        Ok(())
    }
}

impl Drop for ProgressWriterGuard {
    fn drop(&mut self) {
        use std::io::Write;
        let _ = self.flush();
    }
}

// ---------------------------------------------------------------------------
// Progress tracking
// ---------------------------------------------------------------------------

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
    /// When this table started importing
    start_time: Instant,
}

/// Shared mutable state protected by a mutex
struct Inner {
    /// State for all active tables (keyed by name)
    table_states: HashMap<String, TableState>,
    /// Order in which tables started (for visibility priority)
    table_order: VecDeque<String>,
    /// Number of currently visible table bars
    visible_count: usize,
    /// Overflow indicator bar ("+ N more tables...")
    overflow_bar: Option<ProgressBar>,
    /// Current statistics
    stats: ProgressStats,
    /// Rolling window of (timestamp, cumulative_bytes) for throughput calculation
    throughput_samples: VecDeque<(Instant, u64)>,
    /// Completed tables: (name, rows, duration)
    completed_tables: Vec<(String, u64, Duration)>,
}

/// Progress tracking for import operations
///
/// All methods take `&self` — internal state is behind `Arc<Mutex<Inner>>`.
/// A background thread independently ticks spinners and updates throughput.
pub struct ImportProgress {
    /// Multi-progress container (internally thread-safe)
    multi: MultiProgress,
    /// Overall tables progress bar (internally thread-safe)
    overall_bar: ProgressBar,
    /// Whether we're running in a TTY
    is_tty: bool,
    /// Start time of import
    start_time: Instant,
    /// Shared mutable state
    inner: Arc<Mutex<Inner>>,
    /// Signals the ticker thread to stop
    shutdown: Arc<AtomicBool>,
    /// Handle for the background ticker thread
    ticker_handle: Option<thread::JoinHandle<()>>,
}

impl ImportProgress {
    /// Create a new progress tracker
    ///
    /// `tables` - list of tables to import (with estimates)
    /// `skipped` - number of tables already completed (resume scenario)
    pub fn new(tables: &[TableInfo], skipped: usize) -> Self {
        let is_tty = std::io::stderr().is_terminal();
        let multi = MultiProgress::new();

        // Register with the global writer so tracing output routes through us
        *active_multi().lock().unwrap() = Some(multi.clone());

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
            bar.enable_steady_tick(TICK_INTERVAL);
            bar
        } else {
            ProgressBar::hidden()
        };

        let inner = Arc::new(Mutex::new(Inner {
            table_states: HashMap::new(),
            table_order: VecDeque::new(),
            visible_count: 0,
            overflow_bar: None,
            stats: ProgressStats {
                tables_total: total_tables,
                tables_completed: skipped,
                tables_skipped: skipped,
                rows_total: total_rows,
                bytes_total: 0,
            },
            throughput_samples: VecDeque::new(),
            completed_tables: Vec::new(),
        }));

        let shutdown = Arc::new(AtomicBool::new(false));

        // Spawn background ticker thread for independent progress updates
        let ticker_handle = if is_tty {
            let shutdown_flag = shutdown.clone();
            let inner_ref = inner.clone();
            let bar = overall_bar.clone();
            Some(thread::spawn(move || {
                ticker_loop(shutdown_flag, inner_ref, bar);
            }))
        } else {
            None
        };

        Self {
            multi,
            overall_bar,
            is_tty,
            start_time: Instant::now(),
            inner,
            shutdown,
            ticker_handle,
        }
    }

    /// Called when starting to import a table
    pub fn start_table(&self, name: &str, estimated_rows: u64) {
        let mut inner = self.inner.lock().unwrap();

        // Track this table in our order queue
        inner.table_order.push_back(name.to_string());

        let bar = if self.is_tty && inner.visible_count < MAX_VISIBLE_TABLES {
            // Create visible progress bar (insert before overflow bar to keep it at the bottom)
            let pb = ProgressBar::new(estimated_rows.max(1));
            let bar = if let Some(ref overflow) = inner.overflow_bar {
                self.multi.insert_before(overflow, pb)
            } else {
                self.multi.add(pb)
            };
            bar.set_style(
                ProgressStyle::default_bar()
                    .template("   {prefix:.bold} [{bar:30.yellow/white}] {msg}")
                    .expect("valid template")
                    .progress_chars("=>-"),
            );
            bar.set_prefix(name.to_string());
            bar.set_message(format_row_progress(0, estimated_rows));
            inner.visible_count += 1;
            Some(bar)
        } else {
            None
        };

        inner.table_states.insert(
            name.to_string(),
            TableState {
                bar,
                rows_received: 0,
                estimated_rows,
                start_time: Instant::now(),
            },
        );

        // Update overflow indicator
        update_overflow_bar(&self.multi, &mut inner, self.is_tty);
    }

    /// Called when a data chunk is received
    pub fn update_table(&self, name: &str, rows: u32, bytes: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.stats.bytes_total += bytes as u64;

        if let Some(state) = inner.table_states.get_mut(name) {
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

        // Throughput message is now updated by the ticker thread
    }

    /// Called when a table is fully completed
    pub fn finish_table(&self, name: &str, final_rows: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.stats.tables_completed += 1;

        // Remove from order queue
        inner.table_order.retain(|n| n != name);

        // Clean up the table state
        if let Some(state) = inner.table_states.remove(name) {
            inner.completed_tables.push((
                name.to_string(),
                final_rows,
                state.start_time.elapsed(),
            ));
            if let Some(bar) = state.bar {
                bar.set_position(final_rows);
                bar.set_message(format!("{} rows", format_number(final_rows)));
                bar.finish_and_clear();
                inner.visible_count -= 1;
            }
        }

        // Maybe promote a hidden table to visible
        promote_hidden_table(&self.multi, &mut inner, self.is_tty);

        // Update overflow indicator
        update_overflow_bar(&self.multi, &mut inner, self.is_tty);

        if self.is_tty {
            self.overall_bar.inc(1);
            self.overall_bar.set_message(format!("{} done", name));
        } else {
            // Plain log output for non-TTY
            tracing::info!("Imported table {} ({} rows)", name, final_rows);
        }
    }

    /// Called when skipping an already-completed table (resume)
    pub fn skip_table(&self, name: &str) {
        if self.is_tty {
            self.overall_bar.set_message(format!("{} (skipped)", name));
        } else {
            tracing::info!("Skipping table {} (already completed)", name);
        }
    }

    /// Called when import is fully complete
    pub fn finish(&self) {
        // Signal ticker thread to stop
        self.shutdown.store(true, Ordering::Relaxed);

        let inner = self.inner.lock().unwrap();

        // Clear any remaining bars
        for state in inner.table_states.values() {
            if let Some(ref bar) = state.bar {
                bar.finish_and_clear();
            }
        }
        if let Some(ref bar) = inner.overflow_bar {
            bar.finish_and_clear();
        }

        let tables_imported = inner.stats.tables_completed - inner.stats.tables_skipped;
        let tables_completed = inner.stats.tables_completed;
        let bytes_total = inner.stats.bytes_total;
        drop(inner);

        self.overall_bar.finish_and_clear();

        // Unregister from global writer so tracing goes back to plain stderr
        *active_multi().lock().unwrap() = None;

        let elapsed = self.start_time.elapsed();

        tracing::info!(
            "Import complete: {} tables imported in {:.1}s ({} total, {} MB transferred)",
            tables_imported,
            elapsed.as_secs_f64(),
            tables_completed,
            bytes_total / (1024 * 1024)
        );
    }

    /// Get completed table durations: (name, rows, duration)
    pub fn table_durations(&self) -> Vec<(String, u64, Duration)> {
        self.inner.lock().unwrap().completed_tables.clone()
    }
}

impl Drop for ImportProgress {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.ticker_handle.take() {
            let _ = handle.join();
        }
        let inner = self.inner.lock().unwrap();
        for state in inner.table_states.values() {
            if let Some(ref bar) = state.bar {
                bar.finish_and_clear();
            }
        }
        if let Some(ref bar) = inner.overflow_bar {
            bar.finish_and_clear();
        }
        drop(inner);
        self.overall_bar.finish_and_clear();

        // Unregister from global writer
        *active_multi().lock().unwrap() = None;
    }
}

/// Background ticker loop — updates throughput display
fn ticker_loop(shutdown: Arc<AtomicBool>, inner: Arc<Mutex<Inner>>, overall_bar: ProgressBar) {
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(TICK_INTERVAL);

        let mut inner = inner.lock().unwrap();

        // Update throughput display on the overall bar
        // (set_message triggers a redraw; spinner animation is handled by enable_steady_tick)
        update_throughput_message(&mut inner, &overall_bar);
    }
}

/// Recalculate rolling throughput and update the overall bar message
fn update_throughput_message(inner: &mut Inner, overall_bar: &ProgressBar) {
    let now = Instant::now();
    inner
        .throughput_samples
        .push_back((now, inner.stats.bytes_total));

    // Remove samples older than 5 seconds
    let cutoff = now - Duration::from_secs(5);
    while inner
        .throughput_samples
        .front()
        .is_some_and(|(t, _)| *t < cutoff)
    {
        inner.throughput_samples.pop_front();
    }

    if let Some((oldest_time, oldest_bytes)) = inner.throughput_samples.front() {
        let dt = now.duration_since(*oldest_time).as_secs_f64();
        let db = inner.stats.bytes_total - oldest_bytes;
        if dt > 0.1 {
            let mb_per_sec = (db as f64 / (1024.0 * 1024.0)) / dt;
            overall_bar.set_message(format!("{:.1} MB/s", mb_per_sec));
        }
    }
}

/// Promote a hidden table to become visible
fn promote_hidden_table(multi: &MultiProgress, inner: &mut Inner, is_tty: bool) {
    if !is_tty || inner.visible_count >= MAX_VISIBLE_TABLES {
        return;
    }

    // Find first table in order that doesn't have a visible bar
    for table_name in inner.table_order.clone() {
        if let Some(state) = inner.table_states.get_mut(&table_name) {
            if state.bar.is_none() {
                // Create a bar for this table (insert before overflow bar to keep it at the bottom)
                let pb = ProgressBar::new(state.estimated_rows.max(1));
                let bar = if let Some(ref overflow) = inner.overflow_bar {
                    multi.insert_before(overflow, pb)
                } else {
                    multi.add(pb)
                };
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
                inner.visible_count += 1;
                return;
            }
        }
    }
}

/// Update or create/remove the overflow indicator bar
fn update_overflow_bar(multi: &MultiProgress, inner: &mut Inner, is_tty: bool) {
    if !is_tty {
        return;
    }

    let hidden_count = inner.table_states.len().saturating_sub(inner.visible_count);

    if hidden_count > 0 {
        // Need overflow bar
        if inner.overflow_bar.is_none() {
            let bar = multi.add(ProgressBar::new_spinner());
            bar.set_style(
                ProgressStyle::default_spinner()
                    .template("   {spinner:.dim} {msg}")
                    .expect("valid template"),
            );
            bar.enable_steady_tick(TICK_INTERVAL);
            inner.overflow_bar = Some(bar);
        }

        if let Some(ref bar) = inner.overflow_bar {
            bar.set_message(format!(
                "+ {} more table{} in progress...",
                hidden_count,
                if hidden_count == 1 { "" } else { "s" }
            ));
        }
    } else {
        // Remove overflow bar
        if let Some(bar) = inner.overflow_bar.take() {
            bar.finish_and_clear();
        }
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
