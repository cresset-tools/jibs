//! Post-import report formatting

use std::time::Duration;

pub(crate) fn display_report(table_durations: &[(String, u64, Duration)]) {
    if table_durations.is_empty() {
        return;
    }

    let mut sorted: Vec<_> = table_durations.to_vec();
    sorted.sort_by(|a, b| b.2.cmp(&a.2));

    // Find the longest table name for column width
    let max_name_len = sorted.iter().map(|(n, _, _)| n.len()).max().unwrap_or(20);
    let name_width = max_name_len.max(5); // minimum "Table" header width

    eprintln!();
    eprintln!("=== Import Report ===");
    eprintln!();
    eprintln!(
        "  {:<4} {:<width$}  {:>10}  {:>10}  {:>10}",
        "#",
        "Table",
        "Rows",
        "Duration",
        "Rows/s",
        width = name_width
    );
    eprintln!(
        "  {:-<4} {:-<width$}  {:-<10}  {:-<10}  {:-<10}",
        "",
        "",
        "",
        "",
        "",
        width = name_width
    );

    for (i, (name, rows, duration)) in sorted.iter().enumerate() {
        let secs = duration.as_secs_f64();
        let rows_per_sec = if secs > 0.0 {
            (*rows as f64 / secs) as u64
        } else {
            0
        };

        let duration_str = if secs >= 60.0 {
            format!("{:.0}m {:.1}s", (secs / 60.0).floor(), secs % 60.0)
        } else {
            format!("{:.1}s", secs)
        };

        eprintln!(
            "  {:<4} {:<width$}  {:>10}  {:>10}  {:>10}",
            i + 1,
            name,
            format_number(*rows),
            duration_str,
            format_number(rows_per_sec),
            width = name_width
        );
    }

    eprintln!();
}

/// Format a number with thousand separators
pub(crate) fn format_number(n: u64) -> String {
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

