//! The `.jibsdump` on-disk format: a versioned, self-describing, compressed
//! snapshot of an import stream. Produced by `jibs import --dump-to <file>`
//! and consumed by `jibs load <file>`.
//!
//! Layout:
//!
//! ```text
//! [8 bytes magic "JIBSDUMP"][2 bytes format_version u16 LE]
//! <length-prefixed bincode DumpRecord> ...            (Table, Chunk, TableEnd)
//! <length-prefixed bincode DumpRecord::End>           (terminator)
//! ```
//!
//! Every data chunk is stored zstd-compressed: [`DumpWriter::write_chunk`]
//! compresses any chunk that isn't already compressed, so a dump on disk is
//! always compressed regardless of the wire compression that produced it.
//! Stored chunks use the same `[u32 LE original_len][zstd frame]` envelope as
//! the streaming protocol, so [`crate::loader`] can decompress them unchanged.

use std::io::{Read, Write};

use anyhow::{bail, Context, Result};
use bincode::{Decode, Encode};

use jibs_protocol::framing::{bincode_config, write_message_noflush};
use jibs_protocol::{AnonymizeRule, ColumnDef, CompressionMode, PreserveRule, SetRule};

/// Magic bytes at the start of every `.jibsdump` file.
pub(crate) const MAGIC: &[u8; 8] = b"JIBSDUMP";

/// Current on-disk format version. Bump this on any incompatible change to the
/// preamble or [`DumpRecord`]; the loader refuses versions it doesn't know.
pub(crate) const FORMAT_VERSION: u16 = 1;

/// Length of the fixed preamble: 8-byte magic + 2-byte version.
const PREAMBLE_LEN: usize = 10;

/// zstd level used when the writer has to compress an otherwise-uncompressed
/// chunk. Matches the server's streaming compression level.
const ZSTD_LEVEL: i32 = 3;

/// Default per-record read cap (100 MiB), matching the client's default
/// `--max-message-size`. Overridable so dumps written with a raised wire cap
/// stay loadable.
pub(crate) const DEFAULT_MAX_RECORD_SIZE: usize = 100 * 1024 * 1024;

/// One record in a `.jibsdump` stream.
#[derive(Debug, Clone, Encode, Decode)]
pub(crate) enum DumpRecord {
    /// Stream-level metadata, emitted once right after the preamble (before any
    /// `Table`). Carries plan-level rules the loader must honor up front —
    /// currently `preserve` rules, which must back up local rows *before* a
    /// table is dropped and recreated.
    Manifest { preserves: Vec<PreserveRule> },
    /// A table's schema. Emitted once per table, before any of its chunks.
    Table {
        name: String,
        columns: Vec<ColumnDef>,
        /// Anonymization rules that shaped the DDL (e.g. columns forced
        /// nullable). The data is already anonymized server-side; these are
        /// carried only so `load` recreates the exact same table definition.
        anon_rules: Option<Vec<AnonymizeRule>>,
    },
    /// One chunk of TSV rows for a table. `data` is stored per `compression`
    /// (always `Zstd` as written by [`DumpWriter`]).
    Chunk {
        table: String,
        compression: CompressionMode,
        row_count: u16,
        data: Vec<u8>,
    },
    /// Marks a table fully written, with its final row count.
    TableEnd { table: String, row_count: u64 },
    /// Plan-level post-processing to replay after all data is loaded, so a
    /// `load` reproduces exactly what a live import would have produced. Written
    /// once, just before `End`, and only when there is something to run.
    PostProcess {
        /// `set { ... }` upsert blocks from the config.
        sets: Vec<SetRule>,
        /// Raw `after { ... }` SQL statements from the config.
        after_statements: Vec<String>,
    },
    /// Terminator. A complete dump ends with exactly one of these.
    End,
}

/// Writes a `.jibsdump` stream to any [`Write`] sink.
pub(crate) struct DumpWriter<W: Write> {
    inner: W,
}

impl<W: Write> DumpWriter<W> {
    /// Create a writer and emit the preamble (magic + version).
    pub(crate) fn new(mut inner: W) -> Result<Self> {
        inner.write_all(MAGIC)?;
        inner.write_all(&FORMAT_VERSION.to_le_bytes())?;
        Ok(Self { inner })
    }

    /// Write the stream manifest. Call once, right after `new`, before any
    /// table. Skips writing when there is nothing to carry.
    pub(crate) fn write_manifest(&mut self, preserves: &[PreserveRule]) -> Result<()> {
        if preserves.is_empty() {
            return Ok(());
        }
        let rec = DumpRecord::Manifest {
            preserves: preserves.to_vec(),
        };
        write_message_noflush(&mut self.inner, &rec)?;
        Ok(())
    }

    /// Write a table's schema record.
    pub(crate) fn write_table(
        &mut self,
        name: &str,
        columns: &[ColumnDef],
        anon_rules: Option<&[AnonymizeRule]>,
    ) -> Result<()> {
        let rec = DumpRecord::Table {
            name: name.to_string(),
            columns: columns.to_vec(),
            anon_rules: anon_rules.map(<[AnonymizeRule]>::to_vec),
        };
        write_message_noflush(&mut self.inner, &rec)?;
        Ok(())
    }

    /// Write a data chunk, ensuring it is stored zstd-compressed.
    ///
    /// `incoming` is how `data` is currently compressed (as negotiated on the
    /// wire). If it isn't already `Zstd`, the chunk is compressed here so the
    /// dump on disk is always compressed.
    pub(crate) fn write_chunk(
        &mut self,
        table: &str,
        incoming: CompressionMode,
        row_count: u16,
        data: Vec<u8>,
    ) -> Result<()> {
        let data = match incoming {
            CompressionMode::Zstd => data,
            CompressionMode::None | CompressionMode::Auto => compress_zstd(&data)?,
        };
        let rec = DumpRecord::Chunk {
            table: table.to_string(),
            compression: CompressionMode::Zstd,
            row_count,
            data,
        };
        write_message_noflush(&mut self.inner, &rec)?;
        Ok(())
    }

    /// Write a table-complete record.
    pub(crate) fn write_table_end(&mut self, table: &str, row_count: u64) -> Result<()> {
        let rec = DumpRecord::TableEnd {
            table: table.to_string(),
            row_count,
        };
        write_message_noflush(&mut self.inner, &rec)?;
        Ok(())
    }

    /// Write the plan-level post-processing record (upsert `set` blocks and
    /// `after` statements) to replay on load. Skips writing when both are empty.
    pub(crate) fn write_post_process(
        &mut self,
        sets: &[SetRule],
        after_statements: &[String],
    ) -> Result<()> {
        if sets.is_empty() && after_statements.is_empty() {
            return Ok(());
        }
        let rec = DumpRecord::PostProcess {
            sets: sets.to_vec(),
            after_statements: after_statements.to_vec(),
        };
        write_message_noflush(&mut self.inner, &rec)?;
        Ok(())
    }

    /// Emit the terminator and flush the underlying writer.
    pub(crate) fn finish(mut self) -> Result<()> {
        write_message_noflush(&mut self.inner, &DumpRecord::End)?;
        self.inner.flush()?;
        Ok(())
    }
}

/// Reads a `.jibsdump` stream from any [`Read`] source.
pub(crate) struct DumpReader<R: Read> {
    inner: R,
    max_record_size: usize,
}

impl<R: Read> DumpReader<R> {
    /// Create a reader, validating the preamble (magic + version). Records
    /// larger than `max_record_size` bytes are rejected.
    pub(crate) fn with_max_record_size(mut inner: R, max_record_size: usize) -> Result<Self> {
        let mut preamble = [0u8; PREAMBLE_LEN];
        inner
            .read_exact(&mut preamble)
            .context("failed to read dump header (file too short or not a .jibsdump)")?;
        if &preamble[..8] != MAGIC {
            bail!("not a jibs dump file (bad magic bytes)");
        }
        let version = u16::from_le_bytes([preamble[8], preamble[9]]);
        if version != FORMAT_VERSION {
            bail!(
                "unsupported .jibsdump format version {} (this build understands v{}); \
                 re-create the dump with a matching jibs version",
                version,
                FORMAT_VERSION
            );
        }
        Ok(Self {
            inner,
            max_record_size,
        })
    }

    /// Read the next record. Returns `Ok(None)` only at a clean record
    /// boundary (EOF with no bytes pending); a stream that ends part-way
    /// through a record's length prefix or body is a truncation error, not a
    /// clean end.
    pub(crate) fn next_record(&mut self) -> Result<Option<DumpRecord>> {
        // Length prefix — a clean EOF here (0 bytes) means end of stream.
        let mut len_bytes = [0u8; 4];
        match read_full(&mut self.inner, &mut len_bytes)? {
            ReadOutcome::Eof => return Ok(None),
            ReadOutcome::Partial => bail!("truncated dump: incomplete record length prefix"),
            ReadOutcome::Full => {}
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        if len > self.max_record_size {
            bail!(
                "dump record too large: {} bytes (max {} bytes, ~{} MiB); \
                 re-run `jibs load` with a larger --max-message-size",
                len,
                self.max_record_size,
                self.max_record_size / (1024 * 1024)
            );
        }

        let mut buf = vec![0u8; len];
        match read_full(&mut self.inner, &mut buf)? {
            ReadOutcome::Full => {}
            _ => bail!("truncated dump: record body ended early (expected {} bytes)", len),
        }

        let (rec, _) = bincode::decode_from_slice(&buf, bincode_config())
            .map_err(|e| anyhow::anyhow!("failed to decode dump record: {}", e))?;
        Ok(Some(rec))
    }
}

/// Outcome of trying to fill a buffer from a reader.
enum ReadOutcome {
    /// The whole buffer was filled.
    Full,
    /// No bytes were read — a clean EOF at a record boundary.
    Eof,
    /// Some but not all bytes were read before EOF — truncation.
    Partial,
}

/// Fill `buf` completely, distinguishing a clean boundary EOF (zero bytes)
/// from a mid-buffer truncation.
fn read_full<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<ReadOutcome> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..])?;
        if n == 0 {
            return Ok(if filled == 0 {
                ReadOutcome::Eof
            } else {
                ReadOutcome::Partial
            });
        }
        filled += n;
    }
    Ok(ReadOutcome::Full)
}

/// Compress `data` into the `[u32 LE original_len][zstd frame]` envelope that
/// [`crate::loader::maybe_decompress`] expects for `CompressionMode::Zstd`.
fn compress_zstd(data: &[u8]) -> Result<Vec<u8>> {
    // The envelope stores the original length as a u32; guard the cast so an
    // oversized chunk fails loudly instead of silently truncating the length.
    let original_len: u32 = data
        .len()
        .try_into()
        .map_err(|_| anyhow::anyhow!("chunk too large to compress: {} bytes (max {})", data.len(), u32::MAX))?;
    let compressed =
        zstd::encode_all(data, ZSTD_LEVEL).map_err(|e| anyhow::anyhow!("zstd compression failed: {}", e))?;
    let mut out = Vec::with_capacity(4 + compressed.len());
    out.extend_from_slice(&original_len.to_le_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    use crate::loader::maybe_decompress;

    fn open(buf: Vec<u8>) -> Result<DumpReader<Cursor<Vec<u8>>>> {
        DumpReader::with_max_record_size(Cursor::new(buf), DEFAULT_MAX_RECORD_SIZE)
    }

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            type_name: "INT".to_string(),
            full_type: "int".to_string(),
            max_length: None,
            nullable: true,
            is_primary_key: false,
            charset: None,
            collation: None,
            flags: Default::default(),
        }
    }

    #[test]
    fn roundtrip_records() {
        let mut buf = Vec::new();
        {
            let mut w = DumpWriter::new(&mut buf).unwrap();
            w.write_table("users", &[col("id"), col("name")], None).unwrap();
            // Feed an uncompressed chunk; the writer must store it compressed.
            w.write_chunk("users", CompressionMode::None, 2, b"1\tAlice\n2\tBob\n".to_vec())
                .unwrap();
            w.write_table_end("users", 2).unwrap();
            w.finish().unwrap();
        }

        // File must start with the magic + version preamble.
        assert_eq!(&buf[..8], MAGIC);
        assert_eq!(u16::from_le_bytes([buf[8], buf[9]]), FORMAT_VERSION);

        let mut r = open(buf).unwrap();

        match r.next_record().unwrap().unwrap() {
            DumpRecord::Table { name, columns, .. } => {
                assert_eq!(name, "users");
                assert_eq!(columns.len(), 2);
            }
            other => panic!("expected Table, got {other:?}"),
        }

        match r.next_record().unwrap().unwrap() {
            DumpRecord::Chunk { table, compression, row_count, data } => {
                assert_eq!(table, "users");
                // Stored compressed regardless of the uncompressed input.
                assert!(matches!(compression, CompressionMode::Zstd));
                assert_eq!(row_count, 2);
                let tsv = maybe_decompress(data, compression).unwrap();
                assert_eq!(tsv, b"1\tAlice\n2\tBob\n");
            }
            other => panic!("expected Chunk, got {other:?}"),
        }

        assert!(matches!(
            r.next_record().unwrap().unwrap(),
            DumpRecord::TableEnd { row_count: 2, .. }
        ));
        assert!(matches!(r.next_record().unwrap().unwrap(), DumpRecord::End));
        assert!(r.next_record().unwrap().is_none());
    }

    fn expect_err<T>(result: Result<T>) -> anyhow::Error {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        }
    }

    #[test]
    fn rejects_bad_magic() {
        let bytes = vec![0u8; PREAMBLE_LEN];
        let err = expect_err(open(bytes));
        assert!(err.to_string().contains("magic"));
    }

    #[test]
    fn rejects_unknown_version() {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        let err = expect_err(open(bytes));
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn roundtrip_manifest_and_post_process() {
        let preserves = vec![PreserveRule {
            table: "users".to_string(),
            where_clause: "is_admin = 1".to_string(),
        }];
        let sets = vec![SetRule {
            table: "config".to_string(),
            match_clause: vec![],
            assignments: vec![],
        }];
        let after = vec!["UPDATE users SET x = 1".to_string()];

        let mut buf = Vec::new();
        {
            let mut w = DumpWriter::new(&mut buf).unwrap();
            w.write_manifest(&preserves).unwrap();
            w.write_table("users", &[col("id")], None).unwrap();
            w.write_table_end("users", 0).unwrap();
            w.write_post_process(&sets, &after).unwrap();
            w.finish().unwrap();
        }

        let mut r = open(buf).unwrap();
        match r.next_record().unwrap().unwrap() {
            DumpRecord::Manifest { preserves } => {
                assert_eq!(preserves.len(), 1);
                assert_eq!(preserves[0].table, "users");
            }
            other => panic!("expected Manifest first, got {other:?}"),
        }
        assert!(matches!(r.next_record().unwrap().unwrap(), DumpRecord::Table { .. }));
        assert!(matches!(r.next_record().unwrap().unwrap(), DumpRecord::TableEnd { .. }));
        match r.next_record().unwrap().unwrap() {
            DumpRecord::PostProcess { sets, after_statements } => {
                assert_eq!(sets.len(), 1);
                assert_eq!(after_statements, vec!["UPDATE users SET x = 1".to_string()]);
            }
            other => panic!("expected PostProcess, got {other:?}"),
        }
        assert!(matches!(r.next_record().unwrap().unwrap(), DumpRecord::End));
    }

    /// A stream truncated mid-record must be an error, not a clean end.
    #[test]
    fn truncation_is_an_error_not_clean_eof() {
        let mut buf = Vec::new();
        {
            let mut w = DumpWriter::new(&mut buf).unwrap();
            w.write_table("users", &[col("id"), col("name")], None).unwrap();
            w.write_chunk("users", CompressionMode::None, 2, b"1\tAlice\n".to_vec())
                .unwrap();
            w.finish().unwrap();
        }

        // Cut inside a record body (past the End record and into the chunk),
        // so the stream ends mid-record rather than at a clean boundary.
        let truncated = buf[..buf.len() - 8].to_vec();

        let mut r = open(truncated).unwrap();
        // Read until we either hit an error (correct) or a clean EOF (wrong).
        let mut err = None;
        loop {
            match r.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        let err = err.expect("truncated dump must surface an error, not a clean EOF");
        assert!(err.to_string().contains("truncated"), "got: {err}");
    }

    #[test]
    fn rejects_oversized_record() {
        let mut buf = Vec::new();
        {
            let mut w = DumpWriter::new(&mut buf).unwrap();
            w.write_table("users", &[col("id")], None).unwrap();
            w.finish().unwrap();
        }
        // A cap smaller than the Table record forces a rejection.
        let mut r = DumpReader::with_max_record_size(Cursor::new(buf), 4).unwrap();
        let err = expect_err(r.next_record());
        assert!(err.to_string().contains("too large"), "got: {err}");
    }
}
