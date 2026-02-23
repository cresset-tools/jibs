//! Wire format framing for protocol messages
//!
//! Two frame types distinguished by the high bit of the u32 length prefix:
//!
//! - **Bincode message** (high bit clear): `u32 length || bincode payload`
//! - **Raw data chunk** (high bit set): `u32 length|0x80000000 || u16 table_id || u32 row_count || tsv_data`
//!
//! The high bit halves the maximum frame size to ~2 GiB, which is never reached in practice.

use std::io::{self, Read, Write};

use bincode::{config, Decode, Encode};

/// High bit flag on the u32 length prefix that signals a raw data chunk
/// instead of a bincode-encoded message.
pub const RAW_CHUNK_FLAG: u32 = 0x8000_0000;

/// Bincode configuration for protocol messages
pub fn bincode_config() -> impl bincode::config::Config {
    config::standard()
        .with_little_endian()
        .with_variable_int_encoding()
}

/// Write a message to a writer with length-prefix framing
pub fn write_message<W: Write, T: Encode>(writer: &mut W, message: &T) -> io::Result<()> {
    let encoded = bincode::encode_to_vec(message, bincode_config())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let len = encoded.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&encoded)?;
    writer.flush()?;
    Ok(())
}

/// Write a message without flushing — lets the BufWriter coalesce writes.
/// Use this in hot streaming paths where flush would cause unnecessary syscalls.
pub fn write_message_noflush<W: Write, T: Encode>(writer: &mut W, message: &T) -> io::Result<()> {
    let encoded = bincode::encode_to_vec(message, bincode_config())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let len = encoded.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&encoded)?;
    Ok(())
}

/// Read a message from a reader with length-prefix framing
pub fn read_message<R: Read, T: Decode<()>>(reader: &mut R) -> io::Result<T> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes)?;
    let len = u32::from_le_bytes(len_bytes) as usize;

    // Sanity check: messages shouldn't exceed 100MB
    if len > 100 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Message too large: {} bytes", len),
        ));
    }

    let mut buffer = vec![0u8; len];
    reader.read_exact(&mut buffer)?;

    let (message, _) = bincode::decode_from_slice(&buffer, bincode_config())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(message)
}

/// Data message types for streaming
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DataMessageType {
    /// Schema definition for a table
    Schema = 1,
    /// Data chunk (TSV bytes)
    DataChunk = 2,
    /// Table transfer complete
    TableEnd = 3,
    /// Aggregate transfer complete
    AggregateEnd = 4,
    /// Error message
    Error = 5,
}

impl TryFrom<u8> for DataMessageType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, io::Error> {
        match value {
            1 => Ok(Self::Schema),
            2 => Ok(Self::DataChunk),
            3 => Ok(Self::TableEnd),
            4 => Ok(Self::AggregateEnd),
            5 => Ok(Self::Error),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown message type: {}", value),
            )),
        }
    }
}

/// Write a data stream message
///
/// Format: `u32 length || u8 message_type || payload`
pub fn write_data_message<W: Write>(
    writer: &mut W,
    msg_type: DataMessageType,
    payload: &[u8],
) -> io::Result<()> {
    let len = (1 + payload.len()) as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&[msg_type as u8])?;
    writer.write_all(payload)?;
    writer.flush()?;
    Ok(())
}

/// Read a data stream message header
///
/// Returns (message_type, payload_length)
pub fn read_data_message_header<R: Read>(reader: &mut R) -> io::Result<(DataMessageType, usize)> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes)?;
    let len = u32::from_le_bytes(len_bytes) as usize;

    if len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Empty data message",
        ));
    }

    // Sanity check: data chunks shouldn't exceed 100MB
    if len > 100 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Data message too large: {} bytes", len),
        ));
    }

    let mut msg_type_byte = [0u8; 1];
    reader.read_exact(&mut msg_type_byte)?;
    let msg_type = DataMessageType::try_from(msg_type_byte[0])?;

    Ok((msg_type, len - 1))
}

/// A buffered writer optimized for length-prefixed message framing.
///
/// Encodes bincode messages directly into a reusable internal buffer via a
/// temporary [`BufEncoder`] that implements `io::Write`. The buffer may grow for
/// large messages but never shrinks, so repeated messages of similar size
/// allocate nothing.
pub struct MessageWriter<W> {
    inner: W,
    buf: Vec<u8>,
    capacity: usize,
}

impl<W> MessageWriter<W> {
    pub fn with_capacity(capacity: usize, inner: W) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Encode a length-prefixed bincode message into the internal buffer,
    /// returning the encoded bytes. Clears the buffer first.
    ///
    /// Use this for async callers that need to send the bytes themselves.
    pub fn encode_message<T: Encode>(&mut self, message: &T) -> io::Result<&[u8]> {
        self.buf.clear();
        self.encode_into_buf(message)?;
        Ok(&self.buf)
    }

    fn encode_into_buf<T: Encode>(&mut self, message: &T) -> io::Result<()> {
        let len_pos = self.buf.len();
        self.buf.extend_from_slice(&[0u8; 4]);

        let mut encoder = BufEncoder { buf: &mut self.buf };
        bincode::encode_into_std_write(message, &mut encoder, bincode_config())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let msg_len = (self.buf.len() - len_pos - 4) as u32;
        self.buf[len_pos..len_pos + 4].copy_from_slice(&msg_len.to_le_bytes());

        Ok(())
    }
}

impl<W: Write> MessageWriter<W> {
    /// Write a length-prefixed bincode message, then flush.
    pub fn write_message<T: Encode>(&mut self, message: &T) -> io::Result<()> {
        self.write_message_noflush(message)?;
        self.flush()
    }

    /// Write a length-prefixed bincode message without flushing.
    ///
    /// Use this in hot paths where you want the buffer to coalesce multiple
    /// messages before a single flush. Still flushes to the inner writer if
    /// the buffer exceeds capacity, to bound memory usage.
    pub fn write_message_noflush<T: Encode>(&mut self, message: &T) -> io::Result<()> {
        self.encode_into_buf(message)?;

        if self.buf.len() >= self.capacity {
            self.flush_buf()?;
        }

        Ok(())
    }

    /// Flush the internal buffer to the inner writer.
    pub fn flush(&mut self) -> io::Result<()> {
        self.flush_buf()?;
        self.inner.flush()
    }

    /// Write pre-encoded frame bytes through the internal buffer.
    ///
    /// Uses a BufWriter-style strategy: small writes are appended to the
    /// buffer; writes larger than the buffer capacity flush first then go
    /// directly to the inner writer (avoiding a pointless memcpy that would
    /// be immediately flushed anyway).
    pub fn write_preencoded(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.buf.len() + bytes.len() <= self.capacity {
            // Fits in remaining buffer space
            self.buf.extend_from_slice(bytes);
        } else {
            // Flush current buffer contents first
            self.flush_buf()?;
            if bytes.len() <= self.capacity {
                // Fits in the now-empty buffer
                self.buf.extend_from_slice(bytes);
            } else {
                // Larger than the full buffer — write through
                self.inner.write_all(bytes)?;
            }
        }
        Ok(())
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.inner.write_all(&self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }
}

/// Encode a raw data chunk into a complete frame ready for wire transmission.
///
/// Builds: `u32 length|RAW_CHUNK_FLAG || u16 table_id || u32 row_count || tsv_data`
///
/// The returned `Vec<u8>` can be written directly via [`MessageWriter::write_preencoded`].
pub fn encode_data_chunk(table_id: u16, row_count: u32, tsv_data: Vec<u8>) -> Vec<u8> {
    let payload_len = 2 + 4 + tsv_data.len();
    let flagged_len = (payload_len as u32) | RAW_CHUNK_FLAG;

    let mut frame = Vec::with_capacity(4 + payload_len);
    frame.extend_from_slice(&flagged_len.to_le_bytes());
    frame.extend_from_slice(&table_id.to_le_bytes());
    frame.extend_from_slice(&row_count.to_le_bytes());
    frame.extend_from_slice(&tsv_data);
    frame
}

/// Decode a raw data chunk buffer into its components: (table_id, row_count, tsv_data).
///
/// Takes ownership of the buffer. The 6-byte header is parsed and the remaining
/// bytes become the `tsv_data` Vec (reusing the original allocation via `drain`).
pub fn decode_data_chunk(mut buf: Vec<u8>) -> io::Result<(u16, u32, Vec<u8>)> {
    if buf.len() < 6 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Data chunk too short",
        ));
    }

    let table_id = u16::from_le_bytes([buf[0], buf[1]]);
    let row_count = u32::from_le_bytes(
        buf[2..6]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad row_count"))?,
    );

    // Remove header, leaving just tsv_data — reuses the allocation
    buf.drain(..6);
    Ok((table_id, row_count, buf))
}

/// Temporary `Write` adapter that appends into a borrowed `Vec<u8>`.
///
/// Created by [`MessageWriter`] during message encoding so that
/// `bincode::encode_into_std_write` can write directly into the buffer.
struct BufEncoder<'a> {
    buf: &'a mut Vec<u8>,
}

impl Write for BufEncoder<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[derive(Debug, Clone, PartialEq, Encode, Decode)]
    struct TestMessage {
        value: String,
        count: u32,
    }

    #[test]
    fn test_message_roundtrip() {
        let msg = TestMessage {
            value: "hello".to_string(),
            count: 42,
        };

        let mut buffer = Vec::new();
        write_message(&mut buffer, &msg).unwrap();

        let mut cursor = Cursor::new(buffer);
        let decoded: TestMessage = read_message(&mut cursor).unwrap();

        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_data_message_roundtrip() {
        let payload = b"col1\tcol2\nval1\tval2\n";

        let mut buffer = Vec::new();
        write_data_message(&mut buffer, DataMessageType::DataChunk, payload).unwrap();

        let mut cursor = Cursor::new(buffer);
        let (msg_type, len) = read_data_message_header(&mut cursor).unwrap();

        assert_eq!(msg_type, DataMessageType::DataChunk);
        assert_eq!(len, payload.len());

        let mut read_payload = vec![0u8; len];
        cursor.read_exact(&mut read_payload).unwrap();
        assert_eq!(read_payload, payload);
    }

    #[test]
    fn test_message_writer_roundtrip() {
        let msg = TestMessage {
            value: "hello".to_string(),
            count: 42,
        };

        let mut output = Vec::new();
        let mut writer = MessageWriter::with_capacity(4096, &mut output);
        writer.write_message(&msg).unwrap();

        let mut cursor = Cursor::new(output);
        let decoded: TestMessage = read_message(&mut cursor).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_message_writer_noflush_batches() {
        let msg1 = TestMessage { value: "first".to_string(), count: 1 };
        let msg2 = TestMessage { value: "second".to_string(), count: 2 };

        let mut output = Vec::new();
        {
            let mut writer = MessageWriter::with_capacity(4096, &mut output);
            writer.write_message_noflush(&msg1).unwrap();
            writer.write_message_noflush(&msg2).unwrap();

            // Not flushed yet — data is in the internal buffer
            assert!(!writer.buf.is_empty());

            writer.flush().unwrap();
        }

        // Now read both messages
        let mut cursor = Cursor::new(output);
        let decoded1: TestMessage = read_message(&mut cursor).unwrap();
        let decoded2: TestMessage = read_message(&mut cursor).unwrap();
        assert_eq!(msg1, decoded1);
        assert_eq!(msg2, decoded2);
    }

    #[test]
    fn test_encode_decode_data_chunk_roundtrip() {
        let table_id = 7u16;
        let row_count = 42u32;
        let tsv_data = b"id\tname\n1\tAlice\n2\tBob\n";

        let frame = encode_data_chunk(table_id, row_count, tsv_data.to_vec());

        // Verify the frame has RAW_CHUNK_FLAG set
        let raw_len = u32::from_le_bytes(frame[0..4].try_into().unwrap());
        assert!(raw_len & RAW_CHUNK_FLAG != 0);
        let len = (raw_len & !RAW_CHUNK_FLAG) as usize;
        assert_eq!(len, frame.len() - 4);

        // Decode the payload
        let buf = frame[4..].to_vec();
        let (dec_table_id, dec_row_count, dec_tsv) = decode_data_chunk(buf).unwrap();
        assert_eq!(dec_table_id, table_id);
        assert_eq!(dec_row_count, row_count);
        assert_eq!(dec_tsv, tsv_data);
    }

    #[test]
    fn test_write_preencoded_small_buffered() {
        // Small pre-encoded data stays in the buffer until flushed
        let frame = encode_data_chunk(0, 1, b"hello\n".to_vec());

        let mut output = Vec::new();
        {
            let mut writer = MessageWriter::with_capacity(4096, &mut output);
            writer.write_preencoded(&frame).unwrap();
            // Data is in the internal buffer, not yet flushed
            assert!(!writer.buf.is_empty());
            writer.flush().unwrap();
        }
        assert_eq!(output, frame);
    }

    #[test]
    fn test_write_preencoded_large_write_through() {
        // Large pre-encoded data goes directly to the writer
        let big_data = vec![0x42u8; 2048];
        let frame = encode_data_chunk(42, 99, big_data);

        let mut output = Vec::new();
        {
            let mut writer = MessageWriter::with_capacity(256, &mut output);
            writer.write_preencoded(&frame).unwrap();
            // Buffer should be empty — data was written through
            assert!(writer.buf.is_empty());
        }
        assert_eq!(output, frame);
    }

    #[test]
    fn test_write_preencoded_interleaved_with_bincode() {
        let msg = TestMessage { value: "control".to_string(), count: 7 };
        let tsv = b"col1\tcol2\nval1\tval2\n";
        let chunk_frame = encode_data_chunk(5, 2, tsv.to_vec());

        let mut output = Vec::new();
        {
            let mut writer = MessageWriter::with_capacity(4096, &mut output);
            writer.write_message_noflush(&msg).unwrap();
            writer.write_preencoded(&chunk_frame).unwrap();
            writer.flush().unwrap();
        }

        let mut cursor = Cursor::new(&output);

        // First frame: bincode message (high bit clear)
        let mut len_bytes = [0u8; 4];
        cursor.read_exact(&mut len_bytes).unwrap();
        let raw_len = u32::from_le_bytes(len_bytes);
        assert!(raw_len & RAW_CHUNK_FLAG == 0);
        let mut buf = vec![0u8; raw_len as usize];
        cursor.read_exact(&mut buf).unwrap();
        let (decoded, _): (TestMessage, _) =
            bincode::decode_from_slice(&buf, bincode_config()).unwrap();
        assert_eq!(decoded, msg);

        // Second frame: raw data chunk (high bit set)
        cursor.read_exact(&mut len_bytes).unwrap();
        let raw_len = u32::from_le_bytes(len_bytes);
        assert!(raw_len & RAW_CHUNK_FLAG != 0);
        let len = (raw_len & !RAW_CHUNK_FLAG) as usize;
        let mut buf = vec![0u8; len];
        cursor.read_exact(&mut buf).unwrap();
        let (table_id, row_count, data) = decode_data_chunk(buf).unwrap();
        assert_eq!(table_id, 5);
        assert_eq!(row_count, 2);
        assert_eq!(data, tsv);
    }

    #[test]
    fn test_message_writer_compatible_with_free_fn() {
        // Verify MessageWriter produces identical bytes as the free function
        let msg = TestMessage {
            value: "compatibility".to_string(),
            count: 99,
        };

        let mut free_fn_output = Vec::new();
        write_message(&mut free_fn_output, &msg).unwrap();

        let mut writer_output = Vec::new();
        let mut writer = MessageWriter::with_capacity(4096, &mut writer_output);
        writer.write_message(&msg).unwrap();

        assert_eq!(free_fn_output, writer_output);
    }
}
