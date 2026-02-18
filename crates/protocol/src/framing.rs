//! Wire format framing for protocol messages
//!
//! Control messages use bincode with length-prefix framing:
//! `u32 length || bincode payload`

use std::io::{self, Read, Write};

use bincode::{config, Decode, Encode};

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
}
