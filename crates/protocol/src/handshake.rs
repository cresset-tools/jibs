//! Protocol version handshake
//!
//! Before any framed message is exchanged, both sides send an 8-byte
//! preamble: 4 magic bytes (`JIBS`) followed by a little-endian u32 protocol
//! version. The server writes its greeting to stdout immediately on startup;
//! the client writes its preamble before the Credentials message. Each side
//! validates the peer's preamble and aborts with a clear error on mismatch.
//!
//! The preamble deliberately lives OUTSIDE the bincode message framing: a
//! future change to the message schema can never break the ability to
//! *detect* a version mismatch. Without this, a client/server mismatch
//! surfaced as bincode decode garbage.
//!
//! Bump [`PROTOCOL_VERSION`] whenever anything about the wire format
//! changes: message enums (bincode encodes variants by index, so even
//! reordering breaks the wire), plan/metrics struct fields, the framing
//! layer, or the TSV encoding rules. The wire-format snapshot test in this
//! crate exists to make accidental changes loud.

use std::io::{Read, Write};

/// Magic bytes identifying a jibs protocol peer
pub const PROTOCOL_MAGIC: [u8; 4] = *b"JIBS";

/// Current protocol version. Client and server must match exactly.
///
/// History:
/// - v1: initial versioned protocol
/// - v2: Init gains dry_run; ServerMessage gains DryRunReport
pub const PROTOCOL_VERSION: u32 = 2;

/// Total preamble length in bytes (magic + version)
pub const PREAMBLE_LEN: usize = 8;

/// Encode this side's preamble
pub fn encode_preamble() -> [u8; PREAMBLE_LEN] {
    let mut buf = [0u8; PREAMBLE_LEN];
    buf[..4].copy_from_slice(&PROTOCOL_MAGIC);
    buf[4..].copy_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    buf
}

/// Outcome of validating a peer preamble
#[derive(Debug, PartialEq, Eq)]
pub enum HandshakeError {
    /// The peer did not send jibs magic bytes — it is not a jibs peer, or it
    /// predates the handshake and sent a framed message instead
    BadMagic([u8; 4]),
    /// The peer speaks the jibs protocol but at a different version
    VersionMismatch { peer: u32 },
}

impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandshakeError::BadMagic(bytes) => write!(
                f,
                "invalid protocol preamble (got bytes {:02x?}, expected magic \"JIBS\") — \
                 the peer is not a jibs protocol peer or predates protocol versioning",
                bytes
            ),
            HandshakeError::VersionMismatch { peer } => write!(
                f,
                "protocol version mismatch: this side speaks v{}, the peer speaks v{} — \
                 rebuild the client and server from the same source (./scripts/build.sh)",
                PROTOCOL_VERSION, peer
            ),
        }
    }
}

impl std::error::Error for HandshakeError {}

/// Validate a peer preamble against our magic and version
pub fn validate_preamble(buf: &[u8; PREAMBLE_LEN]) -> Result<(), HandshakeError> {
    let magic: [u8; 4] = buf[..4].try_into().unwrap();
    if magic != PROTOCOL_MAGIC {
        return Err(HandshakeError::BadMagic(magic));
    }
    let peer = u32::from_le_bytes(buf[4..].try_into().unwrap());
    if peer != PROTOCOL_VERSION {
        return Err(HandshakeError::VersionMismatch { peer });
    }
    Ok(())
}

/// Write this side's preamble (sync, for the server)
pub fn write_preamble<W: Write>(writer: &mut W) -> std::io::Result<()> {
    writer.write_all(&encode_preamble())?;
    writer.flush()
}

/// Read a peer preamble (sync, for the server)
pub fn read_preamble<R: Read>(reader: &mut R) -> std::io::Result<[u8; PREAMBLE_LEN]> {
    let mut buf = [0u8; PREAMBLE_LEN];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preamble_roundtrip() {
        let encoded = encode_preamble();
        assert_eq!(&encoded[..4], b"JIBS");
        assert!(validate_preamble(&encoded).is_ok());
    }

    #[test]
    fn bad_magic_is_rejected() {
        // An old client's first bytes are a bincode frame length, not magic
        let buf = [0x2a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00];
        assert!(matches!(
            validate_preamble(&buf),
            Err(HandshakeError::BadMagic(_))
        ));
    }

    #[test]
    fn version_mismatch_is_rejected_with_peer_version() {
        let mut buf = encode_preamble();
        buf[4..].copy_from_slice(&(PROTOCOL_VERSION + 1).to_le_bytes());
        match validate_preamble(&buf) {
            Err(HandshakeError::VersionMismatch { peer }) => {
                assert_eq!(peer, PROTOCOL_VERSION + 1);
            }
            other => panic!("expected VersionMismatch, got {:?}", other),
        }
    }

    #[test]
    fn sync_read_write_roundtrip() {
        let mut buf = Vec::new();
        write_preamble(&mut buf).unwrap();
        let read = read_preamble(&mut buf.as_slice()).unwrap();
        assert!(validate_preamble(&read).is_ok());
    }
}
