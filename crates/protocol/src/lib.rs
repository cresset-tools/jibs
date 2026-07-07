//! Jibs Protocol - Shared types for client-server communication
//!
//! This crate defines the wire protocol between the jibs client and server,
//! including message types, execution plans, and checkpoint formats.

pub mod framing;
pub mod handshake;
pub mod messages;
pub mod plan;
pub use framing::{
    decode_data_chunk, read_message, write_message, DataChunk, MessageWriter, RAW_CHUNK_FLAG,
    RAW_CHUNK_HEADER_LEN,
};
pub use handshake::{
    encode_preamble, read_preamble, validate_preamble, write_preamble, HandshakeError,
    PREAMBLE_LEN, PROTOCOL_MAGIC, PROTOCOL_VERSION,
};
pub use messages::{
    AggregateRootCount, ClientMessage, QueryTiming, ServerMessage, ServerMetrics,
    TableDisposition,
};
pub use plan::{
    AnonymizeRule, AnonymizeTarget, Assignment, ColumnDef, ColumnFlags, CompressionMode,
    ExecutionPlan, ForeignKeyDef, IndexColumn, IndexDef, IndexKind, PreserveRule, Relation,
    ResolvedAggregate, SetRule, SortDirection, TableInfo, TableOptions, Value,
};
