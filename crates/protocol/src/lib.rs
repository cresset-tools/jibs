//! Jibs Protocol - Shared types for client-server communication
//!
//! This crate defines the wire protocol between the jibs client and server,
//! including message types, execution plans, and checkpoint formats.

pub mod framing;
pub mod messages;
pub mod plan;
pub use framing::{
    decode_data_chunk, read_message, write_message, DataChunk, MessageWriter, RAW_CHUNK_FLAG,
    RAW_CHUNK_HEADER_LEN,
};
pub use messages::{ClientMessage, QueryTiming, ServerMessage, ServerMetrics, TableDisposition};
pub use plan::{
    AnonymizeRule, AnonymizeTarget, Assignment, ColumnDef, ColumnFlags, CompressionMode,
    ExecutionPlan, PreserveRule, Relation, ResolvedAggregate, SetRule, SortDirection, TableInfo,
    Value,
};
