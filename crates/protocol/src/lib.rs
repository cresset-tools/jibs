//! Jibs Protocol - Shared types for client-server communication
//!
//! This crate defines the wire protocol between the jibs client and server,
//! including message types, execution plans, and checkpoint formats.

pub mod checkpoint;
pub mod framing;
pub mod messages;
pub mod plan;

pub use checkpoint::Checkpoint;
pub use framing::{read_message, write_message};
pub use messages::{ClientMessage, ServerMessage};
pub use plan::{
    AnonymizeRule, AnonymizeTarget, Assignment, ColumnDef, ColumnFlags, CompressionMode,
    ExecutionPlan, PreserveRule, Relation, ResolvedAggregate, SetRule, SortDirection, TableInfo,
    Value,
};
