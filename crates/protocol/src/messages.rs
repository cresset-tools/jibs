//! Protocol message types for client-server communication

use bincode::{Decode, Encode};

use crate::checkpoint::Checkpoint;
use crate::plan::{ColumnDef, CompressionMode, ExecutionPlan, TableInfo};

/// Messages sent from client to server
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ClientMessage {
    /// Initial handshake with full execution plan
    Init {
        plan: ExecutionPlan,
        compression: CompressionMode,
    },
    /// Request data for a specific aggregate (or resume from checkpoint)
    FetchAggregate {
        name: String,
        resume_from: Option<Checkpoint>,
    },
    /// Acknowledge receipt of chunk (for flow control)
    Ack { checkpoint: Checkpoint },
    /// Graceful shutdown
    Shutdown,
}

/// Messages sent from server to client
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ServerMessage {
    /// Server ready, reports discovered tables and estimated row counts
    Ready {
        tables: Vec<TableInfo>,
        compression: CompressionMode,
    },
    /// Schema for a table (sent before first data chunk)
    Schema {
        table: String,
        columns: Vec<ColumnDef>,
    },
    /// Data chunk with rows in TSV format
    Data {
        table: String,
        row_count: u32,
        tsv_data: Vec<u8>,
        checkpoint: Checkpoint,
    },
    /// Table fully transferred
    TableDone { table: String, row_count: u64 },
    /// Aggregate fully transferred
    AggregateDone { name: String },
    /// Error occurred
    Error { message: String, recoverable: bool },
}
