//! Error types for the client

use thiserror::Error;

pub type Result<T> = std::result::Result<T, ClientError>;

/// Client error types
///
/// Some variants are defined for future use or completeness but may not
/// be constructed in the current codebase.
#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum ClientError {
    #[error("IO error during {operation}: {message}")]
    Io { operation: String, message: String },

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Resolution error: {0}")]
    Resolution(String),

    #[error("SSH error during {operation}: {message}")]
    Ssh { operation: String, message: String },

    #[error("MySQL error during {operation}: {source}")]
    MySqlWithContext {
        operation: String,
        #[source]
        source: mysql::Error,
    },

    #[error("MySQL error: {0}")]
    MySql(#[from] mysql::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Variable not defined: {0}")]
    UndefinedVariable(String),

    #[error("Type error: {0}")]
    TypeError(String),

    #[error("Server error: {0}")]
    Server(String),

    #[error("Worker initialization failed: {0}")]
    WorkerInit(String),
}
