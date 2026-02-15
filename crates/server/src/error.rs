//! Error types for the server

use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ServerError>;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum ServerError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("MySQL error: {0}")]
    MySql(#[from] mysql::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Configuration error: {0}")]
    Config(String),
}

impl ServerError {
    /// Check if this error is recoverable (client can retry)
    pub fn is_recoverable(&self) -> bool {
        match self {
            ServerError::Io(_) => true,
            ServerError::MySql(_) => true,
            ServerError::Protocol(_) => false,
            ServerError::NotFound(_) => false,
            ServerError::InvalidData(_) => false,
            ServerError::Config(_) => false,
        }
    }
}
