//! Error types for the client

use thiserror::Error;

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("IO error: {0}")]
    Io(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Resolution error: {0}")]
    Resolution(String),

    #[error("SSH error: {0}")]
    Ssh(String),

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
}
