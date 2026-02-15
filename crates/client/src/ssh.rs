//! SSH session management using russh

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use russh::client::{self, Config, Handle, Msg};
use russh::keys::key::PublicKey;
use russh::{Channel, ChannelMsg};
use sha2::{Digest, Sha256};

use crate::error::{ClientError, Result};

/// SSH connection configuration
pub struct SshConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub identity_file: Option<PathBuf>,
}

impl SshConfig {
    /// Parse a host string like "user@host[:port]"
    pub fn parse(host_str: &str, port: u16, identity_file: Option<PathBuf>) -> Result<Self> {
        let parts: Vec<&str> = host_str.splitn(2, '@').collect();
        if parts.len() != 2 {
            return Err(ClientError::Ssh(
                "Host must be in format user@host".to_string(),
            ));
        }

        let user = parts[0].to_string();
        let host = parts[1].to_string();

        Ok(Self {
            host,
            port,
            user,
            identity_file,
        })
    }
}

/// SSH client handler
#[derive(Clone)]
struct SshHandler;

#[async_trait]
impl client::Handler for SshHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        // TODO: Implement proper host key verification
        // For now, accept all keys (similar to StrictHostKeyChecking=no)
        Ok(true)
    }
}

/// An SSH session to a remote host
pub struct SshSession {
    handle: Handle<SshHandler>,
    #[allow(dead_code)]
    config: SshConfig,
}

impl SshSession {
    /// Connect to a remote host
    pub async fn connect(config: SshConfig) -> Result<Self> {
        let ssh_config = Config::default();

        let mut handle = client::connect(
            Arc::new(ssh_config),
            (&config.host as &str, config.port),
            SshHandler,
        )
        .await
        .map_err(|e| ClientError::Ssh(format!("Failed to connect: {}", e)))?;

        // Authenticate
        Self::authenticate(&mut handle, &config).await?;

        Ok(Self { handle, config })
    }

    /// Authenticate using available methods
    async fn authenticate(handle: &mut Handle<SshHandler>, config: &SshConfig) -> Result<()> {
        // Try identity file if specified
        if let Some(identity_path) = &config.identity_file {
            let key_pair = russh_keys::load_secret_key(identity_path, None)
                .map_err(|e| ClientError::Ssh(format!("Failed to load key: {}", e)))?;

            let auth_result = handle
                .authenticate_publickey(&config.user, Arc::new(key_pair))
                .await;

            if let Ok(true) = auth_result {
                return Ok(());
            }
        }

        // Try default identity files
        if let Some(home) = home::home_dir() {
            let default_keys = ["id_ed25519", "id_rsa", "id_ecdsa"];
            for key_name in &default_keys {
                let key_path = home.join(".ssh").join(key_name);
                if key_path.exists() {
                    if let Ok(key_pair) = russh_keys::load_secret_key(&key_path, None) {
                        let auth_result = handle
                            .authenticate_publickey(&config.user, Arc::new(key_pair))
                            .await;

                        if let Ok(true) = auth_result {
                            return Ok(());
                        }
                    }
                }
            }
        }

        Err(ClientError::Ssh("Authentication failed".to_string()))
    }

    /// Execute a command and wait for it to complete
    pub async fn exec(&self, command: &str) -> Result<(i32, String, String)> {
        let mut channel = self
            .handle
            .channel_open_session()
            .await
            .map_err(|e| ClientError::Ssh(format!("Failed to open channel: {}", e)))?;

        channel
            .exec(true, command)
            .await
            .map_err(|e| ClientError::Ssh(format!("Failed to exec: {}", e)))?;

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_code: Option<i32> = None;
        let mut got_eof = false;

        // SSH protocol sends: Data* -> Eof -> ExitStatus -> Close
        // We need to wait for ExitStatus, not just Eof
        loop {
            match channel.wait().await {
                Some(ChannelMsg::Data { data }) => {
                    stdout.extend_from_slice(&data);
                }
                Some(ChannelMsg::ExtendedData { data, ext }) => {
                    if ext == 1 {
                        // stderr
                        stderr.extend_from_slice(&data);
                    }
                }
                Some(ChannelMsg::ExitStatus { exit_status }) => {
                    exit_code = Some(exit_status as i32);
                    // If we already got Eof, we're done
                    if got_eof {
                        break;
                    }
                }
                Some(ChannelMsg::Eof) => {
                    got_eof = true;
                    // If we already have exit code, we're done
                    if exit_code.is_some() {
                        break;
                    }
                }
                None => break,
                _ => {}
            }
        }

        Ok((
            exit_code.unwrap_or(0),
            String::from_utf8_lossy(&stdout).to_string(),
            String::from_utf8_lossy(&stderr).to_string(),
        ))
    }

    /// Check if a file exists on the remote host
    #[allow(dead_code)]
    pub async fn file_exists(&self, path: &str) -> Result<bool> {
        let (code, _, _) = self.exec(&format!("test -f {}", path)).await?;
        Ok(code == 0)
    }

    /// Upload a file to the remote host using base64 encoding
    pub async fn upload_file(&self, local_data: &[u8], remote_path: &str) -> Result<()> {
        // Use base64 encoding for safe binary transfer
        let encoded = base64_encode(local_data);

        // Split into chunks to avoid command line length limits
        const CHUNK_SIZE: usize = 32768;
        let chunks: Vec<&str> = encoded
            .as_bytes()
            .chunks(CHUNK_SIZE)
            .map(|c| std::str::from_utf8(c).unwrap())
            .collect();

        // Write first chunk (create/truncate file)
        if let Some(first_chunk) = chunks.first() {
            self.exec(&format!(
                "echo '{}' | base64 -d > {}",
                first_chunk, remote_path
            ))
            .await?;
        }

        // Append remaining chunks
        for chunk in chunks.iter().skip(1) {
            self.exec(&format!(
                "echo '{}' | base64 -d >> {}",
                chunk, remote_path
            ))
            .await?;
        }

        // Make executable
        self.exec(&format!("chmod +x {}", remote_path)).await?;

        Ok(())
    }

    /// Start a command and return a bidirectional channel for stdin/stdout
    pub async fn start_process(&self, command: &str) -> Result<RemoteProcess> {
        let channel = self
            .handle
            .channel_open_session()
            .await
            .map_err(|e| ClientError::Ssh(format!("Failed to open channel: {}", e)))?;

        RemoteProcess::new(channel, command).await
    }
}

/// A running process on the remote host with stdin/stdout access
pub struct RemoteProcess {
    channel: Channel<Msg>,
    /// Buffer for incoming data
    stdout_buffer: Vec<u8>,
    /// Whether the process has exited
    exited: bool,
    /// Exit code (if exited)
    exit_code: Option<i32>,
}

impl RemoteProcess {
    async fn new(channel: Channel<Msg>, command: &str) -> Result<Self> {
        channel
            .exec(true, command)
            .await
            .map_err(|e| ClientError::Ssh(format!("Failed to exec: {}", e)))?;

        Ok(Self {
            channel,
            stdout_buffer: Vec::new(),
            exited: false,
            exit_code: None,
        })
    }

    /// Write data to the process stdin
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.channel
            .data(data)
            .await
            .map_err(|e| ClientError::Ssh(format!("Failed to write: {}", e)))?;
        Ok(())
    }

    /// Read data from stdout, returns bytes read
    /// Blocks until data is available or process exits
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // First, drain any buffered data
        if !self.stdout_buffer.is_empty() {
            let len = std::cmp::min(buf.len(), self.stdout_buffer.len());
            buf[..len].copy_from_slice(&self.stdout_buffer[..len]);
            self.stdout_buffer.drain(..len);
            return Ok(len);
        }

        if self.exited {
            return Ok(0);
        }

        // Wait for more data
        loop {
            match self.channel.wait().await {
                Some(ChannelMsg::Data { data }) => {
                    let len = std::cmp::min(buf.len(), data.len());
                    buf[..len].copy_from_slice(&data[..len]);
                    if data.len() > len {
                        self.stdout_buffer.extend_from_slice(&data[len..]);
                    }
                    return Ok(len);
                }
                Some(ChannelMsg::ExtendedData { data, ext }) => {
                    if ext == 1 {
                        // stderr - log it as warning so we can see errors
                        let msg = String::from_utf8_lossy(&data);
                        tracing::warn!("Remote stderr: {}", msg);
                    }
                }
                Some(ChannelMsg::ExitStatus { exit_status }) => {
                    self.exit_code = Some(exit_status as i32);
                }
                Some(ChannelMsg::Eof) | None => {
                    self.exited = true;
                    return Ok(0);
                }
                _ => {}
            }
        }
    }

    /// Read exact number of bytes
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut offset = 0;
        while offset < buf.len() {
            let n = self.read(&mut buf[offset..]).await?;
            if n == 0 {
                return Err(ClientError::Ssh("Unexpected EOF".to_string()));
            }
            offset += n;
        }
        Ok(())
    }

    /// Close stdin (signal EOF to the process)
    #[allow(dead_code)]
    pub async fn close_stdin(&mut self) -> Result<()> {
        self.channel
            .eof()
            .await
            .map_err(|e| ClientError::Ssh(format!("Failed to send EOF: {}", e)))?;
        Ok(())
    }

    /// Check if the process has exited
    #[allow(dead_code)]
    pub fn has_exited(&self) -> bool {
        self.exited
    }

    /// Get exit code (if process has exited)
    #[allow(dead_code)]
    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }
}

/// Compute SHA256 hash of data and return hex string
pub fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Get the remote path for the server binary based on its hash
pub fn get_server_path(server_binary: &[u8]) -> String {
    let hash = compute_hash(server_binary);
    format!("/tmp/jibs-{}", &hash[0..16])
}

/// Base64 encode data
fn base64_encode(data: &[u8]) -> String {
    let mut buf = Vec::new();
    {
        let mut encoder = Base64Encoder {
            writer: &mut buf,
            buffer: [0; 3],
            count: 0,
        };
        encoder.write_all(data).unwrap();
    }
    String::from_utf8(buf).unwrap()
}

struct Base64Encoder<W: Write> {
    writer: W,
    buffer: [u8; 3],
    count: usize,
}

impl<W: Write> Write for Base64Encoder<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        const ALPHABET: &[u8] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        for &byte in buf {
            self.buffer[self.count] = byte;
            self.count += 1;

            if self.count == 3 {
                let out = [
                    ALPHABET[(self.buffer[0] >> 2) as usize],
                    ALPHABET[(((self.buffer[0] & 0x03) << 4) | (self.buffer[1] >> 4)) as usize],
                    ALPHABET[(((self.buffer[1] & 0x0f) << 2) | (self.buffer[2] >> 6)) as usize],
                    ALPHABET[(self.buffer[2] & 0x3f) as usize],
                ];
                self.writer.write_all(&out)?;
                self.count = 0;
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        const ALPHABET: &[u8] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        if self.count > 0 {
            let out = match self.count {
                1 => [
                    ALPHABET[(self.buffer[0] >> 2) as usize],
                    ALPHABET[((self.buffer[0] & 0x03) << 4) as usize],
                    b'=',
                    b'=',
                ],
                2 => [
                    ALPHABET[(self.buffer[0] >> 2) as usize],
                    ALPHABET[(((self.buffer[0] & 0x03) << 4) | (self.buffer[1] >> 4)) as usize],
                    ALPHABET[((self.buffer[1] & 0x0f) << 2) as usize],
                    b'=',
                ],
                _ => unreachable!(),
            };
            self.writer.write_all(&out)?;
            self.count = 0;
        }
        self.writer.flush()
    }
}

impl<W: Write> Drop for Base64Encoder<W> {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
