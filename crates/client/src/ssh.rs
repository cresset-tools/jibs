//! SSH session management using russh

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use russh::client::{self, Config, Handle, Msg};
use russh::keys::key::PublicKey;
use russh::{Channel, ChannelMsg, ChannelStream};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::OpenFlags;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::Mutex;

use crate::error::{ClientError, Result};

/// Host key verification mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostKeyVerification {
    /// Accept all host keys without verification (insecure)
    AcceptAll,
    /// Warn on unknown keys, reject on mismatch (default)
    WarnUnknown,
    /// Strict: reject unknown and mismatched keys
    Strict,
    /// Accept and save new keys, reject mismatched keys
    AcceptNew,
}

impl Default for HostKeyVerification {
    fn default() -> Self {
        Self::WarnUnknown
    }
}

/// SSH connection configuration
pub struct SshConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub identity_file: Option<PathBuf>,
    pub host_key_verification: HostKeyVerification,
}

impl SshConfig {
    /// Parse a host string like "user@host[:port]"
    pub fn parse(
        host_str: &str,
        port: u16,
        identity_file: Option<PathBuf>,
        host_key_verification: HostKeyVerification,
    ) -> Result<Self> {
        let parts: Vec<&str> = host_str.splitn(2, '@').collect();
        if parts.len() != 2 {
            return Err(ClientError::Ssh {
                operation: "parse host".to_string(),
                message: "Host must be in format user@host".to_string(),
            });
        }

        let user = parts[0].to_string();
        let host = parts[1].to_string();

        Ok(Self {
            host,
            port,
            user,
            identity_file,
            host_key_verification,
        })
    }
}

/// Result of host key verification
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostKeyCheckResult {
    /// Key matches known_hosts
    Match,
    /// Key is new (not in known_hosts)
    Unknown,
    /// Key doesn't match known_hosts (potential MITM)
    Mismatch,
}

/// Known hosts database
struct KnownHosts {
    /// Map from (host, port) to list of known public key types and their base64 encodings
    entries: HashMap<(String, u16), Vec<(String, String)>>,
}

impl KnownHosts {
    /// Load known hosts from the default location (~/.ssh/known_hosts)
    fn load() -> Self {
        let mut entries = HashMap::new();

        if let Some(home) = home::home_dir() {
            let known_hosts_path = home.join(".ssh").join("known_hosts");
            if let Ok(content) = fs::read_to_string(&known_hosts_path) {
                for line in content.lines() {
                    if let Some((host_key, key_type, key_data)) = Self::parse_line(line) {
                        entries
                            .entry(host_key)
                            .or_insert_with(Vec::new)
                            .push((key_type, key_data));
                    }
                }
            }
        }

        Self { entries }
    }

    /// Parse a single known_hosts line
    /// Format: hostname[,hostname2][,hostname:port] key-type base64-key [comment]
    fn parse_line(line: &str) -> Option<((String, u16), String, String)> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            return None;
        }

        let host_part = parts[0];
        let key_type = parts[1].to_string();
        let key_data = parts[2].to_string();

        // Parse host patterns - we only handle simple cases
        // Skip hashed entries (starting with |1|)
        if host_part.starts_with('|') {
            return None;
        }

        // Handle [host]:port format
        let (host, port) = if host_part.starts_with('[') {
            // [hostname]:port format
            if let Some(bracket_end) = host_part.find(']') {
                let hostname = &host_part[1..bracket_end];
                let port_str = host_part.get(bracket_end + 2..)?;
                let port: u16 = port_str.parse().ok()?;
                (hostname.to_string(), port)
            } else {
                return None;
            }
        } else {
            // Simple hostname (default port 22)
            // Handle comma-separated hostnames
            let hostname = host_part.split(',').next()?;
            (hostname.to_string(), 22)
        };

        Some(((host, port), key_type, key_data))
    }

    /// Check if a host key matches known hosts
    fn check(&self, host: &str, port: u16, key_type: &str, key_base64: &str) -> HostKeyCheckResult {
        // Check both with and without port (for standard port 22)
        let keys_for_host = self.entries.get(&(host.to_string(), port));

        if let Some(known_keys) = keys_for_host {
            for (known_type, known_data) in known_keys {
                if known_type == key_type {
                    if known_data == key_base64 {
                        return HostKeyCheckResult::Match;
                    } else {
                        return HostKeyCheckResult::Mismatch;
                    }
                }
            }
            // Have keys for this host but different type - treat as unknown
            return HostKeyCheckResult::Unknown;
        }

        HostKeyCheckResult::Unknown
    }

    /// Add a new host key to known_hosts file
    fn add_key(host: &str, port: u16, key_type: &str, key_base64: &str) -> std::io::Result<()> {
        if let Some(home) = home::home_dir() {
            let ssh_dir = home.join(".ssh");
            if !ssh_dir.exists() {
                fs::create_dir_all(&ssh_dir)?;
            }

            let known_hosts_path = ssh_dir.join("known_hosts");
            let entry = if port == 22 {
                format!("{} {} {}\n", host, key_type, key_base64)
            } else {
                format!("[{}]:{} {} {}\n", host, port, key_type, key_base64)
            };

            use std::fs::OpenOptions;
            use std::io::Write;
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(known_hosts_path)?;
            file.write_all(entry.as_bytes())?;
        }
        Ok(())
    }
}

/// Encode a public key to OpenSSH wire format and base64
fn encode_public_key(key: &PublicKey) -> Option<(String, String)> {
    match key {
        PublicKey::Ed25519(key) => {
            let key_type = "ssh-ed25519";
            let key_bytes = key.as_bytes();

            // OpenSSH wire format: string key_type, string key_data
            let mut wire = Vec::new();
            // Key type length and data
            wire.extend_from_slice(&(key_type.len() as u32).to_be_bytes());
            wire.extend_from_slice(key_type.as_bytes());
            // Key data length and data
            wire.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
            wire.extend_from_slice(key_bytes);

            let base64 = base64::engine::general_purpose::STANDARD.encode(&wire);
            Some((key_type.to_string(), base64))
        }
        // Add other key types as needed
        _ => None,
    }
}

/// SSH client handler with host key verification
struct SshHandler {
    host: String,
    port: u16,
    verification_mode: HostKeyVerification,
    /// Shared state for communicating verification result
    verification_result: Arc<Mutex<Option<HostKeyCheckResult>>>,
}

impl Clone for SshHandler {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            port: self.port,
            verification_mode: self.verification_mode,
            verification_result: Arc::clone(&self.verification_result),
        }
    }
}

#[async_trait]
impl client::Handler for SshHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        // Accept all mode - skip verification
        if self.verification_mode == HostKeyVerification::AcceptAll {
            return Ok(true);
        }

        // Try to encode the key
        let (key_type, key_base64) = match encode_public_key(server_public_key) {
            Some(k) => k,
            None => {
                // Unknown key type - accept if not strict
                tracing::warn!("Unknown SSH key type, cannot verify");
                return Ok(self.verification_mode != HostKeyVerification::Strict);
            }
        };

        // Load and check known hosts
        let known_hosts = KnownHosts::load();
        let check_result = known_hosts.check(&self.host, self.port, &key_type, &key_base64);

        // Store the result for the caller to inspect
        *self.verification_result.lock().await = Some(check_result.clone());

        match check_result {
            HostKeyCheckResult::Match => {
                tracing::debug!("Host key verified for {}:{}", self.host, self.port);
                Ok(true)
            }
            HostKeyCheckResult::Unknown => {
                let fingerprint = compute_key_fingerprint(&key_base64);
                match self.verification_mode {
                    HostKeyVerification::Strict => {
                        tracing::error!(
                            "Host key verification failed: unknown host {}:{}\n\
                             Key fingerprint: SHA256:{}",
                            self.host,
                            self.port,
                            fingerprint
                        );
                        Ok(false)
                    }
                    HostKeyVerification::AcceptNew => {
                        tracing::info!(
                            "Adding new host key for {}:{} to known_hosts\n\
                             Key fingerprint: SHA256:{}",
                            self.host,
                            self.port,
                            fingerprint
                        );
                        if let Err(e) =
                            KnownHosts::add_key(&self.host, self.port, &key_type, &key_base64)
                        {
                            tracing::warn!("Failed to save host key: {}", e);
                        }
                        Ok(true)
                    }
                    HostKeyVerification::WarnUnknown => {
                        tracing::warn!(
                            "Unknown host key for {}:{}\n\
                             Key fingerprint: SHA256:{}\n\
                             Use --accept-new-host-keys to add to known_hosts,\n\
                             or --strict-host-key-checking to reject unknown keys.",
                            self.host,
                            self.port,
                            fingerprint
                        );
                        Ok(true)
                    }
                    HostKeyVerification::AcceptAll => Ok(true),
                }
            }
            HostKeyCheckResult::Mismatch => {
                let fingerprint = compute_key_fingerprint(&key_base64);
                tracing::error!(
                    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n\
                     @    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @\n\
                     @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n\
                     IT IS POSSIBLE THAT SOMEONE IS DOING SOMETHING NASTY!\n\
                     Host: {}:{}\n\
                     Received key fingerprint: SHA256:{}\n\
                     The host key does not match the one in your known_hosts file.\n\
                     This could indicate a man-in-the-middle attack.",
                    self.host,
                    self.port,
                    fingerprint
                );
                Ok(false)
            }
        }
    }
}

/// Compute SHA256 fingerprint of a base64-encoded key
fn compute_key_fingerprint(key_base64: &str) -> String {
    if let Ok(key_bytes) = base64::engine::general_purpose::STANDARD.decode(key_base64) {
        let hash = Sha256::digest(&key_bytes);
        base64::engine::general_purpose::STANDARD_NO_PAD.encode(hash)
    } else {
        "invalid".to_string()
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
        let ssh_config = Config {
            window_size: 32 * 1024 * 1024,    // 32 MB (vs 2 MB default)
            maximum_packet_size: 65535,         // max allowed (vs 32 KB default)
            ..Config::default()
        };

        let verification_result = Arc::new(Mutex::new(None));
        let handler = SshHandler {
            host: config.host.clone(),
            port: config.port,
            verification_mode: config.host_key_verification,
            verification_result: Arc::clone(&verification_result),
        };

        let socket = tokio::net::TcpStream::connect((&config.host as &str, config.port))
            .await
            .map_err(|e| ClientError::Ssh {
                operation: "connect".to_string(),
                message: format!("Failed to connect to {}:{}: {}", config.host, config.port, e),
            })?;
        socket.set_nodelay(true).map_err(|e| ClientError::Ssh {
            operation: "set TCP_NODELAY".to_string(),
            message: format!("Failed to set TCP_NODELAY: {}", e),
        })?;

        let mut handle = client::connect_stream(
            Arc::new(ssh_config),
            socket,
            handler,
        )
        .await
        .map_err(|e| ClientError::Ssh {
            operation: "connect".to_string(),
            message: format!("Failed to connect to {}:{}: {}", config.host, config.port, e),
        })?;

        // Authenticate
        Self::authenticate(&mut handle, &config).await?;

        Ok(Self { handle, config })
    }

    /// Authenticate using available methods
    async fn authenticate(handle: &mut Handle<SshHandler>, config: &SshConfig) -> Result<()> {
        // Try identity file if specified
        if let Some(identity_path) = &config.identity_file {
            let key_pair = russh_keys::load_secret_key(identity_path, None).map_err(|e| {
                ClientError::Ssh {
                    operation: "load private key".to_string(),
                    message: format!("Failed to load key from {:?}: {}", identity_path, e),
                }
            })?;

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

        Err(ClientError::Ssh {
            operation: "authenticate".to_string(),
            message: format!(
                "Authentication failed for user '{}' on {}:{}",
                config.user, config.host, config.port
            ),
        })
    }

    /// Execute a command and wait for it to complete
    pub async fn exec(&self, command: &str) -> Result<(i32, String, String)> {
        let mut channel = self
            .handle
            .channel_open_session()
            .await
            .map_err(|e| ClientError::Ssh {
                operation: "open channel".to_string(),
                message: format!("Failed to open channel: {}", e),
            })?;

        channel
            .exec(true, command)
            .await
            .map_err(|e| ClientError::Ssh {
                operation: format!("exec '{}'", command),
                message: e.to_string(),
            })?;

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

    /// Upload a file to the remote host using SFTP
    pub async fn upload_file(&self, local_data: &[u8], remote_path: &str) -> Result<()> {
        let channel = self
            .handle
            .channel_open_session()
            .await
            .map_err(|e| ClientError::Ssh {
                operation: "open SFTP channel".to_string(),
                message: e.to_string(),
            })?;

        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| ClientError::Ssh {
                operation: "request SFTP subsystem".to_string(),
                message: e.to_string(),
            })?;

        let sftp = SftpSession::new(channel.into_stream())
            .await
            .map_err(|e| ClientError::Ssh {
                operation: "init SFTP session".to_string(),
                message: e.to_string(),
            })?;

        let mut file = sftp
            .open_with_flags(
                remote_path,
                OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
            )
            .await
            .map_err(|e| ClientError::Ssh {
                operation: format!("open remote file '{}'", remote_path),
                message: e.to_string(),
            })?;

        file.write_all(local_data)
            .await
            .map_err(|e| ClientError::Ssh {
                operation: format!("write to remote file '{}'", remote_path),
                message: e.to_string(),
            })?;

        file.shutdown()
            .await
            .map_err(|e| ClientError::Ssh {
                operation: format!("close remote file '{}'", remote_path),
                message: e.to_string(),
            })?;

        sftp.close()
            .await
            .map_err(|e| ClientError::Ssh {
                operation: "close SFTP session".to_string(),
                message: e.to_string(),
            })?;

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
            .map_err(|e| ClientError::Ssh {
                operation: "open channel for process".to_string(),
                message: e.to_string(),
            })?;

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
            .map_err(|e| ClientError::Ssh {
                operation: format!("start process '{}'", command),
                message: e.to_string(),
            })?;

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
            .map_err(|e| ClientError::Ssh {
                operation: "write to process stdin".to_string(),
                message: e.to_string(),
            })?;
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
                return Err(ClientError::Ssh {
                    operation: "read from process".to_string(),
                    message: format!(
                        "Unexpected EOF after {} bytes (expected {})",
                        offset,
                        buf.len()
                    ),
                });
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
            .map_err(|e| ClientError::Ssh {
                operation: "close process stdin".to_string(),
                message: e.to_string(),
            })?;
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

    /// Split into independent reader and writer halves for concurrent I/O.
    ///
    /// After splitting, the reader and writer can be used from different tasks.
    /// Note: stderr messages from the remote process will be silently discarded
    /// after splitting (they were only logged before).
    pub fn split(self) -> (ProcessReader, ProcessWriter) {
        let stream = self.channel.into_stream();
        let (reader, writer) = tokio::io::split(stream);
        (ProcessReader { reader }, ProcessWriter { writer })
    }
}

/// Read half of a split RemoteProcess
pub struct ProcessReader {
    reader: ReadHalf<ChannelStream<Msg>>,
}

impl ProcessReader {
    /// Read exact number of bytes from the remote process stdout
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.reader.read_exact(buf).await.map_err(|e| ClientError::Ssh {
            operation: "read from process".to_string(),
            message: e.to_string(),
        })?;
        Ok(())
    }
}

/// Write half of a split RemoteProcess
pub struct ProcessWriter {
    writer: WriteHalf<ChannelStream<Msg>>,
}

impl ProcessWriter {
    /// Write data to the remote process stdin
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.writer.write_all(data).await.map_err(|e| ClientError::Ssh {
            operation: "write to process stdin".to_string(),
            message: e.to_string(),
        })?;
        Ok(())
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

