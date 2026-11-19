//! SSH session management using system ssh command with ControlMaster

use std::path::PathBuf;

use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

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

/// An SSH session to a remote host using system ssh with ControlMaster
pub struct SshSession {
    config: SshConfig,
    control_path: PathBuf,
}

impl SshSession {
    /// Connect to a remote host by establishing a ControlMaster connection
    pub async fn connect(config: SshConfig) -> Result<Self> {
        // Use a short hash to keep the socket path under macOS's 104-byte limit
        let mut hasher = Sha256::new();
        hasher.update(format!("{}@{}:{}-{}", config.user, config.host, config.port, std::process::id()));
        let hash = hex::encode(&hasher.finalize()[..8]);
        let control_path = std::env::temp_dir().join(format!("jibs-{}", hash));

        // Clean up stale socket if it exists
        if control_path.exists() {
            let _ = std::fs::remove_file(&control_path);
        }

        let session = Self {
            config,
            control_path,
        };

        // Establish ControlMaster connection
        let mut args = vec![
            "-fNM".to_string(),
            "-o".to_string(),
            "ControlMaster=yes".to_string(),
        ];
        args.extend(session.base_ssh_args());
        args.push(session.user_host());

        let output = Command::new("ssh")
            .args(&args)
            .output()
            .await
            .map_err(|e| ClientError::Ssh {
                operation: "connect".to_string(),
                message: format!("Failed to start ssh: {}", e),
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ClientError::Ssh {
                operation: "connect".to_string(),
                message: format!(
                    "Failed to connect to {}:{}: {}",
                    session.config.host, session.config.port, stderr.trim()
                ),
            });
        }

        Ok(session)
    }

    /// Execute a command and wait for it to complete
    pub async fn exec(&self, command: &str) -> Result<(i32, String, String)> {
        let mut args = self.base_ssh_args();
        args.push(self.user_host());
        args.push(command.to_string());

        let output = Command::new("ssh")
            .args(&args)
            .output()
            .await
            .map_err(|e| ClientError::Ssh {
                operation: format!("exec '{}'", command),
                message: format!("Failed to run ssh: {}", e),
            })?;

        let code = output.status.code().unwrap_or(-1);
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        Ok((code, stdout, stderr))
    }

    /// Upload a file to the remote host by piping through ssh + cat
    pub async fn upload_file(&self, local_data: &[u8], remote_path: &str) -> Result<()> {
        let mut args = self.base_ssh_args();
        args.push(self.user_host());
        let quoted = shell_quote(remote_path);
        args.push(format!("cat > {quoted} && chmod +x {quoted}"));

        let mut child = Command::new("ssh")
            .args(&args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| ClientError::Ssh {
                operation: format!("upload to '{}'", remote_path),
                message: format!("Failed to start ssh: {}", e),
            })?;

        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(local_data)
            .await
            .map_err(|e| ClientError::Ssh {
                operation: format!("write to remote file '{}'", remote_path),
                message: e.to_string(),
            })?;
        stdin.shutdown().await.map_err(|e| ClientError::Ssh {
            operation: format!("close upload stdin for '{}'", remote_path),
            message: e.to_string(),
        })?;
        drop(stdin);

        let output = child.wait_with_output().await.map_err(|e| ClientError::Ssh {
            operation: format!("upload to '{}'", remote_path),
            message: e.to_string(),
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ClientError::Ssh {
                operation: format!("upload to '{}'", remote_path),
                message: format!("Upload failed: {}", stderr.trim()),
            });
        }

        Ok(())
    }

    /// Start a command and return a bidirectional channel for stdin/stdout
    pub async fn start_process(&self, command: &str) -> Result<RemoteProcess> {
        let mut args = self.base_ssh_args();
        args.push(self.user_host());
        args.push(command.to_string());

        let mut child = Command::new("ssh")
            .args(&args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| ClientError::Ssh {
                operation: format!("start process '{}'", command),
                message: format!("Failed to start ssh: {}", e),
            })?;

        // Spawn a background task to drain stderr and log it
        let stderr = child.stderr.take().unwrap();
        tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt;
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::warn!("Remote stderr: {}", line);
            }
        });

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        Ok(RemoteProcess {
            _child: child,
            stdin: Some(stdin),
            stdout: Some(stdout),
        })
    }

    /// Common SSH args for all commands
    fn base_ssh_args(&self) -> Vec<String> {
        let mut args = Vec::new();

        // ControlMaster socket
        args.push("-o".to_string());
        args.push(format!("ControlPath={}", self.control_path.display()));

        // Reuse existing master connection
        args.push("-o".to_string());
        args.push("ControlMaster=no".to_string());

        // Don't hang on password prompts
        args.push("-o".to_string());
        args.push("BatchMode=yes".to_string());

        // Port
        args.push("-p".to_string());
        args.push(self.config.port.to_string());

        // Identity file
        if let Some(ref identity) = self.config.identity_file {
            args.push("-i".to_string());
            args.push(identity.display().to_string());
        }

        // Host key verification
        match self.config.host_key_verification {
            HostKeyVerification::AcceptAll => {
                args.push("-o".to_string());
                args.push("StrictHostKeyChecking=no".to_string());
                args.push("-o".to_string());
                args.push("UserKnownHostsFile=/dev/null".to_string());
            }
            HostKeyVerification::WarnUnknown | HostKeyVerification::AcceptNew => {
                args.push("-o".to_string());
                args.push("StrictHostKeyChecking=accept-new".to_string());
            }
            HostKeyVerification::Strict => {
                args.push("-o".to_string());
                args.push("StrictHostKeyChecking=yes".to_string());
            }
        }

        // Suppress banners/motd
        args.push("-o".to_string());
        args.push("LogLevel=ERROR".to_string());

        args
    }

    /// Format user@host string
    fn user_host(&self) -> String {
        format!("{}@{}", self.config.user, self.config.host)
    }
}

impl Drop for SshSession {
    fn drop(&mut self) {
        // Tear down the ControlMaster connection
        // Use std::process::Command since we're in a sync Drop
        let _ = std::process::Command::new("ssh")
            .args([
                "-o",
                &format!("ControlPath={}", self.control_path.display()),
                "-O",
                "exit",
                &self.user_host(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        // Clean up the socket file
        let _ = std::fs::remove_file(&self.control_path);
    }
}

/// A running process on the remote host with stdin/stdout access
pub struct RemoteProcess {
    _child: tokio::process::Child,
    stdin: Option<tokio::process::ChildStdin>,
    stdout: Option<tokio::process::ChildStdout>,
}

impl RemoteProcess {
    /// Write data to the process stdin
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        let stdin = self.stdin.as_mut().ok_or_else(|| ClientError::Ssh {
            operation: "write to process stdin".to_string(),
            message: "stdin already taken by split()".to_string(),
        })?;
        stdin.write_all(data).await.map_err(|e| ClientError::Ssh {
            operation: "write to process stdin".to_string(),
            message: e.to_string(),
        })?;
        stdin.flush().await.map_err(|e| ClientError::Ssh {
            operation: "flush process stdin".to_string(),
            message: e.to_string(),
        })?;
        Ok(())
    }

    /// Read data from stdout, returns bytes read
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let stdout = self.stdout.as_mut().ok_or_else(|| ClientError::Ssh {
            operation: "read from process".to_string(),
            message: "stdout already taken by split()".to_string(),
        })?;
        let n = stdout.read(buf).await.map_err(|e| ClientError::Ssh {
            operation: "read from process".to_string(),
            message: e.to_string(),
        })?;
        Ok(n)
    }

    /// Read exact number of bytes
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let stdout = self.stdout.as_mut().ok_or_else(|| ClientError::Ssh {
            operation: "read from process".to_string(),
            message: "stdout already taken by split()".to_string(),
        })?;
        stdout.read_exact(buf).await.map_err(|e| ClientError::Ssh {
            operation: "read from process".to_string(),
            message: format!(
                "Unexpected EOF (expected {} bytes): {}",
                buf.len(),
                e
            ),
        })?;
        Ok(())
    }

    /// Split into independent reader and writer halves for concurrent I/O.
    pub fn split(mut self) -> (ProcessReader, ProcessWriter) {
        let stdout = self.stdout.take().expect("stdout already taken");
        let stdin = self.stdin.take().expect("stdin already taken");
        (
            ProcessReader { reader: stdout },
            ProcessWriter { writer: stdin },
        )
    }
}

/// Read half of the protocol stream. Implemented by the unsplit process
/// (pre-protocol exchange) and the split reader (main loop) so that message
/// receiving code is written once.
#[allow(async_fn_in_trait)]
pub trait ProtocolRead {
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()>;
}

impl ProtocolRead for RemoteProcess {
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        RemoteProcess::read_exact(self, buf).await
    }
}

impl ProtocolRead for ProcessReader {
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        ProcessReader::read_exact(self, buf).await
    }
}

/// Write half of the protocol stream (see [`ProtocolRead`])
#[allow(async_fn_in_trait)]
pub trait ProtocolWrite {
    async fn write(&mut self, data: &[u8]) -> Result<()>;
}

impl ProtocolWrite for RemoteProcess {
    async fn write(&mut self, data: &[u8]) -> Result<()> {
        RemoteProcess::write(self, data).await
    }
}

impl ProtocolWrite for ProcessWriter {
    async fn write(&mut self, data: &[u8]) -> Result<()> {
        ProcessWriter::write(self, data).await
    }
}

/// Read half of a split RemoteProcess
pub struct ProcessReader {
    reader: tokio::process::ChildStdout,
}

impl ProcessReader {
    /// Read exact number of bytes from the remote process stdout
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.reader
            .read_exact(buf)
            .await
            .map_err(|e| ClientError::Ssh {
                operation: "read from process".to_string(),
                message: e.to_string(),
            })?;
        Ok(())
    }
}

/// Write half of a split RemoteProcess
pub struct ProcessWriter {
    writer: tokio::process::ChildStdin,
}

impl ProcessWriter {
    /// Write data to the remote process stdin
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.writer
            .write_all(data)
            .await
            .map_err(|e| ClientError::Ssh {
                operation: "write to process stdin".to_string(),
                message: e.to_string(),
            })?;
        self.writer.flush().await.map_err(|e| ClientError::Ssh {
            operation: "flush process stdin".to_string(),
            message: e.to_string(),
        })?;
        Ok(())
    }

    /// Shut down the write half, sending EOF to the remote process stdin
    pub async fn shutdown(&mut self) -> Result<()> {
        self.writer
            .shutdown()
            .await
            .map_err(|e| ClientError::Ssh {
                operation: "shutdown process stdin".to_string(),
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

/// Remote command that creates the per-user jibs cache directory and prints
/// its absolute path: `$XDG_CACHE_HOME/jibs`, or `~/.cache/jibs` when
/// `XDG_CACHE_HOME` is unset or not absolute (the XDG Base Directory rules).
///
/// Wrapped in `sh -c` so it does not depend on the remote login shell.
pub const RESOLVE_CACHE_DIR_COMMAND: &str = concat!(
    r#"sh -c 'case "${XDG_CACHE_HOME:-}" in /*) d=$XDG_CACHE_HOME ;; *) d=$HOME/.cache ;; esac; "#,
    r#"d=$d/jibs; mkdir -p "$d" && printf %s "$d"'"#,
);

/// Get the remote path for the server binary inside `cache_dir`, named after
/// its hash so that different builds never collide.
pub fn get_server_path(cache_dir: &str, server_binary: &[u8]) -> String {
    let hash = compute_hash(server_binary);
    format!("{}/jibs-{}", cache_dir.trim_end_matches('/'), &hash[0..16])
}

/// Quote a string so a POSIX shell reads it as a single word.
pub fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_path_is_content_addressed_inside_the_cache_dir() {
        let path = get_server_path("/home/deploy/.cache/jibs", b"binary");
        let hash = compute_hash(b"binary");
        assert_eq!(path, format!("/home/deploy/.cache/jibs/jibs-{}", &hash[..16]));
        assert_eq!(get_server_path("/home/deploy/.cache/jibs/", b"binary"), path);
        assert_ne!(get_server_path("/home/deploy/.cache/jibs", b"other"), path);
    }

    #[test]
    fn shell_quote_survives_spaces_and_quotes() {
        assert_eq!(shell_quote("/a b/c"), "'/a b/c'");
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
    }

    /// Run the resolver the way the remote login shell would, against a
    /// throwaway environment.
    fn resolve(home: &std::path::Path, xdg: Option<&str>) -> String {
        let mut cmd = std::process::Command::new("sh");
        cmd.arg("-c").arg(RESOLVE_CACHE_DIR_COMMAND).env("HOME", home);
        match xdg {
            Some(v) => cmd.env("XDG_CACHE_HOME", v),
            None => cmd.env_remove("XDG_CACHE_HOME"),
        };
        let out = cmd.output().unwrap();
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).unwrap()
    }

    #[test]
    fn cache_dir_follows_xdg() {
        let tmp = std::env::temp_dir().join(format!("jibs-cache-dir-test-{}", std::process::id()));
        let home = tmp.join("home dir");
        std::fs::create_dir_all(&home).unwrap();

        let default = format!("{}/.cache/jibs", home.display());
        assert_eq!(resolve(&home, None), default);
        assert!(std::path::Path::new(&default).is_dir());

        // Relative values are invalid per the spec and must be ignored.
        assert_eq!(resolve(&home, Some("relative")), default);
        assert_eq!(resolve(&home, Some("")), default);

        let xdg = tmp.join("xdg");
        assert_eq!(
            resolve(&home, Some(xdg.to_str().unwrap())),
            format!("{}/jibs", xdg.display())
        );

        std::fs::remove_dir_all(&tmp).unwrap();
    }
}
