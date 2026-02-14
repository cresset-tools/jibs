//! Embedded server binaries for different architectures

/// Server binary for x86_64 Linux (musl)
#[cfg(has_server_x86_64)]
pub static SERVER_X86_64: &[u8] = include_bytes!(env!("JIBS_SERVER_X86_64"));

#[cfg(not(has_server_x86_64))]
pub static SERVER_X86_64: &[u8] = &[];

/// Server binary for aarch64 Linux (musl)
#[cfg(has_server_aarch64)]
pub static SERVER_AARCH64: &[u8] = include_bytes!(env!("JIBS_SERVER_AARCH64"));

#[cfg(not(has_server_aarch64))]
pub static SERVER_AARCH64: &[u8] = &[];

/// Get the appropriate server binary for the given architecture
pub fn get_server_binary(arch: &str) -> Option<&'static [u8]> {
    match arch {
        "x86_64" => {
            if SERVER_X86_64.is_empty() {
                None
            } else {
                Some(SERVER_X86_64)
            }
        }
        "aarch64" => {
            if SERVER_AARCH64.is_empty() {
                None
            } else {
                Some(SERVER_AARCH64)
            }
        }
        _ => None,
    }
}

/// Check if any server binary is available
pub fn has_embedded_server() -> bool {
    !SERVER_X86_64.is_empty() || !SERVER_AARCH64.is_empty()
}

/// List available architectures
pub fn available_architectures() -> Vec<&'static str> {
    let mut archs = Vec::new();
    if !SERVER_X86_64.is_empty() {
        archs.push("x86_64");
    }
    if !SERVER_AARCH64.is_empty() {
        archs.push("aarch64");
    }
    archs
}
