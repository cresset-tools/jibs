//! Build script for jibs_client
//!
//! This script checks for pre-built server binaries and sets up
//! environment variables for embedding them.

use std::env;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../target/x86_64-unknown-linux-musl/release/jibs-server");
    println!("cargo:rerun-if-changed=../../target/aarch64-unknown-linux-musl/release/jibs-server");

    // Check for pre-built server binaries
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let workspace_root = Path::new(&manifest_dir).parent().unwrap().parent().unwrap();

    let x86_path = workspace_root
        .join("target/x86_64-unknown-linux-musl/release/jibs-server");
    let arm_path = workspace_root
        .join("target/aarch64-unknown-linux-musl/release/jibs-server");

    if x86_path.exists() {
        println!("cargo:rustc-cfg=has_server_x86_64");
        println!(
            "cargo:rustc-env=JIBS_SERVER_X86_64={}",
            x86_path.display()
        );
    }

    if arm_path.exists() {
        println!("cargo:rustc-cfg=has_server_aarch64");
        println!(
            "cargo:rustc-env=JIBS_SERVER_AARCH64={}",
            arm_path.display()
        );
    }
}
