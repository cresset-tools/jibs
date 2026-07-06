//! Build script for jibs_client
//!
//! Checks for pre-built musl server binaries and sets up environment
//! variables for embedding them into the client (see server_binary.rs).
//!
//! A missing binary produces a build WARNING (a dev building only the client
//! gets a runnable binary that errors at deploy time with instructions), but
//! with JIBS_REQUIRE_EMBEDDED_SERVER=1 it is a hard ERROR — release builds
//! set this (see .github/build-setup.yml) so a release can never ship a
//! client with empty embedded servers.

use std::env;
use std::path::Path;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(has_server_x86_64)");
    println!("cargo::rustc-check-cfg=cfg(has_server_aarch64)");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../target/x86_64-unknown-linux-musl/release/jibs-server");
    println!("cargo:rerun-if-changed=../../target/aarch64-unknown-linux-musl/release/jibs-server");
    println!("cargo:rerun-if-env-changed=JIBS_REQUIRE_EMBEDDED_SERVER");

    // Check for pre-built server binaries
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let workspace_root = Path::new(&manifest_dir).parent().unwrap().parent().unwrap();

    let required = env::var("JIBS_REQUIRE_EMBEDDED_SERVER").is_ok_and(|v| v == "1");
    let mut missing = Vec::new();

    let x86_path = workspace_root.join("target/x86_64-unknown-linux-musl/release/jibs-server");
    let arm_path = workspace_root.join("target/aarch64-unknown-linux-musl/release/jibs-server");

    if x86_path.exists() {
        println!("cargo:rustc-cfg=has_server_x86_64");
        println!("cargo:rustc-env=JIBS_SERVER_X86_64={}", x86_path.display());
    } else {
        missing.push("x86_64-unknown-linux-musl");
    }

    if arm_path.exists() {
        println!("cargo:rustc-cfg=has_server_aarch64");
        println!("cargo:rustc-env=JIBS_SERVER_AARCH64={}", arm_path.display());
    } else {
        missing.push("aarch64-unknown-linux-musl");
    }

    if !missing.is_empty() {
        let message = format!(
            "no pre-built jibs-server binary for {} — the client will not be able \
             to deploy to such remote hosts. Build the servers first with \
             ./scripts/build.sh (requires cargo-zigbuild and zig).",
            missing.join(" or ")
        );
        if required {
            panic!("JIBS_REQUIRE_EMBEDDED_SERVER is set but there is {message}");
        } else {
            println!("cargo:warning={message}");
        }
    }
}
