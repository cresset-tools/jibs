//! Integration test for server protocol

use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::process::{Command, Stdio};

use jibs_protocol::{
    framing::{read_message, write_message},
    ClientMessage, CompressionMode, ExecutionPlan, ResolvedAggregate,
};

#[test]
fn test_echo_mode_parses_init_message() {
    // Create a simple execution plan
    let plan = ExecutionPlan {
        variables: HashMap::new(),
        relations: vec![],
        aggregates: vec![ResolvedAggregate {
            name: "test_aggregate".to_string(),
            root_table: "users".to_string(),
            where_clause: Some("id = 1".to_string()),
            order_by: None,
            order_direction: None,
            limit: Some(100),
            exclude_tables: vec![],
            exclude_patterns: vec![],
            root_only: false,
        }],
        excluded_tables: Default::default(),
        excluded_patterns: Default::default(),
        ignored_tables: Default::default(),
        ignored_patterns: Default::default(),
        ignored_relations: vec![],
        anonymization: Default::default(),
        fakers: Default::default(),
        preserves: vec![],
        sets: vec![],
        full_tables: Default::default(),
        full_patterns: Default::default(),
        after_statements: vec![],
        aggregates_only: false,
    };

    let init_msg = ClientMessage::Init {
        plan,
        compression: CompressionMode::None,
        parallel: 1,
        collect_metrics: false,
    };

    // Serialize the message
    let mut buffer = Vec::new();
    write_message(&mut buffer, &init_msg).unwrap();

    // Run the server in echo mode
    let mut child = Command::new(env!("CARGO_BIN_EXE_jibs-server"))
        .arg("--echo")
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn server");

    // Write the message to stdin
    {
        let stdin = child.stdin.as_mut().expect("Failed to open stdin");
        stdin.write_all(&buffer).expect("Failed to write to stdin");
    }

    // Wait for completion and check output
    let output = child.wait_with_output().expect("Failed to wait on child");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(output.status.success(), "Server failed: {}", stderr);
    assert!(
        stderr.contains("Received Init message"),
        "Missing init message log: {}",
        stderr
    );
    assert!(
        stderr.contains("test_aggregate"),
        "Missing aggregate name: {}",
        stderr
    );
    assert!(
        stderr.contains("users"),
        "Missing root table: {}",
        stderr
    );
}

#[test]
fn test_message_roundtrip() {
    // Test that messages can be serialized and deserialized correctly
    let plan = ExecutionPlan::new();
    let init_msg = ClientMessage::Init {
        plan: plan.clone(),
        compression: CompressionMode::Zstd,
        parallel: 4,
        collect_metrics: false,
    };

    let mut buffer = Vec::new();
    write_message(&mut buffer, &init_msg).unwrap();

    let mut cursor = Cursor::new(buffer);
    let decoded: ClientMessage = read_message(&mut cursor).unwrap();

    match decoded {
        ClientMessage::Init {
            plan: decoded_plan,
            compression,
            parallel,
            collect_metrics: _,
        } => {
            assert_eq!(compression, CompressionMode::Zstd);
            assert_eq!(parallel, 4);
            assert_eq!(decoded_plan.aggregates.len(), plan.aggregates.len());
        }
        _ => panic!("Expected Init message"),
    }
}
