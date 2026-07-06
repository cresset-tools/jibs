//! Wire-format snapshot: pins the encoded bytes of representative messages.
//!
//! If this test fails, the wire format changed — bincode encodes enum
//! variants by index, so even reordering a variant breaks compatibility.
//! When the change is intentional:
//!   1. bump `PROTOCOL_VERSION` in crates/protocol/src/handshake.rs
//!   2. update the expected hex below
//! Shipping a wire change without a version bump makes mixed client/server
//! pairs fail with confusing errors instead of the handshake's clear one.

use jibs_protocol::{
    write_message, ClientMessage, CompressionMode, ExecutionPlan, Relation, ResolvedAggregate,
    ServerMessage, TableInfo,
};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[test]
fn client_init_wire_format_is_stable() {
    // Deterministic plan: only Vec-based fields populated (HashMap iteration
    // order would make the snapshot flaky)
    let mut plan = ExecutionPlan::new();
    plan.relations.push(Relation {
        from_table: "orders".to_string(),
        from_column: "user_id".to_string(),
        to_table: "users".to_string(),
        to_column: "id".to_string(),
    });
    plan.aggregates.push(ResolvedAggregate {
        name: "user_orders".to_string(),
        root_table: "orders".to_string(),
        where_clause: Some("user_id = 1".to_string()),
        order_by: None,
        order_direction: None,
        limit: Some(100),
        exclude_tables: vec![],
        exclude_patterns: vec![],
        root_only: false,
    });

    let msg = ClientMessage::Init {
        plan,
        compression: CompressionMode::Zstd,
        parallel: 4,
        collect_metrics: false,
        dry_run: false,
    };

    let mut buffer = Vec::new();
    write_message(&mut buffer, &msg).unwrap();

    assert_eq!(
        hex(&buffer),
        "54000000010001066f726465727307757365725f6964057573657273026964010b757365725f6f7264657273066f7264657273010b757365725f6964203d2031000001c80000000000000000000000000000000001040000",
        "ClientMessage::Init wire format changed — bump PROTOCOL_VERSION and update this snapshot"
    );
}

#[test]
fn server_ready_wire_format_is_stable() {
    let msg = ServerMessage::Ready {
        tables: vec![TableInfo {
            name: "users".to_string(),
            table_id: 0,
            estimated_rows: 5,
            primary_key: vec!["id".to_string()],
        }],
        compression: CompressionMode::Zstd,
    };

    let mut buffer = Vec::new();
    write_message(&mut buffer, &msg).unwrap();

    assert_eq!(
        hex(&buffer),
        "0f000000000105757365727300050102696401",
        "ServerMessage::Ready wire format changed — bump PROTOCOL_VERSION and update this snapshot"
    );
}
