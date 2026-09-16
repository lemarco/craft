//! Capability proc macros (`#[cap_handler]`, `#[cap_request]`).

use serde::{Deserialize, Serialize};
use trembita::{CapError, CapRequest, cap_handler, cap_request};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DemoAck {
    ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DemoReq {
    n: u32,
}

#[cap_handler(group = "demo")]
async fn demo_run(msg: DemoReq, _state: &mut ()) -> Result<DemoAck, CapError> {
    Ok(DemoAck { ok: msg.n > 0 })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Keyed {
    label: String,
}

#[cap_handler(group = "demo", key = "label")]
async fn keyed_run(msg: Keyed, _state: &mut ()) -> Result<DemoAck, CapError> {
    Ok(DemoAck {
        ok: !msg.label.is_empty(),
    })
}

#[cap_request(group = "orphan", reply = DemoAck)]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrphanDto {
    x: u8,
}

#[test]
fn cap_handler_infers_op_from_request_type() {
    assert_eq!(DemoReq::GROUP, "demo");
    assert_eq!(DemoReq::OP, "demo_req");
    assert_eq!(DemoReq::QUEUE_STREAM, "demo.demo_req");
}

#[test]
fn cap_handler_key_field() {
    let k = Keyed {
        label: "user-1".into(),
    };
    assert_eq!(k.cap_key(), Some("user-1".to_string()));
}

#[test]
fn cap_request_for_dto_without_handler() {
    assert_eq!(OrphanDto::OP, "orphan_dto");
}

#[test]
fn cap_handler_async_register() {
    let _ = demo_run_register;
    let _ = keyed_run_register;
}
