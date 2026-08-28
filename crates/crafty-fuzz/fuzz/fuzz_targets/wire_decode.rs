//! Fuzz every production wire type decoded by `crafty-proto` (testing-strategy / T4).

#![no_main]

use crafty_proto::{
    ActorEnvelope, ClientRequest, GroupMigrateRequest, GroupPeerEnvelope, JoinRequest, LeaveRequest,
    RaftRpc, decode,
};
use libfuzzer_sys::fuzz_target;

fn try_decode<T: serde::de::DeserializeOwned>(data: &[u8]) {
    let _ = decode::<T>(data);
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    try_decode::<RaftRpc>(data);
    try_decode::<ClientRequest>(data);
    try_decode::<JoinRequest>(data);
    try_decode::<LeaveRequest>(data);
    try_decode::<GroupPeerEnvelope>(data);
    try_decode::<GroupMigrateRequest>(data);
    try_decode::<ActorEnvelope>(data);
});
