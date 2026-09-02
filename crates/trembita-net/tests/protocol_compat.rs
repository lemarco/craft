//! Wire protocol version checks (rolling upgrade).

use trembita_net::wire::check_protocol_version;
use trembita_proto::{MIN_COMPATIBLE_PROTOCOL_VERSION, PROTOCOL_VERSION};

#[test]
fn wire_accepts_compatible_protocol_versions() {
    check_protocol_version(None).unwrap();
    check_protocol_version(Some(MIN_COMPATIBLE_PROTOCOL_VERSION)).unwrap();
    check_protocol_version(Some(PROTOCOL_VERSION)).unwrap();
}

#[test]
fn wire_rejects_out_of_band_protocol_versions() {
    assert!(check_protocol_version(Some(0)).is_err());
    assert!(check_protocol_version(Some(PROTOCOL_VERSION + 1)).is_err());
}
