//! Chat capability group — sticky session append + line count.

mod append;
mod line_count;

use append::append_register;
use line_count::line_count_register;
use trembita::{CapManifest, cap_register_chain, CapGroup};

#[derive(Default)]
pub struct State {
    pub(crate) history: Vec<String>,
}

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<State>::for_cap::<Append>()
            .per_node()
            .default_queue_for::<Append>(),
        append_register,
        line_count_register,
    ))
}

pub use append::Append;
pub use line_count::{LineCount, LineCountAck};
