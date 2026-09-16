//! Capability groups for the background-jobs showcase.

pub mod email;
pub mod ledger;

use trembita::CapManifest;

#[must_use]
pub fn manifest() -> CapManifest {
    ledger::manifest()
        .group(email::group())
}
