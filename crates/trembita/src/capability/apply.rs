//! Trait for wiring [`CapGroup`](super::group::CapGroup) into the app builder.

use crate::TrembitaAppBuilder;

use super::runtime::CapRuntime;

/// Type-erased capability group registration.
pub(crate) trait CapGroupApply: Send {
    fn apply(
        self: Box<Self>,
        builder: TrembitaAppBuilder,
        runtime: &mut CapRuntime,
    ) -> TrembitaAppBuilder;
}
