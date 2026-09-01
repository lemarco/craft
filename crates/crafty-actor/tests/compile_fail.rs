//! Compile-fail suite for the `#[crafty_actor::actor]` attribute (backlog
//! D3 / T3, testing-strategy). Each case in `tests/ui/` must fail to compile with the
//! recorded diagnostic — proving the macro rejects misuse: an unknown option,
//! and application to something other than an `impl UserActor for T` block.
//!
//! The generated codecs also enforce that the actor's `Config`/`Message` types
//! are `serde`-serializable (a non-`serde` type fails to compile), but that
//! diagnostic originates in the `serde` trait solver and its exact text drifts
//! across `serde`/`rustc` versions, so it is intentionally not snapshotted
//! here; the positive round-trip coverage lives in `actor_macro.rs`.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
