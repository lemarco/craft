//! `#[actor]` only applies to an `impl UserActor for T` block.

struct Widget;

#[trembita_runtime::actor]
impl Widget {
    fn frob(&self) {}
}

fn main() {}
