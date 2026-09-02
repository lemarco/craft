//! `#[actor]` only applies to an `impl UserActor for T` block.

struct Widget;

#[trembita_actor::actor]
impl Widget {
    fn frob(&self) {}
}

fn main() {}
