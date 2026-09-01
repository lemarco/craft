//! `#[actor]` only applies to an `impl UserActor for T` block.

struct Widget;

#[crafty_actor::actor]
impl Widget {
    fn frob(&self) {}
}

fn main() {}
