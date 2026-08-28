//! `#[remote_actor]` only applies to an `impl UserActor for T` block.

struct Widget;

#[crafty_actor::remote_actor]
impl Widget {
    fn frob(&self) {}
}

fn main() {}
