//! `#[actor(bogus)]` must be rejected with a clear macro error.

struct Widget;

#[trembita_actor::actor(bogus)]
impl trembita_actor::UserActor for Widget {
    type Config = ();
    type Message = ();
    type Error = std::io::Error;

    fn start(_config: Self::Config) -> Result<Self, Self::Error> {
        Ok(Widget)
    }

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn main() {}
