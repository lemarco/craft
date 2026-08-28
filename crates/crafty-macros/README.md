# crafty-macros

Proc-macro derives for the [crafty](https://crates.io/crates/crafty) framework:

- `StateMachine` — boilerplate for Raft state machines
- `UserActor` — actor handler wiring

Re-exported by the [`crafty`](https://crates.io/crates/crafty) facade; depend on
this crate directly only when building a `#![no_std]`-free macro-only layer.

## Documentation

- [docs.rs/crafty-macros](https://docs.rs/crafty-macros)
- [Repository](https://gitlab.com/lemarco/craft)

## License

Dual-licensed under `MIT OR Apache-2.0`.
