# trembita-macros

Proc-macro derives for the [trembita](https://crates.io/crates/trembita) framework:

- `StateMachine` — boilerplate for Raft state machines
- `UserActor` — actor handler wiring

Re-exported by the [`trembita`](https://crates.io/crates/trembita) facade; depend on
this crate directly only when building a `#![no_std]`-free macro-only layer.

## Documentation

- [docs.rs/trembita-macros](https://docs.rs/trembita-macros)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
