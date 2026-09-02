# trembita-proto

Wire types and the [`postcard`](https://crates.io/crates/postcard) codec shared across the
[trembita](https://crates.io/crates/trembita) distributed Raft + actor framework.

This crate defines the on-the-wire messages (Raft RPCs, client requests, actor
envelopes, membership) and the encode/decode helpers used by every other
`trembita-*` crate.

Most applications should depend on the [`trembita`](https://crates.io/crates/trembita)
facade rather than this crate directly.

## Documentation

- [docs.rs/trembita-proto](https://docs.rs/trembita-proto)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
