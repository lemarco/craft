# crafty-proto

Wire types and the [`postcard`](https://crates.io/crates/postcard) codec shared across the
[crafty](https://crates.io/crates/crafty) distributed Raft + actor framework.

This crate defines the on-the-wire messages (Raft RPCs, client requests, actor
envelopes, membership) and the encode/decode helpers used by every other
`crafty-*` crate.

Most applications should depend on the [`crafty`](https://crates.io/crates/crafty)
facade rather than this crate directly.

## Documentation

- [docs.rs/crafty-proto](https://docs.rs/crafty-proto)
- [Repository](https://gitlab.com/lemarco/craft)

## License

Dual-licensed under `MIT OR Apache-2.0`.
