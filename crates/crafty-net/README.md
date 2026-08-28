# crafty-net

HTTP/3 (QUIC) transport with mutual TLS for the
[crafty](https://crates.io/crates/crafty) framework.

Provides the production `Transport` adapter (`QuicTransport`), TLS configuration,
peer routing, and the in-memory `LocalNetwork` used by tests and
[`crafty-sim`](https://crates.io/crates/crafty-sim).

Most applications should depend on the [`crafty`](https://crates.io/crates/crafty)
facade rather than this crate directly.

## Features

- `dev-certs` — mint a throwaway cluster CA and node certificates for local
  development and integration tests. Production deployments supply real PEM
  material instead.

## Documentation

- [docs.rs/crafty-net](https://docs.rs/crafty-net)
- [Repository](https://gitlab.com/lemarco/craft)

## License

Dual-licensed under `MIT OR Apache-2.0`.
