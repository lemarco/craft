# trembita-net

HTTP/3 (QUIC) transport with mutual TLS for the
[trembita](https://crates.io/crates/trembita) framework.

Provides the production `Transport` adapter (`QuicTransport`), TLS configuration,
peer routing, and the in-memory `LocalNetwork` used by tests and
[`trembita-sim`](https://crates.io/crates/trembita-sim).

Most applications should depend on the [`trembita`](https://crates.io/crates/trembita)
facade rather than this crate directly.

## Features

- `dev-certs` — mint a throwaway cluster CA and node certificates for local
  development and integration tests. Production deployments supply real PEM
  material instead.

## Documentation

- [docs.rs/trembita-net](https://docs.rs/trembita-net)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
