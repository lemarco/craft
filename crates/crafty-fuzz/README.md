# crafty-fuzz

LibFuzzer targets for [`crafty-proto`](../crafty-proto) wire decoders.

Requires **nightly** Rust and [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz):

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
cd crates/crafty-fuzz
cargo +nightly fuzz run wire_decode -- -max_total_time=300
```

CI runs the same target on the scheduled **heavy** lane once enabled in `.gitlab-ci.yml`.
