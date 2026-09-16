# trembita-assembly

Cluster boot and wiring for the [trembita](https://crates.io/crates/trembita) workspace: `TrembitaClusterBuilder`, env merge, journals, upgrade hooks.

**Product apps should depend on the [`trembita`](https://crates.io/crates/trembita) facade**, not this crate directly. Published so the `trembita` crate can resolve on crates.io.

See [facade-layering](https://gitlab.com/lemarco/trembita/-/blob/master/docs/decisions/facade-layering.md).
