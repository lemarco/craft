# trembita-ops

Internal operational CLI for [trembita](https://gitlab.com/lemarco/trembita): snapshot
backup and restore of node data directories (local gzip-tar plus S3/GCS via
OpenDAL).

**Not published to crates.io** (`publish = false`). Build from the repository:

```bash
cargo build -p trembita-ops --release
```

See [docs/ops/backup-restore.md](../../docs/ops/backup-restore.md).

## License

Dual-licensed under `MIT OR Apache-2.0`.
