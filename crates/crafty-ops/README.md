# crafty-ops

Internal operational CLI for [crafty](https://gitlab.com/lemarco/craft): snapshot
backup and restore of node data directories (local gzip-tar plus S3/GCS via
OpenDAL).

**Not published to crates.io** (`publish = false`). Build from the repository:

```bash
cargo build -p crafty-ops --release
```

See [docs/ops/backup-restore.md](../../docs/ops/backup-restore.md).

## License

Dual-licensed under `MIT OR Apache-2.0`.
