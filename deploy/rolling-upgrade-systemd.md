# Rolling upgrade on systemd (B-45)

Two supported paths on VPS — both assume **`Restart=always`** in [systemd/trembita.service](systemd/trembita.service) and **`TimeoutStopSec`** ≥ gateway + actor drain ([drain-timeout](../docs/decisions/drain-timeout.md)).

## A. Manual binary roll (always available)

Use when `/cluster/upgrade` is not wired or you prefer SSH + symlink swap.

1. **Preflight:** same `app_version` on all nodes for state-machine changes ([rolling-upgrade](../docs/ops/rolling-upgrade.md)).
2. **Backup:** `trembita-ops backup export` or filesystem snapshot of `TREMBITA_DATA_DIR`.
3. **One node at a time:**
   - Confirm `GET /ready` is 200 on the target.
   - `systemctl stop myapp.service` (graceful SIGTERM → leave + drain).
   - Install new binary: `ln -sfn /opt/myapp/releases/myapp-X.Y.Z /opt/myapp/current`.
   - `systemctl start myapp.service`.
   - Wait until `GET /ready` returns 200 and `join_phase` is `pool_ready` ([B-35](../docs/decisions/cluster-elasticity.md#join-readiness-pipeline-b-35)).
4. Repeat for remaining backends; keep **`TREMBITA_GATEWAY_SESSION_SECRET`** unchanged unless following [runbook § B-40](../docs/ops/production-runbook.md#gateway-session-rotation-b-40).

**LB:** remove draining backend from rotation when `/ready` ≠ 200 ([ingress-lb](../docs/ops/ingress-lb.md)).

## B. In-cluster upgrade coordinator (optional)

When your app embeds upgrade state in its `StateMachine` and exposes HTTP routes:

| Route | Purpose |
|-------|---------|
| `POST /cluster/upgrade/desired` | Leader proposes target artifact (URL + sha256) |
| `GET /cluster/upgrade` | Poll `UpgradeView` — granted node, phase, failures |

Flow (leader grants **one node at a time**):

```text
download → verify → atomic install → graceful leave → exit 0 → systemd restart → Report Ready
```

Details: [upgrade-coordinator](../docs/decisions/upgrade-coordinator.md) · reference binary behaviour: [examples/self-update](../examples/self-update/README.md).

**Atomic install** (symlink fleet):

```bash
install -m755 "$tmp" "/opt/myapp/releases/myapp-$VERSION"
ln -sfn "/opt/myapp/releases/myapp-$VERSION" "/opt/myapp/current"
```

`ExecStart=/opt/myapp/current` in systemd.

## C. External operator (B-54)

CI / release pipeline driver — **`trembita-ops upgrade run`** (path B only):

1. Preflight: `GET /ready`, `GET /cluster/upgrade` (404 → wire `UpgradeApi`; see [upgrade-coordinator](../docs/decisions/upgrade-coordinator.md)).
2. Optional warning if `GET /introspect/ops-summary` reports `join.phase` ≠ `pool_ready`.
3. `POST /cluster/upgrade/desired` with artifact URL + sha256 + target `app_version`.
4. Poll `GET /cluster/upgrade` until `fleet_ready` (leader still grants one node at a time on each backend).
5. Post smoke: `GET /ready`.

Build: `cargo build -p trembita-tools --release --bin trembita-ops`. Details: [capabilities § B-54](../docs/scenarios/capabilities.md#ci-cluster-upgrade-operator-b-54) · [runbook § B-54](../docs/ops/production-runbook.md#ci-cluster-upgrade-operator-b-54).

## Post-upgrade smoke

- [ ] `GET /introspect/ops-summary` — `join.phase` = `pool_ready`, version fields consistent
- [ ] Sample authenticated product request through LB
- [ ] Queue depth / saga introspect if you use jobs/workflows

## B-49 — automated proof vs manual lab

| Layer | Command |
|-------|---------|
| **CI (sim)** | `./scripts/test-fast.sh -p trembita --test rolling_upgrade_proof b49_` — 3-node coordinator dry-run, HTTP `POST/GET /cluster/upgrade*`, shared session on peer, job with one node stopped |
| **Heavy lab (optional, `run-heavy`)** | `./scripts/rolling-upgrade-lab.sh` → [examples/self-update](../examples/self-update/README.md) — real QUIC cluster + `trigger-upgrade.sh` (two-version artifact, dry-run default) |

Session cookie across LB: [capabilities § B-34](../docs/scenarios/capabilities.md#elastic-join--lb-b-34) (`b34_cluster_session_cookie_valid_on_peer_gateway`). Keep **`TREMBITA_GATEWAY_SESSION_SECRET`** unchanged during patch rolls ([runbook § B-40](../docs/ops/production-runbook.md#gateway-session-rotation-b-40)).
