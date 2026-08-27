# mTLS certificates for a craft cluster

Every production network path in craft is mutually authenticated (ADR 006):
peers and clients present a certificate issued by the **cluster CA**, and both
ends verify the other against that CA. This page shows how to mint that PKI —
with the bundled script or by hand with OpenSSL — and how to hand it to
`craft-node` (or your own binary embedding `craft`).

> **Naming contract.** A node's certificate binds it to its `NodeId`: the
> Common Name **and** a DNS Subject Alternative Name must be
> `craft-node-<id>` (e.g. `craft-node-2`). This is the server name craft dials
> over QUIC, so the SAN must match or the handshake fails. The script does this
> for you.

---

## Quick start (the script)

`examples/certs/generate.sh` uses only `openssl`, so it runs on Linux (OpenSSL)
and macOS (LibreSSL) with no Rust toolchain.

```bash
# First VPS — creates the cluster CA next to node 1's cert:
./examples/certs/generate.sh --node-id 1 --out ./certs

# Every additional VPS — reuse the same CA (copy ca.pem/ca.key there, or point at them):
./examples/certs/generate.sh --node-id 2 --out ./certs --ca ./certs/ca.pem --ca-key ./certs/ca.key
./examples/certs/generate.sh --node-id 3 --out ./certs --ca ./certs/ca.pem --ca-key ./certs/ca.key

# A client certificate for an app that uses RemoteClient:
./examples/certs/generate.sh --client --name my-app --out ./certs --ca ./certs/ca.pem --ca-key ./certs/ca.key

# Or just the CA:
./examples/certs/generate.sh --ca-only --out ./certs
```

Artifacts written to `--out`:

| File | Purpose | Distribute to |
|------|---------|---------------|
| `ca.pem` | Cluster CA certificate (public trust anchor) | **every** node and client |
| `ca.key` | Cluster CA private key | **keep offline / secret** — signs new certs only |
| `node-<id>.pem` / `.key` | One node's mTLS identity | that VPS only |
| `client-<name>.pem` / `.key` | A `RemoteClient` identity | that client app only |

Keys are P-256 (ECDSA), which the rustls `ring` provider craft uses supports
natively. The private-key files are written `chmod 600`.

---

## Running `craft-node` with the certs

`craft-node` reads its cert material from the environment. Give **every** node
the shared `ca.pem`, its own `node-<id>` pair, and the full member list:

```bash
# on VPS 1
export CRAFT_NODE_ID=1
export CRAFT_LISTEN=0.0.0.0:7443
export CRAFT_ADMIN=0.0.0.0:8080
export CRAFT_PEERS="1@10.0.0.1:7443,2@10.0.0.2:7443,3@10.0.0.3:7443"
export CRAFT_CA_CERT=/etc/craft/ca.pem
export CRAFT_NODE_CERT=/etc/craft/node-1.pem
export CRAFT_NODE_KEY=/etc/craft/node-1.key
craft-node
```

`CRAFT_PEERS` is the same on every node (id → reachable address for all
members); each node fills in `CRAFT_NODE_ID` / `CRAFT_NODE_*` for itself.

### Environment variable reference

| Variable | Meaning |
|----------|---------|
| `CRAFT_CA_CERT` | PEM cluster CA — the trust anchor for peers **and** clients |
| `CRAFT_NODE_CERT` | PEM certificate chain for this node (leaf `craft-node-<id>`) |
| `CRAFT_NODE_KEY` | PEM private key matching `CRAFT_NODE_CERT` |
| `CRAFT_CLIENT_CERT` / `CRAFT_CLIENT_KEY` | Client identity for a split `RemoteClient` process (used by your client app, not the node) |

### Embedding craft in your own binary

If you build your own binary instead of using `craft-node`, load the PEM files
into a [`Security`](https://docs.rs/craft) and pass it to `start_quic`:

```rust,ignore
use craft::{CraftCluster, NodeId, PeerDirectory, Security};
use craft::net::NodeIdentity;

// Load node-<id>.pem / .key and ca.pem (e.g. with rustls-pemfile), then:
let identity = NodeIdentity::from_der(NodeId(1), cert_chain, key);
let security = Security::from_ca_certs(identity, &ca_certs)?;

let cluster = CraftCluster::builder(NodeId(1), my_state_machine)
    .members([NodeId(1), NodeId(2), NodeId(3)])
    .start_quic(security, "0.0.0.0:7443".parse()?, peers)
    .await?;
```

---

## Manual OpenSSL equivalent

The script is just these steps. Adjust `-days` and the curve to taste.

**1. Cluster CA** (do this once, keep `ca.key` secret):

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1 -out ca.key
cat > ca.cnf <<'EOF'
[req]
distinguished_name = dn
x509_extensions = v3_ca
prompt = no
[dn]
CN = craft cluster CA
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF
openssl req -x509 -new -key ca.key -days 3650 -out ca.pem -config ca.cnf
```

**2. A node certificate** (repeat per node, changing the id):

```bash
ID=1
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1 -out node-$ID.key
openssl req -new -key node-$ID.key -out node-$ID.csr -subj "/CN=craft-node-$ID"
cat > node-$ID.ext <<EOF
subjectAltName = DNS:craft-node-$ID
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
EOF
openssl x509 -req -in node-$ID.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days 825 -extfile node-$ID.ext -out node-$ID.pem
```

A node acts as both TLS **server** (accepting peers/clients) and **client**
(dialing peers), so its cert carries both `serverAuth` and `clientAuth` EKUs.

**3. A client certificate** (for an app using `RemoteClient`):

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1 -out client-app.key
openssl req -new -key client-app.key -out client-app.csr -subj "/CN=craft-client-app"
cat > client-app.ext <<'EOF'
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = clientAuth
EOF
openssl x509 -req -in client-app.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days 825 -extfile client-app.ext -out client-app.pem
```

The server verifies only that a client cert **chains to the CA**, so the client
CN is free-form.

---

## Joining a new VPS

1. Copy `ca.pem` (and, if signing on the box, `ca.key`) to the new host.
2. Mint its cert: `generate.sh --node-id <N> --out ./certs --ca ca.pem --ca-key ca.key`.
3. Point the new node at **one existing member** as a seed and let it join the
   live cluster — no restart of the running nodes, and no cluster-wide address
   list required (ADR 007/017):

   ```rust
   let cluster = CraftCluster::builder(NodeId(N), machine)
       .members(current_voters)          // the cluster's current voter set (not this node)
       .join(seed_id, seed_addr)         // contact any member; it forwards to the leader
       .start_quic(security, listen, [(seed_id, seed_addr)].into_iter().collect())
       .await?;
   ```

   The joiner fetches the peer-address book from the seed over `/cluster/peers`,
   the leader commits a membership change adding it, and addresses propagate both
   ways so every node can reach the newcomer. See the `vps_join` example
   (`cargo run -p craft --example vps_join --features dev-certs`).

> **Static membership still works** for fixed clusters: bootstrap the full member
> set up front via matching `CRAFT_PEERS` + `.members(...)`. Dynamic `join` is the
> elastic path; the reference `craft-node` binary reads a static `CRAFT_PEERS`.

---

## Rotation

### Manual (always works)

1. Reissue the leaf cert from the same CA (`generate.sh --node-id <N> ... --ca ...`).
2. Write the new `node-<N>.pem`/`.key` to the paths in `CRAFT_NODE_*`.
3. Either **hot-reload** (below) or restart that node **one at a time** (rolling
   restart) so the cluster keeps quorum.

### Automatic (ADR 034)

When `CRAFT_NODE_CERT` / `CRAFT_NODE_KEY` / `CRAFT_CA_CERT` are set, `craft-node`
uses [`start_quic_pem`](../crates/craft/src/builder.rs) and **polls** those files
every `CRAFT_CERT_WATCH_SECS` (default **60**). When a renewer (cert-manager,
`step ca renew`, or `generate.sh`) rewrites the PEMs, craft reloads TLS **without
exiting**:

- new QUIC handshakes pick up the fresh server cert;
- outbound pools are evicted so the next dial presents the new client cert;
- **`SIGHUP`** triggers the same reload on Unix (`docker compose kill -s HUP …`).

Reload on the **Raft leader** is rejected unless you call
[`ReloadOpts { allow_leader: true }`](../crates/craft/src/certs.rs) — roll
**followers first**, leader last.

| Environment | Issuer | Example |
|-------------|--------|---------|
| VPS / compose | [step-ca](https://smallstep.com/docs/step-ca/) | [`examples/step-ca/`](../examples/step-ca/) |
| Kubernetes | [cert-manager](https://cert-manager.io/) | [`deploy/kubernetes/`](../deploy/kubernetes/) + [`cert-manager/README.md`](../deploy/kubernetes/cert-manager/README.md) |

On Kubernetes, the StatefulSet mounts cert-manager Secrets per pod ordinal:

```yaml
# craft-0 reads /etc/craft/tls-by-ordinal/0/tls.crt (Secret craft-tls-0)
CRAFT_CERT_ORDINAL_BASE=/etc/craft/tls-by-ordinal
CRAFT_CA_CERT=/etc/craft/ca/tls.crt
POD_NAME=craft-0   # downward API
```

Apply order: populate `craft-ca` → `kubectl apply -f deploy/kubernetes/cert-manager/`
→ wait for `Certificate` Ready → `kubectl apply -f deploy/kubernetes/`.

Public ACME (Let's Encrypt) is **not** supported for craft wire identities — you
need a private CA with `serverAuth` + `clientAuth` and SAN `craft-node-<id>`.

Embedding apps use the same API:

```rust
let pem = PemSecurity::load(node_id, paths)?;
let cluster = CraftCluster::builder(node_id, machine)
    .members(members)
    .cert_watch(Duration::from_secs(60))
    .start_quic_pem(pem, listen, peers)
    .await?;
// cluster.cert_reload() → manual reload_now(...)
```

See [ADR 034](decisions/034-cert-automation.md).

### CA rotation

To rotate the **CA** itself, run a temporary trust bundle containing both old
and new CA certs in `ca.pem` (concatenated), roll every node onto certs signed
by the new CA, then drop the old CA from the bundle. Plan this as a staged
rollout.

---

## Local development

For a single machine you don't need any of this: run `craft-node` with **no**
cert variables and it mints a throwaway in-memory CA for a one-node cluster.
Tests and the simulator use the in-memory `LocalNetwork` transport, which skips
TLS entirely. These paths are **dev-only** — every real deployment uses the
mTLS material above.

---

## Redis TLS (optional)

If you point actor workflow state at Redis (ADR 021) over TLS, that connection
is **managed by you** via the Redis connection URL (`rediss://…`) and your
Redis server's own certificates — it is independent of the cluster CA above.

Use [`craft_store_redis::RedisStore::connect`] when the Redis CA is in the
OS / public trust store, or [`RedisStore::connect_with_tls`] with a PEM trust
anchor (and optional client cert for Redis mTLS):

```rust,no_run
# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
use craft_store_redis::{RedisStore, RedisTlsConfig};

let ca = std::fs::read("/etc/redis/ca.pem")?;
let store = RedisStore::connect_with_tls(
    "rediss://redis.internal:6379",
    &RedisTlsConfig::with_root_ca_pem(ca),
)
.await?;
# Ok(())
# }
```

See [ADR 021](decisions/021-actor-state-redis.md).
