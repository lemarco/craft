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
3. Add `<N>@<addr>` to `CRAFT_PEERS` on every node and restart them (static
   membership; see below).

> **v1 membership is static.** Bootstrap the full member set up front via
> matching `CRAFT_PEERS` + `.members(...)`. Dynamic `JOIN_ADDR` onboarding
> (a node joining a live cluster without a restart) is a planned follow-up.

---

## Rotation

There is **no auto-rotation in v1**. To rotate a node's cert:

1. Reissue it from the same CA (`generate.sh --node-id <N> ... --ca ...`).
2. Deploy the new `node-<N>.pem`/`.key`.
3. Restart that node. Do this **one node at a time** (rolling restart) so the
   cluster keeps quorum throughout.

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
See [ADR 021](decisions/021-actor-state-redis.md).
