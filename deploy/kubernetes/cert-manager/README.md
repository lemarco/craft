# cert-manager for crafty mTLS (cert-automation)

Issues per-pod node certificates for the StatefulSet in `../statefulset.yaml`.

## Bootstrap

1. Install [cert-manager](https://cert-manager.io/docs/installation/).

2. Create the cluster CA Secret (once):

```bash
./examples/certs/generate.sh --ca-only --out /tmp/crafty-ca
kubectl create secret tls crafty-ca \
  --cert=/tmp/crafty-ca/ca.pem \
  --key=/tmp/crafty-ca/ca.key \
  --dry-run=client -o yaml | kubectl apply -f -
```

3. Apply issuers + certificates:

```bash
kubectl apply -f deploy/kubernetes/cert-manager/
kubectl wait --for=condition=Ready certificate/crafty-node-1 --timeout=120s
kubectl wait --for=condition=Ready certificate/crafty-node-2 --timeout=120s
kubectl wait --for=condition=Ready certificate/crafty-node-3 --timeout=120s
```

4. Deploy the cluster:

```bash
kubectl apply -f deploy/kubernetes/
```

## Pod ↔ Secret ↔ NodeId

| Pod       | Secret mount              | crafty `NodeId` | Certificate SAN   |
|-----------|---------------------------|----------------|-------------------|
| `crafty-0` | `crafty-tls-0` → `…/0/`    | 1              | `crafty-node-1`    |
| `crafty-1` | `crafty-tls-1` → `…/1/`    | 2              | `crafty-node-2`    |
| `crafty-2` | `crafty-tls-2` → `…/2/`    | 3              | `crafty-node-3`    |

Each pod mounts **all** ordinal Secrets so the template stays identical; `crafty-node`
selects `tls.crt` / `tls.key` from `/etc/crafty/tls-by-ordinal/<ordinal>/` using
`POD_NAME`.

## Renewal

cert-manager renews leaf certs before expiry. The kubelet syncs updated Secret data
into the mount; `crafty-node` detects the change via `CRAFTY_CERT_WATCH_SECS` (default
60s) and hot-reloads TLS without restarting the pod. Roll followers before the leader
when forcing a manual reload (see [docs/certs.md](../../../docs/certs.md)).

## Scaling

To add a node: increase StatefulSet `replicas`, add a matching `Certificate` CR
(`crafty-tls-N` / `crafty-node-(N+1)`), extend `CRAFTY_PEERS`, and add a `tls-N` volume
+ volumeMount in the pod template.
