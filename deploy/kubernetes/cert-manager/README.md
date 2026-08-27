# cert-manager for craft mTLS (ADR 034)

Issues per-pod node certificates for the StatefulSet in `../statefulset.yaml`.

## Bootstrap

1. Install [cert-manager](https://cert-manager.io/docs/installation/).

2. Create the cluster CA Secret (once):

```bash
./examples/certs/generate.sh --ca-only --out /tmp/craft-ca
kubectl create secret tls craft-ca \
  --cert=/tmp/craft-ca/ca.pem \
  --key=/tmp/craft-ca/ca.key \
  --dry-run=client -o yaml | kubectl apply -f -
```

3. Apply issuers + certificates:

```bash
kubectl apply -f deploy/kubernetes/cert-manager/
kubectl wait --for=condition=Ready certificate/craft-node-1 --timeout=120s
kubectl wait --for=condition=Ready certificate/craft-node-2 --timeout=120s
kubectl wait --for=condition=Ready certificate/craft-node-3 --timeout=120s
```

4. Deploy the cluster:

```bash
kubectl apply -f deploy/kubernetes/
```

## Pod ↔ Secret ↔ NodeId

| Pod       | Secret mount              | craft `NodeId` | Certificate SAN   |
|-----------|---------------------------|----------------|-------------------|
| `craft-0` | `craft-tls-0` → `…/0/`    | 1              | `craft-node-1`    |
| `craft-1` | `craft-tls-1` → `…/1/`    | 2              | `craft-node-2`    |
| `craft-2` | `craft-tls-2` → `…/2/`    | 3              | `craft-node-3`    |

Each pod mounts **all** ordinal Secrets so the template stays identical; `craft-node`
selects `tls.crt` / `tls.key` from `/etc/craft/tls-by-ordinal/<ordinal>/` using
`POD_NAME`.

## Renewal

cert-manager renews leaf certs before expiry. The kubelet syncs updated Secret data
into the mount; `craft-node` detects the change via `CRAFT_CERT_WATCH_SECS` (default
60s) and hot-reloads TLS without restarting the pod. Roll followers before the leader
when forcing a manual reload (see [docs/certs.md](../../../docs/certs.md)).

## Scaling

To add a node: increase StatefulSet `replicas`, add a matching `Certificate` CR
(`craft-tls-N` / `craft-node-(N+1)`), extend `CRAFT_PEERS`, and add a `tls-N` volume
+ volumeMount in the pod template.
