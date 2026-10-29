# Kubernetes deployment

LikhaDB is defined as a Kustomize base with `dev`, `staging`, and `production`
overlays. Each overlay deploys one replica with its own ReadWriteOnce persistent
volume. Multiple replicas are not supported by the current local WAL design.

Render an environment without changing a cluster:

```sh
kustomize build deploy/overlays/dev
kustomize build deploy/overlays/staging
kustomize build deploy/overlays/production
```

The committed image digest is intentionally all zeroes and cannot be pulled. It
prevents an accidental deployment of a mutable image before image publishing and
Kargo promotion are implemented. To test an overlay manually, replace the
`digest` value in its `kustomization.yaml` with a published LikhaDB digest.

Development can start without an API token. Staging and production require a
Secret named `likhadb-secrets` with an `api-token` key. Create it through the
environment's secret-management process; do not commit the token. For example:

```sh
kubectl --namespace likhadb-staging create secret generic likhadb-secrets \
  --from-literal=api-token='<token>'
```

Applying a rendered overlay directly is useful for troubleshooting, but Argo CD
will be the normal reconciler once the GitOps stages are implemented.
