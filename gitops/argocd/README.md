# Argo CD applications

These resources declare one Argo CD project and one application for each LikhaDB
environment. Argo CD is the only controller that applies LikhaDB workload
resources to Kubernetes; Kargo changes the desired Git revision during a
promotion.

## Bootstrap

Install Argo CD before applying this directory. The `argocd` namespace and Argo
CD custom resource definitions must already exist.

```sh
kubectl apply --server-side --kustomize gitops/argocd
```

The applications intentionally track `env/dev`, `env/staging`, and
`env/production`. Until Kargo creates those branches in the promotion phase,
Argo CD will report repository revision errors. Do not change the applications
to track `main` to bypass that state.

If the GitHub repository is private, register it with Argo CD before applying
the applications. Keep repository credentials in Argo CD's credential store or
an external secret manager, not in this directory.

## Cluster destinations

The local reference environment runs all three applications in one cluster and
isolates them with namespaces. Accordingly, the committed applications target
`https://kubernetes.default.svc`.

For separate real clusters, register each cluster with Argo CD, replace each
application's `spec.destination.server` with its registered API server (or use
`spec.destination.name`), and update the matching `AppProject` destination. The
overlay and environment branch mapping does not change.

## Reconciliation behavior

All applications enable automated synchronization, pruning, and self-healing.
Promotion approval is therefore the Git change: after Kargo updates an
environment branch, Argo CD reconciles it without a second deployment approval.
The project limits each application to the repository, namespaces, cluster, and
resource kinds needed by LikhaDB.
