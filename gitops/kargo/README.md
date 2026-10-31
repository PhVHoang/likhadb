# Kargo promotion pipeline

Kargo discovers a LikhaDB image and the exact Git commit that produced it, then
promotes that Freight through `dev`, `staging`, and `production`. Kargo writes
rendered Kubernetes manifests to environment branches; Argo CD remains the only
controller that applies those manifests to a cluster.

```text
Warehouse -> dev (automatic) -> staging (manual) -> production (manual)
```

## Artifact pairing

The Warehouse accepts only image tags matching `sha-<40 lowercase hex
characters>`. Its Freight creation expression requires that tag to equal the
Git commit selected from `main`. This prevents a source-only commit from being
paired with an older image and promoted as a new release.

The image digest, rather than its tag, is written into the rendered manifests.
Staging and production request Freight only from their immediate upstream
Stage, so the exact digest verified upstream continues through the pipeline.

## Promotion output

The reusable `promote-image` task performs these operations:

1. Check out the Freight source commit and the target environment branch.
2. Clear the environment branch working tree while retaining its Git history.
3. Set the selected image digest in the environment overlay.
4. Render the overlay into the environment branch.
5. Commit and push the rendered manifests.
6. Ask Argo CD to synchronize and wait for application health.

The resulting branches are `env/dev`, `env/staging`, and `env/production`.
They are generated deployment records and should not be edited manually.

## Prerequisites

- Kargo and Argo CD are installed in the same management cluster.
- The Argo CD resources in `gitops/argocd` have been applied.
- Argo CD can read the Git repository.
- Kargo has read/write Git credentials so it can create environment branches.
- If the GHCR package is private, Kargo also has read access to the image
  repository.

Kargo repository credentials are Kubernetes Secrets in the `likhadb` project
namespace. Do not commit those Secrets. A Git credential must be labeled
`kargo.akuity.io/cred-type: git` and contain `repoURL`, `username`, and
`password` keys. An image credential uses the same shape with
`kargo.akuity.io/cred-type: image` and the GHCR repository URL.

The Kargo CLI can create Git credentials interactively so tokens do not need to
be placed in shell history:

```sh
kargo create repo-credentials \
  --project likhadb likhadb-git \
  --git \
  --repo-url https://github.com/PhVHoang/likhadb.git \
  --username '<github-user>'
```

The GitHub token needs repository contents read/write access. If the GHCR image
is private, create a second repository credential for
`ghcr.io/phvhoang/likhadb` using the image credential type and a token with
package read access, or provision the equivalent Secret through the selected
secret manager.

## Bootstrap

Creating a Kargo `Project` also creates its namespace and project boilerplate.
Apply it first and wait for the namespace before applying the namespaced
resources:

```sh
kubectl apply -f gitops/kargo/project.yaml
kubectl wait --for=create namespace/likhadb --timeout=60s
kubectl apply --server-side --kustomize gitops/kargo
kubectl apply --server-side --kustomize gitops/argocd
```

Reapplying the Argo CD resources installs the Stage authorization annotations
and configures the applications to read rendered manifests from the root of
their environment branches.

## Operating the pipeline

Development promotion is automatic. After the development Stage is healthy,
promote its eligible Freight to staging:

```sh
kargo promote --project likhadb --stage staging --warehouse likhadb
```

After staging is healthy, promote the same Freight to production:

```sh
kargo promote --project likhadb --stage production --warehouse likhadb
```

To select an older verified release for rollback, use its immutable Freight
name explicitly:

```sh
kargo promote \
  --project likhadb \
  --stage production \
  --freight '<freight-name>'
```

The committed resources contain no credentials. Live promotion cannot succeed
until the repository credentials and Kargo-to-Argo CD integration are present.
