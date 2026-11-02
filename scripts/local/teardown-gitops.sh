#!/usr/bin/env bash

set -euo pipefail

readonly EXPECTED_CONTEXT="${KUBE_CONTEXT:-orbstack}"
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

current_context="$(kubectl config current-context)"
if [[ "${current_context}" != "${EXPECTED_CONTEXT}" ]]; then
  echo "Refusing to modify Kubernetes context '${current_context}'." >&2
  echo "Switch to '${EXPECTED_CONTEXT}', or intentionally set KUBE_CONTEXT=${current_context}." >&2
  exit 1
fi

kubectl delete -f "${ROOT_DIR}/gitops/kargo/project.yaml" --ignore-not-found
helm uninstall kargo --namespace kargo --ignore-not-found
kubectl delete namespace kargo kargo-system-resources kargo-shared-resources argocd --ignore-not-found

echo "Removed the local LikhaDB Kargo project and controller namespaces."
echo "Kargo CRDs are intentionally retained by the chart."
