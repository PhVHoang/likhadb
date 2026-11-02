#!/usr/bin/env bash

set -euo pipefail

readonly ARGO_CD_VERSION="v3.5.6"
readonly KARGO_VERSION="v1.12.5"
readonly CERT_MANAGER_VERSION="v1.21.3"
readonly EXPECTED_CONTEXT="${KUBE_CONTEXT:-orbstack}"
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

for command in kubectl helm curl tar openssl htpasswd; do
  command -v "${command}" >/dev/null || {
    echo "Missing required command: ${command}" >&2
    exit 1
  }
done

current_context="$(kubectl config current-context)"
if [[ "${current_context}" != "${EXPECTED_CONTEXT}" ]]; then
  echo "Refusing to modify Kubernetes context '${current_context}'." >&2
  echo "Switch to '${EXPECTED_CONTEXT}', or intentionally set KUBE_CONTEXT=${current_context}." >&2
  exit 1
fi

kubectl cluster-info >/dev/null

wait_for_workloads() {
  local namespace="$1"
  local workload
  while IFS= read -r workload; do
    kubectl rollout status "${workload}" -n "${namespace}" --timeout=5m
  done < <(kubectl get deployments,statefulsets -n "${namespace}" -o name)
}

echo "Installing Argo CD ${ARGO_CD_VERSION} into ${EXPECTED_CONTEXT}..."
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
curl --fail --silent --show-error --location --retry 3 \
  "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGO_CD_VERSION}/manifests/install.yaml" |
  kubectl apply --server-side --force-conflicts -n argocd -f -
wait_for_workloads argocd

echo "Installing cert-manager ${CERT_MANAGER_VERSION} for Kargo certificates..."
curl --fail --silent --show-error --location --retry 3 \
  "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml" |
  kubectl apply --server-side --force-conflicts -f -
wait_for_workloads cert-manager

chart_dir="$(mktemp -d)"
trap 'rm -rf "${chart_dir}"' EXIT
curl --fail --silent --show-error --location \
  "https://github.com/akuity/kargo/archive/refs/tags/${KARGO_VERSION}.tar.gz" |
  tar -xz -C "${chart_dir}"
kargo_chart="${chart_dir}/kargo-${KARGO_VERSION#v}/charts/kargo"

echo "Installing Kargo ${KARGO_VERSION} into ${EXPECTED_CONTEXT}..."
deployed_release="$(helm list --namespace kargo --deployed --filter '^kargo$' --short 2>/dev/null || true)"
if [[ "${deployed_release}" == "kargo" ]]; then
  helm upgrade kargo "${kargo_chart}" \
    --namespace kargo \
    --reuse-values \
    --set-string image.tag="${KARGO_VERSION}" \
    --wait=legacy \
    --timeout 5m
else
  helm uninstall kargo --namespace kargo --ignore-not-found >/dev/null 2>&1 || true
  for namespace in kargo-shared-resources kargo-system-resources; do
    while kubectl get namespace "${namespace}" >/dev/null 2>&1; do
      sleep 1
    done
  done
  admin_password="$(openssl rand -base64 24 | tr -d '\n')"
  password_hash="$(htpasswd -bnBC 10 '' "${admin_password}" | cut -d: -f2 | tr -d '\n')"
  signing_key="$(openssl rand -base64 32 | tr -d '\n')"

  helm install kargo "${kargo_chart}" \
    --namespace kargo \
    --create-namespace \
    --values "${ROOT_DIR}/gitops/bootstrap/kargo-values.yaml" \
    --set-string image.tag="${KARGO_VERSION}" \
    --set-string api.adminAccount.passwordHash="${password_hash}" \
    --set-string api.adminAccount.tokenSigningKey="${signing_key}" \
    --wait=legacy \
    --timeout 5m

  echo
  echo "Kargo local admin password (shown only on first install): ${admin_password}"
fi

wait_for_workloads kargo

echo "Applying LikhaDB Argo CD and Kargo resources..."
kubectl apply -f "${ROOT_DIR}/gitops/kargo/project.yaml"
for _ in {1..60}; do
  kubectl get namespace likhadb >/dev/null 2>&1 && break
  sleep 1
done
kubectl get namespace likhadb >/dev/null
kubectl apply --server-side --kustomize "${ROOT_DIR}/gitops/kargo"
kubectl apply --server-side --kustomize "${ROOT_DIR}/gitops/argocd"

echo
kubectl get pods -n argocd
kubectl get pods -n kargo
echo
echo "Controllers are ready on Kubernetes context '${EXPECTED_CONTEXT}'."
echo "Argo CD UI: kubectl port-forward service/argocd-server -n argocd 8080:443"
echo "Kargo UI:   kubectl port-forward service/kargo-api -n kargo 8081:443"
