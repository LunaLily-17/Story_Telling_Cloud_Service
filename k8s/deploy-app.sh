#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
image=${1:-story-cloud:1.0.0}
if [[ "$image" != story-cloud:* || ! "$image" =~ ^story-cloud:[A-Za-z0-9_.-]+$ ]]; then
  printf '%s\n' 'Use an explicit local image tag, such as story-cloud:1.0.0.' >&2
  exit 2
fi
case "${K3S_PROFILE:-local}" in
  local) profile=. ;;
  three-node) profile=three-node ;;
  *) printf '%s\n' 'K3S_PROFILE must be local or three-node.' >&2; exit 2 ;;
esac
manifests=$(kubectl kustomize "$profile")
kubectl -n storytelling apply -f postgres-network-policy.yaml
migration=$(sed "s/IMAGE_REPLACE_ME/$image/g" migrate.yaml |
  kubectl -n storytelling create -f - -o name)
if ! kubectl -n storytelling wait --for=condition=complete "$migration" --timeout=180s; then
  kubectl -n storytelling logs "$migration" || true
  exit 1
fi
printf '%s\n' "$manifests" |
  sed "s/IMAGE_REPLACE_ME/$image/g" |
  kubectl -n storytelling apply -f -
for deployment in plot character dialogue refinement story-api story-worker; do
  kubectl -n storytelling rollout status "deployment/$deployment" --timeout=180s
done
