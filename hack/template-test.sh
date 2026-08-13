#!/usr/bin/env bash
# Behavioural checks on the rendered chart. Run by CI; needs helm + python3.
set -euo pipefail

CHART="$(dirname "$0")/../charts/vcluster-private-cloud-controller"
OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT

helm template vpcc "$CHART" \
  --namespace cloud-controller \
  --values "$CHART/ci/test-values.yaml" > "$OUT"

fail() { echo "FAIL: $*" >&2; exit 1; }

count() { python3 - "$OUT" "$1" "$2" <<'EOF'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
print(sum(1 for d in docs if d["kind"] == sys.argv[2] and (not sys.argv[3] or d["metadata"]["name"] == sys.argv[3])))
EOF
}

# One Deployment + ConfigMap per cluster entry.
[ "$(count Deployment "")" = 2 ] || fail "expected 2 Deployments"
[ "$(count ConfigMap "")" = 2 ] || fail "expected 2 ConfigMaps"
# One Role/RoleBinding per distinct vmNamespace.
[ "$(count Role "")" = 2 ] || fail "expected 2 Roles"
[ "$(count RoleBinding "")" = 2 ] || fail "expected 2 RoleBindings"
[ "$(count ServiceAccount "")" = 1 ] || fail "expected 1 ServiceAccount"

# clusterName override must land in --cluster-name; controllers pinned to service.
grep -q -- '--cluster-name=tenant-b-prod' "$OUT" || fail "clusterName override not applied"
grep -q -- '--cluster-name=tenant-a' "$OUT" || fail "default clusterName not applied"
grep -q -- '--controllers=service-lb-controller' "$OUT" || fail "controllers not pinned to service-lb-controller"
grep -q -- '--leader-elect=false' "$OUT" || fail "leader election not disabled"

# instancesV2 must be off — vCluster's embedded cloud provider owns nodes.
grep -A1 'instancesV2' "$OUT" | grep -q 'enabled: false' || fail "instancesV2 not disabled"

# Roles are namespaced to the VM namespaces, with no cluster-scope writes.
grep -q 'namespace: tenant-a-vms' "$OUT" || fail "Role missing in tenant-a-vms"
grep -q 'namespace: tenant-b-vms' "$OUT" || fail "Role missing in tenant-b-vms"
count ClusterRole "" | grep -qx 0 || fail "unexpected ClusterRole"

# A missing kubeconfigSecret must fail the render, not produce broken output.
if helm template vpcc "$CHART" --set 'clusters[0].name=x' --set 'clusters[0].vmNamespace=y' >/dev/null 2>&1; then
  fail "render should fail without kubeconfigSecret"
fi

echo "template tests passed"
