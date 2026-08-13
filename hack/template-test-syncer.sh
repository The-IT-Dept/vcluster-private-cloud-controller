#!/usr/bin/env bash
# Behavioural checks on the rendered tenant-syncer (1.x) chart.
# Run by CI; needs helm + python3 + pyyaml.
set -euo pipefail

CHART="$(dirname "$0")/../charts/tenant-syncer"
OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT

helm template ts "$CHART" \
  --namespace vcluster-cloud-controller \
  --values "$CHART/ci/test-values.yaml" > "$OUT"

fail() { echo "FAIL: $*" >&2; exit 1; }

count() { python3 - "$OUT" "$1" <<'EOF'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
print(sum(1 for d in docs if d["kind"] == sys.argv[2]))
EOF
}

[ "$(count Deployment)" = 1 ] || fail "expected 1 Deployment"
[ "$(count ServiceAccount)" = 1 ] || fail "expected 1 ServiceAccount"
[ "$(count ClusterRole)" = 1 ] || fail "expected 1 ClusterRole"
[ "$(count ClusterRoleBinding)" = 1 ] || fail "expected 1 ClusterRoleBinding"
# leaderElection=true in test values adds the lease Role.
[ "$(count Role)" = 1 ] || fail "expected leader-election Role when leaderElection is on"

grep -q -- '--poll-interval=20s' "$OUT" || fail "pollInterval not applied"
grep -q -- '--leader-elect' "$OUT" || fail "leader election flag missing"
grep -q 'image: "ghcr.io/the-it-dept/vcluster-private-cloud-controller:test-tag"' "$OUT" \
  || fail "image tag override not applied"

# THE RBAC PROPERTY THAT MATTERS: Secrets are readable one at a time (the
# per-guest kubeconfigs), never enumerable. list/watch on secrets appearing
# here would mean a compromised syncer pod could dump every Secret on the host.
python3 - "$OUT" <<'EOF'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
for d in docs:
    if d["kind"] != "ClusterRole":
        continue
    for rule in d["rules"]:
        if "secrets" in rule.get("resources", []):
            assert set(rule["verbs"]) == {"get"}, f"secrets verbs must be exactly ['get'], got {rule['verbs']}"
EOF
[ $? = 0 ] || fail "secrets RBAC too broad"

# The CRD ships with the chart and must have no field for a guest-held host
# credential — the only kubeconfig field is the one INTO the guest.
CRD="$CHART/crds/vcluster.the-it-dept.io_tenantclusters.yaml"
[ -f "$CRD" ] || fail "TenantCluster CRD missing from crds/"
grep -q 'kubeconfigSecretRef' "$CRD" || fail "kubeconfigSecretRef missing from CRD"
if grep -qiE 'hostKubeconfig|hostCredential|hostSecret' "$CRD"; then
  fail "CRD must have no field for a reverse (guest-holds-host) credential"
fi

# Storage RBAC. Two properties:
#  - StorageClasses are read-only: the syncer advertises host classes, it must
#    never be able to change them.
#  - PVC verbs are exactly get/list/watch/create/delete — no update/patch and,
#    critically, no deletecollection: a storage bug that mass-deletes host
#    PVCs destroys tenant data, so the widest deletion verb is simply absent.
python3 - "$OUT" <<'EOF'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
for d in docs:
    if d["kind"] != "ClusterRole":
        continue
    for rule in d["rules"]:
        res = rule.get("resources", [])
        if "storageclasses" in res:
            assert set(rule["verbs"]) <= {"get", "list", "watch"}, \
                f"storageclasses must be read-only, got {rule['verbs']}"
        if "persistentvolumeclaims" in res:
            assert set(rule["verbs"]) == {"get", "list", "watch", "create", "delete"}, \
                f"pvc verbs must be exactly get/list/watch/create/delete, got {rule['verbs']}"
EOF
[ $? = 0 ] || fail "storage RBAC wrong"

# The CSI image flags reach the controller.
grep -q -- '--csi-node-image=ghcr.io/the-it-dept/vcluster-private-cloud-controller-csi-node:csi-test-tag' "$OUT" \
  || fail "csi-node-image flag not rendered from values"
grep -q -- '--csi-registrar-image=registry.k8s.io/sig-storage/csi-node-driver-registrar:' "$OUT" \
  || fail "csi-registrar-image flag not rendered"

# Default (no values): leader election off, no Role rendered, csi node image
# tag falls back to the chart appVersion so both halves ship as one version.
helm template ts "$CHART" --namespace x > "$OUT"
[ "$(count Role)" = 0 ] || fail "no Role expected with default values"
grep -q -- '--poll-interval=15s' "$OUT" || fail "default pollInterval wrong"
APPVER="$(awk '/^appVersion:/ {print $2}' "$CHART/Chart.yaml")"
grep -q -- "--csi-node-image=ghcr.io/the-it-dept/vcluster-private-cloud-controller-csi-node:$APPVER" "$OUT" \
  || fail "csi node image must default to the chart appVersion"

echo "tenant-syncer template tests passed"
