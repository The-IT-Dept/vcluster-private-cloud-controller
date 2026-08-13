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

# Default (no values): leader election off, no Role rendered.
helm template ts "$CHART" --namespace x > "$OUT"
[ "$(count Role)" = 0 ] || fail "no Role expected with default values"
grep -q -- '--poll-interval=15s' "$OUT" || fail "default pollInterval wrong"

echo "tenant-syncer template tests passed"
