package syncer

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
)

// Ownership labels stamped on EVERY host object this controller creates. They
// are both the index (prune lists by them) and the safety interlock: the
// controller refuses to delete or overwrite any host object that does not
// carry them. A host-side controller deleting something it did not create is
// the worst outcome available to it, so ownership is asserted on the object
// itself, never inferred from a name.
const (
	LabelTenantCluster = "vcluster.the-it-dept.io/tenant-cluster"
	// LabelTenantClusterNamespace exists because TenantCluster is namespaced and
	// a name alone cannot route a host-object event back to the right one.
	LabelTenantClusterNamespace = "vcluster.the-it-dept.io/tenant-cluster-namespace"
	LabelGuestNamespace         = "vcluster.the-it-dept.io/guest-namespace"
	LabelGuestName              = "vcluster.the-it-dept.io/guest-name"
	// LabelGuestUID pins the host object to one incarnation of the guest
	// resource. A guest resource deleted and recreated with the same name is a
	// DIFFERENT object, and the host copy must follow the new one — the label is
	// refreshed on sync, so a stale value marks an object due for reconciling.
	LabelGuestUID = "vcluster.the-it-dept.io/guest-uid"
)

// GuestFinalizer is placed on guest resources that have host-side state, so
// their deletion cannot race ahead of host cleanup. Without it, deleting a
// guest Service leaves the host address allocated and answering — the exact
// leak the manual predecessor of this controller demonstrated.
const GuestFinalizer = "vcluster.the-it-dept.io/host-cleanup"

// TenantClusterFinalizer holds a TenantCluster until every host object carrying
// its label is gone.
const TenantClusterFinalizer = "vcluster.the-it-dept.io/tenant-cleanup"

// RefusedAnnotation is written on a guest resource whose hostname(s) were
// refused. Ingress has no status conditions in the API, so this annotation is
// the durable, kubectl-visible record; the Event carries the same text.
const RefusedAnnotation = "vcluster.the-it-dept.io/refused"

// FieldManager identifies this controller's writes.
const FieldManager = "vcluster-tenant-syncer"

// ownershipLabels builds the label set for a host object materialised from one
// guest resource.
func ownershipLabels(tc *v1alpha1.TenantCluster, guest client.Object) map[string]string {
	return map[string]string{
		LabelTenantCluster:          tc.Name,
		LabelTenantClusterNamespace: tc.Namespace,
		LabelGuestNamespace:         guest.GetNamespace(),
		LabelGuestName:              guest.GetName(),
		LabelGuestUID:               string(guest.GetUID()),
	}
}

// ownedBy is THE deletion/overwrite guard. It requires the tenant-cluster
// name AND namespace labels to match, not merely exist: two tenants' objects
// must never be interchangeable, even if a list call was wrongly scoped.
func ownedBy(obj client.Object, tc *v1alpha1.TenantCluster) bool {
	l := obj.GetLabels()
	return l[LabelTenantCluster] == tc.Name && l[LabelTenantClusterNamespace] == tc.Namespace
}

// HostObjectName derives the host-side name for a guest resource. Deterministic
// and UID-free on purpose: a guest resource recreated under the same name maps
// to the SAME host object (whose guest-uid label is refreshed), rather than a
// second host object briefly coexisting with the first — with LoadBalancers
// that "briefly" is a second allocated address from a possibly tiny pool.
func HostObjectName(tcName, guestNamespace, guestName string) string {
	base := sanitizeName(tcName + "-" + guestNamespace + "-" + guestName)
	if len(base) <= validation.DNS1035LabelMaxLength {
		return base
	}
	// Truncation alone could collide two long names; a hash of the FULL name
	// keeps the result deterministic and distinct.
	sum := sha256.Sum256([]byte(base))
	return base[:validation.DNS1035LabelMaxLength-11] + "-" + hex.EncodeToString(sum[:])[:10]
}

func sanitizeName(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-")
}
