package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SecretKeyReference points at one key of a Secret in the TenantCluster's own
// namespace. Deliberately not a cross-namespace reference: the credential and
// the registration live together, so RBAC on one namespace governs both.
type SecretKeyReference struct {
	// Name of the Secret.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key inside the Secret holding the kubeconfig. Defaults to "kubeconfig".
	// +optional
	Key string `json:"key,omitempty"`
}

// DefaultKubeconfigKey is used when SecretKeyReference.Key is empty.
const DefaultKubeconfigKey = "kubeconfig"

// NodeSelector selects the pods that back the guest's nodes (for KubeVirt
// guests, the virt-launcher pods).
//
// This is its own type rather than metav1.LabelSelector because the labels end
// up as a Kubernetes Service selector, and a Service selector is a plain label
// map. Accepting matchExpressions here would promise something a Service
// cannot express and fail only at sync time.
type NodeSelector struct {
	// +kubebuilder:validation:MinProperties=1
	MatchLabels map[string]string `json:"matchLabels"`
}

// SyncConfig switches individual syncers on and off. A nil pointer means
// enabled: registering a guest should give it the full service surface unless
// the operator says otherwise.
type SyncConfig struct {
	// +optional
	Services *bool `json:"services,omitempty"`
	// +optional
	Ingresses *bool `json:"ingresses,omitempty"`
	// +optional
	Gateways *bool `json:"gateways,omitempty"`
	// Storage enables the storage syncer: host StorageClasses labelled
	// tenant-offerable are mirrored into the guest, guest PVCs on those classes
	// become host PVCs hotplugged into the VM backing whichever guest node the
	// consuming pod runs on, and a CSI node plugin DaemonSet is installed in
	// the guest so kubelet can mount the device. Enabled by default like the
	// other syncers, but INERT until the operator labels at least one host
	// StorageClass tenant-offerable — so enabling the syncer never installs
	// anything into a guest by itself.
	// +optional
	Storage *bool `json:"storage,omitempty"`

	// ServiceLoadBalancerClass scopes the Service syncer to guest Services whose
	// spec.loadBalancerClass equals this value. Empty means the standard
	// behaviour: Services with no loadBalancerClass at all.
	//
	// This exists so the syncer can coexist with another LoadBalancer
	// implementation serving the same guest (for example the deprecated
	// per-guest cloud-controller-manager this project previously wrapped): two
	// controllers acting on one Service fight over its status forever, and
	// loadBalancerClass is the API's own mechanism for partitioning them.
	// +optional
	ServiceLoadBalancerClass string `json:"serviceLoadBalancerClass,omitempty"`
}

// TenantClusterSpec registers one guest cluster with the syncer.
//
// TRUST FLOWS ONE WAY. The only credential here is the provider's credential
// INTO the guest. There is no field for a guest-held credential to the host,
// and there must never be one: a customer has cluster-admin on their own
// cluster, so anything placed in a guest is something the customer holds.
type TenantClusterSpec struct {
	// KubeconfigSecretRef names the Secret (in this TenantCluster's namespace)
	// holding a kubeconfig INTO the guest cluster. Provider → tenant.
	KubeconfigSecretRef SecretKeyReference `json:"kubeconfigSecretRef"`

	// HostNamespace is where host-side objects for this tenant are created.
	// Host Services select pods by label, and a Service can only select pods in
	// its own namespace — so this must be the namespace holding the guest's
	// node pods (the KubeVirt virt-launcher pods).
	// +kubebuilder:validation:MinLength=1
	HostNamespace string `json:"hostNamespace"`

	// NodeSelector finds this tenant's node pods on the host. Host Services
	// select by LABEL rather than by address because a VM's pod IP changes on
	// every restart, and a selector survives that without anything tracking it.
	NodeSelector NodeSelector `json:"nodeSelector"`

	// AllowedDomains is the hostname authority: a guest Ingress, Gateway
	// listener or HTTPRoute hostname is synced ONLY if it matches one of these
	// patterns. Without this, tenant A can publish an Ingress for tenant B's
	// hostname — every tenant has cluster-admin on their own cluster and can
	// write any Ingress they like.
	//
	// A leading "*." wildcard matches exactly ONE label, as DNS does:
	// "*.a.example.com" matches "x.a.example.com" but not "x.y.a.example.com"
	// and not "a.example.com".
	//
	// The platform sets this when it creates the TenantCluster; a tenant cannot
	// edit it, because the TenantCluster lives on the host and the tenant has no
	// access to the host.
	// +optional
	AllowedDomains []string `json:"allowedDomains,omitempty"`

	// IngressClassName is stamped on every host Ingress synced for this tenant.
	// The guest's own ingressClassName is ignored: it names a class in the
	// guest's world, not the host's.
	// +optional
	IngressClassName string `json:"ingressClassName,omitempty"`

	// GatewayClassName is stamped on every host Gateway synced for this tenant.
	// Required for Gateway sync to do anything.
	// +optional
	GatewayClassName string `json:"gatewayClassName,omitempty"`

	// Limits caps what this tenant may consume of scarce HOST resources. Like
	// AllowedDomains it lives on the host, so a tenant cannot edit it — which is
	// the whole point: a tenant has cluster-admin in their own cluster and could
	// otherwise create a hundred LoadBalancer Services.
	// +optional
	Limits Limits `json:"limits,omitempty"`

	// +optional
	Sync SyncConfig `json:"sync,omitempty"`
}

// Limits caps a tenant's consumption of scarce host resources.
type Limits struct {
	// LoadBalancers is the number of host objects this tenant may hold that
	// CONSUME A POOL ADDRESS: LoadBalancer Services and Gateways together.
	// Absent means unlimited — the right default for a single-tenant region and
	// the wrong one for a shared one.
	//
	// Counted per LOGICAL ENDPOINT, not per address: a dual-stack Service takes
	// one IPv4 and one IPv6 but counts once, because it is one thing a customer
	// asked for. The scarce family is IPv4 and it is what this number really
	// protects; if IPv6-only ever needs a larger allowance that is a second
	// field, not a reinterpretation of this one.
	//
	// LOWERING IT BELOW CURRENT USAGE TEARS NOTHING DOWN. It stops the tenant
	// growing; what already runs keeps running. The alternative — an operator
	// edits a number and a customer's production endpoint disappears — is not a
	// trade worth having, and it matches how GPU quota already behaves in this
	// platform.
	// +optional
	// +kubebuilder:validation:Minimum=0
	LoadBalancers *int32 `json:"loadBalancers,omitempty"`
}

// LoadBalancerLimit resolves the tri-state limit: (value, true) when set.
func (t *TenantCluster) LoadBalancerLimit() (int, bool) {
	if t.Spec.Limits.LoadBalancers == nil {
		return 0, false
	}
	return int(*t.Spec.Limits.LoadBalancers), true
}

// ResourceCounts is how many guest resources the last completed pass saw in
// scope for syncing.
type ResourceCounts struct {
	Services   int `json:"services"`
	Ingresses  int `json:"ingresses"`
	Gateways   int `json:"gateways"`
	HTTPRoutes int `json:"httpRoutes"`
	// LoadBalancers is the number of LOGICAL ENDPOINTS admitted this pass:
	// LoadBalancer Services and Gateways together, the thing
	// spec.limits.loadBalancers caps. Refused ones are not counted.
	// +optional
	LoadBalancers int `json:"loadBalancers,omitempty"`
	// +optional
	PersistentVolumeClaims int `json:"persistentVolumeClaims,omitempty"`
}

type TenantClusterStatus struct {
	// Connected reports whether the last attempt to reach the guest API
	// succeeded. While false the syncer touches NOTHING on the host: a tenant's
	// published address must not disappear because its API server restarted.
	Connected bool `json:"connected"`

	// LastSyncTime is when a full pass last completed against a reachable
	// guest. Combined with Connected=false it tells an operator how stale the
	// host-side state might be.
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`

	// +optional
	ObservedResources ResourceCounts `json:"observedResources,omitempty"`

	// LoadBalancerUsage is how many logical endpoints (LoadBalancer Services +
	// Gateways) this tenant currently holds on the host — the number checked
	// against spec.limits.loadBalancers. Published so an operator can see how
	// close a tenant is to its cap without counting objects by hand.
	// +optional
	LoadBalancerUsage int `json:"loadBalancerUsage,omitempty"`

	// HostAddressFamilies is what the HOST cluster can allocate, discovered from
	// its ServiceCIDRs. A guest asking for a family not in this list is refused
	// with a reason instead of sitting <pending> forever. Empty means discovery
	// found nothing (an older host, or no permission on ServiceCIDRs) and every
	// family is passed through for the host API server to judge.
	// +optional
	HostAddressFamilies []string `json:"hostAddressFamilies,omitempty"`

	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition types on TenantClusterStatus.
const (
	// CondConnected is whether the guest API is reachable.
	CondConnected = "Connected"
	// CondGatewayAPIAvailable reports whether the HOST cluster has the Gateway
	// API CRDs. False is not an error — the Gateway syncer skips cleanly and
	// the others carry on — but it must be visible, not silent.
	CondGatewayAPIAvailable = "GatewayAPIAvailable"
	// CondSynced is False while any refusal or collision is outstanding, with
	// the details in the message.
	CondSynced = "Synced"
)

// SyncServices etc. resolve the tri-state toggles. Enabled by default.
func (t *TenantCluster) SyncServices() bool  { return t.Spec.Sync.Services == nil || *t.Spec.Sync.Services }
func (t *TenantCluster) SyncIngresses() bool { return t.Spec.Sync.Ingresses == nil || *t.Spec.Sync.Ingresses }
func (t *TenantCluster) SyncGateways() bool  { return t.Spec.Sync.Gateways == nil || *t.Spec.Sync.Gateways }
func (t *TenantCluster) SyncStorage() bool   { return t.Spec.Sync.Storage == nil || *t.Spec.Sync.Storage }

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=tenant
// +kubebuilder:printcolumn:name="Connected",type=boolean,JSONPath=`.status.connected`
// +kubebuilder:printcolumn:name="Services",type=integer,JSONPath=`.status.observedResources.services`
// +kubebuilder:printcolumn:name="Ingresses",type=integer,JSONPath=`.status.observedResources.ingresses`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TenantCluster registers one guest cluster with the tenant syncer.
type TenantCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TenantClusterSpec   `json:"spec,omitempty"`
	Status TenantClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type TenantClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TenantCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TenantCluster{}, &TenantClusterList{})
}
