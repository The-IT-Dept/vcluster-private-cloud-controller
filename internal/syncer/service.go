package syncer

import (
	"fmt"
	"net"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
)

// The Service syncer.
//
// A guest `type: LoadBalancer` Service becomes a host LoadBalancer Service in
// the tenant's host namespace. The host Service selects the tenant's node pods
// (KubeVirt virt-launcher pods) by label and targets the GUEST Service's
// NodePort, so the traffic path is:
//
//	client → host LB address → node pod IP (== VM/guest-node IP under bridge
//	binding) → guest NodePort → guest kube-proxy → guest pod
//
// The host's own LoadBalancer implementation allocates the address; this
// controller writes it back into the guest Service's status. Ingress and
// HTTPRoute backends reuse the same mapping with type ClusterIP — the host
// ingress controller needs a host Service to route to, not an address.

// HostAddressCondition is the condition type written into a guest Service's
// status. Guest Services DO have metav1 conditions (unlike Ingress), so the
// "never a silent nothing" rule can be honoured properly here.
const HostAddressCondition = "vcluster.the-it-dept.io/HostAddressAssigned"

// wantsLoadBalancer decides whether a guest Service is in scope for LB sync.
// The loadBalancerClass check is what lets this syncer share a guest with
// another implementation: Kubernetes' own convention is that a controller
// serves either nil-class Services or exactly its declared class, never both.
func wantsLoadBalancer(tc *v1alpha1.TenantCluster, svc *corev1.Service) bool {
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		return false
	}
	want := tc.Spec.Sync.ServiceLoadBalancerClass
	if want == "" {
		return svc.Spec.LoadBalancerClass == nil
	}
	return svc.Spec.LoadBalancerClass != nil && *svc.Spec.LoadBalancerClass == want
}

// mapService builds the host Service for a guest Service. Pure: everything it
// needs is in its arguments, which is what makes the table tests honest.
//
// The returned refusal (empty when syncable) explains why a Service cannot be
// carried — it ends up as a condition on the guest Service, because a Service
// that silently stays <pending> tells the customer nothing.
// mapService builds the host Service for a guest Service. hostFamilies is what
// the host cluster can allocate (nil = unknown, pass everything through).
func mapService(tc *v1alpha1.TenantCluster, guest *corev1.Service, svcType corev1.ServiceType, hostFamilies []corev1.IPFamily) (*corev1.Service, string) {
	var ports []corev1.ServicePort
	for _, p := range guest.Spec.Ports {
		if p.NodePort == 0 {
			// No NodePort means no host-reachable path to the guest workload: the
			// guest's ClusterIP network does not exist on the host. This happens
			// with allocateLoadBalancerNodePorts=false, or when an Ingress backend
			// references a plain ClusterIP Service.
			return nil, fmt.Sprintf(
				"port %s has no NodePort; the host can only reach this workload via a guest NodePort "+
					"(keep allocateLoadBalancerNodePorts enabled, or use type NodePort for Ingress backends)",
				portName(p))
		}
		ports = append(ports, corev1.ServicePort{
			Name:     p.Name,
			Protocol: p.Protocol,
			// The guest's advertised port is preserved so the customer's published
			// URL works; the NodePort is a private detail of the hop.
			Port:       p.Port,
			TargetPort: intstr.FromInt32(p.NodePort),
		})
	}
	if len(ports) == 0 {
		return nil, "service has no ports; nothing to publish"
	}

	host := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HostObjectName(tc.Name, guest.Namespace, guest.Name),
			Namespace: tc.Spec.HostNamespace,
			Labels:    ownershipLabels(tc, guest),
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: tc.Spec.NodeSelector.MatchLabels,
			Ports:    ports,
			// externalTrafficPolicy is NOT copied. On the host Service "Local"
			// would mean "only nodes running this tenant's VM pods answer", which
			// halves availability without delivering what the tenant asked for —
			// the client IP is lost again at the guest NodePort hop regardless.
		},
	}

	// ADDRESS FAMILIES. A guest that asks for IPv6 and silently receives IPv4
	// has been given the wrong thing, and a dual-stack guest handed one family
	// has been given half of it — so the request is carried through rather than
	// left to the host's default (which is SingleStack in the cluster's primary
	// family, almost always IPv4).
	//
	// Only for the LoadBalancer object. A ClusterIP backend Service exists so a
	// host ingress controller or Envoy can reach the tenant's NodePort; its
	// families are between the host and its own proxies, and pinning them to
	// what a guest asked for its PUBLIC address could leave a host proxy unable
	// to dial the backend at all.
	if svcType == corev1.ServiceTypeLoadBalancer {
		families, policy, refusal := requestedAddressFamilies(guest)
		if refusal != "" {
			return nil, refusal
		}
		if refusal := unsupportedFamilyRefusal(families, hostFamilies); refusal != "" {
			return nil, refusal
		}
		host.Spec.IPFamilies = families
		host.Spec.IPFamilyPolicy = policy
	}

	// Deliberately NOT copied from the guest: annotations, loadBalancerIP, and
	// loadBalancerClass. Annotations on a host Service are instructions to HOST
	// controllers (address-pool selection above all), and forwarding them would
	// let a tenant pick another tenant's pool. Address policy belongs to the
	// operator via the TenantCluster, not to the guest object. loadBalancerIP
	// is the same request in older spelling.
	return host, ""
}

// requestedAddressFamilies works out which families the guest is asking for on
// its PUBLIC host address, from the annotation first and spec.ipFamilies
// second. See IPFamiliesAnnotation for why the annotation has to exist at all:
// a single-stack guest cannot put "IPv6" in spec.ipFamilies, its own API
// server refuses the object.
//
// Returns (nil, nil, "") when the guest expressed no preference, which leaves
// the host to its default — the pre-existing behaviour, unchanged.
func requestedAddressFamilies(guest *corev1.Service) ([]corev1.IPFamily, *corev1.IPFamilyPolicy, string) {
	families := append([]corev1.IPFamily(nil), guest.Spec.IPFamilies...)
	var policy *corev1.IPFamilyPolicy
	if guest.Spec.IPFamilyPolicy != nil {
		p := *guest.Spec.IPFamilyPolicy
		policy = &p
	}

	if raw, ok := guest.Annotations[IPFamiliesAnnotation]; ok {
		parsed, err := parseFamilies(raw)
		if err != nil {
			return nil, nil, fmt.Sprintf("annotation %s: %v", IPFamiliesAnnotation, err)
		}
		families = parsed
		// Derived rather than inherited: the guest's own policy describes its
		// ClusterIP allocation, which has nothing to do with how many families
		// it wants published on the host.
		derived := corev1.IPFamilyPolicySingleStack
		if len(families) > 1 {
			derived = corev1.IPFamilyPolicyRequireDualStack
		}
		policy = &derived
	}
	if raw, ok := guest.Annotations[IPFamilyPolicyAnnotation]; ok {
		p := corev1.IPFamilyPolicy(raw)
		switch p {
		case corev1.IPFamilyPolicySingleStack, corev1.IPFamilyPolicyPreferDualStack, corev1.IPFamilyPolicyRequireDualStack:
			policy = &p
		default:
			return nil, nil, fmt.Sprintf(
				"annotation %s: %q is not a valid policy (SingleStack, PreferDualStack, RequireDualStack)",
				IPFamilyPolicyAnnotation, raw)
		}
	}
	if len(families) == 0 {
		return nil, policy, ""
	}
	return families, policy, ""
}

func parseFamilies(raw string) ([]corev1.IPFamily, error) {
	var out []corev1.IPFamily
	for _, part := range strings.Split(raw, ",") {
		switch f := corev1.IPFamily(strings.TrimSpace(part)); f {
		case corev1.IPv4Protocol, corev1.IPv6Protocol:
			if containsFamily(out, f) {
				return nil, fmt.Errorf("%s is listed twice", f)
			}
			out = append(out, f)
		default:
			return nil, fmt.Errorf("%q is not an address family (want IPv4, IPv6, or IPv4,IPv6)", strings.TrimSpace(part))
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no address family given")
	}
	return out, nil
}

// unsupportedFamilyRefusal names the first requested address family the host
// cannot supply. THIS IS A REFUSAL, NOT A WAIT: a host with no IPv6 will never
// allocate one, so a guest asking for IPv6 there must be told, not left
// <pending> with no explanation for the rest of its life.
//
// hostFamilies nil/empty means discovery could not answer, in which case
// everything passes through and the host API server gets to judge — being
// permissive on an unknown is right, because refusing on a failed lookup would
// break every tenant the moment the ServiceCIDR API moved.
func unsupportedFamilyRefusal(want, hostFamilies []corev1.IPFamily) string {
	if len(hostFamilies) == 0 {
		return ""
	}
	for _, w := range want {
		if !containsFamily(hostFamilies, w) {
			return fmt.Sprintf(
				"address family %s refused: the host cluster cannot allocate %s addresses (it provides %s)",
				w, w, describeFamilies(hostFamilies))
		}
	}
	return ""
}

func containsFamily(list []corev1.IPFamily, f corev1.IPFamily) bool {
	for _, x := range list {
		if x == f {
			return true
		}
	}
	return false
}

func describeFamilies(fams []corev1.IPFamily) string {
	if len(fams) == 0 {
		return "no address families"
	}
	out := make([]string, 0, len(fams))
	for _, f := range fams {
		out = append(out, string(f))
	}
	return strings.Join(out, ", ")
}

// familyOf classifies an allocated address so the write-back can say which
// family each one is, and which requested family never arrived.
func familyOf(addr string) corev1.IPFamily {
	ip := net.ParseIP(addr)
	switch {
	case ip == nil:
		return ""
	case ip.To4() != nil:
		return corev1.IPv4Protocol
	default:
		return corev1.IPv6Protocol
	}
}

func portName(p corev1.ServicePort) string {
	if p.Name != "" {
		return p.Name
	}
	return fmt.Sprintf("%d/%s", p.Port, p.Protocol)
}
