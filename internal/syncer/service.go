package syncer

import (
	"fmt"

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
func mapService(tc *v1alpha1.TenantCluster, guest *corev1.Service, svcType corev1.ServiceType) (*corev1.Service, string) {
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

	// Deliberately NOT copied from the guest: annotations, loadBalancerIP, and
	// loadBalancerClass. Annotations on a host Service are instructions to HOST
	// controllers (address-pool selection above all), and forwarding them would
	// let a tenant pick another tenant's pool. Address policy belongs to the
	// operator via the TenantCluster, not to the guest object. loadBalancerIP
	// is the same request in older spelling.
	return host, ""
}

func portName(p corev1.ServicePort) string {
	if p.Name != "" {
		return p.Name
	}
	return fmt.Sprintf("%d/%s", p.Port, p.Protocol)
}
