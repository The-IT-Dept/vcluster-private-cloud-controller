package syncer

import (
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/hostname"
)

// The Gateway API syncer: guest Gateway and HTTPRoute → host equivalents,
// with the same hostname authority and the same backend-Service reuse as the
// Ingress syncer.
//
// Gateway API is OPTIONAL on both sides. Its CRDs missing on the host is
// reported in TenantCluster status and skipped — never a crash, never a block
// on the other syncers. That check lives in the controller (it needs a
// RESTMapper); this file is only the pure mapping.

// GuestGatewayControllerName is the controller named by the GatewayClass this
// syncer mirrors INTO a guest. Deliberately not the host's controller name
// (Cilium's, say): inside the guest the thing that implements this class is
// the syncer, and naming the host's controller there would both be untrue and
// invite a guest-side installation of that controller to fight us for it. The
// same reasoning as mirrorStorageClass replacing the provisioner with our own
// CSI driver name.
const GuestGatewayControllerName = "vcluster.the-it-dept.io/tenant-syncer"

// mirrorGatewayClass builds the guest GatewayClass advertising the one class a
// tenant may name.
//
// WITHOUT THIS A TENANT CANNOT CREATE A USABLE GATEWAY AT ALL. Every Gateway
// must name a GatewayClass, the host's class name is the operator's choice in
// the TenantCluster, and a tenant has no access to the host to discover it —
// so they would be guessing a string. StorageClasses are mirrored for exactly
// this reason and Gateway API needs the same courtesy.
//
// It carries the host name unchanged, because the pass maps it back by name.
func mirrorGatewayClass(tc *v1alpha1.TenantCluster) *gwv1.GatewayClass {
	// spec.description is capped at 64 bytes by the CRD, so it says only what
	// the class IS. The allowed domains — the thing a tenant most needs and the
	// one part with no length bound — go in an annotation, where they are also
	// machine-readable instead of prose to be parsed.
	desc := "Published on the host cluster by the tenant syncer."
	return &gwv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        tc.Spec.GatewayClassName,
			Labels:      map[string]string{ManagedByLabel: ManagedByValue},
			Annotations: map[string]string{AllowedDomainsAnnotation: describeDomains(tc.Spec.AllowedDomains)},
		},
		Spec: gwv1.GatewayClassSpec{
			ControllerName: GuestGatewayControllerName,
			Description:    &desc,
		},
	}
}

// gatewayClassAcceptedCondition is set on the mirrored guest class. Nothing in
// the guest would otherwise set it, and a GatewayClass sitting at ACCEPTED
// "Unknown" forever reads as broken to every tool that looks.
func gatewayClassAcceptedCondition(generation int64) metav1.Condition {
	return metav1.Condition{
		Type:               string(gwv1.GatewayClassConditionStatusAccepted),
		Status:             metav1.ConditionTrue,
		Reason:             string(gwv1.GatewayClassReasonAccepted),
		Message:            "the host cluster's tenant syncer serves Gateways on this class",
		ObservedGeneration: generation,
	}
}

func describeDomains(domains []string) string {
	if len(domains) == 0 {
		return "(none configured: every Gateway hostname will be refused)"
	}
	return strings.Join(domains, ",")
}

// mapGateway translates one guest Gateway. Listeners whose hostname fails
// validation are dropped with a refusal; a listener with NO hostname is
// refused too, because it would accept traffic for every name the host serves.
func mapGateway(tc *v1alpha1.TenantCluster, guest *gwv1.Gateway) (*gwv1.Gateway, []string) {
	var refusals []string
	var listeners []gwv1.Listener

	if tc.Spec.GatewayClassName == "" {
		// Stamping an empty class produces a host Gateway the API server rejects,
		// once per pass, with the reason visible only to the operator. The tenant
		// is owed the reason on their own object instead.
		return nil, []string{"Gateway sync is not configured for this cluster: the operator has set no gatewayClassName on its TenantCluster registration"}
	}

	// ADDRESS FAMILIES ON A GATEWAY. Gateway API has no ipFamilies field: the
	// only way a guest can express one is spec.addresses, by naming a literal
	// address — and that is a request for a SPECIFIC HOST ADDRESS, which is the
	// loadBalancerIP hazard the Service syncer already refuses to carry (a
	// tenant must not be able to claim, or steer itself onto, another tenant's
	// address). So it is refused rather than copied.
	//
	// Refused VISIBLY, though, and that is the point: dropping it silently would
	// leave a guest that asked for an IPv6 Gateway holding an IPv4 one with
	// nothing anywhere saying why.
	if len(guest.Spec.Addresses) > 0 {
		var asked []string
		for _, a := range guest.Spec.Addresses {
			if a.Value != "" {
				asked = append(asked, a.Value)
			}
		}
		refusals = append(refusals, fmt.Sprintf(
			"spec.addresses %v refused: the host cluster chooses the address (and therefore the address "+
				"family) for a Gateway; the assigned addresses are reported in status.addresses",
			asked))
	}

	for _, l := range guest.Spec.Listeners {
		if l.Hostname == nil || *l.Hostname == "" {
			refusals = append(refusals, fmt.Sprintf(
				"listener %q refused: a listener without a hostname accepts every hostname, which cannot be authorized per-domain", l.Name))
			continue
		}
		if !hostname.Allowed(string(*l.Hostname), tc.Spec.AllowedDomains) {
			refusals = append(refusals, hostname.Refusal(string(*l.Hostname), tc.Spec.AllowedDomains))
			continue
		}
		out := *l.DeepCopy()
		// TLS certificateRefs name Secrets in the GUEST; the host cannot (and
		// must not) resolve them. Host-side issuance is the supported path, as
		// with Ingress TLS.
		if out.TLS != nil {
			out.TLS.CertificateRefs = nil
		}
		listeners = append(listeners, out)
	}

	if len(listeners) == 0 {
		return nil, refusals
	}
	return &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HostObjectName(tc.Name, guest.Namespace, guest.Name),
			Namespace: tc.Spec.HostNamespace,
			Labels:    ownershipLabels(tc, guest),
		},
		Spec: gwv1.GatewaySpec{
			// The class is the operator's, from the TenantCluster — same rule as
			// ingressClassName, same reason.
			GatewayClassName: gwv1.ObjectName(tc.Spec.GatewayClassName),
			Listeners:        listeners,
		},
	}, refusals
}

// routeResult mirrors ingressResult for HTTPRoutes.
type routeResult struct {
	host     *gwv1.HTTPRoute
	backends []string
	refusals []string
}

// mapHTTPRoute translates one guest HTTPRoute. Its parentRefs must name guest
// Gateways this controller synced — those are rewritten to the corresponding
// host Gateways. Hostnames are validated exactly as Ingress hosts are; a route
// with NO hostnames inherits its listeners' names, so it is allowed through on
// the strength of the (already validated) host Gateway listeners it attaches to.
func mapHTTPRoute(tc *v1alpha1.TenantCluster, guest *gwv1.HTTPRoute, resolveBackend func(svcName string) string) routeResult {
	var res routeResult

	var hostnames []gwv1.Hostname
	for _, h := range guest.Spec.Hostnames {
		if hostname.Allowed(string(h), tc.Spec.AllowedDomains) {
			hostnames = append(hostnames, h)
		} else {
			res.refusals = append(res.refusals, hostname.Refusal(string(h), tc.Spec.AllowedDomains))
		}
	}
	if len(guest.Spec.Hostnames) > 0 && len(hostnames) == 0 {
		// Every explicit hostname was refused; attaching the route anyway would
		// publish it on the listeners' names instead, which is not what the
		// tenant wrote.
		return res
	}

	var parents []gwv1.ParentReference
	for _, p := range guest.Spec.ParentRefs {
		if p.Kind != nil && *p.Kind != "Gateway" {
			res.refusals = append(res.refusals, fmt.Sprintf(
				"parentRef %q refused: only Gateway parents can be carried to the host", p.Name))
			continue
		}
		ns := guest.Namespace
		if p.Namespace != nil {
			ns = string(*p.Namespace)
		}
		out := *p.DeepCopy()
		out.Name = gwv1.ObjectName(HostObjectName(tc.Name, ns, string(p.Name)))
		out.Namespace = nil // host objects for one tenant share one namespace
		parents = append(parents, out)
	}
	if len(parents) == 0 {
		res.refusals = append(res.refusals, "route has no usable Gateway parentRefs; nothing to attach to on the host")
		return res
	}

	backendSet := map[string]bool{}
	var rules []gwv1.HTTPRouteRule
	for i, rule := range guest.Spec.Rules {
		out := *rule.DeepCopy()
		out.BackendRefs = nil

		// Rule-level filters carry object references of their own, and an
		// unrewritten one resolves on the HOST against whatever bears that name.
		filters, fRefs, fRefusals := mapFilters(tc, guest.Namespace, rule.Filters, fmt.Sprintf("rule %d", i), resolveBackend)
		out.Filters = filters
		res.refusals = append(res.refusals, fRefusals...)
		for _, n := range fRefs {
			backendSet[n] = true
		}

		for _, b := range rule.BackendRefs {
			if b.Kind != nil && *b.Kind != "Service" {
				res.refusals = append(res.refusals, fmt.Sprintf(
					"rule %d: backendRef %q refused: only Service backends can be carried to the host", i, b.Name))
				continue
			}
			// A cross-namespace backendRef must be refused, not silently
			// flattened: the resolver only ever looks in the route's own
			// namespace, so dropping the namespace would quietly send traffic to a
			// same-named Service in the WRONG namespace.
			if b.Namespace != nil && string(*b.Namespace) != guest.Namespace {
				res.refusals = append(res.refusals, fmt.Sprintf(
					"rule %d: backendRef %q refused: cross-namespace backends are not carried to the host", i, b.Name))
				continue
			}
			if msg := resolveBackend(string(b.Name)); msg != "" {
				res.refusals = append(res.refusals, fmt.Sprintf(
					"rule %d: backend service %q: %s", i, b.Name, msg))
				continue
			}
			ob := *b.DeepCopy()
			ob.Name = gwv1.ObjectName(HostObjectName(tc.Name, guest.Namespace, string(b.Name)))
			ob.Namespace = nil
			// Per-backendRef filters are references too, and are missed by any
			// check that only walks the rule level.
			bFilters, bRefs, bRefusals := mapFilters(tc, guest.Namespace, b.Filters, fmt.Sprintf("rule %d backendRef %q", i, b.Name), resolveBackend)
			ob.Filters = bFilters
			res.refusals = append(res.refusals, bRefusals...)
			for _, n := range bRefs {
				backendSet[n] = true
			}
			out.BackendRefs = append(out.BackendRefs, ob)
			backendSet[string(b.Name)] = true
		}
		if len(out.BackendRefs) > 0 || len(rule.BackendRefs) == 0 {
			rules = append(rules, out)
		}
	}
	if len(rules) == 0 {
		return res
	}

	res.host = &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HostObjectName(tc.Name, guest.Namespace, guest.Name),
			Namespace: tc.Spec.HostNamespace,
			Labels:    ownershipLabels(tc, guest),
		},
		Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: parents},
			Hostnames:       hostnames,
			Rules:           rules,
		},
	}
	for name := range backendSet {
		res.backends = append(res.backends, name)
	}
	sort.Strings(res.backends)
	return res
}

// mapFilters rewrites the object references inside HTTPRoute filters, and is
// the reason this is not simply a DeepCopy of the rule.
//
// A filter copied verbatim keeps GUEST object names, which on the host resolve
// against whatever happens to bear that name in the tenant's host namespace —
// never the guest object the tenant meant, and never checked by the backend
// resolver that guards ordinary backendRefs. Returns the mapped filters, the
// guest Service names they pull in (so host backends get materialised for
// them), and any refusals.
func mapFilters(tc *v1alpha1.TenantCluster, guestNS string, filters []gwv1.HTTPRouteFilter, where string, resolveBackend func(string) string) ([]gwv1.HTTPRouteFilter, []string, []string) {
	var out []gwv1.HTTPRouteFilter
	var backends, refusals []string

	for _, f := range filters {
		switch f.Type {
		case gwv1.HTTPRouteFilterExtensionRef:
			// ExtensionRef names an implementation-specific CRD in the HOST
			// namespace. Those extensions are the provider's, and there is no
			// rewriting that would make one mean what the tenant intended.
			name := ""
			if f.ExtensionRef != nil {
				name = string(f.ExtensionRef.Name)
			}
			refusals = append(refusals, fmt.Sprintf(
				"%s: ExtensionRef filter %q refused: host cluster extensions cannot be referenced from a guest route", where, name))

		case gwv1.HTTPRouteFilterRequestMirror:
			if f.RequestMirror == nil {
				continue
			}
			b := f.RequestMirror.BackendRef
			if b.Kind != nil && *b.Kind != "Service" {
				refusals = append(refusals, fmt.Sprintf(
					"%s: RequestMirror backendRef %q refused: only Service backends can be carried to the host", where, b.Name))
				continue
			}
			if b.Namespace != nil && string(*b.Namespace) != guestNS {
				refusals = append(refusals, fmt.Sprintf(
					"%s: RequestMirror backendRef %q refused: cross-namespace backends are not carried to the host", where, b.Name))
				continue
			}
			if msg := resolveBackend(string(b.Name)); msg != "" {
				refusals = append(refusals, fmt.Sprintf(
					"%s: RequestMirror backend service %q: %s", where, b.Name, msg))
				continue
			}
			nf := *f.DeepCopy()
			nf.RequestMirror.BackendRef.Name = gwv1.ObjectName(HostObjectName(tc.Name, guestNS, string(b.Name)))
			nf.RequestMirror.BackendRef.Namespace = nil
			out = append(out, nf)
			backends = append(backends, string(b.Name))

		default:
			// Header modifiers, RequestRedirect and URLRewrite carry no object
			// reference — only literal values — so they cross unchanged.
			out = append(out, *f.DeepCopy())
		}
	}
	return out, backends, refusals
}
