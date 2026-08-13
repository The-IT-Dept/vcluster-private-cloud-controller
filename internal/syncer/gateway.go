package syncer

import (
	"fmt"
	"sort"

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

// mapGateway translates one guest Gateway. Listeners whose hostname fails
// validation are dropped with a refusal; a listener with NO hostname is
// refused too, because it would accept traffic for every name the host serves.
func mapGateway(tc *v1alpha1.TenantCluster, guest *gwv1.Gateway) (*gwv1.Gateway, []string) {
	var refusals []string
	var listeners []gwv1.Listener

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
		for _, b := range rule.BackendRefs {
			if b.Kind != nil && *b.Kind != "Service" {
				res.refusals = append(res.refusals, fmt.Sprintf(
					"rule %d: backendRef %q refused: only Service backends can be carried to the host", i, b.Name))
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
