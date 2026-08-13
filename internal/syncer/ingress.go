package syncer

import (
	"fmt"
	"sort"

	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/hostname"
)

// The Ingress syncer.
//
// A guest Ingress becomes a host Ingress in the tenant's host namespace, with
// the TenantCluster's ingressClassName and every rule hostname validated
// against allowedDomains. Rule backends reference guest Services; for each one
// the syncer materialises a host backend Service (the same mapping the Service
// syncer uses, with type ClusterIP) and rewrites the rule to point at it.

// ingressResult is what mapIngress decides about one guest Ingress.
type ingressResult struct {
	// host is the Ingress to materialise; nil when nothing survived validation.
	host *netv1.Ingress
	// backends are the guest Services the host Ingress needs host-side backend
	// Services for, keyed by guest Service name.
	backends []string
	// refusals are customer-facing explanations for everything that was NOT
	// synced. Never silently dropped: they become an Event and an annotation on
	// the guest Ingress.
	refusals []string
}

// mapIngress validates and translates one guest Ingress. Pure.
//
// resolveBackend reports whether a guest Service exists and can be reached
// from the host (returns a refusal string when it cannot); it is a parameter
// so the mapping stays a pure function under test.
func mapIngress(tc *v1alpha1.TenantCluster, guest *netv1.Ingress, resolveBackend func(svcName string) string) ingressResult {
	var res ingressResult
	backendSet := map[string]bool{}

	// A default backend catches every hostname pointed at the controller,
	// which no allowedDomains entry can ever authorize.
	if guest.Spec.DefaultBackend != nil {
		res.refusals = append(res.refusals,
			"defaultBackend refused: it would receive traffic for every hostname, which cannot be authorized per-domain")
	}

	for _, rule := range guest.Spec.Rules {
		if !hostname.Allowed(rule.Host, tc.Spec.AllowedDomains) {
			res.refusals = append(res.refusals, hostname.Refusal(rule.Host, tc.Spec.AllowedDomains))
			continue
		}
		if rule.HTTP == nil {
			continue
		}
		outRule := netv1.IngressRule{Host: rule.Host, IngressRuleValue: netv1.IngressRuleValue{HTTP: &netv1.HTTPIngressRuleValue{}}}
		for _, path := range rule.HTTP.Paths {
			b := path.Backend.Service
			if b == nil {
				res.refusals = append(res.refusals, fmt.Sprintf(
					"path %q on host %q refused: only Service backends can be carried to the host", path.Path, rule.Host))
				continue
			}
			if msg := resolveBackend(b.Name); msg != "" {
				res.refusals = append(res.refusals, fmt.Sprintf(
					"path %q on host %q: backend service %q: %s", path.Path, rule.Host, b.Name, msg))
				continue
			}
			hostPath := path.DeepCopy()
			hostPath.Backend.Service = &netv1.IngressServiceBackend{
				Name: HostObjectName(tc.Name, guest.Namespace, b.Name),
				Port: b.Port,
			}
			outRule.HTTP.Paths = append(outRule.HTTP.Paths, *hostPath)
			backendSet[b.Name] = true
		}
		if len(outRule.HTTP.Paths) > 0 {
			if res.host == nil {
				res.host = newHostIngress(tc, guest)
			}
			res.host.Spec.Rules = append(res.host.Spec.Rules, outRule)
		}
	}

	// TLS: keep the section (so the host controller serves HTTPS for the
	// hostnames) but ONLY for validated hostnames, and never the secretName.
	// The named Secret lives in the guest, and certificate private keys do not
	// move between trust domains on a default; host-side issuance (e.g.
	// cert-manager on the ingress class) is the supported path.
	if res.host != nil {
		for _, tls := range guest.Spec.TLS {
			var hosts []string
			for _, h := range tls.Hosts {
				if hostname.Allowed(h, tc.Spec.AllowedDomains) {
					hosts = append(hosts, h)
				} else {
					res.refusals = append(res.refusals, "tls: "+hostname.Refusal(h, tc.Spec.AllowedDomains))
				}
			}
			if len(hosts) > 0 {
				res.host.Spec.TLS = append(res.host.Spec.TLS, netv1.IngressTLS{Hosts: hosts})
			}
		}
	}

	for name := range backendSet {
		res.backends = append(res.backends, name)
	}
	sort.Strings(res.backends)
	return res
}

func newHostIngress(tc *v1alpha1.TenantCluster, guest *netv1.Ingress) *netv1.Ingress {
	var class *string
	if tc.Spec.IngressClassName != "" {
		c := tc.Spec.IngressClassName
		class = &c
	}
	return &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HostObjectName(tc.Name, guest.Namespace, guest.Name),
			Namespace: tc.Spec.HostNamespace,
			Labels:    ownershipLabels(tc, guest),
			// Guest annotations are deliberately NOT copied. Ingress annotations
			// are executable configuration for the HOST controller —
			// configuration-snippets, auth URLs, backend protocol overrides — and
			// forwarding tenant-authored ones is an injection channel into a
			// shared data plane.
		},
		Spec: netv1.IngressSpec{
			// The class comes from the TenantCluster, never the guest object: the
			// guest's ingressClassName names a class in the guest's world.
			IngressClassName: class,
		},
	}
}
