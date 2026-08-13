package syncer

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
)

// Pure-function mapping tests: guest object in, host object (or refusal) out.

func testTC() *v1alpha1.TenantCluster {
	return &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "pn1", Namespace: "syncer", UID: "tc-uid"},
		Spec: v1alpha1.TenantClusterSpec{
			KubeconfigSecretRef: v1alpha1.SecretKeyReference{Name: "pn1-kubeconfig"},
			HostNamespace:       "tenant-pn1",
			NodeSelector:        v1alpha1.NodeSelector{MatchLabels: map[string]string{"vm-of": "pn1"}},
			AllowedDomains:      []string{"app.example.com", "*.apps.example.com"},
			IngressClassName:    "nginx",
			GatewayClassName:    "host-gw",
		},
	}
}

func guestLBService(name string, nodePort int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("svc-uid-" + name)},
		Spec: corev1.ServiceSpec{
			Type:  corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP, NodePort: nodePort}},
		},
	}
}

func TestMapServiceTargetsGuestNodePort(t *testing.T) {
	tc := testTC()
	host, refusal := mapService(tc, guestLBService("echo", 31034), corev1.ServiceTypeLoadBalancer)
	if refusal != "" {
		t.Fatalf("unexpected refusal: %s", refusal)
	}
	// The selector is the TenantCluster's node selector, NOT anything from the
	// guest: the pods being selected are the tenant's VM pods on the host.
	if host.Spec.Selector["vm-of"] != "pn1" {
		t.Errorf("selector = %v, want the TenantCluster nodeSelector", host.Spec.Selector)
	}
	// Port 80 stays the advertised port (the customer's URL), while the target
	// is the guest's NodePort — that hop is the entire mechanism.
	p := host.Spec.Ports[0]
	if p.Port != 80 || p.TargetPort.IntValue() != 31034 {
		t.Errorf("port mapping %d->%v, want 80->31034", p.Port, p.TargetPort)
	}
	if p.NodePort != 0 {
		t.Errorf("host NodePort must be left for the host to assign, got %d", p.NodePort)
	}
	if host.Namespace != "tenant-pn1" {
		t.Errorf("host namespace = %s", host.Namespace)
	}
	for _, l := range []string{LabelTenantCluster, LabelTenantClusterNamespace, LabelGuestNamespace, LabelGuestName, LabelGuestUID} {
		if host.Labels[l] == "" {
			t.Errorf("ownership label %s missing — prune and the deletion guard depend on it", l)
		}
	}
}

func TestMapServiceRefusesWithoutNodePort(t *testing.T) {
	// No NodePort means no host-reachable path; the refusal must say so rather
	// than producing a host Service that black-holes traffic.
	svc := guestLBService("echo", 0)
	if host, refusal := mapService(testTC(), svc, corev1.ServiceTypeLoadBalancer); host != nil || refusal == "" {
		t.Fatalf("want refusal for missing NodePort, got host=%v refusal=%q", host, refusal)
	}
}

func TestMapServiceDropsGuestAnnotationsAndLoadBalancerIP(t *testing.T) {
	// Guest annotations are instructions to HOST controllers (address-pool
	// selection above all); forwarding them would let a tenant pick another
	// tenant's pool.
	svc := guestLBService("echo", 31034)
	svc.Annotations = map[string]string{"lbipam.example.com/pool": "someone-elses-pool"}
	svc.Spec.LoadBalancerIP = "192.0.2.99"
	host, _ := mapService(testTC(), svc, corev1.ServiceTypeLoadBalancer)
	if len(host.Annotations) != 0 {
		t.Errorf("guest annotations must not reach the host Service: %v", host.Annotations)
	}
	if host.Spec.LoadBalancerIP != "" {
		t.Errorf("guest loadBalancerIP must not reach the host Service: %s", host.Spec.LoadBalancerIP)
	}
}

func TestWantsLoadBalancerClassPartitioning(t *testing.T) {
	tc := testTC()
	classed := guestLBService("a", 31000)
	cls := "vcluster.the-it-dept.io/tenant-syncer"
	classed.Spec.LoadBalancerClass = &cls
	plain := guestLBService("b", 31001)

	// Default: serve nil-class Services only — the Kubernetes convention that
	// lets another LB implementation own its declared class.
	if wantsLoadBalancer(tc, classed) {
		t.Error("nil-class mode must ignore classed Services")
	}
	if !wantsLoadBalancer(tc, plain) {
		t.Error("nil-class mode must serve plain Services")
	}

	// Classed mode: serve exactly our class, leave nil-class Services to the
	// incumbent (this is how the syncer coexists with a deployed CCM).
	tc.Spec.Sync.ServiceLoadBalancerClass = cls
	if !wantsLoadBalancer(tc, classed) {
		t.Error("classed mode must serve our class")
	}
	if wantsLoadBalancer(tc, plain) {
		t.Error("classed mode must not steal nil-class Services")
	}
}

func TestMapIngressAllowedAndRefusedRules(t *testing.T) {
	tc := testTC()
	guest := &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: "ing-uid"},
		Spec: netv1.IngressSpec{
			Rules: []netv1.IngressRule{
				ingressRule("app.example.com", "web-svc"),
				ingressRule("evil.example.net", "web-svc"), // another tenant's name
			},
			TLS: []netv1.IngressTLS{{Hosts: []string{"app.example.com", "evil.example.net"}, SecretName: "guest-cert"}},
		},
	}
	res := mapIngress(tc, guest, func(string) string { return "" })

	if res.host == nil {
		t.Fatal("the allowed rule must still sync — one refused hostname must not sink the whole Ingress")
	}
	if len(res.host.Spec.Rules) != 1 || res.host.Spec.Rules[0].Host != "app.example.com" {
		t.Errorf("host rules = %+v, want only app.example.com", res.host.Spec.Rules)
	}
	if len(res.refusals) == 0 || !strings.Contains(strings.Join(res.refusals, " "), "evil.example.net") {
		t.Errorf("the refused hostname must be named in the refusal: %v", res.refusals)
	}
	// The backend is rewritten to the host-side Service for the guest backend.
	got := res.host.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Name
	if want := HostObjectName("pn1", "default", "web-svc"); got != want {
		t.Errorf("backend rewritten to %q, want %q", got, want)
	}
	if cls := res.host.Spec.IngressClassName; cls == nil || *cls != "nginx" {
		t.Errorf("ingressClassName must come from the TenantCluster, got %v", cls)
	}
	// TLS: allowed host kept, refused host dropped, and the secretName NEVER
	// copied — the certificate key lives in the guest's trust domain.
	if len(res.host.Spec.TLS) != 1 || res.host.Spec.TLS[0].SecretName != "" {
		t.Fatalf("TLS = %+v, want one entry with no secretName", res.host.Spec.TLS)
	}
	if got := res.host.Spec.TLS[0].Hosts; len(got) != 1 || got[0] != "app.example.com" {
		t.Errorf("TLS hosts = %v, want only the allowed one", got)
	}
}

func TestMapIngressRefusesDefaultBackendAndEmptyHost(t *testing.T) {
	tc := testTC()
	guest := &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "catchall", Namespace: "default"},
		Spec: netv1.IngressSpec{
			DefaultBackend: &netv1.IngressBackend{Service: &netv1.IngressServiceBackend{Name: "x"}},
			Rules:          []netv1.IngressRule{ingressRule("", "x")},
		},
	}
	res := mapIngress(tc, guest, func(string) string { return "" })
	if res.host != nil {
		t.Fatal("a catch-all Ingress must produce no host object: it would receive every tenant's traffic")
	}
	if len(res.refusals) != 2 {
		t.Errorf("both the defaultBackend and the empty-host rule must be refused: %v", res.refusals)
	}
}

func TestMapIngressBackendRefusalIsExplained(t *testing.T) {
	res := mapIngress(testTC(), &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
		Spec:       netv1.IngressSpec{Rules: []netv1.IngressRule{ingressRule("app.example.com", "missing")}},
	}, func(string) string { return "not found in the guest cluster" })
	if res.host != nil {
		t.Fatal("a rule whose only backend is unusable must not sync")
	}
	if len(res.refusals) != 1 || !strings.Contains(res.refusals[0], "missing") {
		t.Errorf("the refusal must name the backend: %v", res.refusals)
	}
}

func TestMapGatewayListenerValidation(t *testing.T) {
	tc := testTC()
	good := gwv1.Hostname("x.apps.example.com")
	deep := gwv1.Hostname("a.b.apps.example.com")
	gw := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default", UID: "gw-uid"},
		Spec: gwv1.GatewaySpec{
			GatewayClassName: "guest-class",
			Listeners: []gwv1.Listener{
				{Name: "ok", Hostname: &good, Port: 443, Protocol: gwv1.HTTPSProtocolType},
				{Name: "deep", Hostname: &deep, Port: 443, Protocol: gwv1.HTTPSProtocolType},
				{Name: "open", Port: 80, Protocol: gwv1.HTTPProtocolType}, // no hostname
			},
		},
	}
	host, refusals := mapGateway(tc, gw)
	if host == nil || len(host.Spec.Listeners) != 1 || host.Spec.Listeners[0].Name != "ok" {
		t.Fatalf("exactly the validated listener must survive, got %+v", host)
	}
	// Wildcard depth applies here exactly as in Ingress: one label only.
	if len(refusals) != 2 {
		t.Errorf("the deep hostname and the open listener must both be refused: %v", refusals)
	}
	if host.Spec.GatewayClassName != "host-gw" {
		t.Errorf("gatewayClassName must come from the TenantCluster, got %s", host.Spec.GatewayClassName)
	}
}

func TestHostObjectNameStableAndBounded(t *testing.T) {
	a := HostObjectName("pn1", "default", "echo")
	if a != HostObjectName("pn1", "default", "echo") {
		t.Error("names must be deterministic — a recreated guest object must map to the SAME host object")
	}
	long := HostObjectName("pn1", strings.Repeat("verylongnamespace", 4), strings.Repeat("verylongname", 4))
	if len(long) > 63 {
		t.Errorf("name %q exceeds the DNS label limit", long)
	}
	other := HostObjectName("pn1", strings.Repeat("verylongnamespace", 4), strings.Repeat("verylongname", 4)+"x")
	if long == other {
		t.Error("truncated names must not collide — the hash suffix exists for this")
	}
}

func ingressRule(host, backend string) netv1.IngressRule {
	pt := netv1.PathTypePrefix
	return netv1.IngressRule{
		Host: host,
		IngressRuleValue: netv1.IngressRuleValue{HTTP: &netv1.HTTPIngressRuleValue{
			Paths: []netv1.HTTPIngressPath{{
				Path: "/", PathType: &pt,
				Backend: netv1.IngressBackend{Service: &netv1.IngressServiceBackend{
					Name: backend, Port: netv1.ServiceBackendPort{Number: 8080},
				}},
			}},
		}},
	}
}

func TestMapGatewayRefusesWhenNoGatewayClassConfigured(t *testing.T) {
	// An unset gatewayClassName previously stamped "" onto the host Gateway,
	// which the API server rejects once per pass with the reason visible only
	// to the operator. The tenant is owed it on their own object.
	tc := testTC()
	tc.Spec.GatewayClassName = ""
	good := gwv1.Hostname("x.apps.example.com")
	gw := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "default", UID: "gw-uid"},
		Spec: gwv1.GatewaySpec{Listeners: []gwv1.Listener{
			{Name: "ok", Hostname: &good, Port: 80, Protocol: gwv1.HTTPProtocolType},
		}},
	}
	host, refusals := mapGateway(tc, gw)
	if host != nil {
		t.Fatalf("no host Gateway may be built without a class, got %+v", host)
	}
	if len(refusals) != 1 || !strings.Contains(refusals[0], "gatewayClassName") {
		t.Errorf("the refusal must name the missing setting, got %v", refusals)
	}
}

func TestMirrorGatewayClassNamesOurOwnController(t *testing.T) {
	gc := mirrorGatewayClass(testTC())
	if gc.Name != "host-gw" {
		t.Errorf("the guest class must carry the host name the tenant has to write, got %q", gc.Name)
	}
	// Naming the HOST's controller in the guest would be untrue — there is no
	// such controller there — and would invite one to fight us for the class.
	if gc.Spec.ControllerName != GuestGatewayControllerName {
		t.Errorf("guest class must name the syncer, got %q", gc.Spec.ControllerName)
	}
	if gc.Labels[ManagedByLabel] != ManagedByValue {
		t.Errorf("the mirror must be identifiable for prune, got %v", gc.Labels)
	}
	if gc.Spec.Description == nil || !strings.Contains(*gc.Spec.Description, "app.example.com") {
		t.Errorf("the description should tell the tenant which hostnames are allowed, got %v", gc.Spec.Description)
	}
}

// routeWithFilters builds a route whose only backend is "echo", carrying the
// given filters at the rule level.
func routeWithFilters(filters []gwv1.HTTPRouteFilter) *gwv1.HTTPRoute {
	host := gwv1.Hostname("app.example.com")
	return &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "rt", Namespace: "default", UID: "rt-uid"},
		Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{ParentRefs: []gwv1.ParentReference{{Name: "gw"}}},
			Hostnames:       []gwv1.Hostname{host},
			Rules: []gwv1.HTTPRouteRule{{
				Filters:     filters,
				BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "echo"}}}},
			}},
		},
	}
}

func okResolver(string) string { return "" }

func TestMapHTTPRouteRewritesRequestMirrorBackend(t *testing.T) {
	// A RequestMirror backendRef is a Service reference like any other. Left
	// unrewritten it resolves on the HOST against whatever bears that name.
	f := []gwv1.HTTPRouteFilter{{
		Type: gwv1.HTTPRouteFilterRequestMirror,
		RequestMirror: &gwv1.HTTPRequestMirrorFilter{
			BackendRef: gwv1.BackendObjectReference{Name: "echo"},
		},
	}}
	res := mapHTTPRoute(testTC(), routeWithFilters(f), okResolver)
	if res.host == nil {
		t.Fatalf("route must sync, refusals: %v", res.refusals)
	}
	got := res.host.Spec.Rules[0].Filters[0].RequestMirror.BackendRef.Name
	want := gwv1.ObjectName(HostObjectName("pn1", "default", "echo"))
	if got != want {
		t.Errorf("mirror backend must be rewritten to the host name: got %q want %q", got, want)
	}
	// And the host Service for it has to be materialised, or the mirror
	// silently points at nothing.
	if len(res.backends) != 1 || res.backends[0] != "echo" {
		t.Errorf("the mirror backend must be pulled into the backend set, got %v", res.backends)
	}
}

func TestMapHTTPRouteRefusesExtensionRefFilter(t *testing.T) {
	// ExtensionRef names an implementation-specific CRD in the HOST namespace.
	f := []gwv1.HTTPRouteFilter{{
		Type:         gwv1.HTTPRouteFilterExtensionRef,
		ExtensionRef: &gwv1.LocalObjectReference{Group: "cilium.io", Kind: "CiliumEnvoyConfig", Name: "sneaky"},
	}}
	res := mapHTTPRoute(testTC(), routeWithFilters(f), okResolver)
	if res.host != nil && len(res.host.Spec.Rules[0].Filters) != 0 {
		t.Errorf("an ExtensionRef filter must never reach the host, got %+v", res.host.Spec.Rules[0].Filters)
	}
	if len(res.refusals) != 1 || !strings.Contains(res.refusals[0], "ExtensionRef") {
		t.Errorf("the refusal must name the filter, got %v", res.refusals)
	}
}

func TestMapHTTPRouteRefusesCrossNamespaceBackend(t *testing.T) {
	// The resolver only ever looks in the route's own namespace, so silently
	// dropping the namespace would send traffic to a same-named Service in the
	// WRONG one.
	other := gwv1.Namespace("kube-system")
	rt := routeWithFilters(nil)
	rt.Spec.Rules[0].BackendRefs[0].Namespace = &other
	res := mapHTTPRoute(testTC(), rt, okResolver)
	if res.host != nil {
		t.Errorf("no host route should survive its only backend being refused, got %+v", res.host)
	}
	if len(res.refusals) != 1 || !strings.Contains(res.refusals[0], "cross-namespace") {
		t.Errorf("the refusal must say why, got %v", res.refusals)
	}
}
