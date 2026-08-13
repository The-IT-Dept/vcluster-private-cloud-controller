package syncer

import (
	"context"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
)

// Address families (design §3.5) and address limits (§3.6).
//
// The rule underneath both: a guest must never be left holding a <pending>
// with no reason. Whether the cause is a family the host cannot supply, a pool
// with nothing left in it, or a limit the operator set, the guest object says
// so on its own status.

func familiedGuestSvc(name string, policy corev1.IPFamilyPolicy, fams ...corev1.IPFamily) *corev1.Service {
	svc := guestSvc(name, 31034)
	svc.Spec.IPFamilyPolicy = &policy
	svc.Spec.IPFamilies = fams
	return svc
}

func TestServiceCarriesAddressFamiliesToHost(t *testing.T) {
	// A guest asking for IPv6 and silently receiving IPv4 has been given the
	// wrong thing; a dual-stack guest handed one family has been given half.
	for _, tt := range []struct {
		name   string
		policy corev1.IPFamilyPolicy
		fams   []corev1.IPFamily
	}{
		{"v4only", corev1.IPFamilyPolicySingleStack, []corev1.IPFamily{corev1.IPv4Protocol}},
		{"v6only", corev1.IPFamilyPolicySingleStack, []corev1.IPFamily{corev1.IPv6Protocol}},
		{"dual", corev1.IPFamilyPolicyRequireDualStack, []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			guest := familiedGuestSvc("echo", tt.policy, tt.fams...)
			host, refusal := mapService(testTC(), guest, corev1.ServiceTypeLoadBalancer,
				[]corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol})
			if refusal != "" {
				t.Fatalf("unexpected refusal: %s", refusal)
			}
			if host.Spec.IPFamilyPolicy == nil || *host.Spec.IPFamilyPolicy != tt.policy {
				t.Errorf("ipFamilyPolicy = %v, want %v", host.Spec.IPFamilyPolicy, tt.policy)
			}
			if len(host.Spec.IPFamilies) != len(tt.fams) {
				t.Fatalf("ipFamilies = %v, want %v", host.Spec.IPFamilies, tt.fams)
			}
			for i := range tt.fams {
				if host.Spec.IPFamilies[i] != tt.fams[i] {
					t.Errorf("ipFamilies[%d] = %s, want %s", i, host.Spec.IPFamilies[i], tt.fams[i])
				}
			}
		})
	}
}

func TestServiceRefusesFamilyTheHostCannotSupply(t *testing.T) {
	// The host has no IPv6 anywhere. Allocation would never arrive, so this is
	// a refusal now rather than a <pending> for the lifetime of the object.
	guest := familiedGuestSvc("echo", corev1.IPFamilyPolicySingleStack, corev1.IPv6Protocol)
	host, refusal := mapService(testTC(), guest, corev1.ServiceTypeLoadBalancer,
		[]corev1.IPFamily{corev1.IPv4Protocol})
	if host != nil {
		t.Error("no host Service may be created for a family the host cannot allocate")
	}
	if !strings.Contains(refusal, "IPv6") || !strings.Contains(refusal, "IPv4") {
		t.Errorf("refusal must name the family asked for and what the host has, got %q", refusal)
	}
}

func TestUnknownHostFamiliesArePermissive(t *testing.T) {
	// Discovery failing (old host, no RBAC on ServiceCIDRs) must not refuse
	// every tenant; the host API server judges the request instead.
	guest := familiedGuestSvc("echo", corev1.IPFamilyPolicySingleStack, corev1.IPv6Protocol)
	if host, refusal := mapService(testTC(), guest, corev1.ServiceTypeLoadBalancer, nil); host == nil || refusal != "" {
		t.Fatalf("unknown host families must pass through, got host=%v refusal=%q", host, refusal)
	}
}

func TestBackendServicesDoNotInheritGuestFamilies(t *testing.T) {
	// A ClusterIP backend exists so a HOST proxy can dial the tenant's
	// NodePort. Pinning it to the family a guest chose for its PUBLIC address
	// could leave that proxy unable to reach the backend at all.
	guest := familiedGuestSvc("echo", corev1.IPFamilyPolicySingleStack, corev1.IPv6Protocol)
	host, refusal := mapService(testTC(), guest, corev1.ServiceTypeClusterIP, []corev1.IPFamily{corev1.IPv4Protocol})
	if refusal != "" || host == nil {
		t.Fatalf("backend mapping must not refuse on families: %q", refusal)
	}
	if len(host.Spec.IPFamilies) != 0 || host.Spec.IPFamilyPolicy != nil {
		t.Errorf("backend Service must keep host defaults, got %v/%v", host.Spec.IPFamilies, host.Spec.IPFamilyPolicy)
	}
}

func TestEveryAllocatedAddressIsWrittenBack(t *testing.T) {
	// A dual-stack Service has TWO entries in status.loadBalancer.ingress.
	// Truncating to one hides the address half the internet reaches it on.
	guest := familiedGuestSvc("echo", corev1.IPFamilyPolicyRequireDualStack, corev1.IPv4Protocol, corev1.IPv6Protocol)
	f := newFixture(t, true, []client.Object{guest}, nil)
	ctx := context.Background()
	f.reconcile(t)

	hostName := HostObjectName("pn1", "default", "echo")
	var hostSvc corev1.Service
	if err := f.host.Get(ctx, client.ObjectKey{Namespace: "tenant-pn1", Name: hostName}, &hostSvc); err != nil {
		t.Fatalf("host Service not created: %v", err)
	}
	// Only the IPv4 arrives first: half-published, and the guest must be able
	// to tell that apart from success without counting entries itself.
	hostSvc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "192.0.2.10"}}
	if err := f.host.Status().Update(ctx, &hostSvc); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)

	var gsvc corev1.Service
	if err := f.guest.Get(ctx, client.ObjectKey{Namespace: "default", Name: "echo"}, &gsvc); err != nil {
		t.Fatal(err)
	}
	c := meta.FindStatusCondition(gsvc.Status.Conditions, HostAddressCondition)
	if c == nil || c.Reason != "AddressPartiallyAssigned" {
		t.Fatalf("half a dual-stack allocation must not read as success, got %+v", c)
	}
	if !strings.Contains(c.Message, "IPv6") {
		t.Errorf("the message must name the family that is missing, got %q", c.Message)
	}

	// Now both. Both must land in the guest's status, in order.
	if err := f.host.Get(ctx, client.ObjectKey{Namespace: "tenant-pn1", Name: hostName}, &hostSvc); err != nil {
		t.Fatal(err)
	}
	hostSvc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{
		{IP: "192.0.2.10"}, {IP: "2001:db8::10"},
	}
	if err := f.host.Status().Update(ctx, &hostSvc); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)

	if err := f.guest.Get(ctx, client.ObjectKey{Namespace: "default", Name: "echo"}, &gsvc); err != nil {
		t.Fatal(err)
	}
	got := gsvc.Status.LoadBalancer.Ingress
	if len(got) != 2 || got[0].IP != "192.0.2.10" || got[1].IP != "2001:db8::10" {
		t.Fatalf("every allocated address must be written back, got %+v", got)
	}
	if c := meta.FindStatusCondition(gsvc.Status.Conditions, HostAddressCondition); c == nil ||
		c.Status != metav1.ConditionTrue || !strings.Contains(c.Message, "2001:db8::10") {
		t.Errorf("condition must report both addresses, got %+v", c)
	}
}

// --- limits ------------------------------------------------------------------

func withLimit(tc *v1alpha1.TenantCluster, n int32) {
	tc.Spec.Limits.LoadBalancers = &n
}

// setLimit writes the limit onto the TenantCluster held by the fake host, the
// way an operator would.
func (f *fixture) setLimit(t *testing.T, n int32) {
	t.Helper()
	tc := f.getTC(t)
	withLimit(tc, n)
	if err := f.host.Update(context.Background(), tc); err != nil {
		t.Fatalf("setting limit: %v", err)
	}
}

func (f *fixture) clearLimit(t *testing.T) {
	t.Helper()
	tc := f.getTC(t)
	tc.Spec.Limits.LoadBalancers = nil
	if err := f.host.Update(context.Background(), tc); err != nil {
		t.Fatalf("clearing limit: %v", err)
	}
}

func hostLBNames(t *testing.T, f *fixture) []string {
	t.Helper()
	var list corev1.ServiceList
	if err := f.host.List(context.Background(), &list, client.InNamespace("tenant-pn1")); err != nil {
		t.Fatal(err)
	}
	var out []string
	for i := range list.Items {
		if list.Items[i].Spec.Type == corev1.ServiceTypeLoadBalancer {
			out = append(out, list.Items[i].Name)
		}
	}
	return out
}

// agedSvc gives a guest Service an explicit creation time and UID, which is
// exactly what the refusal order is derived from.
func agedSvc(name string, minute int, uid string) *corev1.Service {
	svc := guestSvc(name, 31034)
	svc.CreationTimestamp = metav1.Date(2026, 1, 1, 0, minute, 0, 0, metav1.Now().Location())
	svc.UID = types.UID(uid)
	return svc
}

func TestAddressLimitRefusesTheNewestAndSaysWhy(t *testing.T) {
	f := newFixture(t, true, []client.Object{
		agedSvc("first", 1, "uid-a"),
		agedSvc("second", 2, "uid-b"),
		agedSvc("third", 3, "uid-c"),
	}, nil)
	f.setLimit(t, 2)
	f.reconcile(t)

	got := hostLBNames(t, f)
	if len(got) != 2 {
		t.Fatalf("limit 2 must admit exactly two host LoadBalancers, got %v", got)
	}
	for _, want := range []string{HostObjectName("pn1", "default", "first"), HostObjectName("pn1", "default", "second")} {
		if !contains(got, want) {
			t.Errorf("oldest endpoints must be the admitted ones; %s missing from %v", want, got)
		}
	}

	// The refusal is visible in the guest, with the limit and the count — the
	// same courtesy a rejected hostname gets.
	var third corev1.Service
	if err := f.guest.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "third"}, &third); err != nil {
		t.Fatal(err)
	}
	c := meta.FindStatusCondition(third.Status.Conditions, HostAddressCondition)
	if c == nil || c.Reason != "SyncRefused" {
		t.Fatalf("refused Service must carry a visible reason, got %+v", c)
	}
	if !strings.Contains(c.Message, "2") || !strings.Contains(c.Message, "address limit") {
		t.Errorf("refusal must state the limit and the usage, got %q", c.Message)
	}
	if usage := f.getTC(t).Status.LoadBalancerUsage; usage != 2 {
		t.Errorf("status.loadBalancerUsage = %d, want 2", usage)
	}
}

func TestAddressLimitRefusalIsStableAcrossReconciles(t *testing.T) {
	// If which endpoint is refused flapped, a tenant's working service would
	// cycle up and down as the controller re-sorted a map.
	f := newFixture(t, true, []client.Object{
		agedSvc("first", 1, "uid-a"),
		agedSvc("second", 2, "uid-b"),
		agedSvc("third", 3, "uid-c"),
	}, nil)
	f.setLimit(t, 2)

	var previous []string
	for i := 0; i < 6; i++ {
		f.reconcile(t)
		got := hostLBNames(t, f)
		sortStrings(got)
		if previous != nil && strings.Join(got, ",") != strings.Join(previous, ",") {
			t.Fatalf("admitted set flapped on pass %d: %v then %v", i, previous, got)
		}
		previous = got
	}
}

func TestTiesBreakOnUIDNotMapOrder(t *testing.T) {
	// Two objects created in the same instant must still order deterministically.
	f := newFixture(t, true, []client.Object{
		agedSvc("bbb", 1, "uid-2"),
		agedSvc("aaa", 1, "uid-1"),
	}, nil)
	f.setLimit(t, 1)
	for i := 0; i < 4; i++ {
		f.reconcile(t)
		got := hostLBNames(t, f)
		if len(got) != 1 || got[0] != HostObjectName("pn1", "default", "aaa") {
			t.Fatalf("tie must break on the lower UID, deterministically; got %v", got)
		}
	}
}

func TestLoweringTheLimitTearsNothingDown(t *testing.T) {
	// An operator has to be able to stop a project growing without deleting
	// what it has. The alternative is a customer's production endpoint
	// disappearing because someone edited a number.
	f := newFixture(t, true, []client.Object{
		agedSvc("first", 1, "uid-a"),
		agedSvc("second", 2, "uid-b"),
	}, nil)
	f.clearLimit(t)
	f.reconcile(t)
	if got := hostLBNames(t, f); len(got) != 2 {
		t.Fatalf("unlimited must admit both, got %v", got)
	}

	f.setLimit(t, 1)
	f.reconcile(t)
	if got := hostLBNames(t, f); len(got) != 2 {
		t.Fatalf("lowering the limit must not tear anything down, got %v", got)
	}

	// It stops GROWTH, though: a new one is refused.
	third := agedSvc("third", 3, "uid-c")
	if err := f.guest.Create(context.Background(), third); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if got := hostLBNames(t, f); len(got) != 2 {
		t.Fatalf("a new endpoint must be refused while over the limit, got %v", got)
	}
	var g corev1.Service
	if err := f.guest.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "third"}, &g); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(g.Status.Conditions, HostAddressCondition); c == nil ||
		!strings.Contains(c.Message, "address limit") {
		t.Errorf("the new one must be told why, got %+v", c)
	}
}

func TestGatewaysCountAgainstTheSameLimit(t *testing.T) {
	// LoadBalancer Services and Gateways together, because both consume a pool
	// address; counting them separately would let a tenant double its share.
	gw := guestGateway("gw", "app.example.com")
	gw.CreationTimestamp = metav1.Date(2026, 1, 1, 0, 5, 0, 0, metav1.Now().Location())
	f := newFixture(t, true, []client.Object{agedSvc("first", 1, "uid-a"), gw}, nil)
	f.setLimit(t, 1)
	f.reconcile(t)

	if got := hostLBNames(t, f); len(got) != 1 {
		t.Fatalf("the Service is older and must be the admitted one, got %v", got)
	}
	if names := hostGatewayNames(t, f); len(names) != 0 {
		t.Fatalf("the Gateway must be refused by the shared limit, got %v", names)
	}
	if usage := f.getTC(t).Status.LoadBalancerUsage; usage != 1 {
		t.Errorf("status.loadBalancerUsage = %d, want 1", usage)
	}
}

func TestRefusedLoadBalancerStillServesAsAnIngressBackend(t *testing.T) {
	// The limit is on ADDRESSES. Taking the address away must not also break an
	// Ingress that was within its rights to route to that Service.
	svc := agedSvc("echo", 5, "uid-e")
	ing := guestIngress("web", "app.example.com", "echo")
	f := newFixture(t, true, []client.Object{svc, ing}, nil)
	f.setLimit(t, 0)
	f.reconcile(t)

	if got := hostLBNames(t, f); len(got) != 0 {
		t.Fatalf("limit 0 must admit no LoadBalancer, got %v", got)
	}
	var backend corev1.Service
	err := f.host.Get(context.Background(),
		client.ObjectKey{Namespace: "tenant-pn1", Name: HostObjectName("pn1", "default", "echo")}, &backend)
	if err != nil {
		t.Fatalf("the Ingress backend Service must survive as a ClusterIP: %v", err)
	}
	if backend.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("backend type = %s, want ClusterIP", backend.Spec.Type)
	}
}

func contains(list []string, want string) bool {
	for _, x := range list {
		if x == want {
			return true
		}
	}
	return false
}

func sortStrings(s []string) { sort.Strings(s) }

func guestGateway(name, host string) *gwv1.Gateway {
	h := gwv1.Hostname(host)
	return &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name)},
		Spec: gwv1.GatewaySpec{
			GatewayClassName: "cilium",
			Listeners: []gwv1.Listener{{
				Name: "web", Protocol: gwv1.HTTPProtocolType, Port: 80, Hostname: &h,
			}},
		},
	}
}

func guestIngress(name, host, backend string) *netv1.Ingress {
	pathType := netv1.PathTypePrefix
	return &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name)},
		Spec: netv1.IngressSpec{
			Rules: []netv1.IngressRule{{
				Host: host,
				IngressRuleValue: netv1.IngressRuleValue{HTTP: &netv1.HTTPIngressRuleValue{
					Paths: []netv1.HTTPIngressPath{{
						Path: "/", PathType: &pathType,
						Backend: netv1.IngressBackend{Service: &netv1.IngressServiceBackend{
							Name: backend, Port: netv1.ServiceBackendPort{Number: 80},
						}},
					}},
				}},
			}},
		},
	}
}

func hostGatewayNames(t *testing.T, f *fixture) []string {
	t.Helper()
	var list gwv1.GatewayList
	if err := f.host.List(context.Background(), &list, client.InNamespace("tenant-pn1")); err != nil {
		t.Fatal(err)
	}
	var out []string
	for i := range list.Items {
		out = append(out, list.Items[i].Name)
	}
	return out
}

func TestGatewaySpecAddressesRefusedVisibly(t *testing.T) {
	// Gateway API has no ipFamilies field; spec.addresses is the only way a
	// guest can express one, and it does so by claiming a SPECIFIC host address
	// — the loadBalancerIP hazard. Refused, but never silently: otherwise a
	// guest that asked for IPv6 holds an IPv4 with nothing saying why.
	gw := guestGateway("gw", "app.example.com")
	gw.Spec.Addresses = []gwv1.GatewaySpecAddress{{Value: "192.0.2.50"}}
	tc := testTC()
	tc.Spec.GatewayClassName = "cilium"
	host, refusals := mapGateway(tc, gw)
	if host == nil {
		t.Fatal("the listener is still valid; only the address claim is refused")
	}
	if len(host.Spec.Addresses) != 0 {
		t.Errorf("a guest address claim must not reach the host Gateway: %v", host.Spec.Addresses)
	}
	found := false
	for _, r := range refusals {
		if strings.Contains(r, "spec.addresses") && strings.Contains(r, "192.0.2.50") {
			found = true
		}
	}
	if !found {
		t.Errorf("the claim must be refused visibly, naming what was asked for: %v", refusals)
	}
}

func TestIPFamiliesAnnotationOverridesGuestSpec(t *testing.T) {
	// The case that makes the annotation necessary: a single-stack IPv4 guest
	// whose own API server would reject ipFamilies:[IPv6], asking for an IPv6
	// public address anyway.
	svc := guestSvc("echo", 31034)
	svc.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	single := corev1.IPFamilyPolicySingleStack
	svc.Spec.IPFamilyPolicy = &single
	svc.Annotations = map[string]string{IPFamiliesAnnotation: "IPv6"}

	host, refusal := mapService(testTC(), svc, corev1.ServiceTypeLoadBalancer,
		[]corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol})
	if refusal != "" {
		t.Fatalf("unexpected refusal: %s", refusal)
	}
	if len(host.Spec.IPFamilies) != 1 || host.Spec.IPFamilies[0] != corev1.IPv6Protocol {
		t.Errorf("annotation must win over spec.ipFamilies, got %v", host.Spec.IPFamilies)
	}
	if host.Spec.IPFamilyPolicy == nil || *host.Spec.IPFamilyPolicy != corev1.IPFamilyPolicySingleStack {
		t.Errorf("one family derives SingleStack, got %v", host.Spec.IPFamilyPolicy)
	}
}

func TestIPFamiliesAnnotationDualStackDerivesRequire(t *testing.T) {
	svc := guestSvc("echo", 31034)
	svc.Annotations = map[string]string{IPFamiliesAnnotation: "IPv4, IPv6"}
	host, refusal := mapService(testTC(), svc, corev1.ServiceTypeLoadBalancer, nil)
	if refusal != "" {
		t.Fatalf("unexpected refusal: %s", refusal)
	}
	if len(host.Spec.IPFamilies) != 2 {
		t.Fatalf("both families expected, got %v", host.Spec.IPFamilies)
	}
	// Require, not Prefer: a guest that asked for two and got one has been
	// handed half of what it asked for.
	if *host.Spec.IPFamilyPolicy != corev1.IPFamilyPolicyRequireDualStack {
		t.Errorf("two families derive RequireDualStack, got %v", *host.Spec.IPFamilyPolicy)
	}
}

func TestBadIPFamiliesAnnotationIsRefusedNotIgnored(t *testing.T) {
	// Ignoring a typo would hand the tenant the host default and no reason.
	for _, bad := range []string{"IPv5", "", "IPv4,IPv4", "ipv6 "} {
		svc := guestSvc("echo", 31034)
		svc.Annotations = map[string]string{IPFamiliesAnnotation: bad}
		host, refusal := mapService(testTC(), svc, corev1.ServiceTypeLoadBalancer, nil)
		if host != nil || refusal == "" {
			t.Errorf("%q must be refused, got host=%v refusal=%q", bad, host != nil, refusal)
		}
	}
}
