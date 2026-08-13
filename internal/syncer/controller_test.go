package syncer

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/meta/testrestmapper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
)

// Reconcile tests drive the controller against fake clients on BOTH sides —
// one playing the host cluster, one the guest — and assert the behaviours the
// design hangs on: status write-back, deletion, never-delete-unlabelled, and
// refused-hostname-produces-a-visible-reason.

func newScheme(t *testing.T, withGateway bool) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if withGateway {
		if err := gwv1.Install(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

type fixture struct {
	r     *TenantClusterReconciler
	host  client.Client
	guest client.Client
	tc    *v1alpha1.TenantCluster
	hot   *fakeHotplug
}

// getTC reads the TenantCluster's current state from the fake host.
func (f *fixture) getTC(t *testing.T) *v1alpha1.TenantCluster {
	t.Helper()
	var tc v1alpha1.TenantCluster
	if err := f.host.Get(context.Background(), types.NamespacedName{Name: "pn1", Namespace: "syncer"}, &tc); err != nil {
		t.Fatalf("reading TenantCluster: %v", err)
	}
	return &tc
}

func dsPtr() *appsv1.DaemonSet { return &appsv1.DaemonSet{} }

// newFixture wires a TenantCluster, its kubeconfig Secret, and fake clients
// for both clusters. hostGateway controls whether the fake host "has" the
// Gateway API (via its RESTMapper, exactly the check the real code does).
func newFixture(t *testing.T, hostGateway bool, guestObjs []client.Object, hostObjs []client.Object) *fixture {
	t.Helper()
	hostScheme := newScheme(t, hostGateway)
	guestScheme := newScheme(t, true)

	tc := &v1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pn1", Namespace: "syncer", UID: types.UID("tc-uid"),
			Finalizers: []string{TenantClusterFinalizer},
		},
		Spec: v1alpha1.TenantClusterSpec{
			KubeconfigSecretRef: v1alpha1.SecretKeyReference{Name: "pn1-kubeconfig"},
			HostNamespace:       "tenant-pn1",
			NodeSelector:        v1alpha1.NodeSelector{MatchLabels: map[string]string{"vm-of": "pn1"}},
			AllowedDomains:      []string{"app.example.com", "*.apps.example.com"},
			IngressClassName:    "nginx",
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pn1-kubeconfig", Namespace: "syncer"},
		Data:       map[string][]byte{"kubeconfig": []byte("unused-by-tests")},
	}

	host := fake.NewClientBuilder().WithScheme(hostScheme).
		WithRESTMapper(testrestmapper.TestOnlyStaticRESTMapper(hostScheme)).
		WithObjects(append([]client.Object{tc, secret}, hostObjs...)...).
		WithStatusSubresource(&v1alpha1.TenantCluster{}, &corev1.Service{}, &netv1.Ingress{}).
		Build()
	guest := fake.NewClientBuilder().WithScheme(guestScheme).
		WithObjects(guestObjs...).
		WithStatusSubresource(&corev1.Service{}, &netv1.Ingress{}, &gwv1.Gateway{}, &storagev1.VolumeAttachment{}).
		Build()

	hot := newFakeHotplug("node-a")
	r := &TenantClusterReconciler{
		Client: host, Reader: host, Scheme: hostScheme,
		Interval: time.Second,
		NewGuestClient: func([]byte, *runtime.Scheme) (client.Client, error) {
			return guest, nil
		},
		Hotplug:   hot,
		CSIImages: NodePluginImages{Node: "example.com/csi-node:test", Registrar: "example.com/registrar:test"},
	}
	return &fixture{r: r, host: host, guest: guest, tc: tc, hot: hot}
}

func (f *fixture) reconcile(t *testing.T) {
	t.Helper()
	_, err := f.r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "pn1", Namespace: "syncer"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func guestSvc(name string, nodePort int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name)},
		Spec: corev1.ServiceSpec{
			Type:  corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP, NodePort: nodePort}},
		},
	}
}

func TestServiceSyncAndStatusWriteBack(t *testing.T) {
	f := newFixture(t, true, []client.Object{guestSvc("echo", 31034)}, nil)
	ctx := context.Background()
	f.reconcile(t)

	hostName := HostObjectName("pn1", "default", "echo")
	var hostSvc corev1.Service
	if err := f.host.Get(ctx, client.ObjectKey{Namespace: "tenant-pn1", Name: hostName}, &hostSvc); err != nil {
		t.Fatalf("host Service not created: %v", err)
	}
	if hostSvc.Spec.Ports[0].TargetPort.IntValue() != 31034 {
		t.Errorf("host Service must target the guest NodePort, got %v", hostSvc.Spec.Ports[0].TargetPort)
	}

	// Before the host LB assigns anything, the guest must see an honest
	// "pending" condition — never a silent nothing.
	var gsvc corev1.Service
	if err := f.guest.Get(ctx, client.ObjectKey{Namespace: "default", Name: "echo"}, &gsvc); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(gsvc.Status.Conditions, HostAddressCondition); c == nil || c.Reason != "AddressPending" {
		t.Errorf("guest Service must carry an AddressPending condition, got %+v", c)
	}
	if !hasFinalizer(&gsvc, GuestFinalizer) {
		t.Error("guest Service must be pinned by the finalizer while host state exists")
	}

	// The host LB assigns an address (documentation range); the next pass must
	// write it back — this is what makes the syncer real rather than a bridge.
	hostSvc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "192.0.2.10"}}
	if err := f.host.Status().Update(ctx, &hostSvc); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)

	if err := f.guest.Get(ctx, client.ObjectKey{Namespace: "default", Name: "echo"}, &gsvc); err != nil {
		t.Fatal(err)
	}
	if len(gsvc.Status.LoadBalancer.Ingress) != 1 || gsvc.Status.LoadBalancer.Ingress[0].IP != "192.0.2.10" {
		t.Fatalf("address not written back to guest: %+v", gsvc.Status.LoadBalancer)
	}
	if c := meta.FindStatusCondition(gsvc.Status.Conditions, HostAddressCondition); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("condition must flip to AddressAssigned, got %+v", c)
	}
}

func TestGuestServiceDeletionRemovesHostObject(t *testing.T) {
	f := newFixture(t, true, []client.Object{guestSvc("echo", 31034)}, nil)
	ctx := context.Background()
	f.reconcile(t)

	var gsvc corev1.Service
	if err := f.guest.Get(ctx, client.ObjectKey{Namespace: "default", Name: "echo"}, &gsvc); err != nil {
		t.Fatal(err)
	}
	// The finalizer added by the first pass holds the object through delete.
	if err := f.guest.Delete(ctx, &gsvc); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)

	// Host Service must be gone — this is the address leak the finalizer
	// exists to close.
	var hostSvc corev1.Service
	err := f.host.Get(ctx, client.ObjectKey{Namespace: "tenant-pn1", Name: HostObjectName("pn1", "default", "echo")}, &hostSvc)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("host Service must be deleted with the guest Service, got err=%v", err)
	}
	// And the guest Service must be released (fully gone once the finalizer drops).
	err = f.guest.Get(ctx, client.ObjectKey{Namespace: "default", Name: "echo"}, &gsvc)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("guest Service must be released after host cleanup, got err=%v finalizers=%v", err, gsvc.Finalizers)
	}
}

func TestNeverDeleteOrAdoptUnlabelledHostObjects(t *testing.T) {
	// Two traps: an unrelated Service in the tenant namespace, and a Service
	// that happens to hold the exact name the syncer would use. Neither
	// carries ownership labels, so neither may be deleted OR overwritten —
	// deleting something we did not create is the worst available outcome.
	unrelated := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "operator-thing", Namespace: "tenant-pn1", UID: "u1"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 9090}}},
	}
	squatter := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: HostObjectName("pn1", "default", "echo"), Namespace: "tenant-pn1", UID: "u2"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 1234}}},
	}
	f := newFixture(t, true, []client.Object{guestSvc("echo", 31034)}, []client.Object{unrelated, squatter})
	ctx := context.Background()
	f.reconcile(t)

	var got corev1.Service
	if err := f.host.Get(ctx, client.ObjectKeyFromObject(unrelated), &got); err != nil {
		t.Fatalf("unrelated Service must survive: %v", err)
	}
	if err := f.host.Get(ctx, client.ObjectKeyFromObject(squatter), &got); err != nil {
		t.Fatalf("name-squatting Service must survive: %v", err)
	}
	if got.Spec.Ports[0].Port != 1234 || got.Labels[LabelTenantCluster] != "" {
		t.Errorf("squatter must not be adopted or rewritten: %+v", got)
	}
	// The collision must be reported, not swallowed.
	var tc v1alpha1.TenantCluster
	if err := f.host.Get(ctx, client.ObjectKey{Namespace: "syncer", Name: "pn1"}, &tc); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(tc.Status.Conditions, v1alpha1.CondSynced); c == nil || c.Status != metav1.ConditionFalse {
		t.Errorf("collision must surface as Synced=False, got %+v", c)
	}
}

func TestRefusedHostnameProducesReasonNotObject(t *testing.T) {
	backend := guestSvc("web-svc", 30080)
	backend.Spec.Type = corev1.ServiceTypeNodePort
	ing := &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: "ing-uid"},
		Spec:       netv1.IngressSpec{Rules: []netv1.IngressRule{ingressRule("app.tenant-b.example.net", "web-svc")}},
	}
	f := newFixture(t, true, []client.Object{backend, ing}, nil)
	ctx := context.Background()
	f.reconcile(t)

	// No host Ingress may exist for a refused hostname.
	var ings netv1.IngressList
	if err := f.host.List(ctx, &ings, client.InNamespace("tenant-pn1")); err != nil {
		t.Fatal(err)
	}
	if len(ings.Items) != 0 {
		t.Fatalf("refused hostname must not produce a host Ingress: %+v", ings.Items)
	}

	// The refusal must be visible IN THE GUEST: annotation naming the hostname
	// and the allowed domains, and an Event.
	var ging netv1.Ingress
	if err := f.guest.Get(ctx, client.ObjectKey{Namespace: "default", Name: "web"}, &ging); err != nil {
		t.Fatal(err)
	}
	ann := ging.Annotations[RefusedAnnotation]
	for _, want := range []string{"app.tenant-b.example.net", "app.example.com", "*.apps.example.com"} {
		if !strings.Contains(ann, want) {
			t.Errorf("refusal annotation %q must name %q", ann, want)
		}
	}
	var events corev1.EventList
	if err := f.guest.List(ctx, &events, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range events.Items {
		if e.Reason == "HostnameRefused" && e.InvolvedObject.Name == "web" {
			found = true
		}
	}
	if !found {
		t.Error("a HostnameRefused event must be recorded on the guest Ingress")
	}
}

func TestAllowedIngressSyncsWithBackendService(t *testing.T) {
	backend := guestSvc("web-svc", 30080)
	backend.Spec.Type = corev1.ServiceTypeNodePort
	backend.Spec.Ports[0].Port = 8080
	ing := &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: "ing-uid"},
		Spec:       netv1.IngressSpec{Rules: []netv1.IngressRule{ingressRule("app.example.com", "web-svc")}},
	}
	f := newFixture(t, true, []client.Object{backend, ing}, nil)
	ctx := context.Background()
	f.reconcile(t)

	var hostIng netv1.Ingress
	if err := f.host.Get(ctx, client.ObjectKey{Namespace: "tenant-pn1", Name: HostObjectName("pn1", "default", "web")}, &hostIng); err != nil {
		t.Fatalf("host Ingress not created: %v", err)
	}
	var hostBackend corev1.Service
	if err := f.host.Get(ctx, client.ObjectKey{Namespace: "tenant-pn1", Name: HostObjectName("pn1", "default", "web-svc")}, &hostBackend); err != nil {
		t.Fatalf("host backend Service not created: %v", err)
	}
	// The backend is ClusterIP (the host ingress controller routes to it) and
	// targets the guest NodePort like every host Service here.
	if hostBackend.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("backend Service type = %s, want ClusterIP", hostBackend.Spec.Type)
	}
	if hostBackend.Spec.Ports[0].TargetPort.IntValue() != 30080 {
		t.Errorf("backend targetPort = %v, want the guest NodePort 30080", hostBackend.Spec.Ports[0].TargetPort)
	}

	// Ingress status write-back: give the host Ingress an address, next pass
	// must copy it into the guest.
	hostIng.Status.LoadBalancer.Ingress = []netv1.IngressLoadBalancerIngress{{IP: "192.0.2.30"}}
	if err := f.host.Status().Update(ctx, &hostIng); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	var ging netv1.Ingress
	if err := f.guest.Get(ctx, client.ObjectKey{Namespace: "default", Name: "web"}, &ging); err != nil {
		t.Fatal(err)
	}
	if len(ging.Status.LoadBalancer.Ingress) != 1 || ging.Status.LoadBalancer.Ingress[0].IP != "192.0.2.30" {
		t.Errorf("ingress status not written back: %+v", ging.Status.LoadBalancer)
	}
}

func TestTenantClusterDeletionRemovesEverything(t *testing.T) {
	backend := guestSvc("web-svc", 30080)
	backend.Spec.Type = corev1.ServiceTypeNodePort
	ing := &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: "ing-uid"},
		Spec:       netv1.IngressSpec{Rules: []netv1.IngressRule{ingressRule("app.example.com", "web-svc")}},
	}
	bystander := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "not-ours", Namespace: "tenant-pn1", UID: "u3"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 5000}}},
	}
	f := newFixture(t, true, []client.Object{guestSvc("echo", 31034), backend, ing}, []client.Object{bystander})
	ctx := context.Background()
	f.reconcile(t)

	// Sanity: host objects exist before teardown.
	var svcs corev1.ServiceList
	if err := f.host.List(ctx, &svcs, client.InNamespace("tenant-pn1"), client.MatchingLabels{LabelTenantCluster: "pn1"}); err != nil {
		t.Fatal(err)
	}
	if len(svcs.Items) == 0 {
		t.Fatal("expected synced host Services before teardown")
	}

	var tc v1alpha1.TenantCluster
	if err := f.host.Get(ctx, client.ObjectKey{Namespace: "syncer", Name: "pn1"}, &tc); err != nil {
		t.Fatal(err)
	}
	if err := f.host.Delete(ctx, &tc); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)

	// Everything labelled for this tenant is gone; the bystander survives.
	if err := f.host.List(ctx, &svcs, client.InNamespace("tenant-pn1"), client.MatchingLabels{LabelTenantCluster: "pn1"}); err != nil {
		t.Fatal(err)
	}
	if len(svcs.Items) != 0 {
		t.Errorf("teardown must remove every owned host Service: %+v", svcs.Items)
	}
	var got corev1.Service
	if err := f.host.Get(ctx, client.ObjectKeyFromObject(bystander), &got); err != nil {
		t.Errorf("bystander must survive teardown: %v", err)
	}
	// The TenantCluster itself is released.
	err := f.host.Get(ctx, client.ObjectKey{Namespace: "syncer", Name: "pn1"}, &tc)
	if !apierrors.IsNotFound(err) {
		t.Errorf("TenantCluster must be gone after teardown, got %v", err)
	}
	// And guest objects are unpinned, so the tenant can delete them freely.
	var gsvc corev1.Service
	if err := f.guest.Get(ctx, client.ObjectKey{Namespace: "default", Name: "echo"}, &gsvc); err != nil {
		t.Fatal(err)
	}
	if hasFinalizer(&gsvc, GuestFinalizer) {
		t.Error("guest finalizers must be stripped on TenantCluster deletion")
	}
}

func TestGatewayAPIAbsentOnHostIsReportedAndSkipped(t *testing.T) {
	// Host WITHOUT Gateway API types: the syncer must report it in status and
	// keep syncing everything else — never crash, never block.
	f := newFixture(t, false, []client.Object{guestSvc("echo", 31034)}, nil)
	ctx := context.Background()
	f.reconcile(t)

	var hostSvc corev1.Service
	if err := f.host.Get(ctx, client.ObjectKey{Namespace: "tenant-pn1", Name: HostObjectName("pn1", "default", "echo")}, &hostSvc); err != nil {
		t.Fatalf("Service sync must proceed without Gateway API: %v", err)
	}
	var tc v1alpha1.TenantCluster
	if err := f.host.Get(ctx, client.ObjectKey{Namespace: "syncer", Name: "pn1"}, &tc); err != nil {
		t.Fatal(err)
	}
	c := meta.FindStatusCondition(tc.Status.Conditions, v1alpha1.CondGatewayAPIAvailable)
	if c == nil || c.Status != metav1.ConditionFalse {
		t.Errorf("missing Gateway API must be visible in status, got %+v", c)
	}
}

func TestGuestUnreachableTouchesNothing(t *testing.T) {
	f := newFixture(t, true, []client.Object{guestSvc("echo", 31034)}, nil)
	ctx := context.Background()
	f.reconcile(t)

	hostKey := client.ObjectKey{Namespace: "tenant-pn1", Name: HostObjectName("pn1", "default", "echo")}
	var hostSvc corev1.Service
	if err := f.host.Get(ctx, hostKey, &hostSvc); err != nil {
		t.Fatal(err)
	}

	// The guest goes away. Host objects must remain exactly as they are: a
	// tenant's published address must not disappear because its API server
	// restarted.
	f.r.NewGuestClient = func([]byte, *runtime.Scheme) (client.Client, error) {
		return nil, context.DeadlineExceeded
	}
	f.r.guests = nil // drop the cached client, as a credential rotation would
	f.reconcile(t)

	if err := f.host.Get(ctx, hostKey, &hostSvc); err != nil {
		t.Fatalf("host Service must survive guest unreachability: %v", err)
	}
	var tc v1alpha1.TenantCluster
	if err := f.host.Get(ctx, client.ObjectKey{Namespace: "syncer", Name: "pn1"}, &tc); err != nil {
		t.Fatal(err)
	}
	if tc.Status.Connected {
		t.Error("status must report connected=false")
	}
	if c := meta.FindStatusCondition(tc.Status.Conditions, v1alpha1.CondConnected); c == nil || c.Status != metav1.ConditionFalse {
		t.Errorf("Connected condition must be False, got %+v", c)
	}
}

func hasFinalizer(obj client.Object, f string) bool {
	for _, x := range obj.GetFinalizers() {
		if x == f {
			return true
		}
	}
	return false
}
