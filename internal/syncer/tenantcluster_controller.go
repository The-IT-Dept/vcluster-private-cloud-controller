package syncer

import (
	"context"
	"fmt"
	stdsync "sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
)

// DefaultInterval is the poll cadence against each guest.
//
// THE GUEST IS POLLED, NOT WATCHED, and that is a decision rather than a
// shortcut. An informer on a guest's API server would join this process's
// shared machinery, and guests are the least reliable API servers this
// controller talks to — they restart, they get deleted, their credentials
// rotate. A wedged watch on one tenant must never stall the syncers for every
// other tenant. Fifteen seconds costs a tenant at most that long to see an
// address appear; host-side changes (the LB allocation itself) are watched and
// reconcile immediately.
const DefaultInterval = 15 * time.Second

// GuestClientFactory turns kubeconfig bytes into a client for one guest.
// Injectable so tests can hand the reconciler a fake guest cluster.
type GuestClientFactory func(kubeconfig []byte, scheme *runtime.Scheme) (client.Client, error)

// DefaultGuestClient is the real factory.
func DefaultGuestClient(kubeconfig []byte, scheme *runtime.Scheme) (client.Client, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("parsing guest kubeconfig: %w", err)
	}
	// A hung guest API must cost a bounded slice of a pass, not a whole worker.
	cfg.Timeout = 20 * time.Second
	return client.New(cfg, client.Options{Scheme: scheme})
}

// TenantClusterReconciler runs one full sync pass per TenantCluster per poll
// interval: guest resources in, host objects out, statuses written back.
//
// Trust direction, enforced here and not by convention: the ONLY credential
// involved is the one out of the TenantCluster's kubeconfigSecretRef, read on
// the HOST and pointed INTO the guest. Nothing in this controller creates a
// Secret in a guest or writes host connection details there — the guest-facing
// writes are statuses, conditions, events, annotations and finalizers, all in
// writeback.go.
type TenantClusterReconciler struct {
	client.Client
	// Reader is an UNCACHED host reader. The manager cache is label-filtered to
	// this controller's own objects, so it must never be used to answer "does
	// an unmanaged object already hold this name" — see pass.hostReader.
	Reader   client.Reader
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Interval time.Duration

	NewGuestClient GuestClientFactory

	// Hotplug is the KubeVirt add/remove-volume surface for the storage
	// syncer; CSIImages are the images it deploys into guests as the node
	// plugin. Both injected so tests can fake the VM side.
	Hotplug   Hotplugger
	CSIImages NodePluginImages

	// guests caches one client per TenantCluster, invalidated when the
	// kubeconfig Secret changes. Rebuilding a client each pass would redo
	// discovery against every guest every fifteen seconds for nothing.
	mu     stdsync.Mutex
	guests map[types.UID]cachedGuest
}

type cachedGuest struct {
	secretRV string
	client   client.Client
}

// +kubebuilder:rbac:groups=vcluster.the-it-dept.io,resources=tenantclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=vcluster.the-it-dept.io,resources=tenantclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=vcluster.the-it-dept.io,resources=tenantclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways;httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines;virtualmachineinstances,verbs=get;list
// +kubebuilder:rbac:groups=subresources.kubevirt.io,resources=virtualmachines/addvolume;virtualmachines/removevolume,verbs=update

func (r *TenantClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// The TenantCluster is read UNCACHED, and that matters twice over. A stale
	// cache event can deliver (a) a deleted TenantCluster after a same-name
	// replacement was created — ownership labels carry name and namespace, not
	// UID, so a teardown driven by the stale copy would delete the
	// REPLACEMENT's host objects; or (b) a pre-deletion copy of a TenantCluster
	// that is already gone, which would recreate host objects nothing will ever
	// clean up. One uncached GET per pass buys out both races.
	var tc v1alpha1.TenantCluster
	if err := r.Reader.Get(ctx, req.NamespacedName, &tc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !tc.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &tc)
	}

	// The finalizer goes on before any host object exists, so a TenantCluster
	// deleted mid-first-pass still gets its cleanup.
	if controllerutil.AddFinalizer(&tc, TenantClusterFinalizer) {
		if err := r.Update(ctx, &tc); err != nil {
			return ctrl.Result{}, err
		}
	}

	guest, err := r.guestClientFor(ctx, &tc)
	if err != nil {
		return r.disconnected(ctx, &tc, err)
	}

	gatewayAPIOnHost := r.hostHasGatewayAPI()
	p := newPass(&tc, r.Client, r.Reader, guest, gatewayAPIOnHost, r.Hotplug, r.CSIImages, log)
	if err := p.run(ctx); err != nil {
		// run only fails on guest API errors; host-side trouble lands in
		// p.problems. An unreachable guest is DISCONNECTED, and disconnected
		// means nothing destructive: host objects stay exactly as they are.
		return r.disconnected(ctx, &tc, err)
	}

	tc.Status.Connected = true
	now := metav1.Now()
	tc.Status.LastSyncTime = &now
	tc.Status.ObservedResources = p.counts
	setCond(&tc, v1alpha1.CondConnected, metav1.ConditionTrue, "Connected", "the guest API is reachable")

	if tc.SyncGateways() {
		if gatewayAPIOnHost {
			setCond(&tc, v1alpha1.CondGatewayAPIAvailable, metav1.ConditionTrue, "Available",
				"the host cluster has the Gateway API")
		} else {
			// Reported, skipped, and nothing else: a missing optional API must not
			// crash the controller or block the other syncers.
			setCond(&tc, v1alpha1.CondGatewayAPIAvailable, metav1.ConditionFalse, "HostMissingCRDs",
				"the host cluster has no Gateway API CRDs; Gateway and HTTPRoute sync is skipped")
		}
	}

	issues := append([]string{}, p.problems...)
	refused := 0
	for _, msgs := range p.refusals {
		refused += len(msgs)
	}
	switch {
	case len(issues) > 0:
		setCond(&tc, v1alpha1.CondSynced, metav1.ConditionFalse, "Errors", summarize(issues, 3))
	case refused > 0:
		setCond(&tc, v1alpha1.CondSynced, metav1.ConditionFalse, "Refusals",
			fmt.Sprintf("%d hostname/backend refusal(s); details are on the guest resources", refused))
	default:
		setCond(&tc, v1alpha1.CondSynced, metav1.ConditionTrue, "AllSynced", fmt.Sprintf(
			"synced %d service(s), %d ingress(es), %d gateway(s), %d route(s), %d PVC(s)",
			p.counts.Services, p.counts.Ingresses, p.counts.Gateways, p.counts.HTTPRoutes,
			p.counts.PersistentVolumeClaims))
	}

	if err := r.Status().Update(ctx, &tc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.interval()}, nil
}

// disconnected records that the guest cannot be reached and comes back later.
// It returns nil rather than the error: an unreachable guest is a routine
// state (control plane restarting, cluster being torn down), not something the
// rate limiter should escalate with stack traces.
func (r *TenantClusterReconciler) disconnected(ctx context.Context, tc *v1alpha1.TenantCluster, cause error) (ctrl.Result, error) {
	tc.Status.Connected = false
	last := "never"
	if tc.Status.LastSyncTime != nil {
		last = fmt.Sprintf("%s ago", time.Since(tc.Status.LastSyncTime.Time).Round(time.Second))
	}
	// Host objects are deliberately untouched: a tenant's published address
	// must not disappear because its API server restarted.
	setCond(tc, v1alpha1.CondConnected, metav1.ConditionFalse, "GuestUnreachable",
		fmt.Sprintf("cannot reach the guest cluster (last successful sync: %s): %v", last, cause))
	if err := r.Status().Update(ctx, tc); err != nil {
		logf.FromContext(ctx).Info("could not record disconnected status", "error", err.Error())
	}
	return ctrl.Result{RequeueAfter: r.interval()}, nil
}

// reconcileDelete removes every host object carrying this TenantCluster's
// ownership labels, releases the guest resources it pinned, then lets the
// TenantCluster go. This closes the gap the per-guest CCM model has, where
// guest-cluster deletion orphans host Services because the controller dies
// before it can clean up.
func (r *TenantClusterReconciler) reconcileDelete(ctx context.Context, tc *v1alpha1.TenantCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(tc, TenantClusterFinalizer) {
		return ctrl.Result{}, nil
	}
	log := logf.FromContext(ctx)

	sel := client.MatchingLabels{
		LabelTenantCluster:          tc.Name,
		LabelTenantClusterNamespace: tc.Namespace,
	}
	inNS := client.InNamespace(tc.Spec.HostNamespace)

	// Storage goes first, and can hold the teardown across passes: every host
	// PVC must be unplugged from its VM BEFORE it is deleted, and unplug is
	// asynchronous. Services and Ingresses have no such ordering and are
	// deleted below regardless.
	storageDone, err := r.storageTeardown(ctx, tc)
	if err != nil {
		return ctrl.Result{}, err
	}

	failed := false
	del := func(obj client.Object) {
		if !ownedBy(obj, tc) {
			// Belt and braces on top of the label-scoped list: never delete a
			// host object that does not carry our ownership labels.
			return
		}
		uid := obj.GetUID()
		if err := r.Delete(ctx, obj, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
			failed = true
			log.Error(err, "deleting host object during teardown", "namespace", obj.GetNamespace(), "name", obj.GetName())
		}
	}

	var svcs corev1.ServiceList
	if err := r.Reader.List(ctx, &svcs, inNS, sel); err != nil {
		return ctrl.Result{}, err
	}
	for i := range svcs.Items {
		del(&svcs.Items[i])
	}
	var ings netv1.IngressList
	if err := r.Reader.List(ctx, &ings, inNS, sel); err != nil {
		return ctrl.Result{}, err
	}
	for i := range ings.Items {
		del(&ings.Items[i])
	}
	if r.hostHasGatewayAPI() {
		var gws gwv1.GatewayList
		if err := r.Reader.List(ctx, &gws, inNS, sel); err != nil && !meta.IsNoMatchError(err) {
			return ctrl.Result{}, err
		} else if err == nil {
			for i := range gws.Items {
				del(&gws.Items[i])
			}
		}
		var rts gwv1.HTTPRouteList
		if err := r.Reader.List(ctx, &rts, inNS, sel); err != nil && !meta.IsNoMatchError(err) {
			return ctrl.Result{}, err
		} else if err == nil {
			for i := range rts.Items {
				del(&rts.Items[i])
			}
		}
	}
	if failed || !storageDone {
		return ctrl.Result{RequeueAfter: r.interval()}, nil
	}

	// Release the finalizers this controller put on guest resources.
	// BEST-EFFORT ONLY: the most common reason to delete a TenantCluster is
	// that the guest cluster itself is going away, and an unreachable guest
	// must not hold host-side teardown hostage. A tenant whose cluster
	// survives can always remove the finalizer themselves — they are
	// cluster-admin there.
	if guest, err := r.guestClientFor(ctx, tc); err == nil {
		r.stripGuestFinalizers(ctx, guest)
		r.cleanupGuestStorage(ctx, guest)
	}

	r.mu.Lock()
	delete(r.guests, tc.UID)
	r.mu.Unlock()

	if controllerutil.RemoveFinalizer(tc, TenantClusterFinalizer) {
		if err := r.Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
	}
	log.Info("tenant cluster released", "tenantcluster", tc.Name)
	return ctrl.Result{}, nil
}

func (r *TenantClusterReconciler) stripGuestFinalizers(ctx context.Context, guest client.Client) {
	strip := func(obj client.Object) {
		if controllerutil.RemoveFinalizer(obj, GuestFinalizer) {
			_ = guest.Update(ctx, obj)
		}
	}
	var svcs corev1.ServiceList
	if err := guest.List(ctx, &svcs); err == nil {
		for i := range svcs.Items {
			strip(&svcs.Items[i])
		}
	}
	var ings netv1.IngressList
	if err := guest.List(ctx, &ings); err == nil {
		for i := range ings.Items {
			strip(&ings.Items[i])
		}
	}
	var gws gwv1.GatewayList
	if err := guest.List(ctx, &gws); err == nil {
		for i := range gws.Items {
			strip(&gws.Items[i])
		}
	}
	var rts gwv1.HTTPRouteList
	if err := guest.List(ctx, &rts); err == nil {
		for i := range rts.Items {
			strip(&rts.Items[i])
		}
	}
}

// guestClientFor reads the credential INTO the guest and builds (or reuses) a
// client for it.
func (r *TenantClusterReconciler) guestClientFor(ctx context.Context, tc *v1alpha1.TenantCluster) (client.Client, error) {
	ref := tc.Spec.KubeconfigSecretRef
	var sec corev1.Secret
	// The Secret lives in the TenantCluster's OWN namespace — never a
	// cross-namespace read, and read uncached so the manager cache never holds
	// every Secret on the host.
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: ref.Name}, &sec); err != nil {
		return nil, fmt.Errorf("reading kubeconfig secret %q: %w", ref.Name, err)
	}
	key := ref.Key
	if key == "" {
		key = v1alpha1.DefaultKubeconfigKey
	}
	data := sec.Data[key]
	if len(data) == 0 {
		return nil, fmt.Errorf("kubeconfig secret %q has nothing under key %q", ref.Name, key)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.guests == nil {
		r.guests = map[types.UID]cachedGuest{}
	}
	if g, ok := r.guests[tc.UID]; ok && g.secretRV == sec.ResourceVersion {
		return g.client, nil
	}
	factory := r.NewGuestClient
	if factory == nil {
		factory = DefaultGuestClient
	}
	c, err := factory(data, r.Scheme)
	if err != nil {
		return nil, fmt.Errorf("building guest client: %w", err)
	}
	r.guests[tc.UID] = cachedGuest{secretRV: sec.ResourceVersion, client: c}
	return c, nil
}

// hostHasGatewayAPI asks the host's RESTMapper, fresh each pass: installing
// the CRDs later must be picked up without a restart, and the mapper re-probes
// discovery on a miss.
func (r *TenantClusterReconciler) hostHasGatewayAPI() bool {
	_, err := r.RESTMapper().RESTMapping(
		schema.GroupKind{Group: gwv1.GroupVersion.Group, Kind: "Gateway"}, gwv1.GroupVersion.Version)
	return err == nil
}

func (r *TenantClusterReconciler) interval() time.Duration {
	if r.Interval > 0 {
		return r.Interval
	}
	return DefaultInterval
}

func setCond(tc *v1alpha1.TenantCluster, condType string, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&tc.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: tc.Generation,
	})
}

// mapHostObjectToTenant routes a host object event back to its TenantCluster
// via the ownership labels. This is why host-side LB address assignment shows
// up in the guest within a second instead of a poll interval later.
func mapHostObjectToTenant(_ context.Context, obj client.Object) []reconcile.Request {
	l := obj.GetLabels()
	name, ns := l[LabelTenantCluster], l[LabelTenantClusterNamespace]
	if name == "" || ns == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name, Namespace: ns}}}
}

func (r *TenantClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.TenantCluster{}).
		// Host Services and Ingresses are watched (label-filtered in the cache,
		// see cmd/main.go) so that the host LB allocating an address triggers an
		// immediate pass — the write-back into the guest should not wait out the
		// poll interval. Gateway kinds are NOT watched: their CRDs may not exist
		// on the host, and a watch on a missing kind kills the manager.
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(mapHostObjectToTenant)).
		Watches(&netv1.Ingress{}, handler.EnqueueRequestsFromMapFunc(mapHostObjectToTenant)).
		Complete(r)
}
