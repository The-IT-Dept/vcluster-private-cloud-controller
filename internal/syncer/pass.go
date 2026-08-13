package syncer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
)

// pass is one full sync of one TenantCluster: gather guest state, materialise
// host state, prune what is no longer wanted, write results back to the guest.
//
// It is a struct rather than a long function so each syncer contributes its
// slice (gather*, writeBack*) independently — one file per syncer, one shared
// skeleton — and so the tests can drive a pass with fake clients on both sides.
type pass struct {
	tc    *v1alpha1.TenantCluster
	host  client.Client
	// hostReader is deliberately UNCACHED. The manager cache is label-filtered
	// to our own objects, so a cached Get would report an unlabelled host
	// object as absent — and "absent" is exactly the wrong answer when the
	// question is "would creating this name stomp on something we don't own".
	hostReader client.Reader
	guest      client.Client
	log        logr.Logger

	gatewayAPIOnHost bool

	// Storage collaborators, injected from the reconciler: the KubeVirt
	// hotplug surface and the images for the guest node plugin.
	hot    Hotplugger
	images NodePluginImages
	// storage is the storage slice's working state for this pass; nil when
	// storage sync is off or its gather failed.
	storage *storageState

	// Desired host objects, keyed by host object name.
	services  map[string]*corev1.Service
	ingresses map[string]*netv1.Ingress
	gateways  map[string]*gwv1.Gateway
	routes    map[string]*gwv1.HTTPRoute

	// All guest Services by namespace/name — backends resolve against this.
	guestSvcs map[guestKey]*corev1.Service

	// guestObjs holds every non-deleting guest object in scope, so refusals and
	// finalizers can be written to the object that was actually listed.
	guestObjs map[guestObjRef]client.Object

	// Write-back bookkeeping: which guest object each host object came from.
	lbPairs      []svcPair
	ingressPairs []ingressPair
	gatewayPairs []gatewayPair

	// wantFinalizer holds every guest object that has host-side state right
	// now. Anything we previously pinned that is no longer here gets its
	// finalizer stripped after a clean prune.
	wantFinalizer map[guestObjRef]bool
	// seenFinalizer holds every guest object observed carrying our finalizer.
	seenFinalizer []finalizerHolder

	// refusals per guest object, to surface as events + annotations/conditions.
	refusals map[guestObjRef][]string

	counts   v1alpha1.ResourceCounts
	problems []string // collisions and apply errors, summarised on the TenantCluster
	// pruneFailed blocks finalizer removal: a guest object must not be released
	// while a host object created for it may still exist.
	pruneFailed bool
}

type guestKey struct{ namespace, name string }

type guestObjRef struct {
	gvk schema.GroupVersionKind
	key guestKey
}

type finalizerHolder struct {
	obj client.Object
	ref guestObjRef
}

type svcPair struct {
	guest    *corev1.Service
	hostName string
}
type ingressPair struct {
	guest    *netv1.Ingress
	hostName string
}
type gatewayPair struct {
	guest    *gwv1.Gateway
	hostName string
}

var (
	svcGVK     = corev1.SchemeGroupVersion.WithKind("Service")
	ingressGVK = netv1.SchemeGroupVersion.WithKind("Ingress")
	gatewayGVK = gwv1.SchemeGroupVersion.WithKind("Gateway")
	routeGVK   = gwv1.SchemeGroupVersion.WithKind("HTTPRoute")
)

func newPass(tc *v1alpha1.TenantCluster, host client.Client, hostReader client.Reader, guest client.Client, gatewayAPIOnHost bool, hot Hotplugger, images NodePluginImages, log logr.Logger) *pass {
	return &pass{
		tc: tc, host: host, hostReader: hostReader, guest: guest, log: log,
		gatewayAPIOnHost: gatewayAPIOnHost,
		hot:              hot, images: images,
		services:         map[string]*corev1.Service{},
		ingresses:        map[string]*netv1.Ingress{},
		gateways:         map[string]*gwv1.Gateway{},
		routes:           map[string]*gwv1.HTTPRoute{},
		guestSvcs:        map[guestKey]*corev1.Service{},
		guestObjs:        map[guestObjRef]client.Object{},
		wantFinalizer:    map[guestObjRef]bool{},
		refusals:         map[guestObjRef][]string{},
	}
}

// run executes the pass. The first guest List doubles as the connectivity
// probe: if the guest is unreachable the pass stops BEFORE any host mutation,
// because "guest unreachable" must never become "tenant's addresses deleted".
func (p *pass) run(ctx context.Context) error {
	if err := p.gatherServices(ctx); err != nil {
		return fmt.Errorf("listing guest services: %w", err)
	}
	if p.tc.SyncIngresses() {
		if err := p.gatherIngresses(ctx); err != nil {
			return fmt.Errorf("listing guest ingresses: %w", err)
		}
	}
	if p.tc.SyncGateways() && p.gatewayAPIOnHost {
		// Guest-side absence of the Gateway API is normal (a fresh guest has no
		// CRDs) and is treated as zero resources, not an error.
		if err := p.gatherGateways(ctx); err != nil {
			return fmt.Errorf("listing guest gateway resources: %w", err)
		}
	}
	if err := p.gatherStorage(ctx); err != nil {
		return fmt.Errorf("listing guest storage resources: %w", err)
	}

	// Finalizers go on BEFORE host objects are created, so there is no window
	// in which a guest resource with host-side state can vanish uncleanly.
	p.ensureGuestFinalizers(ctx)

	// A host namespace that cannot be ensured is a HOST problem, reported on
	// the TenantCluster — not a reason to declare the guest disconnected, and
	// not a reason to skip pruning (which finds nothing in a missing namespace).
	if p.ensureHostNamespace(ctx) {
		p.applyAll(ctx)
		p.applyStorage(ctx)
	}
	p.pruneAll(ctx)
	// Storage prune runs BEFORE finalizer release: teardownGuestPVC re-pins
	// (wantFinalizer) every deleting PVC whose host state is not yet fully
	// gone, and that must be decided before releaseGuestFinalizers looks.
	p.pruneStorage(ctx)
	p.releaseGuestFinalizers(ctx)
	p.writeBack(ctx)
	p.writeBackStorage(ctx)
	return nil
}

// --- gather -----------------------------------------------------------------

func (p *pass) gatherServices(ctx context.Context) error {
	var list corev1.ServiceList
	if err := p.guest.List(ctx, &list); err != nil {
		return err
	}
	for i := range list.Items {
		svc := &list.Items[i]
		p.guestSvcs[guestKey{svc.Namespace, svc.Name}] = svc
	}
	if !p.tc.SyncServices() {
		return nil
	}
	for _, svc := range p.guestSvcs {
		if !wantsLoadBalancer(p.tc, svc) {
			continue
		}
		ref := guestObjRef{svcGVK, guestKey{svc.Namespace, svc.Name}}
		p.noteFinalizerHolder(svc, ref)
		if !svc.DeletionTimestamp.IsZero() {
			continue // excluded from desired; prune deletes, then the finalizer drops
		}
		p.guestObjs[ref] = svc
		p.counts.Services++
		hostSvc, refusal := mapService(p.tc, svc, corev1.ServiceTypeLoadBalancer)
		if refusal != "" {
			p.refuse(ref, refusal)
			continue
		}
		p.services[hostSvc.Name] = hostSvc
		p.lbPairs = append(p.lbPairs, svcPair{guest: svc, hostName: hostSvc.Name})
		p.wantFinalizer[ref] = true
	}
	return nil
}

func (p *pass) gatherIngresses(ctx context.Context) error {
	var list netv1.IngressList
	if err := p.guest.List(ctx, &list); err != nil {
		return err
	}
	for i := range list.Items {
		ing := &list.Items[i]
		ref := guestObjRef{ingressGVK, guestKey{ing.Namespace, ing.Name}}
		p.noteFinalizerHolder(ing, ref)
		if !ing.DeletionTimestamp.IsZero() {
			continue
		}
		p.guestObjs[ref] = ing
		p.counts.Ingresses++
		res := mapIngress(p.tc, ing, p.backendResolver(ing.Namespace))
		for _, msg := range res.refusals {
			p.refuse(ref, msg)
		}
		if res.host == nil {
			continue
		}
		p.ingresses[res.host.Name] = res.host
		p.ingressPairs = append(p.ingressPairs, ingressPair{guest: ing, hostName: res.host.Name})
		p.wantFinalizer[ref] = true
		p.addBackends(ing.Namespace, res.backends)
	}
	return nil
}

func (p *pass) gatherGateways(ctx context.Context) error {
	var gws gwv1.GatewayList
	switch err := p.guest.List(ctx, &gws); {
	case meta.IsNoMatchError(err):
		return nil // guest has no Gateway API — nothing to sync, not a failure
	case err != nil:
		return err
	}
	for i := range gws.Items {
		gw := &gws.Items[i]
		ref := guestObjRef{gatewayGVK, guestKey{gw.Namespace, gw.Name}}
		p.noteFinalizerHolder(gw, ref)
		if !gw.DeletionTimestamp.IsZero() {
			continue
		}
		p.guestObjs[ref] = gw
		p.counts.Gateways++
		host, refusals := mapGateway(p.tc, gw)
		for _, msg := range refusals {
			p.refuse(ref, msg)
		}
		if host == nil {
			continue
		}
		p.gateways[host.Name] = host
		p.gatewayPairs = append(p.gatewayPairs, gatewayPair{guest: gw, hostName: host.Name})
		p.wantFinalizer[ref] = true
	}

	var routes gwv1.HTTPRouteList
	switch err := p.guest.List(ctx, &routes); {
	case meta.IsNoMatchError(err):
		return nil
	case err != nil:
		return err
	}
	for i := range routes.Items {
		rt := &routes.Items[i]
		ref := guestObjRef{routeGVK, guestKey{rt.Namespace, rt.Name}}
		p.noteFinalizerHolder(rt, ref)
		if !rt.DeletionTimestamp.IsZero() {
			continue
		}
		p.guestObjs[ref] = rt
		p.counts.HTTPRoutes++
		res := mapHTTPRoute(p.tc, rt, p.backendResolver(rt.Namespace))
		for _, msg := range res.refusals {
			p.refuse(ref, msg)
		}
		if res.host == nil {
			continue
		}
		p.routes[res.host.Name] = res.host
		p.wantFinalizer[ref] = true
		p.addBackends(rt.Namespace, res.backends)
	}
	return nil
}

// backendResolver answers "can the host reach guest Service <name> in <ns>?"
// for the Ingress and HTTPRoute mappings, returning a refusal message when not.
func (p *pass) backendResolver(namespace string) func(string) string {
	return func(name string) string {
		svc, ok := p.guestSvcs[guestKey{namespace, name}]
		if !ok {
			return "not found in the guest cluster"
		}
		if !svc.DeletionTimestamp.IsZero() {
			return "is being deleted in the guest cluster"
		}
		_, refusal := mapService(p.tc, svc, corev1.ServiceTypeClusterIP)
		return refusal
	}
}

// addBackends materialises host backend Services for guest Services referenced
// by an Ingress or HTTPRoute — the same mapping the LB syncer uses, with type
// ClusterIP. If the guest Service is ALSO a synced LoadBalancer, the existing
// LB host Service is reused: a LoadBalancer Service is routable in-cluster
// like any other, and one host object per guest Service keeps prune sane.
func (p *pass) addBackends(namespace string, names []string) {
	for _, name := range names {
		svc := p.guestSvcs[guestKey{namespace, name}]
		if svc == nil {
			continue // the resolver already refused this path
		}
		hostName := HostObjectName(p.tc.Name, namespace, name)
		if _, exists := p.services[hostName]; exists {
			continue
		}
		hostSvc, refusal := mapService(p.tc, svc, corev1.ServiceTypeClusterIP)
		if refusal != "" {
			continue // ditto
		}
		p.services[hostName] = hostSvc
		ref := guestObjRef{svcGVK, guestKey{namespace, name}}
		p.guestObjs[ref] = svc
		p.wantFinalizer[ref] = true
	}
}

// --- apply ------------------------------------------------------------------

func (p *pass) ensureHostNamespace(ctx context.Context) bool {
	var ns corev1.Namespace
	err := p.hostReader.Get(ctx, client.ObjectKey{Name: p.tc.Spec.HostNamespace}, &ns)
	if err == nil {
		return true
	}
	if !apierrors.IsNotFound(err) {
		p.problems = append(p.problems, fmt.Sprintf("checking host namespace %s: %v", p.tc.Spec.HostNamespace, err))
		return false
	}
	// Created but NEVER deleted by this controller: the namespace typically
	// also holds the tenant's VMs, which are emphatically not ours to remove.
	ns = corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   p.tc.Spec.HostNamespace,
		Labels: map[string]string{LabelTenantCluster: p.tc.Name, LabelTenantClusterNamespace: p.tc.Namespace},
	}}
	if err := p.host.Create(ctx, &ns); err != nil && !apierrors.IsAlreadyExists(err) {
		p.problems = append(p.problems, fmt.Sprintf("creating host namespace %s: %v", p.tc.Spec.HostNamespace, err))
		return false
	}
	return true
}

func (p *pass) applyAll(ctx context.Context) {
	for _, name := range sortedKeys(p.services) {
		p.applyService(ctx, p.services[name])
	}
	for _, name := range sortedKeys(p.ingresses) {
		p.applyIngress(ctx, p.ingresses[name])
	}
	for _, name := range sortedKeys(p.gateways) {
		p.applyGateway(ctx, p.gateways[name])
	}
	for _, name := range sortedKeys(p.routes) {
		p.applyRoute(ctx, p.routes[name])
	}
}

// mayTouch is the overwrite guard, shared by every apply. An existing host
// object without our ownership labels is somebody else's, whatever its name
// says; it is left alone and the collision reported where the operator looks.
func (p *pass) mayTouch(have client.Object) bool {
	if ownedBy(have, p.tc) {
		return true
	}
	p.problems = append(p.problems, fmt.Sprintf(
		"host %T %s/%s already exists and is not managed by this TenantCluster; refusing to touch it",
		have, have.GetNamespace(), have.GetName()))
	return false
}

func (p *pass) applyService(ctx context.Context, want *corev1.Service) {
	var have corev1.Service
	err := p.hostReader.Get(ctx, client.ObjectKeyFromObject(want), &have)
	switch {
	case apierrors.IsNotFound(err):
		if err := p.host.Create(ctx, want); err != nil {
			p.applyProblem("Service", want.Name, err)
			return
		}
		// Freshly created: keep the (address-less) object for write-back so the
		// guest sees an honest "pending" rather than nothing.
		p.services[want.Name] = want
	case err != nil:
		p.applyProblem("Service", want.Name, err)
	default:
		if !p.mayTouch(&have) {
			delete(p.services, want.Name) // never write back status from an object we don't own
			return
		}
		// The allocated fields (clusterIP, host-assigned nodePorts, the LB
		// address itself) live on `have` and must survive the update — replacing
		// the whole spec would tear down and re-allocate the address on every
		// drift, and the address IS the product here.
		have.Labels = mergeLabels(have.Labels, want.Labels)
		have.Spec.Type = want.Spec.Type
		have.Spec.Selector = want.Spec.Selector
		have.Spec.Ports = reconcilePorts(have.Spec.Ports, want.Spec.Ports)
		if err := p.host.Update(ctx, &have); err != nil {
			p.applyProblem("Service", want.Name, err)
			return
		}
		p.services[want.Name] = &have
	}
}

// reconcilePorts keeps host-assigned NodePorts for ports that survive, so an
// update does not churn the host Service's own node ports.
func reconcilePorts(have, want []corev1.ServicePort) []corev1.ServicePort {
	byKey := map[string]int32{}
	for _, hp := range have {
		byKey[fmt.Sprintf("%s/%d", hp.Protocol, hp.Port)] = hp.NodePort
	}
	out := make([]corev1.ServicePort, len(want))
	for i, wp := range want {
		out[i] = wp
		out[i].NodePort = byKey[fmt.Sprintf("%s/%d", wp.Protocol, wp.Port)]
	}
	return out
}

func (p *pass) applyIngress(ctx context.Context, want *netv1.Ingress) {
	var have netv1.Ingress
	err := p.hostReader.Get(ctx, client.ObjectKeyFromObject(want), &have)
	switch {
	case apierrors.IsNotFound(err):
		if err := p.host.Create(ctx, want); err != nil {
			p.applyProblem("Ingress", want.Name, err)
		}
	case err != nil:
		p.applyProblem("Ingress", want.Name, err)
	default:
		if !p.mayTouch(&have) {
			delete(p.ingresses, want.Name)
			return
		}
		have.Labels = mergeLabels(have.Labels, want.Labels)
		have.Spec = want.Spec
		if err := p.host.Update(ctx, &have); err != nil {
			p.applyProblem("Ingress", want.Name, err)
			return
		}
		p.ingresses[want.Name] = &have
	}
}

func (p *pass) applyGateway(ctx context.Context, want *gwv1.Gateway) {
	var have gwv1.Gateway
	err := p.hostReader.Get(ctx, client.ObjectKeyFromObject(want), &have)
	switch {
	case apierrors.IsNotFound(err):
		if err := p.host.Create(ctx, want); err != nil {
			p.applyProblem("Gateway", want.Name, err)
		}
	case err != nil:
		p.applyProblem("Gateway", want.Name, err)
	default:
		if !p.mayTouch(&have) {
			delete(p.gateways, want.Name)
			return
		}
		have.Labels = mergeLabels(have.Labels, want.Labels)
		have.Spec = want.Spec
		if err := p.host.Update(ctx, &have); err != nil {
			p.applyProblem("Gateway", want.Name, err)
			return
		}
		p.gateways[want.Name] = &have
	}
}

func (p *pass) applyRoute(ctx context.Context, want *gwv1.HTTPRoute) {
	var have gwv1.HTTPRoute
	err := p.hostReader.Get(ctx, client.ObjectKeyFromObject(want), &have)
	switch {
	case apierrors.IsNotFound(err):
		if err := p.host.Create(ctx, want); err != nil {
			p.applyProblem("HTTPRoute", want.Name, err)
		}
	case err != nil:
		p.applyProblem("HTTPRoute", want.Name, err)
	default:
		if !p.mayTouch(&have) {
			delete(p.routes, want.Name)
			return
		}
		have.Labels = mergeLabels(have.Labels, want.Labels)
		have.Spec = want.Spec
		if err := p.host.Update(ctx, &have); err != nil {
			p.applyProblem("HTTPRoute", want.Name, err)
		}
	}
}

func (p *pass) applyProblem(kind, name string, err error) {
	p.problems = append(p.problems, fmt.Sprintf("applying host %s %q: %v", kind, name, err))
}

// --- prune ------------------------------------------------------------------

// pruneAll deletes host objects carrying THIS TenantCluster's ownership labels
// that are no longer desired. The label selector scopes the list, and
// deleteOwned re-checks ownership on each object — deliberate redundancy,
// because deletion is the one operation with no undo.
func (p *pass) pruneAll(ctx context.Context) {
	sel := client.MatchingLabels{
		LabelTenantCluster:          p.tc.Name,
		LabelTenantClusterNamespace: p.tc.Namespace,
	}
	inNS := client.InNamespace(p.tc.Spec.HostNamespace)

	var svcs corev1.ServiceList
	if err := p.hostReader.List(ctx, &svcs, inNS, sel); err != nil {
		p.pruneProblem(err)
	} else {
		for i := range svcs.Items {
			if _, ok := p.services[svcs.Items[i].Name]; !ok {
				p.deleteOwned(ctx, &svcs.Items[i])
			}
		}
	}

	var ings netv1.IngressList
	if err := p.hostReader.List(ctx, &ings, inNS, sel); err != nil {
		p.pruneProblem(err)
	} else {
		for i := range ings.Items {
			if _, ok := p.ingresses[ings.Items[i].Name]; !ok {
				p.deleteOwned(ctx, &ings.Items[i])
			}
		}
	}

	if p.gatewayAPIOnHost {
		var gws gwv1.GatewayList
		if err := p.hostReader.List(ctx, &gws, inNS, sel); err != nil && !meta.IsNoMatchError(err) {
			p.pruneProblem(err)
		} else if err == nil {
			for i := range gws.Items {
				if _, ok := p.gateways[gws.Items[i].Name]; !ok {
					p.deleteOwned(ctx, &gws.Items[i])
				}
			}
		}
		var rts gwv1.HTTPRouteList
		if err := p.hostReader.List(ctx, &rts, inNS, sel); err != nil && !meta.IsNoMatchError(err) {
			p.pruneProblem(err)
		} else if err == nil {
			for i := range rts.Items {
				if _, ok := p.routes[rts.Items[i].Name]; !ok {
					p.deleteOwned(ctx, &rts.Items[i])
				}
			}
		}
	}
}

func (p *pass) pruneProblem(err error) {
	p.pruneFailed = true
	p.problems = append(p.problems, fmt.Sprintf("pruning host objects: %v", err))
}

// deleteOwned deletes a host object ONLY if it carries this TenantCluster's
// ownership labels, with a UID precondition so a concurrent replacement of the
// object cannot be caught by the delete.
func (p *pass) deleteOwned(ctx context.Context, obj client.Object) {
	if !ownedBy(obj, p.tc) {
		// The list was label-scoped, so reaching here means labels changed
		// between list and now, or a bug upstream of this guard. Either way:
		// not ours, not touched.
		p.log.Info("refusing to delete host object without our ownership labels",
			"namespace", obj.GetNamespace(), "name", obj.GetName())
		return
	}
	uid := obj.GetUID()
	err := p.host.Delete(ctx, obj, client.Preconditions{UID: &uid})
	if err != nil && !apierrors.IsNotFound(err) {
		p.pruneFailed = true
		p.problems = append(p.problems, fmt.Sprintf(
			"deleting host object %s/%s: %v", obj.GetNamespace(), obj.GetName(), err))
		return
	}
	p.log.Info("deleted host object no longer desired",
		"namespace", obj.GetNamespace(), "name", obj.GetName())
}

// --- helpers ----------------------------------------------------------------

func (p *pass) refuse(ref guestObjRef, msg string) {
	p.refusals[ref] = append(p.refusals[ref], msg)
}

func (p *pass) noteFinalizerHolder(obj client.Object, ref guestObjRef) {
	for _, f := range obj.GetFinalizers() {
		if f == GuestFinalizer {
			p.seenFinalizer = append(p.seenFinalizer, finalizerHolder{obj: obj, ref: ref})
			return
		}
	}
}

func mergeLabels(have, want map[string]string) map[string]string {
	if have == nil {
		return want
	}
	for k, v := range want {
		have[k] = v
	}
	return have
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func summarize(msgs []string, max int) string {
	if len(msgs) <= max {
		return strings.Join(msgs, "; ")
	}
	return strings.Join(msgs[:max], "; ") + fmt.Sprintf("; and %d more", len(msgs)-max)
}
