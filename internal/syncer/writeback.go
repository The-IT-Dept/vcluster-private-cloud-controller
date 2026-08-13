package syncer

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// This file is the guest-facing half of a pass: status write-back, refusal
// surfacing, and finalizer lifecycle. Everything here writes into the GUEST —
// statuses, conditions, events, annotations, finalizers — and nothing else.
// It never creates a Secret in the guest and never writes host connection
// details there; the guest learns addresses and reasons, not credentials.

// ensureGuestFinalizers pins every guest object that is about to have host
// state, BEFORE that state is created, so deletion can never race cleanup.
func (p *pass) ensureGuestFinalizers(ctx context.Context) {
	for ref := range p.wantFinalizer {
		obj := p.guestObjs[ref]
		if obj == nil || !obj.GetDeletionTimestamp().IsZero() {
			continue
		}
		if controllerutil.AddFinalizer(obj, GuestFinalizer) {
			if err := p.guest.Update(ctx, obj); err != nil {
				p.problems = append(p.problems, fmt.Sprintf(
					"adding finalizer to guest %s %s/%s: %v", ref.gvk.Kind, ref.key.namespace, ref.key.name, err))
			}
		}
	}
}

// releaseGuestFinalizers lets go of guest objects that no longer have host
// state. Skipped wholesale when any prune failed: releasing a finalizer while
// a host object for it might survive is exactly the leak the finalizer exists
// to prevent.
func (p *pass) releaseGuestFinalizers(ctx context.Context) {
	if p.pruneFailed {
		return
	}
	for _, h := range p.seenFinalizer {
		if p.wantFinalizer[h.ref] {
			continue
		}
		if controllerutil.RemoveFinalizer(h.obj, GuestFinalizer) {
			if err := p.guest.Update(ctx, h.obj); err != nil {
				p.problems = append(p.problems, fmt.Sprintf(
					"removing finalizer from guest %s %s/%s: %v", h.ref.gvk.Kind, h.ref.key.namespace, h.ref.key.name, err))
			}
		}
	}
}

func (p *pass) writeBack(ctx context.Context) {
	p.writeBackServices(ctx)
	p.writeBackIngresses(ctx)
	p.writeBackGateways(ctx)
	p.writeRefusals(ctx)
}

// writeBackServices copies the host-assigned address into the guest Service.
// This is what makes the syncer real rather than a bridge: without it a guest
// LoadBalancer sits <pending> forever and nothing inside the guest ever learns
// its address.
func (p *pass) writeBackServices(ctx context.Context) {
	for _, pair := range p.lbPairs {
		host := p.services[pair.hostName]
		if host == nil {
			continue // collision or apply failure; already reported
		}
		var ingress []corev1.LoadBalancerIngress
		for _, in := range host.Status.LoadBalancer.Ingress {
			// IP and Hostname only. The host entry's ports/ipMode describe the
			// HOST Service's node ports, which mean nothing inside the guest.
			ingress = append(ingress, corev1.LoadBalancerIngress{IP: in.IP, Hostname: in.Hostname})
		}
		g := pair.guest
		cond := metav1.Condition{Type: HostAddressCondition, ObservedGeneration: g.Generation}
		if len(ingress) > 0 {
			cond.Status = metav1.ConditionTrue
			cond.Reason = "AddressAssigned"
			cond.Message = fmt.Sprintf("the host cluster assigned %s", describeIngress(ingress))
		} else {
			// Never a silent nothing: while the host pool has no address to give,
			// the guest Service says so instead of just sitting <pending>.
			cond.Status = metav1.ConditionFalse
			cond.Reason = "AddressPending"
			cond.Message = "waiting for the host cluster to assign an address; if this persists, the host address pool may be exhausted"
		}
		changed := !equality.Semantic.DeepEqual(g.Status.LoadBalancer.Ingress, ingress)
		g.Status.LoadBalancer.Ingress = ingress
		if meta.SetStatusCondition(&g.Status.Conditions, cond) {
			changed = true
		}
		if changed {
			if err := p.guest.Status().Update(ctx, g); err != nil {
				p.problems = append(p.problems, fmt.Sprintf(
					"writing status to guest Service %s/%s: %v", g.Namespace, g.Name, err))
			}
		}
	}
}

func describeIngress(ingress []corev1.LoadBalancerIngress) string {
	out := ""
	for i, in := range ingress {
		if i > 0 {
			out += ", "
		}
		if in.IP != "" {
			out += in.IP
		} else {
			out += in.Hostname
		}
	}
	return out
}

func (p *pass) writeBackIngresses(ctx context.Context) {
	for _, pair := range p.ingressPairs {
		host := p.ingresses[pair.hostName]
		if host == nil {
			continue
		}
		want := host.Status.LoadBalancer.DeepCopy()
		g := pair.guest
		if equality.Semantic.DeepEqual(&g.Status.LoadBalancer, want) {
			continue
		}
		g.Status.LoadBalancer = *want
		if err := p.guest.Status().Update(ctx, g); err != nil {
			p.problems = append(p.problems, fmt.Sprintf(
				"writing status to guest Ingress %s/%s: %v", g.Namespace, g.Name, err))
		}
	}
}

func (p *pass) writeBackGateways(ctx context.Context) {
	for _, pair := range p.gatewayPairs {
		host := p.gateways[pair.hostName]
		if host == nil {
			continue
		}
		g := pair.guest
		if equality.Semantic.DeepEqual(g.Status.Addresses, host.Status.Addresses) {
			continue
		}
		g.Status.Addresses = host.Status.Addresses
		if err := p.guest.Status().Update(ctx, g); err != nil {
			p.problems = append(p.problems, fmt.Sprintf(
				"writing status to guest Gateway %s/%s: %v", g.Namespace, g.Name, err))
		}
	}
}

// writeRefusals makes every refusal visible on the guest object it concerns —
// never a silent drop. Services carry a real status condition; Ingress,
// Gateway and HTTPRoute have no writable condition surface a tenant reads, so
// the durable record is an annotation, and an Event fires on every change.
func (p *pass) writeRefusals(ctx context.Context) {
	for ref, obj := range p.guestObjs {
		msgs := p.refusals[ref]

		if ref.gvk == svcGVK {
			if len(msgs) == 0 {
				continue // healthy Services get their condition from writeBackServices
			}
			svc, ok := obj.(*corev1.Service)
			if !ok || !wantsLoadBalancer(p.tc, svc) {
				continue
			}
			cond := metav1.Condition{
				Type: HostAddressCondition, Status: metav1.ConditionFalse,
				Reason: "SyncRefused", Message: summarize(msgs, 5), ObservedGeneration: svc.Generation,
			}
			if meta.SetStatusCondition(&svc.Status.Conditions, cond) {
				if err := p.guest.Status().Update(ctx, svc); err != nil {
					p.problems = append(p.problems, fmt.Sprintf(
						"writing refusal to guest Service %s/%s: %v", svc.Namespace, svc.Name, err))
					continue
				}
				p.guestEvent(ctx, obj, ref.gvk, corev1.EventTypeWarning, "SyncRefused", cond.Message)
			}
			continue
		}

		desired := summarize(msgs, 5)
		current := obj.GetAnnotations()[RefusedAnnotation]
		if desired == current {
			continue
		}
		ann := obj.GetAnnotations()
		if desired == "" {
			delete(ann, RefusedAnnotation)
		} else {
			if ann == nil {
				ann = map[string]string{}
			}
			ann[RefusedAnnotation] = desired
		}
		obj.SetAnnotations(ann)
		if err := p.guest.Update(ctx, obj); err != nil {
			p.problems = append(p.problems, fmt.Sprintf(
				"annotating guest %s %s/%s: %v", ref.gvk.Kind, ref.key.namespace, ref.key.name, err))
			continue
		}
		if desired != "" {
			p.guestEvent(ctx, obj, ref.gvk, corev1.EventTypeWarning, "HostnameRefused", desired)
		}
	}
}

// guestEvent records an Event IN THE GUEST, where the tenant's kubectl
// describe actually looks. Best-effort by design: an event that cannot be
// written must not fail a sync that otherwise worked.
func (p *pass) guestEvent(ctx context.Context, obj client.Object, gvk schema.GroupVersionKind, evType, reason, msg string) {
	now := metav1.Now()
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: obj.GetNamespace(),
			Name:      fmt.Sprintf("%s.%x", obj.GetName(), now.UnixNano()),
		},
		InvolvedObject: corev1.ObjectReference{
			APIVersion: gvk.GroupVersion().String(),
			Kind:       gvk.Kind,
			Namespace:  obj.GetNamespace(),
			Name:       obj.GetName(),
			UID:        obj.GetUID(),
		},
		Type:           evType,
		Reason:         reason,
		Message:        msg,
		Source:         corev1.EventSource{Component: FieldManager},
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
	}
	if err := p.guest.Create(ctx, ev); err != nil {
		p.log.Info("could not record guest event", "reason", reason, "error", err.Error())
	}
}
