package syncer

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/csinode"
	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/kubevirt"
)

// The storage slice of a pass. Sequencing is the substance here:
//
//	provision:  guest PVC (scheduler picked a node) → host PVC
//	attach:     guest VolumeAttachment appears      → hotplug into that node's VM
//	move:       old VA deleted, new VA appears      → unplug VM A, hotplug VM B
//	delete:     guest PVC deleting                  → unplug FIRST, host PVC second
//
// The unplug-before-delete order is non-negotiable: deleting a host PVC that
// is still attached to a running VM leaves a volume the host CSI cannot
// reclaim. Every path below that deletes a host PVC proves the volume is out
// of every VM first, this pass, from live VMI status — attach state is never
// cached, because a cached "detached" is how data gets destroyed.

var (
	pvcGVK = corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")
)

type storageState struct {
	// offerable host classes by name; mirrored into the guest.
	offerable map[string]*storagev1.StorageClass
	// guest-side objects the syncer manages, from this pass's listing.
	guestSCs []storagev1.StorageClass
	guestPVs map[string]*corev1.PersistentVolume
	// every guest PVC, by UID — the never-delete-orphans index. Scope is
	// deliberately ALL PVCs, not just mirrored-class ones: un-offering a class
	// must strand no data.
	pvcUIDs map[string]bool
	// provisionable guest PVCs (mirrored class, not deleting, not refused).
	provisionable []*corev1.PersistentVolumeClaim
	// deleting guest PVCs carrying our finalizer: teardown work.
	deleting []*corev1.PersistentVolumeClaim
	// guest VolumeAttachments for our driver.
	attachments []storagev1.VolumeAttachment
	// host-side state.
	hostPVCs []corev1.PersistentVolumeClaim
	vms      map[string]map[string]kubevirt.VolumeState
	// vaStatus collects desired VolumeAttachment status writes; applied in
	// writeBackStorage so every guest-facing write stays in the write-back
	// phase like the other syncers.
	vaStatus []vaStatusWrite
	// pluginWanted: the node plugin DaemonSet should exist in the guest.
	pluginWanted bool
}

type vaStatusWrite struct {
	va       *storagev1.VolumeAttachment
	attached bool
	metadata map[string]string
	errMsg   string
}

// storageProblem records a host-side storage error and, because storage prune
// decisions depend on complete information, blocks finalizer release.
func (p *pass) storageProblem(format string, args ...any) {
	p.pruneFailed = true
	p.problems = append(p.problems, fmt.Sprintf(format, args...))
}

// --- gather -----------------------------------------------------------------

// gatherStorage lists both sides. Guest list errors abort the pass (guest
// unreachable ⇒ nothing destructive); host or KubeVirt errors disable the
// storage slice for this pass and block prune, but let the other syncers run.
func (p *pass) gatherStorage(ctx context.Context) error {
	if !p.tc.SyncStorage() {
		return nil
	}
	if p.hot == nil {
		p.problems = append(p.problems, "storage sync enabled but no hotplug client configured")
		return nil
	}
	st := &storageState{
		offerable: map[string]*storagev1.StorageClass{},
		guestPVs:  map[string]*corev1.PersistentVolume{},
		pvcUIDs:   map[string]bool{},
	}

	// Guest side first: these failing means the guest is unreachable, and the
	// pass must stop BEFORE any host mutation.
	var pvcs corev1.PersistentVolumeClaimList
	if err := p.guest.List(ctx, &pvcs); err != nil {
		return err
	}
	var pvs corev1.PersistentVolumeList
	if err := p.guest.List(ctx, &pvs, client.MatchingLabels{ManagedByLabel: ManagedByValue}); err != nil {
		return err
	}
	var scs storagev1.StorageClassList
	if err := p.guest.List(ctx, &scs, client.MatchingLabels{ManagedByLabel: ManagedByValue}); err != nil {
		return err
	}
	var vas storagev1.VolumeAttachmentList
	if err := p.guest.List(ctx, &vas); err != nil {
		return err
	}

	for i := range pvs.Items {
		st.guestPVs[pvs.Items[i].Name] = &pvs.Items[i]
	}
	st.guestSCs = scs.Items
	for _, va := range vas.Items {
		if va.Spec.Attacher == csinode.DriverName {
			st.attachments = append(st.attachments, va)
		}
	}

	// Host side. A failure here must not be mistaken for "nothing offered" —
	// that would tear down guest StorageClasses on a blip.
	var hostSCs storagev1.StorageClassList
	if err := p.hostReader.List(ctx, &hostSCs, client.MatchingLabels{LabelTenantOfferable: "true"}); err != nil {
		p.storageProblem("listing tenant-offerable host StorageClasses: %v", err)
		return nil
	}
	for i := range hostSCs.Items {
		st.offerable[hostSCs.Items[i].Name] = &hostSCs.Items[i]
	}
	var hostPVCs corev1.PersistentVolumeClaimList
	if err := p.hostReader.List(ctx, &hostPVCs, client.InNamespace(p.tc.Spec.HostNamespace), client.MatchingLabels{
		LabelTenantCluster:          p.tc.Name,
		LabelTenantClusterNamespace: p.tc.Namespace,
	}); err != nil {
		p.storageProblem("listing host PVCs: %v", err)
		return nil
	}
	st.hostPVCs = hostPVCs.Items
	vms, err := p.hot.VMs(ctx, p.tc.Spec.HostNamespace)
	if err != nil {
		p.storageProblem("listing VMs for hotplug state: %v", err)
		return nil
	}
	st.vms = vms

	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		st.pvcUIDs[string(pvc.UID)] = true
		ref := guestObjRef{pvcGVK, guestKey{pvc.Namespace, pvc.Name}}
		p.noteFinalizerHolder(pvc, ref)

		if !pvc.DeletionTimestamp.IsZero() {
			if controllerutil.ContainsFinalizer(pvc, GuestFinalizer) {
				st.deleting = append(st.deleting, pvc)
			}
			continue
		}
		if pvc.Spec.StorageClassName == nil {
			continue
		}
		if _, ours := st.offerable[*pvc.Spec.StorageClassName]; !ours {
			continue
		}
		p.guestObjs[ref] = pvc
		p.counts.PersistentVolumeClaims++
		if _, refusal := mapPVC(p.tc, pvc, *pvc.Spec.StorageClassName); refusal != "" {
			p.refuse(ref, refusal)
			continue
		}
		st.provisionable = append(st.provisionable, pvc)
		// Finalizer BEFORE any host state exists (ensureGuestFinalizers runs
		// before apply), so a PVC deleted mid-provision still gets cleanup.
		p.wantFinalizer[ref] = true
	}

	// The node plugin stays while anything still depends on it: mounted
	// volumes outlive a class being un-offered.
	st.pluginWanted = len(st.offerable) > 0 || len(st.guestPVs) > 0

	p.storage = st
	return nil
}

// --- apply ------------------------------------------------------------------

func (p *pass) applyStorage(ctx context.Context) {
	st := p.storage
	if st == nil {
		return
	}
	p.applyGuestPlugin(ctx)
	p.applyGuestStorageClasses(ctx)

	for _, pvc := range st.provisionable {
		p.provisionPVC(ctx, pvc)
	}
	for i := range st.attachments {
		p.reconcileAttachment(ctx, &st.attachments[i])
	}
}

// provisionPVC creates the host PVC and the guest PV for one guest PVC, once
// the guest scheduler has picked a node. Until then nothing exists anywhere:
// that is WaitForFirstConsumer doing its job.
func (p *pass) provisionPVC(ctx context.Context, guestPVC *corev1.PersistentVolumeClaim) {
	st := p.storage
	hostName := HostPVCName(p.tc, string(guestPVC.UID))

	var haveHost *corev1.PersistentVolumeClaim
	for i := range st.hostPVCs {
		if st.hostPVCs[i].Name == hostName {
			haveHost = &st.hostPVCs[i]
			break
		}
	}
	if haveHost == nil {
		// No trigger yet? A PVC with no selected node has no VM to serve, and
		// provisioning early would pin resources for a pod that may never come.
		if guestPVC.Annotations[selectedNodeAnnotation] == "" && guestPVC.Spec.VolumeName == "" {
			return
		}
		want, refusal := mapPVC(p.tc, guestPVC, *guestPVC.Spec.StorageClassName)
		if refusal != "" {
			return // already surfaced in gather
		}
		// The label-scoped list cannot see an UNLABELLED host PVC squatting on
		// this name; check uncached before creating, as every apply does.
		var have corev1.PersistentVolumeClaim
		err := p.hostReader.Get(ctx, client.ObjectKey{Namespace: p.tc.Spec.HostNamespace, Name: hostName}, &have)
		switch {
		case apierrors.IsNotFound(err):
			if err := p.host.Create(ctx, want); err != nil {
				p.storageProblem("creating host PVC %q: %v", hostName, err)
				return
			}
		case err != nil:
			p.storageProblem("checking host PVC %q: %v", hostName, err)
			return
		default:
			if !p.mayTouch(&have) {
				return
			}
		}
	}

	// Guest PV, bound to the claim. Created once and then left alone: PV
	// specs are essentially immutable and there is nothing to reconcile.
	pvName := "pvc-" + string(guestPVC.UID)
	if _, ok := st.guestPVs[pvName]; ok {
		return
	}
	pv := guestPVForClaim(guestPVC, hostName, *guestPVC.Spec.StorageClassName)
	if err := p.guest.Create(ctx, pv); err != nil && !apierrors.IsAlreadyExists(err) {
		p.problems = append(p.problems, fmt.Sprintf("creating guest PV %q: %v", pvName, err))
		return
	}
	st.guestPVs[pvName] = pv
}

// reconcileAttachment drives one guest VolumeAttachment towards its truth:
// active VA → volume hotplugged into that node's VM and attached=true;
// deleting VA → volume out of the VM, then our finalizer released so the
// guest's attach/detach controller can move on (its multi-attach guard holds
// the NEXT node's attach until this object is gone — that ordering is what
// makes a move safe).
func (p *pass) reconcileAttachment(ctx context.Context, va *storagev1.VolumeAttachment) {
	st := p.storage
	if va.Spec.Source.PersistentVolumeName == nil {
		return
	}
	pv, ok := st.guestPVs[*va.Spec.Source.PersistentVolumeName]
	if !ok || pv.Spec.CSI == nil {
		// Not a volume this pass can name. But a DELETING VA carrying our
		// finalizer whose PV is already gone must still be RELEASED: our PVs
		// are only ever deleted after the unplug paths ran (PVC teardown) or
		// during TenantCluster teardown (whose host-side sweep unplugs every
		// labelled PVC independently), so the finalizer is holding nothing —
		// and without this, the VA is stuck forever and the guest's
		// attach/detach controller stalls on the volume. Found live: the
		// teardown-with-attached-volume test left exactly this zombie.
		if !va.DeletionTimestamp.IsZero() && controllerutil.ContainsFinalizer(va, GuestFinalizer) {
			if controllerutil.RemoveFinalizer(va, GuestFinalizer) {
				if err := p.guest.Update(ctx, va); err != nil {
					p.problems = append(p.problems, fmt.Sprintf(
						"releasing orphaned guest VolumeAttachment %s: %v", va.Name, err))
				}
			}
		}
		return
	}
	hostPVC := pv.Spec.CSI.VolumeHandle
	volName := VolumeName(hostPVC)
	serial := csinode.SerialFor(hostPVC)
	// The guest node's VM carries the node's name — the platform creates VMs
	// named for the nodes they back.
	vmName := va.Spec.NodeName
	vols, vmExists := st.vms[vmName]

	if !va.DeletionTimestamp.IsZero() {
		vol, present := vols[volName]
		switch {
		case !vmExists, !present && vol.Phase == "":
			// VM gone, or volume fully out of it: release the VA. With the VM
			// deleted this is the "release, don't strand" rule — the volume must
			// be claimable by the next pod elsewhere.
			if controllerutil.RemoveFinalizer(va, GuestFinalizer) {
				if err := p.guest.Update(ctx, va); err != nil {
					p.problems = append(p.problems, fmt.Sprintf(
						"releasing guest VolumeAttachment %s: %v", va.Name, err))
				}
			}
		case vol.InSpec:
			if err := p.hot.RemoveVolume(ctx, p.tc.Spec.HostNamespace, vmName, volName); err != nil {
				p.storageProblem("unplugging volume %s from VM %s: %v", volName, vmName, err)
			}
			// Finalizer stays until a later pass observes the volume gone.
		}
		return
	}

	// Active attachment. Pin it first: the VA must not vanish between passes
	// with a volume still plugged in.
	if controllerutil.AddFinalizer(va, GuestFinalizer) {
		if err := p.guest.Update(ctx, va); err != nil {
			p.problems = append(p.problems, fmt.Sprintf(
				"pinning guest VolumeAttachment %s: %v", va.Name, err))
			return
		}
	}
	if !vmExists {
		p.storage.vaStatus = append(p.storage.vaStatus, vaStatusWrite{va: va,
			errMsg: fmt.Sprintf("no VirtualMachine %q in the host namespace backs node %q", vmName, va.Spec.NodeName)})
		return
	}
	vol := vols[volName]
	if !vol.InSpec && vol.Phase == "" {
		if err := p.hot.AddVolume(ctx, p.tc.Spec.HostNamespace, vmName, volName, hostPVC, serial); err != nil {
			p.storageProblem("hotplugging volume %s into VM %s: %v", volName, vmName, err)
			return
		}
	}
	if vol.Phase == kubevirt.Ready {
		p.storage.vaStatus = append(p.storage.vaStatus, vaStatusWrite{
			va: va, attached: true, metadata: map[string]string{"serial": serial},
		})
	}
}

// applyGuestStorageClasses mirrors offerable host classes into the guest,
// read-only in effect: the tenant can edit the object (they are cluster-admin
// in their own cluster) but every pass restores the advertised shape.
func (p *pass) applyGuestStorageClasses(ctx context.Context) {
	st := p.storage
	existing := map[string]*storagev1.StorageClass{}
	for i := range st.guestSCs {
		existing[st.guestSCs[i].Name] = &st.guestSCs[i]
	}
	for name, hostSC := range st.offerable {
		want := mirrorStorageClass(hostSC)
		have, ok := existing[name]
		if !ok {
			if err := p.guest.Create(ctx, want); err != nil && !apierrors.IsAlreadyExists(err) {
				p.problems = append(p.problems, fmt.Sprintf("creating guest StorageClass %q: %v", name, err))
			}
			continue
		}
		if scEqual(have, want) {
			continue
		}
		// StorageClass provisioner/bindingMode are immutable: restoring a
		// drifted mirror means delete + recreate, not update.
		if err := p.guest.Delete(ctx, have); err != nil && !apierrors.IsNotFound(err) {
			p.problems = append(p.problems, fmt.Sprintf("replacing guest StorageClass %q: %v", name, err))
			continue
		}
		if err := p.guest.Create(ctx, want); err != nil {
			p.problems = append(p.problems, fmt.Sprintf("recreating guest StorageClass %q: %v", name, err))
		}
	}
}

func scEqual(have, want *storagev1.StorageClass) bool {
	return have.Provisioner == want.Provisioner &&
		equality.Semantic.DeepEqual(have.VolumeBindingMode, want.VolumeBindingMode) &&
		equality.Semantic.DeepEqual(have.ReclaimPolicy, want.ReclaimPolicy) &&
		equality.Semantic.DeepEqual(have.Parameters, want.Parameters) &&
		have.Annotations["storageclass.kubernetes.io/is-default-class"] == want.Annotations["storageclass.kubernetes.io/is-default-class"]
}

// applyGuestPlugin installs (or upgrades) the node plugin in the guest. The
// syncer's guest kubeconfig is the platform's channel into the guest: no
// tenant cooperation needed to ship a new plugin version.
func (p *pass) applyGuestPlugin(ctx context.Context) {
	st := p.storage
	if !st.pluginWanted {
		return
	}
	ns, sa, ds := guestNodePlugin(p.images)
	if err := p.guest.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		p.problems = append(p.problems, fmt.Sprintf("creating guest namespace %s: %v", ns.Name, err))
		return
	}
	if err := p.guest.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
		p.problems = append(p.problems, fmt.Sprintf("creating guest ServiceAccount: %v", err))
	}

	drv := guestCSIDriver()
	var haveDrv storagev1.CSIDriver
	if err := p.guest.Get(ctx, client.ObjectKey{Name: drv.Name}, &haveDrv); apierrors.IsNotFound(err) {
		if err := p.guest.Create(ctx, drv); err != nil && !apierrors.IsAlreadyExists(err) {
			p.problems = append(p.problems, fmt.Sprintf("creating guest CSIDriver: %v", err))
		}
	}

	var haveDS appsv1.DaemonSet
	err := p.guest.Get(ctx, client.ObjectKey{Namespace: ds.Namespace, Name: ds.Name}, &haveDS)
	switch {
	case apierrors.IsNotFound(err):
		if err := p.guest.Create(ctx, ds); err != nil {
			p.problems = append(p.problems, fmt.Sprintf("creating guest node plugin DaemonSet: %v", err))
		}
	case err != nil:
		p.problems = append(p.problems, fmt.Sprintf("checking guest node plugin DaemonSet: %v", err))
	default:
		// Upgrade on image change only. Diffing a full DaemonSet spec against
		// the API server's defaulted copy would find phantom drift every pass.
		if len(haveDS.Spec.Template.Spec.Containers) == 2 &&
			haveDS.Spec.Template.Spec.Containers[0].Image == p.images.Node &&
			haveDS.Spec.Template.Spec.Containers[1].Image == p.images.Registrar {
			return
		}
		haveDS.Spec = ds.Spec
		if err := p.guest.Update(ctx, &haveDS); err != nil {
			p.problems = append(p.problems, fmt.Sprintf("updating guest node plugin DaemonSet: %v", err))
		}
	}
}

// --- prune / teardown -------------------------------------------------------

// pruneStorage runs the ordered teardown paths. It never deletes a host PVC
// while any VM still carries its volume, and never touches an unlabelled one.
func (p *pass) pruneStorage(ctx context.Context) {
	st := p.storage
	if st == nil {
		return
	}

	for _, pvc := range st.deleting {
		p.teardownGuestPVC(ctx, pvc)
	}

	// Orphaned host PVCs: labelled ours, but no guest PVC with that UID exists
	// anymore (force-deleted, or deleted while the syncer was away). Same
	// unplug-first order. Keyed on UID against ALL guest PVCs — a host PVC
	// whose guest PVC merely changed class is not an orphan.
	for i := range st.hostPVCs {
		hostPVC := &st.hostPVCs[i]
		if st.pvcUIDs[hostPVC.Labels[LabelGuestUID]] {
			continue
		}
		if p.volumeUnplugged(ctx, hostPVC.Name) {
			p.deleteOwned(ctx, hostPVC)
		}
	}

	// Guest StorageClasses whose host class is no longer offerable. PVCs and
	// volumes on the class are untouched: un-offering stops NEW provisioning,
	// it does not strand data.
	for i := range st.guestSCs {
		sc := &st.guestSCs[i]
		if _, ok := st.offerable[sc.Name]; ok {
			continue
		}
		if err := p.guest.Delete(ctx, sc); err != nil && !apierrors.IsNotFound(err) {
			p.problems = append(p.problems, fmt.Sprintf("deleting guest StorageClass %q: %v", sc.Name, err))
		}
	}
}

// teardownGuestPVC advances a deleting guest PVC's cleanup by whatever step is
// ready this pass, keeping the finalizer until every trace is gone:
// VolumeAttachments first (they unplug via their own path), then the volume
// out of every VM, then the host PVC, then the guest PV, then release.
func (p *pass) teardownGuestPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) {
	st := p.storage
	ref := guestObjRef{pvcGVK, guestKey{pvc.Namespace, pvc.Name}}
	hostName := HostPVCName(p.tc, string(pvc.UID))
	pvName := "pvc-" + string(pvc.UID)

	// While a VolumeAttachment still references this volume, a pod may still
	// be using it; the VA lifecycle owns the unplug. Wait.
	for i := range st.attachments {
		va := &st.attachments[i]
		if va.Spec.Source.PersistentVolumeName != nil && *va.Spec.Source.PersistentVolumeName == pvName {
			p.wantFinalizer[ref] = true
			return
		}
	}
	if !p.volumeUnplugged(ctx, hostName) {
		p.wantFinalizer[ref] = true
		return
	}

	// Volume is out of every VM: the host PVC can go. UID precondition and
	// label check as with every host delete.
	for i := range st.hostPVCs {
		if st.hostPVCs[i].Name == hostName {
			p.deleteOwned(ctx, &st.hostPVCs[i])
			if p.pruneFailed {
				p.wantFinalizer[ref] = true
				return
			}
			break
		}
	}
	if pv, ok := st.guestPVs[pvName]; ok {
		if err := p.guest.Delete(ctx, pv); err != nil && !apierrors.IsNotFound(err) {
			p.problems = append(p.problems, fmt.Sprintf("deleting guest PV %q: %v", pvName, err))
			p.wantFinalizer[ref] = true
			return
		}
	}
	// Nothing left: the generic release strips the finalizer (the holder was
	// noted in gather and wantFinalizer stays false).
}

// volumeUnplugged reports whether no VM carries the volume for hostPVCName,
// issuing removevolume where one still does. From live VM state, this pass.
func (p *pass) volumeUnplugged(ctx context.Context, hostPVCName string) bool {
	volName := VolumeName(hostPVCName)
	clear := true
	for vmName, vols := range p.storage.vms {
		vol, ok := vols[volName]
		if !ok {
			continue
		}
		clear = false
		if vol.InSpec {
			if err := p.hot.RemoveVolume(ctx, p.tc.Spec.HostNamespace, vmName, volName); err != nil {
				p.storageProblem("unplugging volume %s from VM %s: %v", volName, vmName, err)
			}
		}
		// InSpec false but still in VMI status: mid-unplug, just wait.
	}
	return clear
}

// --- TenantCluster teardown -------------------------------------------------

// storageTeardown is the storage half of deleting a TenantCluster: unplug and
// delete every host PVC labelled for the tenant, BEFORE the finalizer drops.
// Returns done=false while unplugs are still in flight — the caller requeues
// rather than dropping the finalizer over live attachments.
func (r *TenantClusterReconciler) storageTeardown(ctx context.Context, tc *v1alpha1.TenantCluster) (bool, error) {
	var pvcs corev1.PersistentVolumeClaimList
	if err := r.Reader.List(ctx, &pvcs, client.InNamespace(tc.Spec.HostNamespace), client.MatchingLabels{
		LabelTenantCluster:          tc.Name,
		LabelTenantClusterNamespace: tc.Namespace,
	}); err != nil {
		return false, err
	}
	if len(pvcs.Items) == 0 {
		return true, nil
	}
	if r.Hotplug == nil {
		// Host PVCs exist but there is no way to prove they are unplugged.
		// Refusing to guess is the only safe answer.
		return false, fmt.Errorf("host PVCs remain for TenantCluster %s but no hotplug client is configured", tc.Name)
	}
	vms, err := r.Hotplug.VMs(ctx, tc.Spec.HostNamespace)
	if err != nil {
		return false, err
	}

	done := true
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if !ownedBy(pvc, tc) {
			continue // label-scoped list, but the guard is cheap and deletion is forever
		}
		volName := VolumeName(pvc.Name)
		attached := false
		for vmName, vols := range vms {
			vol, ok := vols[volName]
			if !ok {
				continue
			}
			attached = true
			if vol.InSpec {
				if err := r.Hotplug.RemoveVolume(ctx, tc.Spec.HostNamespace, vmName, volName); err != nil {
					logf.FromContext(ctx).Error(err, "unplugging volume during teardown", "vm", vmName, "volume", volName)
				}
			}
		}
		if attached {
			done = false
			continue
		}
		uid := pvc.GetUID()
		if err := r.Delete(ctx, pvc, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Error(err, "deleting host PVC during teardown", "name", pvc.Name)
			done = false
		}
	}
	return done, nil
}

// cleanupGuestStorage removes everything the syncer put INSIDE the guest —
// mirrored StorageClasses, PVs, the node plugin — and strips finalizers from
// PVCs and VolumeAttachments. Best-effort, like all guest-side teardown: an
// unreachable guest must not hold host cleanup hostage.
func (r *TenantClusterReconciler) cleanupGuestStorage(ctx context.Context, guest client.Client) {
	var pvcs corev1.PersistentVolumeClaimList
	if err := guest.List(ctx, &pvcs); err == nil {
		for i := range pvcs.Items {
			if controllerutil.RemoveFinalizer(&pvcs.Items[i], GuestFinalizer) {
				_ = guest.Update(ctx, &pvcs.Items[i])
			}
		}
	}
	var vas storagev1.VolumeAttachmentList
	if err := guest.List(ctx, &vas); err == nil {
		for i := range vas.Items {
			if controllerutil.RemoveFinalizer(&vas.Items[i], GuestFinalizer) {
				_ = guest.Update(ctx, &vas.Items[i])
			}
		}
	}
	ours := client.MatchingLabels{ManagedByLabel: ManagedByValue}
	var scs storagev1.StorageClassList
	if err := guest.List(ctx, &scs, ours); err == nil {
		for i := range scs.Items {
			_ = guest.Delete(ctx, &scs.Items[i])
		}
	}
	var pvs corev1.PersistentVolumeList
	if err := guest.List(ctx, &pvs, ours); err == nil {
		for i := range pvs.Items {
			_ = guest.Delete(ctx, &pvs.Items[i])
		}
	}
	var drv storagev1.CSIDriver
	if err := guest.Get(ctx, client.ObjectKey{Name: csinode.DriverName}, &drv); err == nil {
		_ = guest.Delete(ctx, &drv)
	}
	// The namespace delete takes the DaemonSet and ServiceAccount with it.
	var ns corev1.Namespace
	if err := guest.Get(ctx, client.ObjectKey{Name: GuestCSINamespace}, &ns); err == nil &&
		ns.Labels[ManagedByLabel] == ManagedByValue {
		_ = guest.Delete(ctx, &ns)
	}
}

// --- write-back -------------------------------------------------------------

func (p *pass) writeBackStorage(ctx context.Context) {
	st := p.storage
	if st == nil {
		return
	}
	for _, w := range st.vaStatus {
		va := w.va
		desired := va.Status.DeepCopy()
		desired.Attached = w.attached
		desired.AttachmentMetadata = w.metadata
		if w.errMsg != "" {
			desired.AttachError = &storagev1.VolumeError{Message: w.errMsg, Time: metav1.Now()}
		} else {
			desired.AttachError = nil
		}
		if va.Status.Attached == desired.Attached &&
			equality.Semantic.DeepEqual(va.Status.AttachmentMetadata, desired.AttachmentMetadata) &&
			(va.Status.AttachError == nil) == (desired.AttachError == nil) {
			continue
		}
		va.Status = *desired
		if err := p.guest.Status().Update(ctx, va); err != nil {
			p.problems = append(p.problems, fmt.Sprintf(
				"writing status to guest VolumeAttachment %s: %v", va.Name, err))
		}
	}
}
