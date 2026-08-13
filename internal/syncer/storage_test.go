package syncer

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/csinode"
	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/kubevirt"
)

// --- mapping tests ----------------------------------------------------------

func offerableHostSC(name string) *storagev1.StorageClass {
	immediate := storagev1.VolumeBindingImmediate
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      map[string]string{LabelTenantOfferable: "true"},
			Annotations: map[string]string{"storageclass.kubernetes.io/is-default-class": "true"},
		},
		Provisioner:       "csi.host.example.com",
		VolumeBindingMode: &immediate,
	}
}

func TestMirrorStorageClassForcesWaitForFirstConsumer(t *testing.T) {
	// The host class binds Immediate — and the mirror must NOT: without
	// WaitForFirstConsumer there is no selected node, and without a node
	// there is no VM to hotplug into. The host's own mode is irrelevant to
	// the guest-side mechanism.
	sc := mirrorStorageClass(offerableHostSC("fast"))
	if sc.VolumeBindingMode == nil || *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
		t.Fatalf("mirrored class must be WaitForFirstConsumer, got %v", sc.VolumeBindingMode)
	}
	if sc.Provisioner != csinode.DriverName {
		t.Errorf("provisioner = %q, want our driver", sc.Provisioner)
	}
	if sc.Annotations["storageclass.kubernetes.io/is-default-class"] != "true" {
		t.Errorf("default-class marker not carried over")
	}
}

func guestPVC(name string, uid types.UID, class string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: uid},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &class,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
}

func TestMapPVCIsBlockModeOnHost(t *testing.T) {
	tc := testTC()
	host, refusal := mapPVC(tc, guestPVC("data", "pvc-uid-1", "fast"), "fast")
	if refusal != "" {
		t.Fatalf("unexpected refusal: %s", refusal)
	}
	// The host PVC is raw Block whatever the guest asked: KubeVirt hotplugs a
	// device, and the filesystem is the guest node plugin's job.
	if host.Spec.VolumeMode == nil || *host.Spec.VolumeMode != corev1.PersistentVolumeBlock {
		t.Errorf("host volumeMode = %v, want Block", host.Spec.VolumeMode)
	}
	if *host.Spec.StorageClassName != "fast" {
		t.Errorf("host class = %q", *host.Spec.StorageClassName)
	}
	if host.Labels[LabelGuestUID] != "pvc-uid-1" {
		t.Errorf("ownership labels missing: %v", host.Labels)
	}
	if !strings.Contains(host.Name, "pvc-uid-1") {
		// UID-based on purpose: a PVC recreated under the same guest NAME is a
		// different volume and must never adopt the old disk.
		t.Errorf("host PVC name %q must be derived from the guest UID", host.Name)
	}
}

func TestMapPVCRefusesWhatCannotMove(t *testing.T) {
	tc := testTC()

	rwx := guestPVC("shared", "u1", "fast")
	rwx.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
	if _, refusal := mapPVC(tc, rwx, "fast"); !strings.Contains(refusal, "ReadWriteOnce") {
		t.Errorf("RWX refusal = %q, want ReadWriteOnce explanation", refusal)
	}

	blk := guestPVC("raw", "u2", "fast")
	blockMode := corev1.PersistentVolumeBlock
	blk.Spec.VolumeMode = &blockMode
	if _, refusal := mapPVC(tc, blk, "fast"); refusal == "" {
		t.Error("guest Block volumeMode must be refused")
	}

	empty := guestPVC("empty", "u3", "fast")
	empty.Spec.Resources = corev1.VolumeResourceRequirements{}
	if _, refusal := mapPVC(tc, empty, "fast"); refusal == "" {
		t.Error("a PVC with no storage request must be refused")
	}
}

func TestGuestPVCanMove(t *testing.T) {
	pvc := guestPVC("data", "pvc-uid-1", "fast")
	pv := guestPVForClaim(pvc, "pn1-pvc-pvc-uid-1", "fast")
	// THE architectural assertion: no node affinity. A local PV's affinity is
	// what pins it forever; a CSI PV without one is what lets the attach/
	// detach cycle move the volume with the workload.
	if pv.Spec.NodeAffinity != nil {
		t.Fatalf("guest PV must have NO node affinity, got %v", pv.Spec.NodeAffinity)
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("reclaim policy must be Retain (the syncer orders deletion itself)")
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle != "pn1-pvc-pvc-uid-1" {
		t.Errorf("volume handle must be the host PVC name")
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != types.UID("pvc-uid-1") {
		t.Errorf("claimRef must pre-bind to the guest PVC by UID")
	}
	if pv.Spec.CSI.VolumeAttributes["serial"] != csinode.SerialFor("pn1-pvc-pvc-uid-1") {
		t.Errorf("serial attribute must match the derived serial")
	}
}

func TestSerialShape(t *testing.T) {
	a, b := csinode.SerialFor("x"), csinode.SerialFor("y")
	if len(a) != 20 || len(b) != 20 {
		t.Fatalf("serials must be 20 chars (QEMU truncates long SCSI serials): %q %q", a, b)
	}
	if a == b {
		t.Fatal("distinct handles must give distinct serials")
	}
	if a != csinode.SerialFor("x") {
		t.Fatal("serial must be deterministic")
	}
}

// TestNodePluginHoldsNoCredentials pins THE security property of the split
// driver: the only thing installed in a guest must hold no credential of any
// kind. If this test starts failing, stop and re-read the design.
func TestNodePluginHoldsNoCredentials(t *testing.T) {
	_, sa, ds := guestNodePlugin(NodePluginImages{Node: "node:img", Registrar: "reg:img"})
	if sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken {
		t.Error("node plugin ServiceAccount must not automount a token")
	}
	spec := ds.Spec.Template.Spec
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Error("node plugin pod must not automount a service account token")
	}
	for _, v := range spec.Volumes {
		if v.Secret != nil || v.Projected != nil {
			t.Errorf("node plugin must mount no Secret or projected volume, found %q", v.Name)
		}
	}
	for _, c := range spec.Containers {
		if len(c.EnvFrom) != 0 {
			t.Errorf("container %q must not use envFrom", c.Name)
		}
		for _, e := range c.Env {
			if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
				t.Errorf("container %q reads a Secret into env %q", c.Name, e.Name)
			}
			if strings.Contains(strings.ToLower(e.Name), "kubeconfig") {
				t.Errorf("container %q has a kubeconfig-shaped env %q", c.Name, e.Name)
			}
		}
	}
}

// --- reconcile tests --------------------------------------------------------

// fakeHotplug plays KubeVirt. AddVolume attaches instantly (InSpec + Ready);
// the pass still needs a SECOND reconcile to observe it, because attach state
// is read at gather time — exactly the real controller's cadence.
type fakeHotplug struct {
	vms     map[string]map[string]kubevirt.VolumeState
	added   []string
	removed []string
}

func newFakeHotplug(vmNames ...string) *fakeHotplug {
	f := &fakeHotplug{vms: map[string]map[string]kubevirt.VolumeState{}}
	for _, n := range vmNames {
		f.vms[n] = map[string]kubevirt.VolumeState{}
	}
	return f
}

func (f *fakeHotplug) AddVolume(_ context.Context, _, vm, vol, _, _ string) error {
	if _, ok := f.vms[vm]; !ok {
		return apierrors.NewNotFound(corev1.Resource("virtualmachines"), vm)
	}
	f.vms[vm][vol] = kubevirt.VolumeState{InSpec: true, Phase: kubevirt.Ready}
	f.added = append(f.added, vm+"/"+vol)
	return nil
}

func (f *fakeHotplug) RemoveVolume(_ context.Context, _, vm, vol string) error {
	if m, ok := f.vms[vm]; ok {
		delete(m, vol)
	}
	f.removed = append(f.removed, vm+"/"+vol)
	return nil
}

func (f *fakeHotplug) VMs(_ context.Context, _ string) (map[string]map[string]kubevirt.VolumeState, error) {
	out := map[string]map[string]kubevirt.VolumeState{}
	for vm, vols := range f.vms {
		cp := map[string]kubevirt.VolumeState{}
		for k, v := range vols {
			cp[k] = v
		}
		out[vm] = cp
	}
	return out, nil
}

func scheduledPVC(name string, uid types.UID, class, node string) *corev1.PersistentVolumeClaim {
	pvc := guestPVC(name, uid, class)
	pvc.Annotations = map[string]string{selectedNodeAnnotation: node}
	return pvc
}

func attachment(name, pvName, node string) *storagev1.VolumeAttachment {
	pv := pvName
	return &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: csinode.DriverName,
			NodeName: node,
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv},
		},
	}
}

func TestStorageClassMirroredAndPluginInstalled(t *testing.T) {
	f := newFixture(t, false, nil, []client.Object{offerableHostSC("fast")})
	f.reconcile(t)

	var sc storagev1.StorageClass
	if err := f.guest.Get(context.Background(), client.ObjectKey{Name: "fast"}, &sc); err != nil {
		t.Fatalf("mirrored guest StorageClass missing: %v", err)
	}
	if *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
		t.Errorf("guest class must be WaitForFirstConsumer")
	}
	var drv storagev1.CSIDriver
	if err := f.guest.Get(context.Background(), client.ObjectKey{Name: csinode.DriverName}, &drv); err != nil {
		t.Fatalf("guest CSIDriver missing: %v", err)
	}
	if drv.Spec.AttachRequired == nil || !*drv.Spec.AttachRequired {
		t.Error("CSIDriver must require attach — VolumeAttachments are the move mechanism")
	}
	if err := f.guest.Get(context.Background(), client.ObjectKey{Namespace: GuestCSINamespace, Name: GuestCSIDaemonSet}, dsPtr()); err != nil {
		t.Fatalf("node plugin DaemonSet missing: %v", err)
	}
}

func TestNoOfferableClassesMeansNothingInstalled(t *testing.T) {
	// Storage sync is on by default, but with no offerable host class it must
	// install NOTHING in the guest.
	f := newFixture(t, false, nil, nil)
	f.reconcile(t)
	if err := f.guest.Get(context.Background(), client.ObjectKey{Namespace: GuestCSINamespace, Name: GuestCSIDaemonSet}, dsPtr()); !apierrors.IsNotFound(err) {
		t.Fatalf("expected no DaemonSet, got err=%v", err)
	}
}

func TestPVCWaitsForConsumerThenProvisions(t *testing.T) {
	pvc := guestPVC("data", "uid-1", "fast")
	f := newFixture(t, false, []client.Object{pvc}, []client.Object{offerableHostSC("fast")})
	f.reconcile(t)

	hostName := HostPVCName(f.tc, "uid-1")
	var hostPVC corev1.PersistentVolumeClaim
	err := f.host.Get(context.Background(), client.ObjectKey{Namespace: "tenant-pn1", Name: hostName}, &hostPVC)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("host PVC must not exist before a pod schedules (WFFC), got err=%v", err)
	}

	// The guest scheduler picks a node.
	var g corev1.PersistentVolumeClaim
	if err := f.guest.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "data"}, &g); err != nil {
		t.Fatal(err)
	}
	g.Annotations = map[string]string{selectedNodeAnnotation: "node-a"}
	if err := f.guest.Update(context.Background(), &g); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)

	if err := f.host.Get(context.Background(), client.ObjectKey{Namespace: "tenant-pn1", Name: hostName}, &hostPVC); err != nil {
		t.Fatalf("host PVC missing after node selection: %v", err)
	}
	var pv corev1.PersistentVolume
	if err := f.guest.Get(context.Background(), client.ObjectKey{Name: "pvc-uid-1"}, &pv); err != nil {
		t.Fatalf("guest PV missing: %v", err)
	}
	if pv.Spec.NodeAffinity != nil {
		t.Error("guest PV must have no node affinity")
	}
	// The guest PVC is pinned before host state exists.
	if err := f.guest.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "data"}, &g); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&g, GuestFinalizer) {
		t.Error("guest PVC must carry the cleanup finalizer")
	}
}

func TestAttachThenMoveToAnotherNode(t *testing.T) {
	pvc := scheduledPVC("data", "uid-1", "fast", "node-a")
	hostName := "pn1-pvc-uid-1"
	pvName := "pvc-uid-1"
	volName := VolumeName(hostName)
	va := attachment("va-a", pvName, "node-a")
	f := newFixture(t, false, []client.Object{pvc, va}, []client.Object{offerableHostSC("fast")})
	f.hot = newFakeHotplug("node-a", "node-b")
	f.r.Hotplug = f.hot

	f.reconcile(t) // provisions + issues AddVolume
	if len(f.hot.added) != 1 || f.hot.added[0] != "node-a/"+volName {
		t.Fatalf("expected hotplug into node-a's VM, got %v", f.hot.added)
	}
	f.reconcile(t) // observes Ready, marks attached
	var gotVA storagev1.VolumeAttachment
	if err := f.guest.Get(context.Background(), client.ObjectKey{Name: "va-a"}, &gotVA); err != nil {
		t.Fatal(err)
	}
	if !gotVA.Status.Attached {
		t.Fatal("VolumeAttachment must be marked attached once the VMI reports Ready")
	}
	if gotVA.Status.AttachmentMetadata["serial"] != csinode.SerialFor(hostName) {
		t.Error("attachment metadata must carry the disk serial for NodeStage")
	}

	// THE MOVE. The guest A/D controller deletes va-a and creates va-b.
	if err := f.guest.Delete(context.Background(), &gotVA); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t) // sees deleting va-a → unplug from node-a
	if len(f.hot.removed) != 1 || f.hot.removed[0] != "node-a/"+volName {
		t.Fatalf("expected unplug from node-a, got %v", f.hot.removed)
	}
	f.reconcile(t) // observes volume gone → releases va-a's finalizer (object deletes)
	if err := f.guest.Get(context.Background(), client.ObjectKey{Name: "va-a"}, &gotVA); !apierrors.IsNotFound(err) {
		t.Fatalf("va-a should be gone after unplug, err=%v", err)
	}

	vb := attachment("va-b", pvName, "node-b")
	if err := f.guest.Create(context.Background(), vb); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t) // hotplug into node-b
	if len(f.hot.added) != 2 || f.hot.added[1] != "node-b/"+volName {
		t.Fatalf("expected hotplug into node-b's VM, got %v", f.hot.added)
	}
	f.reconcile(t)
	if err := f.guest.Get(context.Background(), client.ObjectKey{Name: "va-b"}, &gotVA); err != nil {
		t.Fatal(err)
	}
	if !gotVA.Status.Attached {
		t.Fatal("volume must be attached on node-b after the move")
	}
}

func TestGuestPVCDeleteUnplugsBeforeHostDelete(t *testing.T) {
	pvc := scheduledPVC("data", "uid-1", "fast", "node-a")
	va := attachment("va-a", "pvc-uid-1", "node-a")
	f := newFixture(t, false, []client.Object{pvc, va}, []client.Object{offerableHostSC("fast")})
	f.hot = newFakeHotplug("node-a")
	f.r.Hotplug = f.hot

	f.reconcile(t)
	f.reconcile(t) // provisioned + attached
	hostName := HostPVCName(f.tc, "uid-1")

	// Tenant deletes the PVC. Pod teardown deletes the VA first (kubelet
	// unmounts, A/D controller detaches); the PVC waits on our finalizer.
	var gotVA storagev1.VolumeAttachment
	if err := f.guest.Get(context.Background(), client.ObjectKey{Name: "va-a"}, &gotVA); err != nil {
		t.Fatal(err)
	}
	if err := f.guest.Delete(context.Background(), &gotVA); err != nil {
		t.Fatal(err)
	}
	var g corev1.PersistentVolumeClaim
	if err := f.guest.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "data"}, &g); err != nil {
		t.Fatal(err)
	}
	if err := f.guest.Delete(context.Background(), &g); err != nil {
		t.Fatal(err)
	}

	f.reconcile(t) // unplug issued; host PVC MUST still exist
	var hostPVC corev1.PersistentVolumeClaim
	if err := f.host.Get(context.Background(), client.ObjectKey{Namespace: "tenant-pn1", Name: hostName}, &hostPVC); err != nil {
		t.Fatalf("host PVC deleted while volume still attached — the exact bug the ordering exists to prevent: %v", err)
	}

	f.reconcile(t) // VA gone
	f.reconcile(t) // volume observed unplugged → host PVC + guest PV deleted, finalizer released
	if err := f.host.Get(context.Background(), client.ObjectKey{Namespace: "tenant-pn1", Name: hostName}, &hostPVC); !apierrors.IsNotFound(err) {
		t.Fatalf("host PVC should be gone, err=%v", err)
	}
	if err := f.guest.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "data"}, &g); !apierrors.IsNotFound(err) {
		t.Fatalf("guest PVC should have been released and deleted, err=%v", err)
	}
	var pv corev1.PersistentVolume
	if err := f.guest.Get(context.Background(), client.ObjectKey{Name: "pvc-uid-1"}, &pv); !apierrors.IsNotFound(err) {
		t.Fatalf("guest PV should be gone, err=%v", err)
	}
}

func TestNeverDeleteUnlabelledHostPVC(t *testing.T) {
	// An unlabelled host PVC squatting on OUR derived name: never adopted,
	// never deleted — not during sync, not during PVC teardown. Stricter here
	// than anywhere: deleting an unlabelled PVC destroys someone's data.
	pvc := scheduledPVC("data", "uid-1", "fast", "node-a")
	tcStub := testTC()
	squatter := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HostPVCName(tcStub, "uid-1"),
			Namespace: "tenant-pn1",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")},
			},
		},
	}
	f := newFixture(t, false, []client.Object{pvc}, []client.Object{offerableHostSC("fast"), squatter})
	f.reconcile(t)

	var got corev1.PersistentVolumeClaim
	if err := f.host.Get(context.Background(), client.ObjectKey{Namespace: "tenant-pn1", Name: squatter.Name}, &got); err != nil {
		t.Fatalf("unlabelled host PVC must survive: %v", err)
	}
	if got.Labels[LabelTenantCluster] != "" {
		t.Fatal("unlabelled host PVC must not be adopted")
	}

	// And the collision is reported, not silent.
	var tcNow = f.getTC(t)
	found := false
	for _, c := range tcNow.Status.Conditions {
		if c.Type == "Synced" && c.Status == metav1.ConditionFalse {
			found = true
		}
	}
	if !found {
		t.Error("collision with an unmanaged host PVC must surface as Synced=False")
	}

	// Guest PVC deleted: teardown must still not touch the squatter.
	var g corev1.PersistentVolumeClaim
	if err := f.guest.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "data"}, &g); err != nil {
		t.Fatal(err)
	}
	if err := f.guest.Delete(context.Background(), &g); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	f.reconcile(t)
	if err := f.host.Get(context.Background(), client.ObjectKey{Namespace: "tenant-pn1", Name: squatter.Name}, &got); err != nil {
		t.Fatalf("unlabelled host PVC deleted during teardown: %v", err)
	}
}

func TestTenantClusterTeardownUnplugsAndDeletesStorage(t *testing.T) {
	pvc := scheduledPVC("data", "uid-1", "fast", "node-a")
	va := attachment("va-a", "pvc-uid-1", "node-a")
	f := newFixture(t, false, []client.Object{pvc, va}, []client.Object{offerableHostSC("fast")})
	f.hot = newFakeHotplug("node-a")
	f.r.Hotplug = f.hot
	f.reconcile(t)
	f.reconcile(t) // provisioned + attached

	tc := f.getTC(t)
	if err := f.host.Delete(context.Background(), tc); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t) // teardown pass 1: unplug issued, finalizer must hold
	volName := VolumeName(HostPVCName(f.tc, "uid-1"))
	if len(f.hot.removed) == 0 || f.hot.removed[0] != "node-a/"+volName {
		t.Fatalf("teardown must unplug before deleting host PVCs, removed=%v", f.hot.removed)
	}
	f.reconcile(t) // teardown pass 2: unplugged → host PVC deleted, finalizer drops

	var pvcs corev1.PersistentVolumeClaimList
	if err := f.host.List(context.Background(), &pvcs, client.InNamespace("tenant-pn1"),
		client.MatchingLabels{LabelTenantCluster: "pn1"}); err != nil {
		t.Fatal(err)
	}
	if len(pvcs.Items) != 0 {
		t.Fatalf("labelled host PVCs must all be gone, found %d", len(pvcs.Items))
	}
	if err := f.host.Get(context.Background(), client.ObjectKey{Name: "pn1", Namespace: "syncer"}, f.tc); !apierrors.IsNotFound(err) {
		t.Fatalf("TenantCluster should be released, err=%v", err)
	}
	// Guest-side platform objects cleaned up best-effort.
	var sc storagev1.StorageClass
	if err := f.guest.Get(context.Background(), client.ObjectKey{Name: "fast"}, &sc); !apierrors.IsNotFound(err) {
		t.Errorf("mirrored guest StorageClass should be deleted, err=%v", err)
	}
}
