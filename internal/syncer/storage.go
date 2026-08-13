package syncer

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/csinode"
	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/kubevirt"
)

// The storage syncer.
//
// A guest PVC must become a real volume attached to whichever VM backs the
// guest node the consuming pod is scheduled onto — and MOVE when that pod is
// rescheduled elsewhere. A `local` PV cannot move (its node affinity is part
// of the object), so this is a CSI driver split across the trust boundary:
//
//   - the CONTROLLER half is this file plus storage_pass.go, running on the
//     HOST inside the tenant syncer. It plays the roles the upstream
//     external-provisioner and external-attacher sidecars would play — watch
//     guest PVCs, create host PVCs; watch guest VolumeAttachments, hotplug
//     the host PVC into the right VM via KubeVirt — implemented natively in
//     the syncer's pass rather than as per-tenant sidecar Deployments,
//     because one syncer serves every guest and already owns the
//     ownership-label and guest-unreachable safety machinery.
//   - the NODE half (internal/csinode) is a DaemonSet the syncer installs
//     INTO the guest: find the disk by serial, format, mount. It holds no
//     credential of any kind. Every host operation stays on the host.
//
// The upstream kubevirt-csi-driver model — the whole driver in the guest with
// an infra kubeconfig — is rejected; a tenant must hold NO credential to the
// host.

// LabelTenantOfferable marks a HOST StorageClass as offered to tenants. The
// storage syncer is enabled by default but inert until the operator labels at
// least one class, so switching the syncer on never installs anything into a
// guest by itself.
const LabelTenantOfferable = "vcluster.the-it-dept.io/tenant-offerable"

// ManagedByLabel marks the objects the syncer manages INSIDE a guest
// (StorageClasses, PVs, the node plugin). Guest-side ownership is a different
// question from host-side ownership: the tenant is cluster-admin and can strip
// any label, so this is bookkeeping for the syncer's own idempotence, not a
// security boundary.
const (
	ManagedByLabel = "app.kubernetes.io/managed-by"
	ManagedByValue = FieldManager
)

// GuestCSINamespace is where the node plugin lives inside a guest. The
// syncer's guest kubeconfig is the platform's channel into the guest, so the
// plugin is installable and upgradable by the provider without tenant
// cooperation — it is platform, not tenant workload.
const (
	GuestCSINamespace = "vcluster-csi"
	GuestCSIDaemonSet = "vcluster-csi-node"
)

// NodePluginImages are the images the syncer deploys into guests. Set from
// controller flags; per-tenant overrides are deliberately absent — the plugin
// is platform-managed and versioned with the platform.
type NodePluginImages struct {
	Node      string
	Registrar string
}

// Hotplugger is the KubeVirt surface the storage syncer needs, as an
// interface so reconcile tests can fake the VM side.
type Hotplugger interface {
	AddVolume(ctx context.Context, namespace, vm, volume, claim, serial string) error
	RemoveVolume(ctx context.Context, namespace, vm, volume string) error
	VMs(ctx context.Context, namespace string) (map[string]map[string]kubevirt.VolumeState, error)
}

// selectedNodeAnnotation is set on a PVC by the guest scheduler once a pod
// using it has been placed. Its appearance is the provisioning trigger: with
// WaitForFirstConsumer there is no node — and therefore no VM to ever attach
// to — until it exists.
const selectedNodeAnnotation = "volume.kubernetes.io/selected-node"

// provisionedByAnnotation marks the guest PVs this syncer creates.
const provisionedByAnnotation = "pv.kubernetes.io/provisioned-by"

// HostPVCName derives the host PVC name for a guest PVC from the guest UID:
// unlike HostObjectName this is deliberately NOT name-based, because a PVC
// recreated under the same guest name is a different volume with different
// data, and must never silently adopt the old one's disk.
func HostPVCName(tc *v1alpha1.TenantCluster, guestUID string) string {
	return sanitizeName(tc.Name + "-pvc-" + guestUID)
}

// VolumeName is the hotplug volume's name inside the VM spec, derived from the
// serial so teardown can find it knowing only the host PVC.
func VolumeName(hostPVCName string) string {
	return "csi-" + csinode.SerialFor(hostPVCName)
}

// mirrorStorageClass builds the guest StorageClass for a tenant-offerable host
// class. Same name on both sides — the tenant asks for what the operator
// advertised, and the pass maps it back by name.
func mirrorStorageClass(hostSC *storagev1.StorageClass) *storagev1.StorageClass {
	wffc := storagev1.VolumeBindingWaitForFirstConsumer
	reclaim := corev1.PersistentVolumeReclaimDelete
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:   hostSC.Name,
			Labels: map[string]string{ManagedByLabel: ManagedByValue},
		},
		Provisioner: csinode.DriverName,
		// WaitForFirstConsumer is NOT a tuning choice, it is the mechanism: a
		// PVC has no node until a pod using it is scheduled, and without a node
		// there is no VM to hotplug into. Immediate binding would force
		// attaching to a guess. The HOST class's own binding mode is
		// deliberately ignored here.
		VolumeBindingMode: &wffc,
		ReclaimPolicy:     &reclaim,
	}
	// The default-class marker is carried over so a guest whose operator
	// offers the host's default gets working `storageClassName`-less PVCs.
	if v, ok := hostSC.Annotations["storageclass.kubernetes.io/is-default-class"]; ok {
		sc.Annotations = map[string]string{"storageclass.kubernetes.io/is-default-class": v}
	}
	return sc
}

// mapPVC builds the host PVC for a guest PVC. The refusal (empty when
// syncable) is surfaced on the guest PVC — never a silent Pending.
func mapPVC(tc *v1alpha1.TenantCluster, guest *corev1.PersistentVolumeClaim, hostClass string) (*corev1.PersistentVolumeClaim, string) {
	for _, m := range guest.Spec.AccessModes {
		if m != corev1.ReadWriteOnce {
			return nil, fmt.Sprintf(
				"access mode %s is not supported: the volume is a block device attached to one VM at a "+
					"time (it moves with the workload, but two nodes cannot share it); only ReadWriteOnce", m)
		}
	}
	if vm := guest.Spec.VolumeMode; vm != nil && *vm != corev1.PersistentVolumeFilesystem {
		return nil, fmt.Sprintf("volumeMode %s is not supported; only Filesystem", *vm)
	}
	req, ok := guest.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok || req.IsZero() {
		return nil, "no storage request; nothing to provision"
	}

	// The HOST PVC is volumeMode Block whatever the guest asked for: KubeVirt
	// hotplugs a raw device (a Filesystem-mode host PVC would need a disk.img
	// inside it), and the guest-side node plugin puts the filesystem ON that
	// device. The tenant's Filesystem semantics live entirely in the guest.
	blockMode := corev1.PersistentVolumeBlock
	host := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HostPVCName(tc, string(guest.UID)),
			Namespace: tc.Spec.HostNamespace,
			Labels:    ownershipLabels(tc, guest),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:       &blockMode,
			StorageClassName: &hostClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: req},
			},
		},
	}
	return host, ""
}

// guestPVForClaim builds the guest PV that binds a guest PVC to its host
// volume. A CSI PV with NO node affinity — that absence is the point: the
// attach/detach cycle decides where the volume is, so Kubernetes is free to
// reschedule the pod anywhere and ask for the volume there.
func guestPVForClaim(guest *corev1.PersistentVolumeClaim, hostPVCName, storageClassName string) *corev1.PersistentVolume {
	req := guest.Spec.Resources.Requests[corev1.ResourceStorage]
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pvc-" + string(guest.UID),
			Labels:      map[string]string{ManagedByLabel: ManagedByValue},
			Annotations: map[string]string{provisionedByAnnotation: csinode.DriverName},
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity:    corev1.ResourceList{corev1.ResourceStorage: req},
			// Retain, because the DELETION path must be ordered by the syncer:
			// unplug from the VM first, then delete the host PVC, then this PV.
			// A Delete policy would have the guest's PV controller racing that
			// order with nothing standing behind it — there is no in-guest
			// provisioner to honour it.
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              storageClassName,
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: guest.Namespace,
				Name:      guest.Name,
				UID:       guest.UID,
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: csinode.DriverName,
					// The volume handle IS the host PVC name. The node plugin
					// derives the disk serial from it; the host controller maps it
					// straight back to the object to hotplug.
					VolumeHandle: hostPVCName,
					FSType:       "ext4",
					VolumeAttributes: map[string]string{
						"serial": csinode.SerialFor(hostPVCName),
					},
				},
			},
		},
	}
}

// guestCSIDriver is the CSIDriver registration object for the guest.
func guestCSIDriver() *storagev1.CSIDriver {
	attach := true
	podInfo := false
	capacity := false
	fsGroup := storagev1.FileFSGroupPolicy
	return &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name:   csinode.DriverName,
			Labels: map[string]string{ManagedByLabel: ManagedByValue},
		},
		Spec: storagev1.CSIDriverSpec{
			// attachRequired drives everything: it makes the guest's
			// attach/detach controller create VolumeAttachments, which are the
			// syncer's signal to hotplug — and their deletion the signal to
			// unplug. Without it there is no move.
			AttachRequired:       &attach,
			PodInfoOnMount:       &podInfo,
			StorageCapacity:      &capacity,
			FSGroupPolicy:        &fsGroup,
			VolumeLifecycleModes: []storagev1.VolumeLifecycleMode{storagev1.VolumeLifecyclePersistent},
		},
	}
}

// guestNodePlugin renders the node plugin DaemonSet. NOTE WHAT IS ABSENT: no
// Secret volumes, no kubeconfig, no API access of any kind. The plugin talks
// to kubelet over a hostPath socket and to /dev; the ServiceAccount below
// exists only so the pod does not run as `default`, and is bound to nothing.
// A unit test asserts the no-credential property stays true.
func guestNodePlugin(images NodePluginImages) (*corev1.Namespace, *corev1.ServiceAccount, *appsv1.DaemonSet) {
	labels := map[string]string{
		ManagedByLabel: ManagedByValue,
		"app":          GuestCSIDaemonSet,
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   GuestCSINamespace,
		Labels: map[string]string{ManagedByLabel: ManagedByValue},
	}}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: GuestCSIDaemonSet, Namespace: GuestCSINamespace,
		Labels: map[string]string{ManagedByLabel: ManagedByValue},
	}}
	automount := false
	sa.AutomountServiceAccountToken = &automount

	hostPathDir := corev1.HostPathDirectory
	hostPathDirOrCreate := corev1.HostPathDirectoryOrCreate
	privileged := true
	bidirectional := corev1.MountPropagationBidirectional

	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: GuestCSIDaemonSet, Namespace: GuestCSINamespace, Labels: labels,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": GuestCSIDaemonSet}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName:           GuestCSIDaemonSet,
					AutomountServiceAccountToken: &automount,
					PriorityClassName:            "system-node-critical",
					// Storage must come up on every node, tainted or not — the
					// plugin gates volume mounts for whatever runs there.
					Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
					Containers: []corev1.Container{
						{
							Name:  "csi-node",
							Image: images.Node,
							Args: []string{
								"csi-node",
								"--endpoint=unix:///csi/csi.sock",
							},
							Env: []corev1.EnvVar{{
								Name:      "KUBE_NODE_NAME",
								ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}},
							}},
							SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "plugin-dir", MountPath: "/csi"},
								{Name: "kubelet-dir", MountPath: "/var/lib/kubelet", MountPropagation: &bidirectional},
								{Name: "dev", MountPath: "/dev"},
							},
						},
						{
							Name:  "registrar",
							Image: images.Registrar,
							Args: []string{
								"--csi-address=/csi/csi.sock",
								"--kubelet-registration-path=/var/lib/kubelet/plugins/" + csinode.DriverName + "/csi.sock",
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "plugin-dir", MountPath: "/csi"},
								{Name: "registration-dir", MountPath: "/registration"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{Name: "plugin-dir", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
							Path: "/var/lib/kubelet/plugins/" + csinode.DriverName, Type: &hostPathDirOrCreate}}},
						{Name: "registration-dir", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
							Path: "/var/lib/kubelet/plugins_registry", Type: &hostPathDir}}},
						{Name: "kubelet-dir", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
							Path: "/var/lib/kubelet", Type: &hostPathDir}}},
						{Name: "dev", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
							Path: "/dev", Type: &hostPathDir}}},
					},
				},
			},
		},
	}
	return ns, sa, ds
}
