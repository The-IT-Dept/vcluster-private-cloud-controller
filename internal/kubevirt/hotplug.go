// Package kubevirt is the host-side hotplug surface: add/remove a PVC-backed
// disk on a running VirtualMachine and observe what each VM currently carries.
//
// It deliberately does NOT import kubevirt.io client packages. The three
// operations here touch a sliver of the KubeVirt API — two subresource PUTs
// and two unstructured LISTs — and the option structs involved are stable v1
// API; a minimal local mirror of them keeps this module's dependency graph the
// one controller-runtime already implies.
package kubevirt

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

var (
	vmGVR  = schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachines"}
	vmiGVR = schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachineinstances"}
)

// VolumeState is what the syncer needs to know about one hotplug volume on
// one VM: whether the request is recorded in the VM spec, and how far the
// attach has actually progressed on the running VMI.
type VolumeState struct {
	// InSpec: the volume is in the VirtualMachine's template spec — or in a
	// queued status.volumeRequests add that virt-controller has not applied to
	// the spec yet. A queued add IS desired state: it will materialize whether
	// or not this controller still wants it, so treating it as anything less
	// lets a teardown observe "clean" moments before the volume reappears.
	InSpec bool
	// Phase is the VMI volumeStatus phase: "AttachedToNode", "Ready", etc.
	// Empty until the VMI reports the volume.
	Phase string
	// Claim is the PVC the volume points at, when the volume is PVC-backed.
	// The syncer uses it to spot orphans: a hotplug volume whose claim no
	// longer exists can never attach, and blocks every later hotplug on the VM.
	Claim string
}

// Ready is the VMI volumeStatus phase meaning the disk is visible inside the
// guest OS.
const Ready = "Ready"

// Client performs hotplug operations against one host cluster.
type Client struct {
	dyn dynamic.Interface
	sub rest.Interface
}

func New(cfg *rest.Config) (*Client, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building dynamic client: %w", err)
	}
	subCfg := rest.CopyConfig(cfg)
	subCfg.GroupVersion = &schema.GroupVersion{Group: "subresources.kubevirt.io", Version: "v1"}
	subCfg.APIPath = "/apis"
	subCfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	sub, err := rest.RESTClientFor(subCfg)
	if err != nil {
		return nil, fmt.Errorf("building subresource client: %w", err)
	}
	return &Client{dyn: dyn, sub: sub}, nil
}

// addVolumeOptions mirrors kubevirt.io/v1 AddVolumeOptions, restricted to the
// fields this controller uses.
type addVolumeOptions struct {
	Name         string       `json:"name"`
	Disk         disk         `json:"disk"`
	VolumeSource volumeSource `json:"volumeSource"`
}

type disk struct {
	Name   string  `json:"name"`
	Serial string  `json:"serial,omitempty"`
	Disk   diskBus `json:"disk"`
}

type diskBus struct {
	// Hotplugged disks MUST be bus scsi: QEMU cannot hotplug virtio disks into
	// a running q35 machine, and KubeVirt rejects anything else here.
	Bus string `json:"bus"`
}

type volumeSource struct {
	PersistentVolumeClaim pvcSource `json:"persistentVolumeClaim"`
}

type pvcSource struct {
	ClaimName    string `json:"claimName"`
	Hotpluggable bool   `json:"hotpluggable"`
}

type removeVolumeOptions struct {
	Name string `json:"name"`
}

// AddVolume hotplugs a PVC into a running VM as a SCSI disk carrying the given
// serial. Applied via the VM (not the VMI) so the volume is part of the VM's
// desired state and survives a VM restart — "the volume belongs to the node,
// and the node is the VM".
func (c *Client) AddVolume(ctx context.Context, namespace, vm, volume, claim, serial string) error {
	body, err := json.Marshal(addVolumeOptions{
		Name:         volume,
		Disk:         disk{Name: volume, Serial: serial, Disk: diskBus{Bus: "scsi"}},
		VolumeSource: volumeSource{PersistentVolumeClaim: pvcSource{ClaimName: claim, Hotpluggable: true}},
	})
	if err != nil {
		return err
	}
	return c.put(ctx, namespace, vm, "addvolume", body)
}

// RemoveVolume unplugs a hotplugged volume from a running VM.
func (c *Client) RemoveVolume(ctx context.Context, namespace, vm, volume string) error {
	body, err := json.Marshal(removeVolumeOptions{Name: volume})
	if err != nil {
		return err
	}
	return c.put(ctx, namespace, vm, "removevolume", body)
}

func (c *Client) put(ctx context.Context, namespace, vm, verb string, body []byte) error {
	err := c.sub.Put().
		AbsPath("/apis/subresources.kubevirt.io/v1/namespaces/" + namespace + "/virtualmachines/" + vm + "/" + verb).
		Body(body).
		Do(ctx).Error()
	if err != nil {
		return fmt.Errorf("%s on VM %s/%s: %w", verb, namespace, vm, err)
	}
	return nil
}

// claimOf extracts persistentVolumeClaim.claimName from a VM spec volume or an
// addVolumeOptions volumeSource — the two places KubeVirt spells a PVC-backed
// volume the same way.
func claimOf(m map[string]any) string {
	pvc, ok := m["persistentVolumeClaim"].(map[string]any)
	if !ok {
		return ""
	}
	claim, _ := pvc["claimName"].(string)
	return claim
}

// VMs lists the VirtualMachines in a namespace with the state of every volume
// on each, VM spec and VMI status merged. One call per pass, not one per
// VolumeAttachment.
func (c *Client) VMs(ctx context.Context, namespace string) (map[string]map[string]VolumeState, error) {
	out := map[string]map[string]VolumeState{}

	vms, err := c.dyn.Resource(vmGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing VirtualMachines in %s: %w", namespace, err)
	}
	for _, vm := range vms.Items {
		vols := map[string]VolumeState{}
		specVols, _, _ := unstructured.NestedSlice(vm.Object, "spec", "template", "spec", "volumes")
		for _, v := range specVols {
			m, ok := v.(map[string]any)
			if !ok {
				continue
			}
			name, _ := m["name"].(string)
			if name != "" {
				vols[name] = VolumeState{InSpec: true, Claim: claimOf(m)}
			}
		}
		// Queued volume requests. KubeVirt's addvolume/removevolume subresources
		// do not edit the spec synchronously — they append to
		// status.volumeRequests, and virt-controller applies them later. An
		// unapplied add must count as InSpec: observed live, a TenantCluster
		// teardown that raced this queue saw the volume as gone, deleted the
		// host PVC, and the applied-afterwards add left an orphan hotplug volume
		// that blocked every subsequent attach on that VM.
		reqs, _, _ := unstructured.NestedSlice(vm.Object, "status", "volumeRequests")
		for _, rq := range reqs {
			m, ok := rq.(map[string]any)
			if !ok {
				continue
			}
			add, ok := m["addVolumeOptions"].(map[string]any)
			if !ok {
				continue
			}
			name, _ := add["name"].(string)
			if name == "" {
				continue
			}
			st := vols[name]
			st.InSpec = true
			if st.Claim == "" {
				if vs, ok := add["volumeSource"].(map[string]any); ok {
					st.Claim = claimOf(vs)
				}
			}
			vols[name] = st
		}
		out[vm.GetName()] = vols
	}

	vmis, err := c.dyn.Resource(vmiGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing VirtualMachineInstances in %s: %w", namespace, err)
	}
	for _, vmi := range vmis.Items {
		vols, ok := out[vmi.GetName()]
		if !ok {
			// A VMI without a VM object is not something this controller manages
			// volumes on; skip rather than invent a VM.
			continue
		}
		statuses, _, _ := unstructured.NestedSlice(vmi.Object, "status", "volumeStatus")
		for _, s := range statuses {
			m, ok := s.(map[string]any)
			if !ok {
				continue
			}
			name, _ := m["name"].(string)
			phase, _ := m["phase"].(string)
			if name == "" {
				continue
			}
			st := vols[name]
			st.Phase = phase
			// A volume mid-unplug can be gone from the VM spec while the VMI
			// still reports it; keep it visible until the VMI lets go, because
			// "still attached" is the answer that keeps host-PVC deletion honest.
			vols[name] = st
		}
		out[vmi.GetName()] = vols
	}
	return out, nil
}
