package kubevirt

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
)

func vmObj(name string, spec map[string]any, status map[string]any) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachine",
		"metadata":   map[string]any{"name": name, "namespace": "ns"},
	}
	if spec != nil {
		obj["spec"] = spec
	}
	if status != nil {
		obj["status"] = status
	}
	return &unstructured.Unstructured{Object: obj}
}

func vmiObj(name string, volumeStatus []any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachineInstance",
		"metadata":   map[string]any{"name": name, "namespace": "ns"},
		"status":     map[string]any{"volumeStatus": volumeStatus},
	}}
}

// TestVMsMergesSpecStatusAndQueuedRequests pins the three sources VMs() must
// merge — VM spec volumes, VMI volumeStatus, and KubeVirt's ASYNC
// status.volumeRequests queue. The queued add counting as InSpec is the fix
// for a live-found race: addvolume/removevolume do not edit the spec
// synchronously, so a teardown that read only the spec saw "clean" moments
// before the queued add materialized an orphan hotplug volume.
func TestVMsMergesSpecStatusAndQueuedRequests(t *testing.T) {
	vm := vmObj("node-a",
		map[string]any{"template": map[string]any{"spec": map[string]any{"volumes": []any{
			map[string]any{"name": "csi-aaa", "persistentVolumeClaim": map[string]any{"claimName": "pvc-a"}},
			map[string]any{"name": "cloudinit"},
		}}}},
		map[string]any{"volumeRequests": []any{
			map[string]any{"addVolumeOptions": map[string]any{
				"name": "csi-queued",
				"volumeSource": map[string]any{
					"persistentVolumeClaim": map[string]any{"claimName": "pvc-q"},
				},
			}},
		}},
	)
	vmi := vmiObj("node-a", []any{
		map[string]any{"name": "csi-aaa", "phase": "Ready"},
	})

	scheme := runtime.NewScheme()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		vmGVR:  "VirtualMachineList",
		vmiGVR: "VirtualMachineInstanceList",
	}, vm, vmi)
	c := &Client{dyn: dyn}

	out, err := c.VMs(context.Background(), "ns")
	if err != nil {
		t.Fatal(err)
	}
	vols := out["node-a"]
	if got := vols["csi-aaa"]; !got.InSpec || got.Phase != Ready || got.Claim != "pvc-a" {
		t.Errorf("spec+status volume wrong: %+v", got)
	}
	if got := vols["cloudinit"]; !got.InSpec || got.Claim != "" {
		t.Errorf("non-PVC volume wrong: %+v", got)
	}
	if got := vols["csi-queued"]; !got.InSpec || got.Claim != "pvc-q" {
		t.Errorf("a QUEUED addvolume must count as InSpec with its claim: %+v", got)
	}
}
