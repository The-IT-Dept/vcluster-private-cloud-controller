package csinode

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
)

// SerialFor derives the disk serial for a volume handle. It must match what
// the host-side syncer stamps on the KubeVirt hotplug disk, and it is derived
// rather than carried so the node plugin can always recompute it from the
// volume ID alone — publish context is treated as a hint, not a dependency.
//
// 20 hex characters because QEMU truncates long SCSI serials; 80 bits of hash
// is far beyond collision concern for volumes attached to one VM.
func SerialFor(volumeHandle string) string {
	sum := sha256.Sum256([]byte(volumeHandle))
	return hex.EncodeToString(sum[:])[:20]
}

// serialContextKey is where the syncer puts the serial in the PV's
// volumeAttributes and the VolumeAttachment's attachmentMetadata.
const serialContextKey = "serial"

// fsType is fixed: one well-understood filesystem beats a matrix of
// tenant-selectable ones, and ext4 resizes online if expansion arrives later.
const fsType = "ext4"

// deviceWait bounds how long NodeStage waits for the hotplugged disk to show
// up in /dev/disk/by-id. Attach completes host-side before the VolumeAttachment
// is marked attached, so this only covers udev settling — but a VM that is
// slow to see the SCSI bus rescan should get patience, not a failure that
// kubelet backs off on.
const deviceWait = 30 * time.Second

// mounter is the mount surface NodeStage/NodePublish need; a seam so unit
// tests do not need real block devices or root.
type mounter interface {
	FormatAndMount(source, target, fstype string, options []string) error
	Mount(source, target, fstype string, options []string) error
	IsLikelyNotMountPoint(path string) (bool, error)
	CleanupMountPoint(path string) error
}

type realMounter struct{ *mount.SafeFormatAndMount }

func (m realMounter) CleanupMountPoint(path string) error {
	return mount.CleanupMountPoint(path, m.SafeFormatAndMount.Interface, false)
}

func newMounter() mounter {
	return realMounter{&mount.SafeFormatAndMount{
		Interface: mount.New(""),
		Exec:      utilexec.New(),
	}}
}

// --- Node service -----------------------------------------------------------

func (d *driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	// The node ID is the Kubernetes node name. The host-side attacher maps it
	// to the VM backing this node; nothing else identifies a node here.
	return &csi.NodeGetInfoResponse{NodeId: d.nodeID}, nil
}

func (d *driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME},
			},
		}},
	}, nil
}

// NodeStageVolume finds the hotplugged disk by serial, formats it on first
// use, and mounts it at the staging path.
func (d *driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.VolumeId == "" || req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID and staging target path are required")
	}
	if blk := req.GetVolumeCapability().GetBlock(); blk != nil {
		return nil, status.Error(codes.InvalidArgument, "raw block volumes are not supported")
	}

	serial := req.GetPublishContext()[serialContextKey]
	if serial == "" {
		serial = req.GetVolumeContext()[serialContextKey]
	}
	if serial == "" {
		serial = SerialFor(req.VolumeId)
	}

	dev, err := waitForDevice(ctx, serial, deviceWait)
	if err != nil {
		return nil, status.Errorf(codes.NotFound,
			"no disk with serial %q appeared within %s: %v", serial, deviceWait, err)
	}

	if notMnt, err := d.mounter.IsLikelyNotMountPoint(req.StagingTargetPath); err == nil && !notMnt {
		return &csi.NodeStageVolumeResponse{}, nil // already staged; kubelet retries are normal
	}
	if err := os.MkdirAll(req.StagingTargetPath, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "creating staging path: %v", err)
	}
	// FormatAndMount formats ONLY when the device carries no filesystem, which
	// is precisely the move-with-the-workload contract: first attach formats,
	// every later attach on any other node just mounts the existing data.
	if err := d.mounter.FormatAndMount(dev, req.StagingTargetPath, fsType, nil); err != nil {
		return nil, status.Errorf(codes.Internal, "mounting %s at %s: %v", dev, req.StagingTargetPath, err)
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func (d *driver) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if err := d.mounter.CleanupMountPoint(req.StagingTargetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "unmounting staging path: %v", err)
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (d *driver) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.StagingTargetPath == "" || req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging and target paths are required")
	}
	if notMnt, err := d.mounter.IsLikelyNotMountPoint(req.TargetPath); err == nil && !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}
	if err := os.MkdirAll(req.TargetPath, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "creating target path: %v", err)
	}
	opts := []string{"bind"}
	if req.Readonly {
		opts = append(opts, "ro")
	}
	if err := d.mounter.Mount(req.StagingTargetPath, req.TargetPath, "", opts); err != nil {
		return nil, status.Errorf(codes.Internal, "bind-mounting into pod: %v", err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *driver) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if err := d.mounter.CleanupMountPoint(req.TargetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "unmounting target path: %v", err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// --- device lookup ----------------------------------------------------------

// byIDDir is where udev publishes stable disk identities. KubeVirt exposes a
// hotplugged SCSI disk's serial as scsi-0QEMU_QEMU_HARDDISK_<serial> (and a
// second SQEMU… alias), so matching on the serial substring finds it without
// hardcoding QEMU's prefix du jour.
var byIDDir = "/dev/disk/by-id"

func findDeviceBySerial(serial string) (string, error) {
	entries, err := os.ReadDir(byIDDir)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if !strings.Contains(e.Name(), serial) {
			continue
		}
		dev, err := filepath.EvalSymlinks(filepath.Join(byIDDir, e.Name()))
		if err != nil {
			return "", err
		}
		return dev, nil
	}
	return "", fmt.Errorf("no entry in %s matches serial %q", byIDDir, serial)
}

func waitForDevice(ctx context.Context, serial string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		dev, err := findDeviceBySerial(serial)
		if err == nil {
			return dev, nil
		}
		if time.Now().After(deadline) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
