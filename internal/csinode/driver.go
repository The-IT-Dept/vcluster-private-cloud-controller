// Package csinode is the guest-side half of the storage syncer: a minimal CSI
// node plugin that runs as a DaemonSet inside a tenant's cluster.
//
// THE TRUST BOUNDARY IS THE WHOLE DESIGN HERE. This process talks to exactly
// two things: the local kubelet (over a unix socket kubelet dials) and local
// block devices. It holds NO Kubernetes credential at all — not to the guest,
// and emphatically not to the host. Every operation that needs a credential
// (provisioning the host PVC, hotplugging it into the right VM) happens in the
// tenant syncer on the HOST; by the time kubelet calls NodeStageVolume the
// disk is already sitting in /dev, put there from outside the trust boundary.
// The upstream kubevirt-csi-driver model — the whole driver in the guest with
// an infra kubeconfig — is exactly what this split exists to avoid.
package csinode

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// DriverName is the CSI driver identity, shared by the guest StorageClasses
// the syncer mirrors, the PVs it creates, and this plugin's registration.
const DriverName = "csi.vcluster.the-it-dept.io"

// driverVersion is reported through the CSI Identity service. Informational.
const driverVersion = "1.1.0"

// Main is the entrypoint for the `csi-node` subcommand. It parses its own
// flags so the controller's flag set never leaks into the node binary's UX.
func Main(args []string) {
	fs := flag.NewFlagSet("csi-node", flag.ExitOnError)
	endpoint := fs.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint to listen on")
	nodeName := fs.String("node-name", os.Getenv("KUBE_NODE_NAME"),
		"this node's Kubernetes name; reported as the CSI node ID")
	_ = fs.Parse(args)

	if *nodeName == "" {
		fmt.Fprintln(os.Stderr, "csi-node: --node-name (or KUBE_NODE_NAME) is required")
		os.Exit(1)
	}
	if err := Serve(*endpoint, *nodeName); err != nil {
		fmt.Fprintf(os.Stderr, "csi-node: %v\n", err)
		os.Exit(1)
	}
}

// Serve listens on the CSI endpoint until the process is killed.
func Serve(endpoint, nodeName string) error {
	path, ok := strings.CutPrefix(endpoint, "unix://")
	if !ok {
		return fmt.Errorf("only unix:// endpoints are supported, got %q", endpoint)
	}
	// A stale socket from a previous container instance would make Listen fail.
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing stale socket: %w", err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", path, err)
	}

	srv := grpc.NewServer()
	d := &driver{nodeID: nodeName, mounter: newMounter()}
	csi.RegisterIdentityServer(srv, d)
	csi.RegisterNodeServer(srv, d)
	fmt.Printf("csi-node: serving %s on %s as node %q\n", DriverName, endpoint, nodeName)
	return srv.Serve(lis)
}

type driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	nodeID  string
	mounter mounter
}

// --- Identity ---------------------------------------------------------------

func (d *driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: DriverName, VendorVersion: driverVersion}, nil
}

// GetPluginCapabilities advertises NOTHING, deliberately: no CONTROLLER_SERVICE
// (the controller half lives on the host, outside this cluster entirely) and
// no topology constraints (the volume follows the workload to any node — that
// mobility is the reason the design is a CSI driver at all).
func (d *driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (d *driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}
