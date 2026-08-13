// The tenant syncer: a host-side controller that gives private-nodes vCluster
// guests LoadBalancer Services, Ingress, Gateway API and storage, materialised
// on the host cluster. It holds one kubeconfig INTO each guest; the only thing
// ever installed in a guest is the credential-less CSI node plugin (this same
// binary, `csi-node` mode) — see the README for why that direction is the
// point.
package main

import (
	"flag"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/the-it-dept/vcluster-private-cloud-controller/api/v1alpha1"
	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/csinode"
	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/kubevirt"
	"github.com/the-it-dept/vcluster-private-cloud-controller/internal/syncer"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	// Gateway API types are in the scheme unconditionally — the scheme is a
	// type registry, not a promise the CRDs exist. Availability is checked at
	// runtime and reported in TenantCluster status.
	utilruntime.Must(gwv1.Install(scheme))
}

func main() {
	// The same binary is both the host-side controller and the guest-side CSI
	// node plugin: one image family, one version, no drift between the halves.
	// The node plugin is selected explicitly and parses its own flags — it
	// must never accidentally start a controller inside a guest.
	if len(os.Args) > 1 && os.Args[1] == "csi-node" {
		csinode.Main(os.Args[2:])
		return
	}

	var metricsAddr, probeAddr string
	var leaderElect bool
	var interval time.Duration
	var csiNodeImage, csiRegistrarImage string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint bind address")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe bind address")
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election")
	flag.DurationVar(&interval, "poll-interval", syncer.DefaultInterval,
		"how often each guest cluster is fully re-synced")
	flag.StringVar(&csiNodeImage, "csi-node-image",
		"ghcr.io/the-it-dept/vcluster-private-cloud-controller-csi-node:latest",
		"image for the CSI node plugin DaemonSet deployed into guests")
	flag.StringVar(&csiRegistrarImage, "csi-registrar-image",
		"registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.13.0",
		"image for the CSI node-driver-registrar sidecar deployed into guests")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	// The cache only ever holds objects this controller owns: host Services and
	// Ingresses are label-filtered to our ownership label. Without the filter,
	// one syncer pod would hold an informer over every Service on the host.
	ourObjects, err := labels.Parse(syncer.LabelTenantCluster)
	if err != nil {
		panic(err)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "tenant-syncer.vcluster.the-it-dept.io",
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Service{}: {Label: ourObjects},
				&netv1.Ingress{}:  {Label: ourObjects},
			},
		},
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	hotplug, err := kubevirt.New(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "unable to build KubeVirt hotplug client")
		os.Exit(1)
	}

	if err := (&syncer.TenantClusterReconciler{
		Client:   mgr.GetClient(),
		Reader:   mgr.GetAPIReader(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tenant-syncer"),
		Interval: interval,
		Hotplug:  hotplug,
		CSIImages: syncer.NodePluginImages{
			Node:      csiNodeImage,
			Registrar: csiRegistrarImage,
		},
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TenantCluster")
		os.Exit(1)
	}

	utilruntime.Must(mgr.AddHealthzCheck("healthz", healthz.Ping))
	utilruntime.Must(mgr.AddReadyzCheck("readyz", healthz.Ping))

	setupLog.Info("starting tenant syncer")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}
