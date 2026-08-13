// Package v1alpha1 contains the TenantCluster registration API.
//
// A TenantCluster tells the tenant syncer about one guest cluster it should
// serve: how to reach INTO the guest, where on the host that guest's objects
// materialise, and which hostnames the guest is entitled to publish.
//
// +kubebuilder:object:generate=true
// +groupName=vcluster.the-it-dept.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "vcluster.the-it-dept.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
