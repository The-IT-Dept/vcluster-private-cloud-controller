# vcluster-private-cloud-controller

`type: LoadBalancer` Services for **private-nodes vCluster guest clusters whose
nodes are KubeVirt VMs**, with every address allocated and routed by the
**host** cluster.

A private-nodes vCluster has no cloud provider that implements LoadBalancer:
a `type: LoadBalancer` Service created inside the guest sits `<pending>`
forever. The vendor-suggested alternative — running MetalLB *inside* each
guest — puts address allocation in tenant hands, which is exactly what a
multi-tenant host cannot allow. This project keeps allocation on the host: the
host's own LoadBalancer implementation (Cilium, MetalLB, a cloud LB — anything
that assigns addresses to host Services) remains the single IPAM authority.

## What this repo is

A Helm chart (plus a container-image mirror) that deploys the **upstream
[kubevirt/cloud-provider-kubevirt](https://github.com/kubevirt/cloud-provider-kubevirt)**
cloud-controller-manager, one instance per guest cluster, configured for the
vCluster private-nodes topology. We evaluated writing our own controller and
chose the upstream project instead — see the decision record below.

## How it works

```
guest cluster (vCluster, private nodes = KubeVirt VMs)
  Service tenant-app  type=LoadBalancer         <- created by the tenant
        |  watched via guest kubeconfig
        v
kubevirt-cloud-controller-manager (runs on the HOST, one per guest)
        |  creates + owns
        v
host cluster, namespace containing the guest's VMs
  Service a1b2c3...   type=LoadBalancer
    selector: cluster.x-k8s.io/cluster-name=<guest>, cluster.x-k8s.io/role=worker
    port -> targetPort = the guest Service's NodePort
        |  host LB (e.g. Cilium pool) assigns an address
        v
  status written back to the GUEST Service's status.loadBalancer.ingress
```

Traffic path: external client → host LB address → virt-launcher pod IP
(which, with bridge binding, **is** the VM/guest-node IP) → guest NodePort →
guest kube-proxy → guest pod.

Because the host Service uses a **label selector on the virt-launcher pods**
rather than hand-written endpoint IPs, it survives VM restarts: a restarted VM
gets a new pod with a new IP, the selector picks it up, and the endpoints
follow automatically.

Deletion is handled by the standard Kubernetes service controller running
inside the CCM: it puts the `service.kubernetes.io/load-balancer-cleanup`
finalizer on the guest Service, and deleting the guest Service deletes the
host Service, which releases the address back to the host pool.

## Decision record: why upstream, not a bespoke controller

Evaluated 2026-08 against a working hand-built prototype (host Service with no
selector + hand-written EndpointSlice pointing at guest-node-IP:NodePort).

**Chosen: `kubevirt/cloud-provider-kubevirt` (v0.6.0).** Reasons:

1. **It is exactly the required shape.** Its LoadBalancer implementation
   creates a Service in the infra (host) cluster whose ports target the tenant
   Service's NodePort — the same mechanism the prototype proved — and it runs
   outside the guest, so allocation stays on the host.
2. **Selector-based endpoints beat hand-written EndpointSlices.** VM restarts
   change the guest node's IP (the VM inherits the replacement virt-launcher
   pod's IP under bridge binding). A selector re-resolves automatically;
   hand-written endpoints must be re-tracked by extra controller code that
   upstream simply does not need.
3. **Status write-back and release-on-delete come from the battle-tested
   Kubernetes cloud-provider service controller**, including the cleanup
   finalizer. The prototype demonstrably leaked addresses when only the guest
   Service was deleted; the finalizer closes that hole.
4. **It is maintained**: v0.6.0 released 2026-03, commits through 2026-07,
   multi-arch images published per release.

**Adaptations needed for vCluster private nodes** (configuration only, no
forking — all encoded in this chart):

- **Only the `service` controller is enabled.** vCluster's embedded cloud
  provider owns guest-node initialisation and lifecycle and sets
  `providerID: vcluster://<node>`, which the kubevirt provider's InstancesV2
  cannot parse. Running the kubevirt cloud-node/cloud-node-lifecycle
  controllers against such nodes would be wrong and potentially destructive,
  so `instancesV2` is disabled in cloud-config and `--controllers=service` is
  pinned. vCluster's embedded provider implements no LoadBalancer for private
  nodes, so there is no overlap in the other direction.
- **VM labels are your job.** Upstream selects VM pods by the Cluster API
  labels; vCluster's node provisioning does not add them. Put them on every
  guest-node VirtualMachine (see below).
- **One CCM Deployment per guest cluster** (upstream's model). The chart makes
  this declarative: a `clusters:` list, one Deployment + ConfigMap per entry.
- `--leader-elect=false` with a single replica, so no lease is written into
  the guest's kube-system beside vCluster's own components.

**What would have justified writing our own** (and did not apply): needing a
single multi-cluster controller process, needing EndpointSlice mode with
continuous node-address tracking, or upstream being unmaintained.

## Requirements

- Host cluster with KubeVirt, and something that assigns addresses to host
  `type: LoadBalancer` Services (Cilium LB-IPAM pools, MetalLB, cloud LB...).
- Guest clusters whose nodes are KubeVirt VMs with a **routable pod-network
  IP** (e.g. bridge binding on the pod network: the VM inherits the
  virt-launcher pod IP, so `node-IP:NodePort` is reachable from the host
  datapath).
- A kubeconfig per guest cluster, reachable from host pods. For a vCluster,
  point it at the vcluster's in-cluster Service
  (`https://<vcluster-name>.<vcluster-namespace>.svc`) rather than a public
  hostname.
- Guest Services must keep `allocateLoadBalancerNodePorts: true` (the
  default) — the host Service targets the guest NodePort.

## Install

### 1. Label the guest-node VMs

On every VirtualMachine backing a guest node, in
`spec.template.metadata.labels` (KubeVirt copies these to each virt-launcher
pod it creates, so they survive VM restarts):

```yaml
cluster.x-k8s.io/cluster-name: tenant-a   # must equal the chart's clusterName
cluster.x-k8s.io/role: worker
```

For an already-running VM, also add the same labels to the current VMI (or
restart the VM) so the existing virt-launcher pod is selectable.

### 2. Create the guest kubeconfig Secret

```sh
kubectl -n cloud-controller create secret generic tenant-a-guest-kubeconfig \
  --from-file=kubeconfig=./tenant-a.yaml   # server: https://<vcluster>.<ns>.svc
```

(For a vCluster, derive this from the platform-managed kubeconfig Secret and
rewrite `server` to the in-cluster Service URL.)

### 3. Install the chart

```sh
helm install vpcc oci://ghcr.io/the-it-dept/charts/vcluster-private-cloud-controller \
  --namespace cloud-controller --create-namespace \
  --values my-values.yaml
```

```yaml
# my-values.yaml
clusters:
  - name: tenant-a
    vmNamespace: tenant-a-vms          # host namespace holding the guest's VMs
    kubeconfigSecret:
      name: tenant-a-guest-kubeconfig
      key: kubeconfig
```

See [examples/guest-cluster-values.yaml](examples/guest-cluster-values.yaml)
and [charts/.../values.yaml](charts/vcluster-private-cloud-controller/values.yaml)
for all options.

### 4. Use it

Inside the guest:

```sh
kubectl create deployment echo --image=ealen/echo-server --port=80
kubectl expose deployment echo --port=80 --type=LoadBalancer
kubectl get svc echo    # EXTERNAL-IP appears within seconds, from the HOST pool
```

`kubectl delete svc echo` deletes the host-side Service and releases the
address.

## Operational notes

- **Guest-cluster teardown:** deleting a guest cluster does not, by itself,
  delete host Services created for it (the CCM is gone before it can clean
  up). Remove them with the labels stamped on every managed Service:
  `kubectl -n <vmNamespace> delete svc -l cluster.x-k8s.io/cluster-name=<name>`.
  Wire that into whatever automation deletes guest clusters.
- **`externalTrafficPolicy: Local`** on the guest Service is propagated to the
  host Service, but client source IPs are still not preserved end to end (the
  guest NodePort hop masquerades). Treat it as unsupported.
- The CCM writes events and Service status into the guest, so the guest
  kubeconfig needs those permissions (a vCluster admin kubeconfig has them).
- Address selection: `spec.loadBalancerIP` is copied to the host Service if
  set. Pool-selection annotations (e.g. Cilium's) are copied too — the guest
  Service's annotations are propagated to the host Service.

## Security

This chart's host-side ServiceAccount can only create/delete Services in the
configured VM namespaces (namespaced Roles, no cluster-wide write). Guest
kubeconfigs live in Secrets you create; nothing here ever writes credentials
to disk or logs.

## Licence

Apache 2.0, same as the upstream project this deploys.
