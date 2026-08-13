# vcluster-private-cloud-controller

Platform networking for **private-nodes vCluster guest clusters whose nodes
are KubeVirt VMs**, provided entirely by the **host** cluster.

This repository ships two things, versioned apart:

| series | what it is | status |
|---|---|---|
| **1.x — the tenant syncer** | Our own host-side controller: LoadBalancer Services, **Ingress**, **Gateway API** and (1.1+) **storage** for guest clusters, with per-tenant hostname authority. | current |
| **0.x — upstream CCM wrapper** | A Helm chart deploying the upstream `kubevirt-cloud-controller-manager`, one per guest, LoadBalancer only. | **deprecated** — kept until every deployment has moved to 1.x |

Both are published as OCI charts under the same name
(`oci://ghcr.io/the-it-dept/charts/vcluster-private-cloud-controller`); pin the
major version you want.

---

## 1.x — the tenant syncer

### The security property, which is the whole point

**Trust flows one way: the provider can reach into a tenant; a tenant can
never reach the host.**

The syncer runs on the **host**, in the provider's namespace, and holds one
kubeconfig **per guest** — a credential INTO each guest. No host kubeconfig,
no host token, no host service account ever exists in a guest. The one
component that runs inside a guest (the storage node plugin, below) holds **no
credential of any kind** — it talks only to the local kubelet and local block
devices, and a unit test pins that property.

This is not a preference. A customer has **cluster-admin on their own
cluster** by design, so any credential placed in a guest is a credential the
customer holds. The upstream CCM's normal deployment model — running in the
tenant with an infrastructure kubeconfig — hands every tenant a way into the
provider's cluster. This controller exists to avoid that pattern (our 0.x
chart already ran the CCM host-side; 1.x keeps that direction and extends it
past LoadBalancers).

Enforced in code, not convention:

- The `TenantCluster` API has a field for the credential into the guest and
  **no field for the reverse**.
- The syncer never creates a Secret in a guest and never writes host
  connection details there. Its guest-facing writes are statuses, conditions,
  events, annotations and finalizers — a guest learns its addresses and the
  reasons for refusals, never credentials.
- The syncer **only ever deletes host objects carrying its own ownership
  labels**, re-checked object by object at the moment of deletion.

### How it works

```
guest cluster (vCluster, private nodes = KubeVirt VMs)
  Service / Ingress / Gateway / HTTPRoute        <- created by the tenant
        |  watched via a guest kubeconfig held ON THE HOST
        v
tenant syncer (one Deployment on the host, all guests)
        |  creates + owns (labelled) host equivalents
        v
host namespace holding the guest's VMs
  Service  -> selects the tenant's virt-launcher pods, targets the guest NodePort
  Ingress  -> host ingress class, hostnames validated, backends rewritten
  Gateway/HTTPRoute -> same, when the host has the Gateway API
        |  host LB / ingress controller assigns addresses
        v
  status written back to the GUEST resource
```

Traffic path for a LoadBalancer: client → host LB address → virt-launcher pod
IP (with bridge binding, that **is** the VM/guest-node IP) → guest NodePort →
guest kube-proxy → guest pod. The host Service selects the VM pods **by
label**, so VM restarts (which change pod IPs) need no tracking.

### Registering a guest: the `TenantCluster` CRD

```yaml
apiVersion: vcluster.the-it-dept.io/v1alpha1
kind: TenantCluster
metadata:
  name: tenant-a
  namespace: vcluster-cloud-controller
spec:
  # Credential INTO the guest. Provider -> tenant. There is no field for the
  # reverse, and there must never be one.
  kubeconfigSecretRef:
    name: tenant-a-kubeconfig     # Secret in this namespace, key "kubeconfig"

  # Where host-side objects go. Must be the namespace holding the guest's VM
  # pods: a Service selector only reaches pods in its own namespace.
  hostNamespace: tenant-a-vms

  # How to find the guest's VM pods. Labels, not addresses: a VM's pod IP
  # changes on restart; a selector survives that.
  nodeSelector:
    matchLabels:
      cluster.x-k8s.io/cluster-name: tenant-a
      cluster.x-k8s.io/role: worker

  # THE HOSTNAME AUTHORITY. A guest Ingress/Gateway/HTTPRoute hostname is
  # synced only if it matches one of these. Wildcards match ONE label:
  # "*.tenant-a.example.com" covers "x.tenant-a.example.com" but not
  # "x.y.tenant-a.example.com" and not "tenant-a.example.com".
  allowedDomains:
    - tenant-a.example.com
    - "*.tenant-a.example.com"

  ingressClassName: nginx     # host ingress class stamped on synced Ingresses
  # gatewayClassName: host-gw # required if syncing Gateways

  # ADDRESS LIMITS. How many host objects this tenant may hold that consume a
  # pool address — LoadBalancer Services and Gateways TOGETHER. Absent means
  # unlimited. A public IP pool is usually small (a /29 is six usable
  # addresses), and a tenant with cluster-admin in their own cluster can
  # otherwise create a hundred LoadBalancers and exhaust the region.
  limits:
    loadBalancers: 3

  sync:
    services: true
    ingresses: true
    gateways: true
    storage: true   # inert until a host StorageClass is labelled tenant-offerable
    # Optional: only sync guest LB Services with this loadBalancerClass.
    # Lets the syncer coexist with another LB implementation (e.g. the 0.x
    # CCM) serving the same guest during a migration.
    # serviceLoadBalancerClass: vcluster.the-it-dept.io/tenant-syncer
```

Because the `TenantCluster` lives on the host, a tenant can never edit their
own `allowedDomains` — that is what makes it an authority rather than a hint.

### Address families

Guest Services carry `ipFamilyPolicy` and `ipFamilies` through to the host
Service, so a guest that asks for IPv6 gets IPv6 and a dual-stack guest gets
both — rather than silently receiving whatever the host's default family is.
**Every** allocated address is written back into the guest's
`status.loadBalancer.ingress`, not just the first.

**Most guests cannot use `spec.ipFamilies` for this, so there is an annotation
too.** `spec.ipFamilies` governs the *guest's own* ClusterIP allocation: a
single-stack IPv4 guest's API server rejects `ipFamilies: [IPv6]` outright
(`not configured on this cluster`), and the family of a **public** address on
the host is a different question. So the family of the published endpoint can
be asked for directly:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: echo
  annotations:
    # IPv4 | IPv6 | IPv4,IPv6 — the families of the HOST address.
    vcluster.the-it-dept.io/ip-families: "IPv4,IPv6"
    # Optional; derived otherwise (one family SingleStack, two RequireDualStack).
    # vcluster.the-it-dept.io/ip-family-policy: RequireDualStack
spec:
  type: LoadBalancer
```

The annotation wins where both are present; an unparseable value is refused
with a reason rather than quietly falling back to the host default.

Three ways it can fail, and all three are visible on the guest object rather
than a `<pending>` with no reason:

| | what the guest is told |
|---|---|
| host cannot allocate that family at all | refused up front: `address family IPv6 refused: the host cluster cannot allocate IPv6 addresses (it provides IPv4)`. Discovered from the host's `ServiceCIDR` objects; if that lookup cannot answer, the request is passed through for the host API server to judge rather than refused on a guess. |
| host has the family but no address left | `AddressPartiallyAssigned` / `AddressPending`, naming the family that did not arrive |
| host API server rejects the object | the rejection is copied onto the guest object, not just the `TenantCluster` the tenant cannot read |

**Gateways are different, and it is worth knowing why.** Gateway API has no
`ipFamilies` field. The only way a guest can express a family is
`spec.addresses`, which does it by naming a literal host address — the same
`loadBalancerIP` hazard the Service syncer refuses to carry, since it would let
a tenant claim or steer onto an address that is not theirs. So a guest
`spec.addresses` is **refused visibly** and the host chooses; whatever it
assigns is written back to `status.addresses` in full. In practice the family a
Gateway gets is whatever the host's Gateway implementation puts on its derived
LoadBalancer Service.

### Address limits

`spec.limits.loadBalancers` caps how many **logical endpoints** a tenant may
publish. A dual-stack Service takes an IPv4 and an IPv6 but counts **once**: it
is one thing a customer asked for, and the scarce family is IPv4.

- **Deterministic and stable.** Endpoints are ordered by creation timestamp,
  ties broken by UID, and the ones past the limit are refused. Which endpoint
  is refused cannot change between reconciles — otherwise a tenant's working
  endpoint would cycle up and down as the controller re-sorted a map.
- **Lowering a limit tears nothing down.** Endpoints already published on the
  host are grandfathered; the limit stops growth. An operator must be able to
  stop a tenant growing without a customer's production endpoint disappearing
  because someone edited a number.
- **The refusal is visible in the guest**, with the limit and the current
  count, exactly as a rejected hostname is.
- A LoadBalancer Service refused by the limit but referenced by a synced
  Ingress is **downgraded to a ClusterIP backend**, not dropped: the limit is
  on addresses, and taking the address away must not also break an Ingress
  that was within its rights.

### Hostname authority

Every tenant can write any Ingress they like in their own cluster. Without
validation, tenant A publishes an Ingress for tenant B's hostname on the
shared host ingress. So every hostname (Ingress rule and TLS hosts, Gateway
listener, HTTPRoute hostname) is checked against `allowedDomains`, and a
refusal is **never silent**: the guest resource gets a Warning event and a
durable record (a status condition on Services, an annotation
`vcluster.the-it-dept.io/refused` on Ingress/Gateway/HTTPRoute) naming the
hostname and the allowed domains.

Refused as a matter of policy, not just non-matching: rules with **no** host,
`defaultBackend`, and listeners with no hostname — each would capture traffic
for every hostname the host serves, which per-domain grants cannot authorize.

An HTTPRoute with **no** hostnames is deliberately *not* refused, and the
distinction matters. Gateway API confines such a route to the hostnames of the
listeners it attaches to, and every listener hostname has already been through
the authority above — so it can only ever be published on names the tenant was
granted. Refusing it would break the ordinary case of a route inheriting its
Gateway's hostname. The invariant this rests on is the listener check, which is
exactly why a hostname-less *listener* is refused outright.

Object references inside HTTPRoute filters are rewritten or refused, never
copied: a `RequestMirror` backendRef is a Service reference like any other and
would otherwise resolve host-side against whatever bears that name, and an
`ExtensionRef` names a host CRD the tenant has no business reaching.
Cross-namespace backendRefs are refused rather than silently flattened into the
route's own namespace.

### Gateway API needs a GatewayClass in the guest

Every `Gateway` must name a `GatewayClass`, and `gatewayClassName` is the
operator's choice on the `TenantCluster` — which lives on the host, where a
tenant cannot look. So the syncer **mirrors it into the guest**, the same way
and for the same reason it mirrors offerable StorageClasses; without it a
tenant is guessing a string, and a Gateway naming the wrong one is simply never
served. The mirrored class keeps the host class's *name* (the pass maps it back
by name) but names **this syncer** as its controller rather than the host's —
inside the guest, the syncer is what implements it, and claiming otherwise
would invite a guest-side installation of that controller to fight for it. It
is marked `Accepted` because nothing else in the guest ever would, withdrawn if
the operator renames the class or turns Gateway sync off, and removed on
`TenantCluster` teardown.

### Storage (1.1+): volumes that move with the workload

A guest PVC becomes a real host volume attached to whichever VM backs the
guest node the consuming pod runs on — and it **moves** when the pod is
rescheduled to another node. A `local` PV cannot do that (its node affinity is
part of the object), so this is a **CSI driver split across the trust
boundary**:

| half | runs | does | host credentials |
|---|---|---|---|
| controller (provisioner + attacher roles) | **host**, inside the syncer | guest PVC → host PVC; guest VolumeAttachment → KubeVirt hotplug into that node's VM; VA deleted → unplug | the provider's own |
| node plugin + node-driver-registrar | **guest**, a DaemonSet the syncer installs | find the hotplugged disk by serial, format on first use, mount | **none** |

How to offer storage: label a host StorageClass
`vcluster.the-it-dept.io/tenant-offerable: "true"`. The syncer mirrors it into
every storage-enabled guest under the same name — provisioner replaced with
`csi.vcluster.the-it-dept.io` and **`volumeBindingMode:
WaitForFirstConsumer`** always, because binding is what tells us which node
(and therefore which VM) the volume belongs on. The mirror is re-asserted
every pass; tenant edits to it do not stick.

The flow, end to end: pod scheduled → guest scheduler annotates the PVC with
its node → syncer creates a **Block-mode host PVC** on the real host class and
a guest CSI PV (no node affinity — that absence is the mobility) → the guest's
attach/detach controller creates a VolumeAttachment → syncer hotplugs the host
PVC into that node's VM as a SCSI disk with a deterministic serial → node
plugin finds `/dev/disk/by-id/*<serial>*`, formats ext4 on first use, mounts.
On reschedule the VA for the old node is deleted (syncer unplugs, then
releases it — the A/D controller's multi-attach guard holds the new node's
attach until then) and a VA for the new node appears (syncer hotplugs there).
Same disk, same data, different VM.

Deletion is ordered, and the order is the point: unplug from the VM **first**,
then delete the host PVC, then the guest PV, then release the guest PVC's
finalizer. Deleting a host PVC still attached to a running VM leaves a volume
the host CSI cannot reclaim. Attach state is read live from VMI status every
pass — never cached — before any PVC deletion.

Storage limits, stated plainly:

- **ReadWriteOnce only.** The volume is one block device in one VM at a time.
  It moves with the workload; it cannot be shared across nodes. (RWX needs a
  network filesystem class, which this is not.)
- **A move is a hotplug cycle**, detach + attach, driven by the poll interval:
  expect tens of seconds, not milliseconds — same class of delay as any
  attach/detach storage.
- Guest `volumeMode: Block` PVCs and volume expansion are not supported yet;
  refusals are surfaced on the PVC, never silent.
- The guest node's VM must carry the node's name (the platform's convention).

### Lifecycle and ownership

Every host object carries ownership labels (tenant cluster, guest namespace,
guest name, guest UID). The syncer:

- deletes a host object when its guest resource goes — enforced with a
  finalizer on the guest resource, so deletion cannot race cleanup and no
  address is left allocated and answering;
- deletes **everything** for a tenant when the `TenantCluster` is deleted
  (finalizer on the `TenantCluster`), which the per-guest CCM model could
  not do — it died with the guest before it could clean up;
- does **nothing destructive while a guest is unreachable** — status reports
  `connected: false` with the last sync time, host objects stay put: a
  tenant's published address must not disappear because its API server
  restarted;
- **never deletes a host object lacking its ownership labels**, whatever the
  desired state says, and never adopts or overwrites one either — a name
  collision is reported on the `TenantCluster` and left alone.

Missing optional APIs degrade cleanly: a host without the Gateway API CRDs
gets `GatewayAPIAvailable: False` in status and the other syncers carry on.

### Requirements and limits

- Host: something assigning addresses to `type: LoadBalancer` Services
  (Cilium LB-IPAM, MetalLB, cloud LB), an ingress controller, optionally the
  Gateway API. KubeVirt guests need VM pods with routable pod-network IPs
  (bridge binding) carrying the `nodeSelector` labels.
- Guest workloads must be reachable via a **guest NodePort**: LoadBalancer
  Services keep `allocateLoadBalancerNodePorts: true` (the default); Ingress
  and HTTPRoute backends must be `NodePort` (or LoadBalancer) Services. The
  guest's ClusterIP network does not exist on the host.
- Guest TLS Secrets are **not** copied to the host. Host-side issuance (e.g.
  cert-manager on the ingress class) is the supported path; moving private
  keys between trust domains would need an explicit opt-in that does not
  exist yet.
- Guest Service annotations are **not** forwarded to host Services: on the
  host they are instructions to host controllers (address-pool selection),
  and a tenant must not steer those.
- `externalTrafficPolicy: Local` is not propagated; client source IPs are not
  preserved through the guest NodePort hop either way.
- Guest `Gateway.spec.addresses` is refused, not carried: address (and
  therefore address-family) selection for a Gateway belongs to the host.
- Address-family discovery reads the host's `ServiceCIDR` objects
  (`networking.k8s.io/v1`, Kubernetes 1.33+). Without them the syncer cannot
  tell what the host supports and passes every request through.

### Install

```sh
helm install tenant-syncer \
  oci://ghcr.io/the-it-dept/charts/vcluster-private-cloud-controller \
  --version '^1.0.0' \
  --namespace vcluster-cloud-controller --create-namespace
```

Then create a kubeconfig Secret and a `TenantCluster` per guest (see above).
For a vCluster guest, point the kubeconfig's `server` at the vcluster's
in-cluster Service (`https://<name>.<namespace>.svc`).

```sh
kubectl -n vcluster-cloud-controller get tenantclusters
NAME       CONNECTED   SERVICES   INGRESSES   AGE
tenant-a   true        2          1           5m
```

### Decision record (1.x): why we replaced the upstream CCM we had just adopted

The 0.x decision below was correct for what it evaluated: for LoadBalancer
Services alone, upstream was the better mechanism, and it was proved live.
What changed is the requirement, not the assessment:

1. **The cloud-provider interface stops at LoadBalancers and node lifecycle.**
   Ingress and Gateway API are not in it and never will be. The product needs
   hostname-based routing, which means a controller that watches guest
   Ingress/Gateway objects — something a CCM structurally cannot grow into.
2. **Multi-tenant hostname authority has to live somewhere.** The moment
   guest Ingresses materialise on a shared host ingress, some component must
   decide which tenant may claim which hostname. That is a policy decision
   tied to tenant registration, which is exactly what the `TenantCluster`
   CRD is.
3. **One process for all guests, with teardown.** The per-guest CCM model
   leaks host Services when a guest cluster is deleted (the controller dies
   before it can clean up — documented as an operational note in 0.x). A
   single registration-driven controller can hold a finalizer on the
   registration and guarantee cleanup.
4. **What 1.x keeps from upstream's design:** selector-based endpoints
   (labels on VM pods, no address tracking), NodePort targeting, status
   write-back, finalizer-based release — the mechanisms 0.x validated in
   production are all retained; they are just implemented once, host-side,
   behind a CRD.

The 0.x chart remains published and deployable until 1.x has displaced it
everywhere it runs.

---

## 0.x — upstream CCM wrapper (**deprecated**)

> **Deprecated:** superseded by the 1.x tenant syncer above, which covers
> LoadBalancers via the same host-side mechanism plus Ingress and Gateway
> API. 0.x remains published for existing deployments; no new features.

`type: LoadBalancer` Services for private-nodes vCluster guests, deployed as
one upstream
[kubevirt/cloud-provider-kubevirt](https://github.com/kubevirt/cloud-provider-kubevirt)
cloud-controller-manager per guest cluster, running on the host.

A private-nodes vCluster has no cloud provider that implements LoadBalancer:
a `type: LoadBalancer` Service created inside the guest sits `<pending>`
forever. The vendor-suggested alternative — running MetalLB *inside* each
guest — puts address allocation in tenant hands, which is exactly what a
multi-tenant host cannot allow. This chart keeps allocation on the host: the
host's own LoadBalancer implementation remains the single IPAM authority.

### How it works (0.x)

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

Deletion is handled by the standard Kubernetes service controller running
inside the CCM: it puts the `service.kubernetes.io/load-balancer-cleanup`
finalizer on the guest Service, and deleting the guest Service deletes the
host Service, which releases the address back to the host pool.

### Decision record (0.x): why upstream, not a bespoke controller

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
forking — all encoded in the chart):

- **Only the `service` controller is enabled.** vCluster's embedded cloud
  provider owns guest-node initialisation and lifecycle and sets
  `providerID: vcluster://<node>`, which the kubevirt provider's InstancesV2
  cannot parse. Running the kubevirt cloud-node/cloud-node-lifecycle
  controllers against such nodes would be wrong and potentially destructive,
  so `instancesV2` is disabled in cloud-config and
  `--controllers=service-lb-controller` is pinned (the full controller name —
  upstream registers no aliases, so the short name `service` is rejected).
  vCluster's embedded provider implements no LoadBalancer for private nodes,
  so there is no overlap in the other direction.
- **VM labels are your job.** Upstream selects VM pods by the Cluster API
  labels; vCluster's node provisioning does not add them. Put them on every
  guest-node VirtualMachine.
- **One CCM Deployment per guest cluster** (upstream's model). The chart makes
  this declarative: a `clusters:` list, one Deployment + ConfigMap per entry.
- `--leader-elect=false` with a single replica, so no lease is written into
  the guest's kube-system beside vCluster's own components.

**What would have justified writing our own** (and did not apply at the time):
needing a single multi-cluster controller process, needing EndpointSlice mode
with continuous node-address tracking, or upstream being unmaintained. *(See
the 1.x decision record above for what did, later, justify it: Ingress and
Gateway API, which are outside the cloud-provider interface entirely.)*

### Install (0.x)

```sh
helm install vpcc oci://ghcr.io/the-it-dept/charts/vcluster-private-cloud-controller \
  --version '^0.1.0' \
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

Label every guest-node VirtualMachine in `spec.template.metadata.labels` with
`cluster.x-k8s.io/cluster-name: <name>` and `cluster.x-k8s.io/role: worker`
(KubeVirt copies these to each virt-launcher pod). See
[examples/guest-cluster-values.yaml](examples/guest-cluster-values.yaml).

### Operational notes (0.x)

- **Guest-cluster teardown:** deleting a guest cluster does not, by itself,
  delete host Services created for it (the CCM is gone before it can clean
  up). Remove them with the labels stamped on every managed Service:
  `kubectl -n <vmNamespace> delete svc -l cluster.x-k8s.io/cluster-name=<name>`.
  The 1.x syncer closes this gap with its `TenantCluster` finalizer.
- **`externalTrafficPolicy: Local`** on the guest Service is propagated to the
  host Service, but client source IPs are still not preserved end to end (the
  guest NodePort hop masquerades). Treat it as unsupported.
- The CCM writes events and Service status into the guest, so the guest
  kubeconfig needs those permissions (a vCluster admin kubeconfig has them).
- Address selection: `spec.loadBalancerIP` and pool-selection annotations are
  copied from the guest Service to the host Service. *(The 1.x syncer
  deliberately does not do this; see its limits section.)*

---

## Security

- The 1.x syncer's ServiceAccount can read Secrets **only by name** (`get`,
  never `list`/`watch`): a compromised syncer pod cannot enumerate host
  Secrets. The 0.x chart's ServiceAccount is namespaced to the configured VM
  namespaces.
- Guest kubeconfigs live in Secrets you create on the host; nothing in this
  repository ever writes credentials to disk, to logs, or into a guest.
- Examples throughout use `example.com` and RFC 5737 documentation addresses.

## Licence

Apache 2.0.
