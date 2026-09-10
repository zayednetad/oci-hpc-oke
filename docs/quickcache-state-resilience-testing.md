# QuickCache state resilience: deployment and acceptance tests

This branch adds compact JSON state, a pre-write size guard, bounded Kubernetes
API calls, a heartbeat recovery window, optional external state snapshots, and
read-only diagnostic tools. Friendly cache paths and shard ownership algorithms
are unchanged. The default remains 1,024 virtual shards.

The local regression suite and synthetic capacity report do not establish
production throughput or API-server capacity. Run the cluster tests below at the
customer's intended scale before claiming production qualification.

## What changed

| Area | Behavior |
| --- | --- |
| State format | The same ConfigMap keys contain compact JSON. Existing agents, `jq` commands and readable node-local files remain compatible. One ConfigMap update publishes a consistent snapshot of all maps and the plan. |
| Size limit | Count UTF-8 bytes of all data keys and values before writes. Warn at 750 KiB; reject above 900 KiB (921,600 bytes), below Kubernetes' 1 MiB data limit. Rejection leaves the last valid live state in place and the controller unready. |
| Why compact instead of split | Splitting mutable active/pending/previous maps across ConfigMaps requires an additional generation-commit protocol. Compact JSON retains the existing atomic publication model with a smaller change. It is still bounded, not unlimited storage. |
| Corrupt state | Malformed JSON, missing active/peer fields and invalid generations stop reconciliation rather than silently regenerating ownership. Restore valid state before proceeding. A completely missing ConfigMap still allows fresh initialization. |
| API failure | Kubernetes calls use 5-second connect and 15-second read timeouts, with SDK retries disabled. Errors return to the reconciliation loop. They are per-request timeouts, not an end-to-end limit for a whole reconciliation. |
| Recovery | On controller startup and after a failed reconciliation, wait a heartbeat window (120 seconds by default), checking API connectivity. Agents can refresh heartbeats before membership is evaluated. This is a grace period, not a guarantee that every agent will recover within 120 seconds. |
| Worker restart | Node and migration agents retry their initial Node lookup after an API failure. Existing local maps/mounts are retained on ordinary reconciliation errors. Normal node-agent termination still runs host teardown. |
| External backup | Optional CronJob snapshots the full state every five minutes to a private S3-compatible bucket, with unique keys and a checksum. Kubernetes history backups remain available but share the etcd failure domain. |
| Restore | Inspect/checksum verification first. Restore is create-only, requires the controller scaled to zero, matching namespace/name and existing owner UIDs. Only stable snapshots can be restored by this helper; in-flight migration snapshots are retained for investigation. |
| Diagnostics | `check-state.py status` reports state size, missing owners, stale heartbeats, unready QuickCache pods and long-running rebalance phases; optionally checks external-backup age. Nonzero exit status lets an existing monitoring runner alert. It does not install a paging/notification service. |

The API reader remains a polling design. At 256 cache servers with 30-second
intervals, node agents alone normally perform about 17 Node PATCH requests/sec
and 8.5 state GET requests/sec. Migration agents add about 8.5 state GET/sec even
when idle. Controller calls, migration status updates, client agents and
heartbeats during long mount loops add more. Measure actual traffic during load
tests. Full-mesh NFS involves 256 × 255 = 65,280 directed peer relationships;
compacting state does not remove that scaling cost.

## 1. Build and deploy this branch

Check out `codex/quickcache-state-resilience` and package/deploy the Terraform
directory using your usual Resource Manager procedure. Leave shards at 1,024
for the first test. No new GPU nodes are required just to test these changes.

For an existing release, a chart upgrade can apply the runtime changes. A
DaemonSet rollout can tear down and recreate host NFS mounts; pause active
QuickCache training/benchmarks for that maintenance window. Preserve your
existing Helm values. Using a newly created test stack avoids this disruption.

From the repository root:

```bash
python3 -m unittest discover -s test/quickcache -p 'test_*.py'
helm lint terraform/files/oci-quickcache
uv run manifests/quickcache/check-state.py capacity
```

Representative synthetic 1→256-node manual rebalance (including estimate data):

| Virtual shards | Previous pretty JSON bytes | Compact JSON bytes | Headroom to 900 KiB guard |
| --- | ---: | ---: | ---: |
| 1,024 | 298,834 | 249,434 | 672,166 |
| 2,048 | 551,186 | 458,914 | 462,686 |
| 4,096 | 1,055,884 | 877,868 | 43,732 |

These use representative 36-character node UIDs, peer paths and estimate
values. They cover serialization, not running 256 real nodes. Actual values
vary. 4,096 shards deliberately produce a size warning in this case; the runtime
guard always makes the final decision. Do not change the shard count on a warm
cluster as a remedy for a size warning. Plan such a format/placement change
separately; increasing/decreasing shard count changes object-to-shard hashing.

## 2. Check readiness and hit/miss behavior

```bash
kubectl config current-context
kubectl get nodes -l oci-hpc-oke.oracle.com/quickcache=true -o wide
kubectl -n kube-system get deployment,daemonset,pods \
  -l app.kubernetes.io/instance=quickcache
uv run manifests/quickcache/check-state.py status
```

Allow the initial 120-second heartbeat window plus normal reconciliation and
mount setup. The status command returns JSON and exit code 0 when checks pass.
Stable state can be old: `updated_at` is the last state change, not a heartbeat.

Use a newly uploaded object and the existing `quickcache-s3` Secret in `default`.
The [functional test guide](using-quickcache-on-oke.md#functional-test) covers
credentials and upload. Re-run the test:

```bash
kubectl -n default delete job quickcache-boto3-test --ignore-not-found --wait=true
kubectl -n default apply -f manifests/quickcache/boto3-getobject-job.yaml
kubectl -n default wait --for=condition=complete job/quickcache-boto3-test --timeout=5m
kubectl -n default logs job/quickcache-boto3-test
```

For a new key expect first read `False`, subsequent full/chunked/range reads
`True`, and all content matches `True`. A repeated key may be warm on its first
read. Inspect the friendly path and compare its SHA-256 with the uploaded file
as in the existing functional guide.

## 3. Enable and test independent Object Storage backups

Create a private backup bucket and grant a dedicated Customer Secret Key owner
permission to create objects there. Use a unique prefix per OKE cluster.
The bucket is not provisioned by this change. Configure its lifecycle/retention
according to your recovery policy: this tool never deletes backup objects.

Copy the example to a private local file, enter the correct credentials,
regional S3 endpoint and bucket, then apply it. Do not use the benchmark bucket
credentials unless they are intentionally also authorized for backups.

```bash
umask 077
cp manifests/quickcache/state-backup-secret.example.yaml /tmp/qc-state-backup-secret.yaml
vi /tmp/qc-state-backup-secret.yaml
kubectl apply -f /tmp/qc-state-backup-secret.yaml
```

Enable these Resource Manager advanced Storage variables and Apply:

```hcl
quickcache_state_backup_secret = "quickcache-state-backup"
quickcache_state_backup_prefix = "quickcache-state/MY-CLUSTER"
```

Only the Secret's name goes into Terraform state. External backup is disabled
until the Secret name is supplied. For direct Helm deployment the equivalent is
`stateBackup.enabled=true`, `stateBackup.secretName=quickcache-state-backup`,
`stateBackup.prefix=quickcache-state/MY-CLUSTER`. The Helm schedule is configurable
through `stateBackup.schedule` (default `*/5 * * * *`).

After Apply, run one backup immediately:

```bash
QC_BACKUP_JOB="qc-state-backup-$(date +%s)"
kubectl -n kube-system create job "${QC_BACKUP_JOB}" \
  --from=cronjob/quickcache-oci-quickcache-state-backup
kubectl -n kube-system wait --for=condition=complete "job/${QC_BACKUP_JOB}" --timeout=5m
kubectl -n kube-system logs "job/${QC_BACKUP_JOB}"
```

Expect `event=state_backup_succeeded`, the object key, byte count and `stable=True`
when no rebalance is active. List that prefix through OCI CLI or Console and
verify the object exists. Download it with `oci os object get` and inspect it:

```bash
uv run terraform/files/oci-quickcache/files/state_backup.py inspect \
  --file /tmp/qc-state-snapshot.json
```

After a **scheduled** run (manual Job runs do not update the CronJob's last
success field), verify:

```bash
uv run manifests/quickcache/check-state.py status \
  --backup-name quickcache-oci-quickcache-state-backup
```

The age check warns after 15 minutes; adjust monitoring if you override the
schedule. Schedule interval is a target recovery point, not a guarantee:
control-plane outages prevent CronJob scheduling, access failures prevent uploads,
and the most recent automatically restorable **stable** snapshot may be older
than five minutes during a long rebalance. In-flight snapshots preserve evidence
but cannot be blindly restored. Snapshots contain cluster metadata; keep the
bucket private. Credentials and cached object contents are not included.

## 4. Test controller pause, then a controlled API outage

On a disposable test cluster with a warmed test object, pause the controller:

```bash
kubectl -n kube-system scale deployment quickcache-oci-quickcache-controller --replicas=0
```

Run the same hit test and a new-object miss test while it is paused, then resume:

```bash
kubectl -n kube-system scale deployment quickcache-oci-quickcache-controller --replicas=1
kubectl -n kube-system rollout status deployment/quickcache-oci-quickcache-controller --timeout=10m
uv run manifests/quickcache/check-state.py status
```

This proves independence from the QuickCache controller process. **It is not an
API-server outage test.**

For the API-outage test, coordinate a bounded network fault with the OKE/platform
team on a disposable cluster. Block QuickCache controller and agent access to the
Kubernetes API for longer than the heartbeat timeout, with an independent timed
rollback. Keep NFS and Object Storage reachable. Node agents use host networking:
ordinary pod NetworkPolicy may not apply, and host firewall changes can affect
kubelet and other workloads. Do not run an unreviewed cluster-wide firewall rule.

Start a long-running application before the fault; during the fault `kubectl`
and new Job scheduling may not work. It must repeatedly read a warmed key and
unique cold keys, record `from_cache` and content checksums, and retain its logs.
Test these cases and record results:

| Case | Acceptance evidence |
| --- | --- |
| API unavailable; existing owners healthy | Running workload still returns correct bytes; warm reads remain hits; cold reads can use Object Storage; shard-map files/mounts remain present. |
| Controller restart during API failure | It does not publish a new ownership map until API connectivity and the heartbeat recovery window return. |
| Node-agent restart during API failure | Initial lookup retries; pod may be unready. Do not require uninterrupted local cache access: normal termination can tear down mounts. |
| Cache-owner failure during API failure | Surviving workloads return correct bytes through fallback where possible; measure NFS timeout and throughput degradation. No claim of uninterrupted performance. |
| API recovery | Heartbeats refresh, controller resumes, agents converge, and subsequent rebalance progresses without manual map edits. Record any cold shards after owner loss. |

Local tests inject API exceptions and verify retained local-state/mount behavior,
startup retry paths, optimistic concurrency and the heartbeat recovery window.
They do not reproduce every OKE networking or NFS-kernel failure mode.

## 5. Scale, rebalance and measure actual API/NFS load

At each intended stage (for example 3, 20, 64, 128, 256 nodes), record status before,
during and after adding nodes. Warm keys spanning multiple owners beforehand.
With `automatic` rebalance, observe copying before cutover, previous-owner
fallback and cleanup after the grace period. Confirm old keys remain readable
and checksums match after cutover. Use the existing migration tests in the main
guide for manual approval and immediate mode. Immediate mode is expected to make
moved shards cold.

Collect a small read-only client latency sample while workloads run:

```bash
uv run manifests/quickcache/check-state.py probe --samples 60 --interval 2
uv run manifests/quickcache/check-state.py status
kubectl -n kube-system logs deployment/quickcache-oci-quickcache-controller --since=30m
```

The probe issues one sequential ConfigMap GET per sample; it does not emulate
Node PATCH load or measure API-server capacity. Obtain request counts, write
latency, 429/5xx errors and control-plane resource/etcd metrics from the platform
team where available. Record baseline and loaded results from the real agents.
Also capture worker CPU/network, peer-probe duration and NFS errors/timeouts.

Follow the [multi-node benchmark preparation](using-quickcache-on-oke.md#multi-node-benchmark)
to create keys covering all owners and configure the benchmark Secret, then run:

```bash
bash manifests/quickcache/run-multinode-benchmark.sh
```

Agree throughput, first-byte latency and API error/latency budgets with the
customer before testing. Compare Slurm and OKE using the same shapes, node count,
objects, network, concurrency and warm/cold conditions. Saving a report without
meeting agreed thresholds is not production acceptance.

Monitor `state_size_warning`, `state_size_rejected`, `controller_recovery_wait`,
`state_backup_succeeded`, failed backup Jobs, stale heartbeats and failed/long
migrations. Feed `check-state.py status` JSON/exit codes and container logs into
your existing monitoring/notification system. Backups and diagnostics cannot
report through the API while that API itself is unavailable; external monitoring
must detect the control-plane outage independently.

## 6. Recovery drill

Use a disposable test stack and a verified stable snapshot from that same stack.
Pause training and any migration; scale the controller to zero and wait until
its pods are gone. Export the current state for investigation before any removal.
The restore helper deliberately refuses to replace an existing ConfigMap.

If the live state is already missing (or after an explicitly planned deletion
in a disposable recovery drill):

```bash
uv run terraform/files/oci-quickcache/files/state_backup.py restore \
  --file /tmp/qc-state-snapshot.json \
  --namespace kube-system \
  --state-name oci-quickcache-state \
  --confirm-restore
kubectl -n kube-system scale deployment quickcache-oci-quickcache-controller --replicas=1
kubectl -n kube-system rollout status deployment/quickcache-oci-quickcache-controller --timeout=10m
uv run manifests/quickcache/check-state.py status
```

Use `--controller` if the deployment has a non-default name. Repeat hit/miss and
checksum tests before resuming training. A stable snapshot can be stale relative
to later placement and cleanup, so restored ownership does not guarantee a warm
cache. The helper does not migrate files or recover terminated-node NVMe data.

If all workers were replaced or the cluster was recreated, old owner UIDs are
invalid and the helper rejects restore. Reinstall QuickCache and let the
controller initialize state for new nodes; Object Storage repopulates the cache.
If the only available snapshots contain in-flight migrations, keep workloads
paused and review maps, generations, actual copies and cleanup status with the
maintainer instead of automatically replaying approvals or deletions.
