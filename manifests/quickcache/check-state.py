# /// script
# requires-python = ">=3.12"
# dependencies = ["kubernetes==31.0.0"]
# ///
"""Read-only QuickCache diagnostics and offline state-capacity measurements."""

import argparse
import json
import statistics
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "terraform/files/oci-quickcache/files")
)
from state_store import (
    STATE_MAX_BYTES,
    STATE_WARN_BYTES,
    data_size,
    empty_state,
    state_data,
)
from sharding import rebalance_shards
from rebalance import new_plan


def capacity_state(shards, initial, nodes):
    uids = [str(uuid.UUID(int=i + 1)) for i in range(nodes)]
    state = empty_state()
    state["peers"] = {
        uid: {
            "nodeName": f"10.140.{i // 250}.{i % 250 + 1}",
            "internalIP": f"10.140.{i // 250}.{i % 250 + 1}",
            "mountPath": f"/var/lib/ociqc/mounts/{uid}",
        }
        for i, uid in enumerate(uids)
    }
    state["active"] = rebalance_shards({}, uids[:initial], shards)
    state["pending"] = rebalance_shards(state["active"], uids, shards)
    state["generation"] = 1
    state["rebalance"] = new_plan(1, "manual", state["active"], state["pending"], 1)
    # Include estimate overhead for each source node, as in manual approval.
    state["rebalance"]["estimate"] = {
        "status": "complete",
        "sourceNodes": initial,
        "shards": shards,
        "files": 500000000,
        "bytes": 1000000000000000,
        "perSource": {
            uid: {
                "shards": shards // initial,
                "files": 500000000,
                "bytes": 1000000000000000,
            }
            for uid in uids[:initial]
        },
    }
    return state


def capacity_report():
    rows = []
    for shards in (1024, 2048, 4096):
        for initial, nodes in ((1, 256), (2, 256), (128, 256)):
            state = capacity_state(shards, initial, nodes)
            data = state_data(state, now=1)
            pretty = {
                k: json.dumps(json.loads(v), indent=2, sort_keys=True)
                if k.endswith(".json")
                else v
                for k, v in data.items()
            }
            rows.append(
                {
                    "shards": shards,
                    "initialNodes": initial,
                    "targetNodes": nodes,
                    "moves": len(state["rebalance"]["moves"]),
                    "oldPrettyBytes": data_size(pretty),
                    "compactBytes": data_size(data),
                    "limitBytes": STATE_MAX_BYTES,
                    "headroomBytes": STATE_MAX_BYTES - data_size(data),
                }
            )
    return rows


def inspect(
    core,
    namespace,
    name,
    label,
    heartbeat_annotation,
    heartbeat_timeout,
    stall_seconds,
    backup_name,
):
    cm = core.read_namespaced_config_map(name, namespace)
    data = cm.data or {}
    alerts = []
    size = data_size(data)
    if size >= STATE_WARN_BYTES:
        alerts.append("state_size_warning")
    peers = json.loads(data.get("peers.json", "{}"))
    active = json.loads(data.get("shard_map.json", "{}"))
    plan = json.loads(data.get("rebalance.json", "{}"))
    now = int(time.time())
    stale = []
    for node in core.list_node(label_selector=f"{label}=true").items:
        raw = (node.metadata.annotations or {}).get(heartbeat_annotation, "0")
        try:
            age = abs(now - int(raw))
        except (ValueError, TypeError):
            age = heartbeat_timeout + 1
        if age > heartbeat_timeout:
            stale.append(node.metadata.name)
    if stale:
        alerts.append("stale_heartbeats")
    if not active:
        alerts.append("no_active_map")
    elif not set(active.values()).issubset(peers):
        alerts.append("active_owner_missing_from_peers")
    phase_age = 0
    if plan:
        changed = max(
            int(plan.get(k, 0))
            for k in ("createdAt", "approvedAt", "activatedAt", "cleanupStartedAt")
        )
        phase_age = now - changed
        if phase_age > stall_seconds:
            alerts.append("long_running_rebalance_check_progress")
    result = {
        "event": "quickcache_state_check",
        "stateBytes": size,
        "stateLimitBytes": STATE_MAX_BYTES,
        "peers": len(peers),
        "shards": len(active),
        "generation": data.get("generation"),
        "phase": plan.get("phase", "stable"),
        "phaseAgeSeconds": phase_age,
        "staleNodes": stale,
        "alerts": alerts,
    }
    pods = core.list_namespaced_pod(
        namespace, label_selector="app.kubernetes.io/name=oci-quickcache"
    ).items
    unready = []
    controller_seen = False
    for pod in pods:
        component = (pod.metadata.labels or {}).get("app.kubernetes.io/component")
        if component not in {"controller", "node-agent", "client-agent"}:
            continue
        controller_seen |= component == "controller"
        if not any(
            c.type == "Ready" and c.status == "True"
            for c in (pod.status.conditions or [])
        ):
            unready.append(pod.metadata.name)
    result["unreadyPods"] = unready
    if unready:
        alerts.append("quickcache_pods_not_ready")
    if not controller_seen:
        alerts.append("controller_pod_missing")
    if backup_name:
        from kubernetes import client

        cron = client.BatchV1Api(core.api_client).read_namespaced_cron_job(
            backup_name, namespace
        )
        successful = cron.status.last_successful_time
        age = now - int(successful.timestamp()) if successful else None
        result["lastBackupAgeSeconds"] = age
        if age is None or age > 900:
            alerts.append("external_backup_missing_or_older_than_15m")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("capacity", "status", "probe"))
    parser.add_argument("--namespace", default="kube-system")
    parser.add_argument("--state-name", default="oci-quickcache-state")
    parser.add_argument("--label", default="oci-hpc-oke.oracle.com/quickcache")
    parser.add_argument(
        "--heartbeat-annotation", default="oci-hpc-oke.oracle.com/quickcache-heartbeat"
    )
    parser.add_argument("--heartbeat-timeout", type=int, default=120)
    parser.add_argument("--stall-seconds", type=int, default=86400)
    parser.add_argument(
        "--backup-name", help="check this backup CronJob's last success"
    )
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--interval", type=float, default=2)
    args = parser.parse_args()
    if args.command == "capacity":
        print(json.dumps(capacity_report(), indent=2))
        return
    from kubernetes import config
    from agent_common import core_api

    config.load_kube_config()
    core = core_api()
    if args.command == "status":
        result = inspect(
            core,
            args.namespace,
            args.state_name,
            args.label,
            args.heartbeat_annotation,
            args.heartbeat_timeout,
            args.stall_seconds,
            args.backup_name,
        )
        print(json.dumps(result, indent=2))
        sys.exit(1 if result["alerts"] else 0)
    if not 1 <= args.samples <= 300 or args.interval < 0.5:
        parser.error("probe requires 1..300 samples and interval >= 0.5 seconds")
    durations, errors = [], []
    for i in range(args.samples):
        started = time.monotonic()
        try:
            core.read_namespaced_config_map(args.state_name, args.namespace)
            durations.append(time.monotonic() - started)
        except Exception as exc:
            errors.append(type(exc).__name__)
        if i + 1 < args.samples:
            time.sleep(args.interval)
    ordered = sorted(durations)
    print(
        json.dumps(
            {
                "event": "api_read_probe",
                "samples": args.samples,
                "successes": len(durations),
                "errors": errors,
                "medianSeconds": statistics.median(durations) if durations else None,
                "p95Seconds": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
                if ordered
                else None,
                "note": "read-only client probe; not an API write-load benchmark",
            },
            indent=2,
        )
    )
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
