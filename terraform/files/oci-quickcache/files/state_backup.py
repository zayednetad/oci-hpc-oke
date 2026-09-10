# /// script
# requires-python = ">=3.12"
# dependencies = ["kubernetes==31.0.0", "boto3==1.35.99"]
# ///
"""Export full state to S3-compatible storage; validate explicit offline recovery.

Use a dedicated private backup bucket and Customer Secret Key credentials.
No cache objects or application credentials are included in the snapshot.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import uuid

from agent_common import core_api
from state_store import STATE_FIELDS, checked_data


def validate_data(data: dict) -> dict:
    if not isinstance(data, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in data.items()
    ):
        raise ValueError("snapshot data must be a string dictionary")
    for key in STATE_FIELDS.values():
        if key not in data or not isinstance(json.loads(data[key]), dict):
            raise ValueError(f"snapshot requires object field {key}")
    generation = int(data["generation"])
    if generation < 0:
        raise ValueError("generation cannot be negative")
    active = json.loads(data["shard_map.json"])
    count = len(active)
    if count < 16 or count > 16384 or count & (count - 1):
        raise ValueError("snapshot shard count must be a power of two from 16 to 16384")
    if not active or set(active) != {str(i) for i in range(len(active))}:
        raise ValueError("snapshot requires a non-empty contiguous shard map")
    peers = json.loads(data["peers.json"])
    for key in ("shard_map.json", "pending_shard_map.json", "previous_shard_map.json"):
        shard_map = json.loads(data[key])
        if shard_map and set(shard_map) != set(active):
            raise ValueError("snapshot shard counts do not match")
        if any(
            not isinstance(uid, str) or uid not in peers for uid in shard_map.values()
        ):
            raise ValueError("snapshot references an unknown peer")
    return checked_data(data)


def encode_snapshot(data: dict, namespace: str, name: str) -> bytes:
    validate_data(data)
    payload = {
        "format": 1,
        "namespace": namespace,
        "name": name,
        "createdAt": int(time.time()),
        "data": data,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return json.dumps(
        {"payload": payload, "sha256": hashlib.sha256(canonical).hexdigest()},
        sort_keys=True,
    ).encode()


def decode_snapshot(raw: bytes) -> dict:
    document = json.loads(raw)
    payload = document["payload"]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    if document.get("sha256") != hashlib.sha256(canonical).hexdigest():
        raise ValueError("snapshot checksum mismatch")
    if payload.get("format") != 1:
        raise ValueError("unsupported snapshot format")
    validate_data(payload["data"])
    return payload


def restore_body(payload: dict, namespace: str, name: str) -> dict:
    """Create-only recovery: never replay migration approvals or partial copies."""
    if (payload["namespace"], payload["name"]) != (namespace, name):
        raise ValueError("snapshot namespace/name does not match target")
    data = validate_data(payload["data"])
    if any(
        json.loads(data[key])
        for key in (
            "pending_shard_map.json",
            "previous_shard_map.json",
            "rebalance.json",
        )
    ):
        raise ValueError(
            "restore requires a stable snapshot; in-flight snapshots are forensic only"
        )
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {"app.kubernetes.io/name": "oci-quickcache"},
        },
        "data": data,
    }


def main() -> None:
    from kubernetes import config
    from pathlib import Path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("backup", "inspect", "restore"))
    parser.add_argument("--file", help="local snapshot for inspect/restore")
    parser.add_argument(
        "--namespace", default=os.getenv("POD_NAMESPACE", "kube-system")
    )
    parser.add_argument(
        "--state-name",
        default=os.getenv("STATE_CONFIGMAP_NAME", "oci-quickcache-state"),
    )
    parser.add_argument("--controller", default="quickcache-oci-quickcache-controller")
    parser.add_argument("--confirm-restore", action="store_true")
    args = parser.parse_args()
    if args.command in {"inspect", "restore"}:
        if not args.file:
            parser.error("--file is required")
        payload = decode_snapshot(Path(args.file).read_bytes())
        print(json.dumps({k: v for k, v in payload.items() if k != "data"}))
        if args.command == "inspect":
            print(
                json.dumps(
                    {
                        "generation": payload["data"]["generation"],
                        "rebalance": json.loads(payload["data"]["rebalance.json"]),
                    }
                )
            )
            return
        body = restore_body(payload, args.namespace, args.state_name)
        if not args.confirm_restore:
            parser.error(
                "restore requires --confirm-restore after stopping workloads and controller"
            )
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        config.load_incluster_config()
    else:
        config.load_kube_config()
    core = core_api()
    if args.command == "restore":
        from kubernetes import client

        apps = client.AppsV1Api(core.api_client)
        deployment = apps.read_namespaced_deployment(args.controller, args.namespace)
        if deployment.spec.replicas != 0 or (deployment.status.replicas or 0) != 0:
            raise ValueError(
                "scale controller to zero and wait for its pods to terminate first"
            )
        nodes = {str(n.metadata.uid) for n in core.list_node().items}
        owners = set(json.loads(body["data"]["shard_map.json"]).values())
        if not owners.issubset(nodes):
            raise ValueError(
                "snapshot owners no longer exist; use fresh initialization instead"
            )
        core.create_namespaced_config_map(args.namespace, body)
        print("event=state_restored mode=create_only")
        return

    import boto3
    from botocore.config import Config

    # A single GET is a consistent snapshot, including during a rebalance.
    cm = core.read_namespaced_config_map(args.state_name, args.namespace)
    raw = encode_snapshot(cm.data or {}, args.namespace, args.state_name)
    bucket = os.environ["S3_BUCKET"]
    prefix = os.environ.get("S3_PREFIX", "quickcache-state").strip("/")
    if not prefix:
        raise ValueError("S3_PREFIX must not be empty")
    stable = not any(
        json.loads(cm.data[key])
        for key in (
            "rebalance.json",
            "pending_shard_map.json",
            "previous_shard_map.json",
        )
    )
    key = (
        f"{prefix}/{args.namespace}/{args.state_name}/"
        f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-"
        f"g{cm.data['generation']}-{'stable' if stable else 'inflight'}-{uuid.uuid4().hex}.json"
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        config=Config(
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=30,
            retries={"max_attempts": 2},
            s3={"addressing_style": "path"},
        ),
    )
    s3.put_object(Bucket=bucket, Key=key, Body=raw, ContentType="application/json")
    print(f"event=state_backup_succeeded key={key} bytes={len(raw)} stable={stable}")


if __name__ == "__main__":
    main()
