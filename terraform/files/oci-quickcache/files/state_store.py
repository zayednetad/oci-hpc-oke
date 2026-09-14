"""Kubernetes persistence for QuickCache live state and map backups."""

from __future__ import annotations

import hashlib
import json
import logging
import time

from kubernetes import client
from kubernetes.client.rest import ApiException


LOG = logging.getLogger("quickcache-state-store")
STATE_MAX_BYTES = 900 * 1024
STATE_WARN_BYTES = 750 * 1024
STATE_FIELDS = {
    "peers": "peers.json",
    "active": "shard_map.json",
    "pending": "pending_shard_map.json",
    "previous": "previous_shard_map.json",
    "rebalance": "rebalance.json",
}


def data_size(data: dict[str, str]) -> int:
    """Conservative UTF-8 budget: include keys as well as ConfigMap values."""
    return sum(
        len(key.encode("utf-8")) + len(value.encode("utf-8"))
        for key, value in data.items()
    )


def checked_data(data: dict[str, str]) -> dict[str, str]:
    size = data_size(data)
    if size > STATE_MAX_BYTES:
        LOG.error("event=state_size_rejected bytes=%d limit=%d", size, STATE_MAX_BYTES)
        raise ValueError(
            f"QuickCache state uses {size} bytes; safety limit is {STATE_MAX_BYTES}"
        )
    if size >= STATE_WARN_BYTES:
        LOG.warning("event=state_size_warning bytes=%d limit=%d", size, STATE_MAX_BYTES)
    return data


def state_data(state: dict, now: int | None = None) -> dict[str, str]:
    """Keep one atomic snapshot and existing JSON keys, without whitespace waste."""
    data = {
        key: json.dumps(state[field], sort_keys=True, separators=(",", ":"))
        for field, key in STATE_FIELDS.items()
    }
    data.update(
        generation=str(state["generation"]),
        updated_at=str(int(time.time()) if now is None else now),
    )
    return checked_data(data)


def empty_state() -> dict:
    return {
        "peers": {},
        "active": {},
        "pending": {},
        "previous": {},
        "rebalance": {},
        "generation": 0,
    }


def _json_object(data: dict, key: str) -> dict:
    try:
        value = json.loads(data.get(key, "{}"))
    except (TypeError, json.JSONDecodeError):
        raise ValueError(
            f"QuickCache state field {key} is invalid; restore valid state"
        )
    if not isinstance(value, dict):
        raise ValueError(f"QuickCache state field {key} must be an object")
    return value


def load_state(
    core: client.CoreV1Api,
    namespace: str,
    name: str,
) -> tuple[dict, str | None, dict[str, str]]:
    """Load and defensively decode the live QuickCache ConfigMap."""
    try:
        configmap = core.read_namespaced_config_map(name, namespace)
    except ApiException as exc:
        if exc.status == 404:
            return empty_state(), None, {}
        raise
    data = configmap.data or {}
    if "peers.json" not in data or "shard_map.json" not in data:
        raise ValueError(
            "QuickCache state is missing peers.json or shard_map.json; restore valid state"
        )
    try:
        generation = int(data.get("generation", "0"))
        if generation < 0:
            raise ValueError("negative generation")
    except (TypeError, ValueError):
        raise ValueError("QuickCache generation is invalid; restore valid state")
    state = {
        "peers": _json_object(data, "peers.json"),
        "active": _json_object(data, "shard_map.json"),
        "pending": _json_object(data, "pending_shard_map.json"),
        "previous": _json_object(data, "previous_shard_map.json"),
        "rebalance": _json_object(data, "rebalance.json"),
        "generation": generation,
    }
    return (
        state,
        configmap.metadata.resource_version,
        dict(getattr(configmap.metadata, "annotations", None) or {}),
    )


def write_state(
    core: client.CoreV1Api,
    namespace: str,
    name: str,
    state: dict,
    resource_version: str | None,
    annotations: dict[str, str],
) -> None:
    """Create or replace the complete live state with optimistic concurrency."""
    body = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels={"app.kubernetes.io/name": "oci-quickcache"},
            annotations=annotations,
            resource_version=resource_version,
        ),
        data=state_data(state),
    )
    if resource_version:
        core.replace_namespaced_config_map(name, namespace, body)
    else:
        core.create_namespaced_config_map(namespace, body)
    LOG.info(
        "event=state_published bytes=%d generation=%s",
        data_size(body.data),
        state["generation"],
    )


def map_digest(shard_map: dict) -> str:
    serialized = json.dumps(shard_map, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def backup_shard_map(
    core: client.CoreV1Api,
    namespace: str,
    state_name: str,
    old_map: dict,
    new_map: dict,
    generation: int,
    retention: int,
    now: int,
) -> None:
    """Persist the complete pre-cutover map in a bounded ConfigMap history."""
    if not old_map or old_map == new_map:
        return
    old_digest = map_digest(old_map)
    new_digest = map_digest(new_map)
    name = (
        f"{state_name[:160]}-map-g{generation:08d}-{old_digest[:12]}-{new_digest[:12]}"
    )
    owner_id = hashlib.sha256(state_name.encode("utf-8")).hexdigest()[:16]
    timestamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime(now))
    body = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels={
                "app.kubernetes.io/name": "oci-quickcache",
                "app.kubernetes.io/component": "shard-map-backup",
                "oci-hpc-oke.oracle.com/state-backup-owner": owner_id,
            },
            annotations={
                "oci-hpc-oke.oracle.com/quickcache-backup-created-at": str(now),
                "oci-hpc-oke.oracle.com/quickcache-old-map-sha256": old_digest,
                "oci-hpc-oke.oracle.com/quickcache-new-map-sha256": new_digest,
            },
        ),
        data=checked_data(
            {
                f"shard_map.{timestamp}.bak": json.dumps(
                    old_map, separators=(",", ":"), sort_keys=True
                ),
                "generation": str(generation),
                "created_at": str(now),
            }
        ),
    )
    try:
        core.create_namespaced_config_map(namespace, body)
        LOG.info("created full shard-map backup %s", name)
    except ApiException as exc:
        if exc.status != 409:
            raise
        existing = core.read_namespaced_config_map(name, namespace)
        backup_values = [
            value
            for key, value in (existing.data or {}).items()
            if key.endswith(".bak")
        ]
        try:
            existing_map = json.loads(backup_values[0])
        except (IndexError, TypeError, json.JSONDecodeError) as invalid:
            raise RuntimeError(
                f"existing shard-map backup {name} is invalid"
            ) from invalid
        if (
            len(backup_values) != 1
            or not isinstance(existing_map, dict)
            or map_digest(existing_map) != old_digest
        ):
            raise RuntimeError(
                f"existing shard-map backup {name} does not match cutover"
            )

    # Cutover requires the backup. Retention is best effort so a failed delete
    # cannot leave a healthy cache permanently unbalanced.
    try:
        backups = core.list_namespaced_config_map(
            namespace,
            label_selector=(
                "app.kubernetes.io/name=oci-quickcache,"
                "app.kubernetes.io/component=shard-map-backup,"
                f"oci-hpc-oke.oracle.com/state-backup-owner={owner_id}"
            ),
        ).items
        backups.sort(
            key=lambda item: (
                int(
                    (item.metadata.annotations or {}).get(
                        "oci-hpc-oke.oracle.com/quickcache-backup-created-at", "0"
                    )
                ),
                item.metadata.name,
            )
        )
        for backup in backups[:-retention]:
            core.delete_namespaced_config_map(backup.metadata.name, namespace)
    except (ApiException, TypeError, ValueError):
        LOG.warning("could not enforce shard-map backup retention", exc_info=True)
