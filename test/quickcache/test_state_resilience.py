"""Failure-path and serialized-capacity regression tests; no cluster required."""

import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from test_controller import controller, FakeApiException
from test_node_agent import node_agent
import state_store
import state_backup
import agent_common

spec = importlib.util.spec_from_file_location(
    "check_state",
    Path(__file__).resolve().parents[2] / "manifests/quickcache/check-state.py",
)
check_state = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_state)


def stable_state():
    state = state_store.empty_state()
    state.update(
        peers={"a": {"nodeName": "node-a"}}, active={str(i): "a" for i in range(16)}
    )
    return state


class StateResilienceTests(unittest.TestCase):
    def test_api_client_replaces_sdk_default_none_timeout(self):
        calls = []

        class ApiClient:
            def __init__(self, configuration):
                self.configuration = configuration

            def call_api(self, *args, **kwargs):
                calls.append(kwargs)

        configuration = SimpleNamespace(retries=None)
        sdk = types.ModuleType("kubernetes")
        sdk.client = SimpleNamespace(
            ApiClient=ApiClient,
            Configuration=SimpleNamespace(get_default_copy=lambda: configuration),
            CoreV1Api=lambda api: SimpleNamespace(api_client=api),
        )
        with mock.patch.dict(sys.modules, {"kubernetes": sdk}):
            core = agent_common.core_api()
            core.api_client.call_api("/api", "GET", _request_timeout=None)
            core.api_client.call_api("/api", "GET", _request_timeout=(1, 2))
        self.assertEqual(calls[0]["_request_timeout"], (5, 15))
        self.assertEqual(calls[1]["_request_timeout"], (1, 2))
        self.assertEqual(configuration.retries, 0)

    def test_capacity_matrix_keeps_large_scaleouts_below_budget(self):
        rows = check_state.capacity_report()
        self.assertEqual(len(rows), 9)
        for row in rows:
            self.assertLess(row["compactBytes"], state_store.STATE_MAX_BYTES)
        large = next(r for r in rows if r["shards"] == 4096 and r["initialNodes"] == 1)
        self.assertGreater(large["oldPrettyBytes"], 1024 * 1024)

    def test_compaction_preserves_all_fields_including_unicode(self):
        state = stable_state()
        state["peers"]["a"]["displayName"] = "東京"
        data = state_store.state_data(state, 123)
        for field, key in state_store.STATE_FIELDS.items():
            self.assertEqual(json.loads(data[key]), state[field])
        self.assertEqual(data["updated_at"], "123")
        self.assertEqual(state_store.data_size({"é": "東京"}), 8)

    def test_oversize_update_never_calls_kubernetes(self):
        state = stable_state()
        state["rebalance"] = {"oversize": "x" * state_store.STATE_MAX_BYTES}
        core = mock.Mock()
        with (
            mock.patch.object(
                state_store.client, "V1ConfigMap", SimpleNamespace, create=True
            ),
            mock.patch.object(
                state_store.client, "V1ObjectMeta", SimpleNamespace, create=True
            ),
        ):
            with self.assertRaisesRegex(ValueError, "safety limit"):
                state_store.write_state(core, "ns", "state", state, "42", {})
        core.replace_namespaced_config_map.assert_not_called()
        core.create_namespaced_config_map.assert_not_called()

    def test_size_warning_precedes_rejection(self):
        with self.assertLogs(state_store.LOG, "WARNING") as records:
            state_store.checked_data({"k": "x" * state_store.STATE_WARN_BYTES})
        self.assertIn("state_size_warning", records.output[0])

    def test_api_conflict_does_not_retry_with_stale_resource_version(self):
        core = mock.Mock()
        core.replace_namespaced_config_map.side_effect = FakeApiException(409)
        with (
            mock.patch.object(
                state_store.client, "V1ConfigMap", SimpleNamespace, create=True
            ),
            mock.patch.object(
                state_store.client, "V1ObjectMeta", SimpleNamespace, create=True
            ),
        ):
            with self.assertRaises(FakeApiException):
                state_store.write_state(core, "ns", "state", stable_state(), "42", {})
        self.assertEqual(core.replace_namespaced_config_map.call_count, 1)
        self.assertEqual(
            core.replace_namespaced_config_map.call_args.args[
                2
            ].metadata.resource_version,
            "42",
        )

    def test_api_outage_is_not_treated_as_empty_state(self):
        core = mock.Mock()
        for status in (403, 429, 500, 503):
            core.read_namespaced_config_map.side_effect = FakeApiException(status)
            with self.assertRaises(FakeApiException):
                state_store.load_state(core, "ns", "state")
        core.read_namespaced_config_map.side_effect = FakeApiException(404)
        state, version, _ = state_store.load_state(core, "ns", "state")
        self.assertEqual(state, state_store.empty_state())
        self.assertIsNone(version)

    def test_existing_state_missing_active_map_does_not_reinitialize(self):
        core = mock.Mock()
        core.read_namespaced_config_map.return_value = SimpleNamespace(
            data={"peers.json": "{}"}
        )
        with self.assertRaisesRegex(ValueError, "missing"):
            state_store.load_state(core, "ns", "state")

    def test_status_exposes_stale_nodes_and_missing_controller(self):
        core = mock.Mock()
        core.read_namespaced_config_map.return_value = SimpleNamespace(
            data=state_store.state_data(stable_state())
        )
        core.list_node.return_value = SimpleNamespace(
            items=[
                SimpleNamespace(
                    metadata=SimpleNamespace(
                        name="node-a", annotations={"heartbeat": "0"}
                    )
                )
            ]
        )
        core.list_namespaced_pod.return_value = SimpleNamespace(items=[])
        result = check_state.inspect(
            core, "ns", "state", "enabled", "heartbeat", 120, 86400, None
        )
        self.assertIn("stale_heartbeats", result["alerts"])
        self.assertIn("controller_pod_missing", result["alerts"])
        self.assertEqual(result["staleNodes"], ["node-a"])

    def test_backup_roundtrip_and_safe_create_only_restore(self):
        data = state_store.state_data(stable_state())
        payload = state_backup.decode_snapshot(
            state_backup.encode_snapshot(data, "ns", "state")
        )
        body = state_backup.restore_body(payload, "ns", "state")
        self.assertEqual(body["data"], data)
        self.assertNotIn("annotations", body["metadata"])
        self.assertNotIn("resourceVersion", body["metadata"])

    def test_backup_checksum_detects_modified_data(self):
        document = json.loads(
            state_backup.encode_snapshot(
                state_store.state_data(stable_state()), "ns", "state"
            )
        )
        document["payload"]["data"]["generation"] = "9"
        with self.assertRaisesRegex(ValueError, "checksum"):
            state_backup.decode_snapshot(json.dumps(document).encode())

    def test_restore_rejects_inflight_state_and_wrong_target(self):
        state = stable_state()
        state["pending"] = dict(state["active"])
        payload = state_backup.decode_snapshot(
            state_backup.encode_snapshot(state_store.state_data(state), "ns", "state")
        )
        with self.assertRaisesRegex(ValueError, "stable snapshot"):
            state_backup.restore_body(payload, "ns", "state")
        with self.assertRaisesRegex(ValueError, "does not match"):
            state_backup.restore_body(payload, "different", "state")

    def test_backup_rejects_unknown_owner_or_missing_shard(self):
        for change in (
            lambda s: s["active"].update({"0": "missing"}),
            lambda s: s["active"].pop("0"),
        ):
            state = stable_state()
            change(state)
            with self.assertRaises(ValueError):
                state_backup.encode_snapshot(
                    state_store.state_data(state), "ns", "state"
                )

    def test_controller_waits_for_heartbeats_on_startup_and_after_failure(self):
        clock = [0.0]
        next_times = iter([121.0, 122.0, 243.0])

        def sleep(_seconds):
            try:
                clock[0] = next(next_times)
            except StopIteration:
                raise KeyboardInterrupt()

        core = mock.Mock()
        with (
            mock.patch.object(controller.config, "load_incluster_config", create=True),
            mock.patch.object(controller, "core_api", return_value=core),
            mock.patch.object(controller, "HEALTH_FILE"),
            mock.patch.object(controller, "READY_FILE"),
            mock.patch.object(
                controller.time, "monotonic", side_effect=lambda: clock[0]
            ),
            mock.patch.object(controller.time, "sleep", side_effect=sleep),
            mock.patch.dict(controller.os.environ, {"HEARTBEAT_TIMEOUT": "120"}),
            mock.patch.object(
                controller, "reconcile", side_effect=[ConnectionError("outage"), None]
            ) as reconcile,
        ):
            with self.assertRaises(KeyboardInterrupt):
                controller.main()
        self.assertEqual(reconcile.call_count, 2)
        self.assertEqual(core.list_node.call_count, 2)

    def test_node_keeps_published_maps_and_mounts_when_api_read_fails(self):
        with (
            mock.patch.object(node_agent, "_copy_runtime"),
            mock.patch.object(node_agent, "_run_host_setup"),
            mock.patch.object(node_agent, "_patch_status"),
            mock.patch.object(
                node_agent, "_read_state", side_effect=ConnectionError("outage")
            ),
            mock.patch.object(node_agent, "_write_json_atomic") as write,
            mock.patch.object(node_agent, "_remove_stale_mounts") as unmount,
            mock.patch.dict(
                node_agent.os.environ, {"HOST_RUNTIME_ROOT": "/var/lib/ociqc/runtime"}
            ),
        ):
            with self.assertRaises(ConnectionError):
                node_agent.reconcile(mock.Mock(), "a")
        write.assert_not_called()
        unmount.assert_not_called()

    def test_node_initial_lookup_retries_without_exiting(self):
        core = mock.Mock()
        core.read_node.side_effect = [
            ConnectionError("outage"),
            SimpleNamespace(
                metadata=SimpleNamespace(uid="00000000-0000-0000-0000-000000000001")
            ),
        ]
        stop = mock.Mock()
        stop.is_set.side_effect = [False, False, True]
        with (
            mock.patch.object(node_agent, "_validate_configuration"),
            mock.patch.object(node_agent.config, "load_incluster_config", create=True),
            mock.patch.object(agent_common, "core_api", return_value=core),
            mock.patch.object(node_agent, "STOP_EVENT", stop),
            mock.patch.object(node_agent.signal, "signal"),
            mock.patch.object(node_agent, "HEALTH_FILE"),
            mock.patch.object(node_agent, "READY_FILE"),
            mock.patch.object(node_agent, "_patch_status"),
            mock.patch.object(node_agent, "_run_host_teardown"),
            mock.patch.object(node_agent, "reconcile") as reconcile,
            mock.patch.dict(node_agent.os.environ, {"NODE_NAME": "node-a"}),
        ):
            node_agent.main()
        self.assertEqual(core.read_node.call_count, 2)
        reconcile.assert_called_once_with(core, "00000000-0000-0000-0000-000000000001")


if __name__ == "__main__":
    unittest.main()
