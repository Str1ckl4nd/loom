from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from pathlib import Path


TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))

import loom_hub
import loom_manifest
from loom_cache import git_source_descriptor
from loom_http import request_json


class ResourceAdmissionTests(unittest.TestCase):
    token = "resource-test-token"

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.hub = loom_hub.ControllerServer(("127.0.0.1", 0), loom_hub.Handler)
        self.hub.benchmark_root = root
        self.hub.db_path = root / "hub.sqlite"
        self.hub.artifact_root = root / "artifacts"
        self.hub.artifact_root.mkdir(parents=True, exist_ok=True)
        self.hub.control_log_path = root / "hub.jsonl"
        self.hub.upload_lock = threading.Lock()
        self.hub.auth_token = self.token
        self.hub.runner_token_env = "TEST_RUNNER_TOKEN"
        with loom_hub.connect(self.hub.db_path) as conn:
            loom_hub.ensure_schema(conn)
            conn.commit()
        self.thread = threading.Thread(target=self.hub.serve_forever, daemon=True)
        self.thread.start()
        self.controller = f"http://127.0.0.1:{self.hub.server_address[1]}"
        request_json(
            self.controller + "/api/workers/register",
            {
                "worker_id": "reserved-worker",
                "runner_api_version": 1,
                "capabilities": ["linux"],
                "max_concurrency": 3,
                "initial_concurrency": 3,
                "concurrency_policy": "fixed",
                "resource_capacity": {"cpu_millis": 2000, "memory_mb": 1024, "disk_mb": 4096, "gpu_count": 0},
                "health": {"resources": {"cpu_count": 2, "mem_available_mb": 1024, "disk_available_mb": 4096}},
            },
            token=self.token,
        )

    def tearDown(self) -> None:
        self.hub.shutdown()
        self.hub.server_close()
        self.temp.cleanup()

    def dispatch(self, task_id: str, profile: dict[str, object]) -> None:
        request_json(
            self.controller + "/api/tasks/dispatch",
            {
                "schema_version": 1,
                "operator": "resource-test",
                "tasks": [
                    {
                        "task_id": task_id,
                        "required_capability": "linux",
                        "payload": {"runner": "noop", "execution_profile": profile},
                    }
                ],
            },
            token=self.token,
        )

    def claim(self) -> dict[str, object]:
        return request_json(
            self.controller + "/api/tasks/claim",
            {"worker_id": "reserved-worker", "limit": 3, "lease_seconds": 120},
            token=self.token,
        )

    def test_manifest_merges_execution_profile_per_case(self) -> None:
        payload = loom_manifest.normalize(
            {
                "schema_version": 1,
                "campaign_id": "resource-profile",
                "source": {"type": "local", "path": "/tmp/source"},
                "defaults": {
                    "runner": "repo",
                    "commands": ["true"],
                    "execution_profile": {
                        "placement": "shared",
                        "resources": {"cpu_millis": 500, "memory_mb": 256},
                    },
                },
                "cases": [
                    {
                        "case_id": "case-a",
                        "setting_id": "baseline",
                        "run_id": "001",
                        "execution_profile": {"resources": {"memory_mb": 768}},
                    }
                ],
            },
            operator="test",
        )
        profile = payload["tasks"][0]["payload"]["execution_profile"]
        self.assertEqual(profile["placement"], "shared")
        self.assertEqual(profile["resources"]["cpu_millis"], 500)
        self.assertEqual(profile["resources"]["memory_mb"], 768)

    def test_claim_reserves_resources_and_explains_deferred_work(self) -> None:
        shared = {"placement": "shared", "resources": {"cpu_millis": 500, "memory_mb": 600}}
        exclusive = {"placement": "exclusive", "resources": {"cpu_millis": 100, "memory_mb": 64}}
        self.dispatch("resource-a", shared)
        self.dispatch("resource-b", shared)
        self.dispatch("resource-exclusive", exclusive)

        first = self.claim()
        self.assertEqual([task["task_id"] for task in first["tasks"]], ["resource-a"])

        capacity = request_json(self.controller + "/api/data/worker-capacity", token=self.token)
        worker = capacity["workers"][0]
        self.assertEqual(worker["reserved"]["memory_mb"], 600)
        self.assertEqual(worker["available"]["memory_mb"], 424)

        admission = request_json(self.controller + "/api/data/task-admission?task_id=resource-b", token=self.token)
        self.assertEqual(admission["eligible_worker_count"], 0)
        self.assertEqual(admission["workers"][0]["reason"], "resource_reservation_unavailable")

        request_json(
            self.controller + "/api/admin/cancel-task",
            {"task_id": "resource-a", "operator": "resource-test"},
            token=self.token,
        )
        second = self.claim()
        self.assertEqual([task["task_id"] for task in second["tasks"]], ["resource-b"])
        request_json(
            self.controller + "/api/admin/cancel-task",
            {"task_id": "resource-b", "operator": "resource-test"},
            token=self.token,
        )
        third = self.claim()
        self.assertEqual([task["task_id"] for task in third["tasks"]], ["resource-exclusive"])

        self.dispatch("resource-after-exclusive", {"placement": "shared", "resources": {"cpu_millis": 100}})
        blocked = request_json(self.controller + "/api/data/task-admission?task_id=resource-after-exclusive", token=self.token)
        self.assertEqual(blocked["workers"][0]["reason"], "worker_has_exclusive_task")

    def test_task_admission_counts_leases_before_a_runner_heartbeat(self) -> None:
        request_json(
            self.controller + "/api/admin/set-worker-concurrency",
            {
                "worker_id": "reserved-worker",
                "desired_concurrency": 1,
                "operator": "resource-test",
                "reason": "exercise hard cap",
            },
            token=self.token,
        )
        shared = {"placement": "shared", "resources": {"cpu_millis": 100, "memory_mb": 64}}
        self.dispatch("concurrency-a", shared)
        self.dispatch("concurrency-b", shared)

        first = self.claim()
        self.assertEqual([task["task_id"] for task in first["tasks"]], ["concurrency-a"])
        second = self.claim()
        self.assertEqual(second["tasks"], [])

        admission = request_json(self.controller + "/api/data/task-admission?task_id=concurrency-b", token=self.token)
        worker = admission["workers"][0]
        self.assertFalse(worker["ok"])
        self.assertEqual(worker["reason"], "worker_concurrency_reached")
        self.assertEqual(worker["concurrency"]["leased_active"], 1)
        self.assertEqual(worker["concurrency"]["active_count"], 1)

    def test_pull_claim_prefers_a_cache_hit_without_overriding_priority(self) -> None:
        cached_source = {
            "type": "git",
            "url": "https://example.test/acme/cached.git",
            "commit": "a" * 40,
        }
        missed_source = {
            "type": "git",
            "url": "https://example.test/acme/missed.git",
            "commit": "b" * 40,
        }
        cached_descriptor = git_source_descriptor(cached_source)
        self.assertIsNotNone(cached_descriptor)
        request_json(
            self.controller + "/api/workers/heartbeat",
            {
                "worker_id": "reserved-worker",
                "current_runs": [],
                "health": {
                    "source_cache": {
                        "enabled": True,
                        "entry_count": 1,
                        "bytes": 123,
                        "max_bytes": 1024,
                        "keys": [cached_descriptor["cache_key"]],
                    }
                },
            },
            token=self.token,
        )
        profile = {"placement": "shared", "resources": {"cpu_millis": 100, "memory_mb": 64}}
        request_json(
            self.controller + "/api/tasks/dispatch",
            {
                "schema_version": 1,
                "operator": "resource-test",
                "tasks": [
                    {
                        "task_id": "high-priority-cache-miss",
                        "required_capability": "linux",
                        "priority": 10,
                        "payload": {"runner": "repo", "source": missed_source, "execution_profile": profile},
                    },
                    {
                        "task_id": "cache-miss-created-first",
                        "required_capability": "linux",
                        "payload": {"runner": "repo", "source": missed_source, "execution_profile": profile},
                    },
                    {
                        "task_id": "cache-hit-created-second",
                        "required_capability": "linux",
                        "payload": {"runner": "repo", "source": cached_source, "execution_profile": profile},
                    },
                ],
            },
            token=self.token,
        )

        high_priority_claim = request_json(
            self.controller + "/api/tasks/claim",
            {"worker_id": "reserved-worker", "limit": 1, "lease_seconds": 120},
            token=self.token,
        )
        self.assertEqual([task["task_id"] for task in high_priority_claim["tasks"]], ["high-priority-cache-miss"])
        claim = request_json(
            self.controller + "/api/tasks/claim",
            {"worker_id": "reserved-worker", "limit": 1, "lease_seconds": 120},
            token=self.token,
        )

        self.assertEqual([task["task_id"] for task in claim["tasks"]], ["cache-hit-created-second"])
        affinity = claim["tasks"][0]["controller_concurrency"]["cache_affinity"]
        self.assertTrue(affinity["cache_hit"])
        admission = request_json(
            self.controller + "/api/data/task-admission?task_id=cache-hit-created-second",
            token=self.token,
        )
        self.assertTrue(admission["workers"][0]["cache_affinity"]["cache_hit"])


if __name__ == "__main__":
    unittest.main()
