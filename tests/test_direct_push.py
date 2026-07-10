from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import zipfile
from pathlib import Path
from urllib.error import HTTPError


TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))

import loom_hub
import loom_runner
from loom_http import request_json


class DirectPushTests(unittest.TestCase):
    hub_token = "hub-test-token"
    runner_token = "runner-test-token"

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
        self.hub.auth_token = self.hub_token
        self.hub.runner_token_env = "TEST_RUNNER_TOKEN"
        with loom_hub.connect(self.hub.db_path) as conn:
            loom_hub.ensure_schema(conn)
            conn.commit()
        self.hub_thread = threading.Thread(target=self.hub.serve_forever, daemon=True)
        self.hub_thread.start()
        self.controller = f"http://127.0.0.1:{self.hub.server_address[1]}"

        self.runner_args = argparse.Namespace(
            controller=self.controller,
            controller_token=self.hub_token,
            worker_id="direct-test-worker",
            capability=["linux", "*"],
            work_dir=root / "worker",
            poll_seconds=1,
            lease_seconds=120,
            max_concurrency=1,
            initial_concurrency=1,
            max_tasks=0,
            once=False,
            fail_fast=True,
            connection_mode="direct-api",
            concurrency_policy="fixed",
            resource_capacity={"cpu_millis": 1000, "memory_mb": 1024, "disk_mb": 4096, "gpu_count": 0},
            claim_wait_seconds=0,
            serve_host="127.0.0.1",
            serve_port=0,
            direct_api_run_on_start=False,
            direct_api_token=self.runner_token,
        )
        self.runner_args.work_dir.mkdir(parents=True, exist_ok=True)
        loom_runner.register(self.runner_args)
        self.runner = loom_runner.DirectWorkerServer(("127.0.0.1", 0), loom_runner.DirectWorkerHandler)
        self.runner.worker_args = self.runner_args
        self.runner.work_root = self.runner_args.work_dir
        self.runner.state_lock = threading.Lock()
        self.runner.run_thread = None
        self.runner.last_result = {}
        self.runner.direct_api_token = self.runner_token
        self.runner.push_executor = loom_runner.ThreadPoolExecutor(max_workers=1)
        self.runner.push_futures = {}
        self.runner_thread = threading.Thread(target=self.runner.serve_forever, daemon=True)
        self.runner_thread.start()
        self.runner_url = f"http://127.0.0.1:{self.runner.server_address[1]}"
        os.environ["TEST_RUNNER_TOKEN"] = self.runner_token
        request_json(
            self.controller + "/api/admin/register-worker-hosts",
            {
                "operator": "test",
                "inventory": {
                    "inventory_version": 1,
                    "workers": [
                        {
                            "worker_id": self.runner_args.worker_id,
                            "connection_mode": "direct-worker-api",
                            "worker_url": self.runner_url,
                            "direct_api_token_env": "TEST_RUNNER_TOKEN",
                            "capabilities": ["linux", "*"],
                            "max_concurrency": 1,
                            "initial_concurrency": 1,
                            "concurrency_policy": "fixed",
                        }
                    ]
                },
            },
            token=self.hub_token,
        )

    def tearDown(self) -> None:
        self.runner.shutdown()
        self.runner.server_close()
        self.runner.push_executor.shutdown(wait=True)
        self.hub.shutdown()
        self.hub.server_close()
        self.temp.cleanup()
        os.environ.pop("TEST_RUNNER_TOKEN", None)

    def test_authenticated_hub_leases_and_pushes_an_exact_task(self) -> None:
        with self.assertRaises(HTTPError):
            request_json(self.controller + "/api/healthz")
        with self.assertRaises(HTTPError):
            request_json(self.runner_url + "/api/healthz")

        hub_meta = request_json(self.controller + "/api/meta", token=self.hub_token)
        runner_meta = request_json(self.runner_url + "/api/meta", token=self.runner_token)
        self.assertIn("hub-api-v1", hub_meta["capabilities"])
        self.assertIn("runner-api-v1", runner_meta["capabilities"])
        self.assertIn("task-extensions-v1", hub_meta["capabilities"])
        self.assertIn("task-extensions-v1", runner_meta["capabilities"])
        self.assertEqual(runner_meta["concurrency_policy"], "fixed")

        with self.assertRaises(HTTPError) as missing_schema:
            request_json(
                self.controller + "/api/tasks/dispatch",
                {"operator": "test", "tasks": []},
                token=self.hub_token,
            )
        self.assertEqual(missing_schema.exception.code, 400)

        task_id = "direct-push__case-a__setting-a__run-001"
        request_json(
            self.controller + "/api/tasks/dispatch",
            {
                "schema_version": 1,
                "operator": "test",
                "extensions": {
                    "org.example.dispatch": {"source": "dispatch"},
                    "org.example.shared": {"source": "dispatch"},
                },
                "payload": {
                    "extensions": {
                        "org.example.dispatch-payload": {"source": "dispatch-payload"},
                        "org.example.shared": {"source": "dispatch-payload"},
                    }
                },
                "tasks": [
                    {
                        "task_id": task_id,
                        "case_id": "case-a",
                        "setting_id": "setting-a",
                        "run_id": "001",
                        "required_capability": "linux",
                        "extensions": {
                            "org.example.task": {"source": "task"},
                            "org.example.shared": {"source": "task"},
                        },
                        "payload": {
                            "runner": "shell",
                            "command": "python3 -c \"print('direct push ok')\"",
                            "timeout_seconds": 60,
                            "extensions": {
                                "org.example.task-payload": {"source": "task-payload"},
                                "org.example.shared": {"source": "task-payload"},
                            },
                        },
                    }
                ],
            },
            token=self.hub_token,
        )
        pushed = request_json(
            self.controller + "/api/admin/push-task",
            {"task_id": task_id, "worker_id": self.runner_args.worker_id, "operator": "test"},
            token=self.hub_token,
        )
        self.assertTrue(pushed["ok"])

        deadline = time.time() + 20
        while time.time() < deadline:
            rows = request_json(self.controller + "/api/tasks?task_id=" + task_id, token=self.hub_token)["tasks"]
            if rows and rows[0]["state"] == "clean":
                break
            time.sleep(0.2)
        self.assertEqual(rows[0]["state"], "clean")
        self.assertEqual(rows[0]["lease_worker_id"], self.runner_args.worker_id)
        results = request_json(self.controller + "/api/data/new-results?cursor=0", token=self.hub_token)["results"]
        self.assertEqual([row["task_id"] for row in results], [task_id])
        expected_extensions = {
            "org.example.dispatch": {"source": "dispatch"},
            "org.example.dispatch-payload": {"source": "dispatch-payload"},
            "org.example.task": {"source": "task"},
            "org.example.task-payload": {"source": "task-payload"},
            "org.example.shared": {"source": "task-payload"},
        }
        with zipfile.ZipFile(results[0]["path"]) as archive:
            task_payload = json.loads(archive.read("task.json").decode("utf-8"))["payload"]
            worker_result = json.loads(archive.read("worker-result.json").decode("utf-8"))
        self.assertEqual(task_payload["extensions"], expected_extensions)
        self.assertEqual(worker_result["task_extensions"], expected_extensions)

    def test_dispatch_rejects_non_object_extensions(self) -> None:
        with self.assertRaises(HTTPError) as invalid:
            request_json(
                self.controller + "/api/tasks/dispatch",
                {
                    "schema_version": 1,
                    "operator": "test",
                    "extensions": ["not-an-object"],
                    "tasks": [{"task_id": "invalid-extensions", "payload": {"runner": "noop"}}],
                },
                token=self.hub_token,
            )
        self.assertEqual(invalid.exception.code, 400)
        self.assertEqual(json.loads(invalid.exception.read().decode("utf-8"))["error"], "invalid_task_extensions")

    def test_direct_push_rejects_a_second_task_when_reservation_is_full(self) -> None:
        first_id = "direct-resource__case-a__setting-a__run-001"
        second_id = "direct-resource__case-b__setting-a__run-001"
        profile = {"placement": "shared", "resources": {"cpu_millis": 100, "memory_mb": 600}}
        request_json(
            self.controller + "/api/tasks/dispatch",
            {
                "schema_version": 1,
                "operator": "test",
                "tasks": [
                    {
                        "task_id": first_id,
                        "required_capability": "linux",
                        "payload": {
                            "runner": "shell",
                            "command": "python3 -c \"import time; time.sleep(1)\"",
                            "execution_profile": profile,
                        },
                    },
                    {
                        "task_id": second_id,
                        "required_capability": "linux",
                        "payload": {"runner": "noop", "execution_profile": profile},
                    },
                ],
            },
            token=self.hub_token,
        )
        request_json(
            self.controller + "/api/admin/push-task",
            {"task_id": first_id, "worker_id": self.runner_args.worker_id, "operator": "test"},
            token=self.hub_token,
        )
        with self.assertRaises(HTTPError) as unavailable:
            request_json(
                self.controller + "/api/admin/push-task",
                {"task_id": second_id, "worker_id": self.runner_args.worker_id, "operator": "test"},
                token=self.hub_token,
            )
        self.assertEqual(unavailable.exception.code, 409)
        self.assertEqual(json.loads(unavailable.exception.read().decode("utf-8"))["error"], "worker_resource_unavailable")


if __name__ == "__main__":
    unittest.main()
