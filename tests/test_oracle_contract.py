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

import loom_export
import loom_evaluation
import loom_hub
import loom_manifest
import loom_runner
from loom_http import request_json


class OracleContractTests(unittest.TestCase):
    hub_token = "hub-oracle-test-token"
    runner_token = "runner-oracle-test-token"

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.hub = loom_hub.ControllerServer(("127.0.0.1", 0), loom_hub.Handler)
        self.hub.benchmark_root = self.root
        self.hub.db_path = self.root / "hub.sqlite"
        self.hub.artifact_root = self.root / "artifacts"
        self.hub.artifact_root.mkdir(parents=True, exist_ok=True)
        self.hub.control_log_path = self.root / "hub.jsonl"
        self.hub.upload_lock = threading.Lock()
        self.hub.auth_token = self.hub_token
        self.hub.runner_token_env = "TEST_ORACLE_RUNNER_TOKEN"
        with loom_hub.connect(self.hub.db_path) as conn:
            loom_hub.ensure_schema(conn)
            conn.commit()
        self.hub_thread = threading.Thread(target=self.hub.serve_forever, daemon=True)
        self.hub_thread.start()
        self.controller = f"http://127.0.0.1:{self.hub.server_address[1]}"

        self.runner_args = argparse.Namespace(
            controller=self.controller,
            controller_token=self.hub_token,
            worker_id="oracle-direct-worker",
            capability=["linux", "oracle", "*"],
            work_dir=self.root / "worker",
            source_cache_dir=self.root / "worker" / "source-cache",
            source_cache_max_mb=0,
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
        os.environ["TEST_ORACLE_RUNNER_TOKEN"] = self.runner_token
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
                            "direct_api_token_env": "TEST_ORACLE_RUNNER_TOKEN",
                            "capabilities": ["linux", "oracle", "*"],
                            "max_concurrency": 1,
                            "initial_concurrency": 1,
                            "concurrency_policy": "fixed",
                        }
                    ],
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
        os.environ.pop("TEST_ORACLE_RUNNER_TOKEN", None)

    def wait_for_task(self, task_id: str, state: str, timeout: float = 20) -> dict[str, object]:
        deadline = time.time() + timeout
        last: dict[str, object] = {}
        while time.time() < deadline:
            rows = request_json(self.controller + "/api/tasks?task_id=" + task_id, token=self.hub_token)["tasks"]
            if rows:
                last = rows[0]
                if last.get("state") == state:
                    return last
            time.sleep(0.1)
        self.fail(f"task {task_id} did not reach {state}: {last}")

    def push(self, task_id: str) -> None:
        deadline = time.time() + 20
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                response = request_json(
                    self.controller + "/api/admin/push-task",
                    {"task_id": task_id, "worker_id": self.runner_args.worker_id, "operator": "test"},
                    token=self.hub_token,
                )
                self.assertTrue(response["ok"])
                return
            except HTTPError as exc:
                last_error = exc
                if exc.code != 429:
                    raise
                time.sleep(0.1)
        self.fail(f"could not push {task_id}: {last_error}")

    @staticmethod
    def oracle_spec(name: str, version: str, outcome: str, *, retry: bool = False) -> dict[str, object]:
        if retry:
            body = (
                "import json, os, zipfile; from pathlib import Path; "
                "p=Path(os.environ['LOOM_EXECUTION_RESULT_ZIP']); "
                "assert 'worker-result.json' in zipfile.ZipFile(p).namelist(); "
                "attempt=int(os.environ['LOOM_ATTEMPT_NO']); "
                "outcome='error' if attempt == 1 else 'inconclusive'; "
                "Path(os.environ['LOOM_ORACLE_RESULT_PATH']).write_text(json.dumps({'schema_version': 1, 'outcome': outcome, 'oracle_version': 'retry-v1'}))"
            )
        else:
            body = (
                "import json, os, zipfile; from pathlib import Path; "
                "p=Path(os.environ['LOOM_EXECUTION_RESULT_ZIP']); "
                "assert 'worker-result.json' in zipfile.ZipFile(p).namelist(); "
                f"payload={{'schema_version': 1, 'outcome': {outcome!r}, 'oracle_version': {version!r}}}; "
                "payload['reward']={'value': 0.75, 'components': {'task': 0.5, 'safety': 0.25}, 'metadata': {'scale': 'unit'}} if payload['outcome'] == 'pass' else None; "
                "payload={key: value for key, value in payload.items() if value is not None}; "
                "Path(os.environ['LOOM_ORACLE_RESULT_PATH']).write_text(json.dumps(payload))"
            )
        spec: dict[str, object] = {
            "schema_version": 1,
            "name": name,
            "oracle_version": version,
            "required_capability": "oracle",
            "payload": {"runner": "shell", "command": ["python3", "-c", body], "timeout_seconds": 60},
        }
        if retry:
            spec["retry_policy"] = {"max_attempts": 2, "retry_categories": ["oracle_error"]}
        return spec

    def test_manifest_keeps_opt_in_oracle_and_trajectory_contracts(self) -> None:
        manifest = {
            "schema_version": 1,
            "campaign_id": "oracle-contract",
            "defaults": {
                "runner": "shell",
                "command": "true",
                "trajectory_export": {
                    "schema_version": 1,
                    "source_path": "traces/{case_id}.json",
                    "max_bytes": 4096,
                    "redaction": {"patterns": ["secret-[0-9]+"]},
                },
                "oracle": self.oracle_spec("judge", "manifest-v1", "pass"),
            },
            "cases": [{"case_id": "case-a", "setting_id": "baseline", "run_id": "001"}],
        }

        task = loom_manifest.normalize(manifest, operator="test")["tasks"][0]

        self.assertEqual(task["payload"]["oracle"]["oracle_version"], "manifest-v1")
        self.assertEqual(task["payload"]["trajectory_export"]["source_path"], "traces/case-a.json")
        self.assertTrue(task["payload"]["trajectory_export"]["enabled"])
        with self.assertRaisesRegex(ValueError, "must be different"):
            loom_evaluation.normalize_trajectory_export(
                {"schema_version": 1, "source_path": "trace.json", "export_path": "trace.json"}
            )

    def test_repo_artifact_collection_cannot_retain_raw_trajectory(self) -> None:
        source = self.root / "repo-source"
        source.mkdir()
        (source / "README.txt").write_text("fixture\n", encoding="utf-8")
        task_id = "repo-trajectory-raw-exclusion"
        command = (
            "import json, os; from pathlib import Path; "
            "Path(os.environ['LOOM_TRAJECTORY_PATH']).write_text(json.dumps({'schema_version': 1, 'events': [{'kind': 'message', 'content': 'api_key=top-secret'}]})); "
            "Path('declared.txt').write_text('safe')"
        )
        request_json(
            self.controller + "/api/tasks/dispatch",
            {
                "schema_version": 1,
                "operator": "test",
                "tasks": [
                    {
                        "task_id": task_id,
                        "case_id": "case-repo",
                        "setting_id": "baseline",
                        "run_id": "001",
                        "required_capability": "linux",
                        "payload": {
                            "runner": "repo",
                            "source": {"type": "local", "path": str(source)},
                            "commands": [{"command": ["python3", "-c", command]}],
                            "artifact_paths": [".loom-trajectory.raw.json", "declared.txt"],
                            "timeout_seconds": 60,
                            "trajectory_export": {"schema_version": 1, "max_bytes": 4096},
                        },
                    }
                ],
            },
            token=self.hub_token,
        )
        self.push(task_id)
        self.wait_for_task(task_id, "clean")

        rows = request_json(self.controller + "/api/data/new-results?cursor=0", token=self.hub_token)["results"]
        result = next(row for row in rows if row["task_id"] == task_id)
        with zipfile.ZipFile(result["path"]) as archive:
            names = set(archive.namelist())
            trajectory = json.loads(archive.read("trajectory.json").decode("utf-8"))
        self.assertIn("trajectory.json", names)
        self.assertIn("artifacts/declared.txt", names)
        self.assertNotIn(".loom-trajectory.raw.json", names)
        self.assertNotIn("artifacts/.loom-trajectory.raw.json", names)
        self.assertNotIn("top-secret", json.dumps(trajectory))

    def test_execution_result_oracle_can_follow_a_failed_process_attempt(self) -> None:
        task_id = "oracle-after-process-error"
        oracle = self.oracle_spec("diagnostic", "diagnostic-v1", "inconclusive")
        oracle["when"] = "execution_result"
        request_json(
            self.controller + "/api/tasks/dispatch",
            {
                "schema_version": 1,
                "operator": "test",
                "tasks": [
                    {
                        "task_id": task_id,
                        "case_id": "case-error",
                        "setting_id": "baseline",
                        "run_id": "001",
                        "required_capability": "linux",
                        "payload": {
                            "runner": "shell",
                            "command": ["python3", "-c", "raise SystemExit(3)"],
                            "timeout_seconds": 60,
                            "oracle": oracle,
                        },
                    }
                ],
            },
            token=self.hub_token,
        )
        self.push(task_id)
        self.wait_for_task(task_id, "run_error")

        results = request_json(self.controller + "/api/data/new-results?cursor=0", token=self.hub_token)["results"]
        execution_result = next(row for row in results if row["task_id"] == task_id)
        children = request_json(self.controller + "/api/tasks?task_kind=oracle", token=self.hub_token)["tasks"]
        child = next(row for row in children if row["execution_task_id"] == task_id)
        self.assertEqual(child["state"], "queued")
        self.assertEqual(child["execution_result"]["result_id"], execution_result["result_id"])

    def test_execution_oracles_trajectory_rewards_and_export_selectors(self) -> None:
        execution_id = "oracle-flow__case-a__baseline__run-001"
        execution_command = (
            "import json, os; from pathlib import Path; "
            "Path(os.environ['LOOM_TRAJECTORY_PATH']).write_text(json.dumps({'schema_version': 1, 'events': ["
            "{'kind': 'message', 'content': 'api_key=top-secret'}, {'kind': 'tool_call', 'name': 'lookup'}]})); "
            "Path('execution.txt').write_text('complete')"
        )
        request_json(
            self.controller + "/api/tasks/dispatch",
            {
                "schema_version": 1,
                "operator": "test",
                "tasks": [
                    {
                        "task_id": execution_id,
                        "case_id": "case-a",
                        "setting_id": "baseline",
                        "run_id": "001",
                        "required_capability": "linux",
                        "payload": {
                            "runner": "shell",
                            "command": ["python3", "-c", execution_command],
                            "timeout_seconds": 60,
                            "trajectory_export": {
                                "schema_version": 1,
                                "max_bytes": 4096,
                                "redaction": {"patterns": []},
                            },
                            "oracle": self.oracle_spec("pass", "pass-v1", "pass"),
                        },
                    }
                ],
            },
            token=self.hub_token,
        )
        self.push(execution_id)
        execution = self.wait_for_task(execution_id, "clean")
        self.assertEqual(execution["task_kind"], "execution")
        self.assertEqual(execution["attempt_no"], 1)

        results = request_json(self.controller + "/api/data/new-results?cursor=0", token=self.hub_token)["results"]
        execution_result = next(row for row in results if row["task_id"] == execution_id)
        with zipfile.ZipFile(execution_result["path"]) as archive:
            names = set(archive.namelist())
            trajectory = json.loads(archive.read("trajectory.json").decode("utf-8"))
            trajectory_summary = json.loads(archive.read("trajectory-summary.json").decode("utf-8"))
        self.assertIn("trajectory.json", names)
        self.assertNotIn(".loom-trajectory.raw.json", names)
        self.assertNotIn("top-secret", json.dumps(trajectory))
        self.assertEqual(trajectory_summary["status"], "exported")

        children = request_json(self.controller + "/api/tasks?task_kind=oracle", token=self.hub_token)["tasks"]
        pass_task = next(row for row in children if row["oracle_name"] == "pass")
        self.assertEqual(pass_task["execution_result"]["result_id"], execution_result["result_id"])
        self.push(pass_task["task_id"])
        self.wait_for_task(pass_task["task_id"], "clean")

        with self.assertRaises(HTTPError) as conflict:
            request_json(
                self.controller + "/api/oracles/dispatch",
                {
                    "schema_version": 1,
                    "operator": "test",
                    "execution_result_id": execution_result["result_id"],
                    "oracle": self.oracle_spec("pass", "different-pass-v1", "pass"),
                },
                token=self.hub_token,
            )
        self.assertEqual(conflict.exception.code, 409)

        for name, version, outcome in (("fail", "fail-v1", "fail"), ("retry", "retry-v1", "error")):
            response = request_json(
                self.controller + "/api/oracles/dispatch",
                {
                    "schema_version": 1,
                    "operator": "test",
                    "execution_result_id": execution_result["result_id"],
                    "oracle": self.oracle_spec(name, version, outcome, retry=name == "retry"),
                },
                token=self.hub_token,
            )
            self.assertTrue(response["ok"])
            child_id = response["oracle"]["task_id"]
            self.push(child_id)
            expected_state = "retry_queued" if name == "retry" else "clean"
            retry_child = self.wait_for_task(child_id, expected_state)
            if name == "retry":
                self.assertEqual(retry_child["attempt_no"], 2)
                self.push(child_id)
                self.wait_for_task(child_id, "clean")

        outcomes = request_json(self.controller + "/api/data/oracle-outcomes?limit=20", token=self.hub_token)["outcomes"]
        self.assertEqual({row["outcome"] for row in outcomes}, {"pass", "fail", "error", "inconclusive"})
        pass_outcome = next(row for row in outcomes if row["outcome"] == "pass")
        self.assertEqual(pass_outcome["reward"]["value"], 0.75)
        self.assertEqual(pass_outcome["reward"]["components"], {"task": 0.5, "safety": 0.25})
        self.assertEqual(pass_outcome["reward"]["metadata"], {"scale": "unit"})

        current_execution = self.wait_for_task(execution_id, "clean")
        self.assertEqual(current_execution["attempt_no"], 1)
        execution_rows = [row for row in request_json(self.controller + "/api/data/new-results?cursor=0", token=self.hub_token)["results"] if row["task_id"] == execution_id]
        self.assertEqual(len(execution_rows), 1)

        expected_counts = {"all_attempts": 5, "execution_clean": 1, "oracle_decided": 3, "oracle_pass": 2}
        for selector, count in expected_counts.items():
            selected = request_json(self.controller + "/api/data/export?selector=" + selector, token=self.hub_token)
            self.assertEqual(selected["total"], count)
            self.assertEqual(selected["retention"], "raw_attempt_packages_are_never_deleted_by_export")

        export_dir = self.root / "oracle-pass-export"
        export = loom_export.export_results(self.controller, "oracle_pass", export_dir, token=self.hub_token)
        self.assertTrue(export["ok"])
        self.assertEqual(export["downloaded_count"], 2)
        self.assertTrue((export_dir / "export-manifest.json").is_file())


if __name__ == "__main__":
    unittest.main()
