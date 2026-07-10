from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
sys.path.insert(0, str(TOOLS))

import loom_hub
import loom_manifest
import loom_matrix
import loom_runner


class CorePreviewContractTests(unittest.TestCase):
    def test_inventory_v1_preserves_explicit_initial_concurrency(self) -> None:
        inventory = {
            "inventory_version": 1,
            "controller": {"connection_mode": "prestarted"},
            "controller_public_url": "http://127.0.0.1:8765",
            "workers": [
                {
                    "worker_id": "fixed-worker",
                    "host": "127.0.0.1",
                    "connection_mode": "direct-worker-api",
                    "max_concurrency": 4,
                    "initial_concurrency": 3,
                    "concurrency_policy": "fixed",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "inventory.json"
            path.write_text(json.dumps(inventory), encoding="utf-8")
            loaded = loom_matrix.load_inventory(path)
        worker = loaded["workers"][0]
        self.assertEqual(worker["initial_concurrency"], 3)
        self.assertEqual(worker["max_concurrency"], 4)
        self.assertEqual(worker["concurrency_policy"], "fixed")

    def test_inventory_v1_rejects_initial_above_hard_max(self) -> None:
        inventory = {
            "inventory_version": 1,
            "controller": {"connection_mode": "prestarted"},
            "controller_public_url": "http://127.0.0.1:8765",
            "workers": [
                {
                    "worker_id": "invalid-worker",
                    "host": "127.0.0.1",
                    "max_concurrency": 2,
                    "initial_concurrency": 3,
                    "concurrency_policy": "fixed",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "inventory.json"
            path.write_text(json.dumps(inventory), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "initial_concurrency"):
                loom_matrix.load_inventory(path)

    def test_inventory_v1_requires_workers_to_be_an_array(self) -> None:
        inventory = {
            "inventory_version": 1,
            "controller": {"connection_mode": "prestarted"},
            "controller_public_url": "http://127.0.0.1:8765",
            "workers": {"worker_id": "not-an-array"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "inventory.json"
            path.write_text(json.dumps(inventory), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "workers must be an array"):
                loom_matrix.load_inventory(path)

    def test_manifest_requires_an_explicit_schema_version(self) -> None:
        manifest = {
            "campaign_id": "missing-version",
            "source": {"type": "local", "path": "/tmp/source"},
            "defaults": {"runner": "repo", "commands": ["true"]},
            "cases": [{"case_id": "case", "setting_id": "setting", "run_id": "001"}],
        }
        with self.assertRaisesRegex(ValueError, "schema_version"):
            loom_manifest.normalize(manifest, operator="test")

    def test_runner_rejects_an_initial_value_above_the_hard_maximum(self) -> None:
        with self.assertRaisesRegex(ValueError, "initial_concurrency"):
            loom_runner.parse_args(
                [
                    "--controller",
                    "http://127.0.0.1:8765",
                    "--max-concurrency",
                    "2",
                    "--initial-concurrency",
                    "3",
                ]
            )

    def test_fixed_policy_only_backs_off_for_resource_or_rate_limit_signals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "hub.sqlite"
            now = loom_hub.utc_now()
            with loom_hub.connect(db_path) as conn:
                loom_hub.ensure_schema(conn)
                conn.execute(
                    """
                    INSERT INTO workers(
                      worker_id,status,capabilities_json,max_concurrency,initial_concurrency,concurrency_policy,
                      desired_concurrency,registered_at,last_seen_at,updated_at,health_json,resource_json,tuning_json
                    ) VALUES (?, 'alive', '[]', ?, ?, ?, ?, ?, ?, ?, '{}', ?, '{}')
                    """,
                    (
                        "fixed-worker",
                        4,
                        3,
                        "fixed",
                        3,
                        now,
                        now,
                        now,
                        json.dumps({"cpu_count": 4, "mem_total_mb": 4096}),
                    ),
                )
                clean = {
                    "issues": [],
                    "worker_result": {"controller_concurrency": {"desired_concurrency": 3, "claim_batch_size": 1}},
                }
                loom_hub.update_worker_concurrency_from_result(conn, None, "fixed-worker", "clean-task", "clean", clean)
                self.assertEqual(conn.execute("SELECT desired_concurrency FROM workers WHERE worker_id='fixed-worker'").fetchone()[0], 3)

                network = {"issues": ["network_unavailable"], "worker_result": clean["worker_result"]}
                loom_hub.update_worker_concurrency_from_result(conn, None, "fixed-worker", "network-task", "run_error", network)
                self.assertEqual(conn.execute("SELECT desired_concurrency FROM workers WHERE worker_id='fixed-worker'").fetchone()[0], 3)

                limited = {"issues": ["rate_limited"], "worker_result": clean["worker_result"]}
                loom_hub.update_worker_concurrency_from_result(conn, None, "fixed-worker", "limited-task", "run_error", limited)
                self.assertEqual(conn.execute("SELECT desired_concurrency FROM workers WHERE worker_id='fixed-worker'").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
