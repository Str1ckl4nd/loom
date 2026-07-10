from __future__ import annotations

import sys
import unittest
from pathlib import Path


TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))

import loom_matrix


class MatrixContractTests(unittest.TestCase):
    def test_runner_start_preserves_inventory_concurrency_contract(self) -> None:
        commands: list[str] = []
        original_ssh = loom_matrix.ssh
        original_write_remote_env = loom_matrix.write_remote_env
        original_hub_token_env = loom_matrix.HUB_TOKEN_ENV
        try:
            loom_matrix.ssh = lambda _host, command, **_kwargs: commands.append(command) or ""  # type: ignore[assignment]
            loom_matrix.write_remote_env = lambda _host, _remote_dir, _env: None  # type: ignore[assignment]
            loom_matrix.HUB_TOKEN_ENV = "TEST_HUB_TOKEN"
            result = loom_matrix.start_worker(
                {
                    "worker_id": "fixed-worker",
                    "host": "127.0.0.1",
                    "connection_mode": "ssh-start",
                    "max_concurrency": 4,
                    "initial_concurrency": 3,
                    "concurrency_policy": "fixed",
                    "resource_capacity": {"cpu_millis": 4000, "memory_mb": 8192},
                    "capabilities": ["linux"],
                },
                "http://10.0.0.1:8765",
                "/tmp/loom",
                {},
            )
        finally:
            loom_matrix.ssh = original_ssh  # type: ignore[assignment]
            loom_matrix.write_remote_env = original_write_remote_env  # type: ignore[assignment]
            loom_matrix.HUB_TOKEN_ENV = original_hub_token_env

        self.assertEqual(result["initial_concurrency"], 3)
        self.assertEqual(result["max_concurrency"], 4)
        self.assertEqual(result["concurrency_policy"], "fixed")
        self.assertEqual(result["resource_capacity"], {"cpu_millis": 4000, "memory_mb": 8192})
        self.assertEqual(len(commands), 1)
        self.assertIn("--max-concurrency 4", commands[0])
        self.assertIn("--initial-concurrency 3", commands[0])
        self.assertIn("--concurrency-policy fixed", commands[0])
        self.assertIn("--resource-capacity-json", commands[0])
        self.assertIn('"cpu_millis":4000', commands[0])
        self.assertIn("--controller-token-env TEST_HUB_TOKEN", commands[0])


if __name__ == "__main__":
    unittest.main()
