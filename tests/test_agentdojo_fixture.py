from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
sys.path.insert(0, str(TOOLS))

import loom_manifest
import loom_agentdojo_regression


class AgentDojoFixtureTests(unittest.TestCase):
    def test_fixed_fixture_has_four_tasks_and_eight_recoverable_attempts(self) -> None:
        manifest_path = ROOT / "examples" / "agentdojo" / "agentdojo-eight-slot.manifest.json"
        payload = loom_manifest.normalize(loom_manifest.read_json_or_jsonl(manifest_path), operator="test")
        task_ids, matrix = loom_agentdojo_regression.validate_manifest(payload)
        self.assertEqual(len(task_ids), 4)
        self.assertEqual(matrix, {"task_count": 4, "case_count": 2, "run_count": 2})
        for task in payload["tasks"]:
            self.assertEqual(task["expected"]["attempt_no"], 2)
            self.assertEqual(task["expected"]["min_result_count"], 2)
            self.assertEqual(task["payload"]["retry_policy"]["max_attempts"], 2)
            self.assertEqual([phase["phase"] for phase in task["payload"]["phases"]], ["prepare", "evaluate", "collect"])


if __name__ == "__main__":
    unittest.main()
