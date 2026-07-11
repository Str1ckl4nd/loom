from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path


TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))

from loom_cache import git_source_descriptor
import loom_runner


class SourceCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.work_root = self.root / "runs"
        self.cache_root = self.root / "source-cache"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def make_repo(self, name: str, text: str) -> tuple[Path, str]:
        repo = self.root / name
        repo.mkdir(parents=True)
        subprocess.run(["git", "init"], cwd=repo, check=True, text=True, capture_output=True)
        subprocess.run(["git", "config", "user.name", "Loom test"], cwd=repo, check=True, text=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "loom@example.test"], cwd=repo, check=True, text=True, capture_output=True)
        (repo / "input.txt").write_text(text, encoding="utf-8")
        subprocess.run(["git", "add", "input.txt"], cwd=repo, check=True, text=True, capture_output=True)
        subprocess.run(["git", "commit", "-m", "fixture"], cwd=repo, check=True, text=True, capture_output=True)
        commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, check=True, text=True, capture_output=True).stdout.strip()
        return repo, commit

    def task(self, task_id: str, repo: Path, commit: str) -> dict[str, object]:
        source = {"type": "git", "url": str(repo), "commit": commit}
        descriptor = git_source_descriptor(source)
        self.assertIsNotNone(descriptor)
        return {
            "task_id": task_id,
            "attempt_no": 1,
            "assigned_worker_id": "cache-test-worker",
            "payload": {
                "runner": "repo",
                "source": source,
                "source_descriptor": descriptor,
                "commands": [
                    {
                        "name": "write-artifact",
                        "command": [
                            "python3",
                            "-c",
                            "from pathlib import Path; Path('result.txt').write_text(Path('input.txt').read_text(), encoding='utf-8')",
                        ],
                    }
                ],
                "artifact_paths": ["result.txt"],
                "timeout_seconds": 60,
            },
        }

    def run_cached(self, task: dict[str, object], *, max_bytes: int = 32 * 1024 * 1024) -> tuple[Path, dict[str, object]]:
        return loom_runner.run_task(
            task,
            self.work_root,
            source_cache_dir=self.cache_root,
            source_cache_max_bytes=max_bytes,
        )

    def test_reuses_and_repairs_a_pinned_git_cache(self) -> None:
        repo, commit = self.make_repo("upstream", "cached fixture\n")
        first_zip, first = self.run_cached(self.task("cache-first", repo, commit))
        first_cache = first["source_cache"]
        self.assertTrue(first_cache["enabled"])
        self.assertFalse(first_cache["hit"])
        self.assertEqual(first_cache["state"], "miss")
        self.assertGreater(first_cache["transferred_bytes"], 0)
        self.assertFalse((self.work_root / "cache-first" / "attempt-001" / "workspace").exists())
        with zipfile.ZipFile(first_zip) as archive:
            self.assertIn("artifacts/result.txt", archive.namelist())
            self.assertFalse(any(name.startswith("workspace/") for name in archive.namelist()))

        _second_zip, second = self.run_cached(self.task("cache-second", repo, commit))
        second_cache = second["source_cache"]
        self.assertTrue(second_cache["hit"])
        self.assertEqual(second_cache["transferred_bytes"], 0)

        descriptor = git_source_descriptor({"type": "git", "url": str(repo), "commit": commit})
        self.assertIsNotNone(descriptor)
        _entry, repo_dir, _metadata = loom_runner.cache_entry_paths(self.cache_root, descriptor["cache_key"])
        shutil.rmtree(repo_dir)
        repo_dir.mkdir()

        _third_zip, third = self.run_cached(self.task("cache-repaired", repo, commit))
        repaired_cache = third["source_cache"]
        self.assertFalse(repaired_cache["hit"])
        self.assertTrue(repaired_cache["repaired"])
        self.assertEqual(repaired_cache["state"], "repaired")
        self.assertNotIn("fallback", repaired_cache)
        self.assertEqual(loom_runner.source_cache_inventory(self.cache_root)["entry_count"], 1)

    def test_budget_evicts_an_inactive_older_entry(self) -> None:
        first_repo, first_commit = self.make_repo("first-upstream", "first\n")
        second_repo, second_commit = self.make_repo("second-upstream", "second\n")
        self.run_cached(self.task("budget-first", first_repo, first_commit), max_bytes=1)
        self.run_cached(self.task("budget-second", second_repo, second_commit), max_bytes=1)

        second_descriptor = git_source_descriptor({"type": "git", "url": str(second_repo), "commit": second_commit})
        self.assertIsNotNone(second_descriptor)
        inventory = loom_runner.source_cache_inventory(self.cache_root)
        self.assertEqual(inventory["entry_count"], 1)
        self.assertEqual(inventory["keys"], [second_descriptor["cache_key"]])


if __name__ == "__main__":
    unittest.main()
