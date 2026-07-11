from __future__ import annotations

import sys
import unittest
from pathlib import Path


TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))

import loom_manifest
from loom_cache import attach_source_descriptor, git_source_descriptor


class ManifestContractTests(unittest.TestCase):
    def test_phases_merge_and_case_run_selection(self) -> None:
        manifest = {
            "schema_version": 1,
            "campaign_id": "release-check",
            "source": {"type": "local", "path": "/tmp/source"},
            "defaults": {
                "runner": "repo",
                "env": {"GLOBAL": "default", "SHARED": "global"},
                "phases": [
                    {
                        "name": "prepare",
                        "command": ["python3", "prepare.py"],
                        "env": {"PHASE": "prepare", "SHARED": "phase"},
                    },
                    {
                        "name": "evaluate",
                        "command": "python3 evaluate.py",
                        "args": ["--case", "{case_id}", "--run", "{run_id}"],
                    },
                ],
            },
            "cases": [
                {
                    "case_id": "case-a",
                    "setting_id": "setting-a",
                    "run_id": "001",
                    "env": {"GLOBAL": "case"},
                    "phases": [{"name": "prepare", "env": {"SHARED": "case-phase"}}],
                },
                {"case_id": "case-b", "setting_id": "setting-a", "run_id": "002"},
            ],
        }

        result = loom_manifest.normalize(manifest, operator="test", case_ids={"case-a"}, run_ids={"001"})

        self.assertEqual(result["schema_version"], 1)
        self.assertEqual(len(result["tasks"]), 1)
        task = result["tasks"][0]
        self.assertEqual(task["task_id"], "release-check__case-a__setting-a__run-001")
        phases = task["payload"]["phases"]
        self.assertEqual([phase["phase"] for phase in phases], ["prepare", "evaluate"])
        self.assertEqual(phases[0]["env"], {"PHASE": "prepare", "SHARED": "case-phase"})
        self.assertEqual(phases[1]["args"], ["--case", "case-a", "--run", "001"])
        self.assertEqual(task["payload"]["env"], {"GLOBAL": "case", "SHARED": "global"})

    def test_unknown_phase_field_is_rejected(self) -> None:
        manifest = {
            "schema_version": 1,
            "campaign_id": "invalid",
            "source": {"type": "local", "path": "/tmp/source"},
            "defaults": {
                "runner": "repo",
                "phases": [{"name": "prepare", "command": "true", "not_supported": True}],
            },
            "cases": [{"case_id": "case", "setting_id": "setting", "run_id": "001"}],
        }

        with self.assertRaisesRegex(ValueError, "unsupported fields"):
            loom_manifest.normalize(manifest, operator="test")

    def test_extensions_merge_by_namespace_with_case_precedence(self) -> None:
        manifest = {
            "schema_version": 1,
            "campaign_id": "extensions",
            "extensions": {
                "org.example.campaign": {"source": "campaign"},
                "org.example.shared": {"source": "campaign", "stale": True},
            },
            "defaults": {
                "runner": "shell",
                "command": "true",
                "extensions": {
                    "org.example.defaults": {"source": "defaults"},
                    "org.example.shared": {"source": "defaults"},
                },
                "payload": {
                    "extensions": {
                        "org.example.default-payload": {"source": "default-payload"},
                        "org.example.shared": {"source": "default-payload"},
                    }
                },
            },
            "cases": [
                {
                    "case_id": "case-a",
                    "setting_id": "baseline",
                    "run_id": "001",
                    "extensions": {
                        "org.example.case": {"source": "case"},
                        "org.example.shared": {"source": "case"},
                    },
                    "payload": {
                        "extensions": {
                            "org.example.case-payload": {"source": "case-payload"},
                            "org.example.shared": {"source": "case-payload"},
                        }
                    },
                }
            ],
        }

        task = loom_manifest.normalize(manifest, operator="test")["tasks"][0]

        self.assertEqual(
            task["payload"]["extensions"],
            {
                "org.example.campaign": {"source": "campaign"},
                "org.example.defaults": {"source": "defaults"},
                "org.example.default-payload": {"source": "default-payload"},
                "org.example.case": {"source": "case"},
                "org.example.case-payload": {"source": "case-payload"},
                "org.example.shared": {"source": "case-payload"},
            },
        )

    def test_extensions_require_an_object(self) -> None:
        manifest = {
            "schema_version": 1,
            "campaign_id": "invalid-extensions",
            "defaults": {"runner": "shell", "command": "true", "extensions": ["not-an-object"]},
            "cases": [{"case_id": "case-a", "setting_id": "baseline", "run_id": "001"}],
        }

        with self.assertRaisesRegex(ValueError, "defaults.extensions must be an object"):
            loom_manifest.normalize(manifest, operator="test")

    def test_immutable_git_source_gets_a_credential_free_cache_descriptor(self) -> None:
        commit = "a" * 40
        manifest = {
            "schema_version": 1,
            "campaign_id": "immutable-source",
            "source": {
                "type": "git",
                "url": "https://user:secret@example.test/org/repo.git/",
                "resolved_commit": commit,
            },
            "defaults": {"runner": "repo", "commands": ["true"]},
            "cases": [{"case_id": "case-a", "setting_id": "baseline", "run_id": "001"}],
        }

        task = loom_manifest.normalize(manifest, operator="test")["tasks"][0]
        descriptor = task["payload"]["source_descriptor"]

        self.assertEqual(descriptor["type"], "git")
        self.assertEqual(descriptor["commit"], commit)
        self.assertEqual(descriptor["canonical_url"], "https://example.test/org/repo.git")
        self.assertTrue(descriptor["cache_key"].startswith("git-sha256:"))
        self.assertNotIn("secret", str(descriptor))

    def test_mutable_git_ref_does_not_advertise_cache_reuse(self) -> None:
        manifest = {
            "schema_version": 1,
            "campaign_id": "mutable-source",
            "source": {"type": "git", "url": "https://example.test/org/repo.git", "ref": "main"},
            "defaults": {"runner": "repo", "commands": ["true"]},
            "cases": [{"case_id": "case-a", "setting_id": "baseline", "run_id": "001"}],
        }

        task = loom_manifest.normalize(manifest, operator="test")["tasks"][0]

        self.assertNotIn("source_descriptor", task["payload"])

    def test_raw_descriptor_must_match_every_immutable_source_field(self) -> None:
        source = {
            "type": "git",
            "url": "https://example.test/org/repo.git",
            "commit": "b" * 40,
        }
        descriptor = git_source_descriptor(source)
        self.assertIsNotNone(descriptor)
        payload = {
            "source": source,
            "source_descriptor": {
                **descriptor,
                "canonical_url": "https://example.test/other/repo.git",
            },
        }

        with self.assertRaisesRegex(ValueError, "does not match"):
            attach_source_descriptor(payload)


if __name__ == "__main__":
    unittest.main()
