from __future__ import annotations

import tempfile
import unittest
from os import environ
from pathlib import Path
from unittest.mock import patch

from peetsfea_runner import __version__

import runner


class TestRunnerConfig(unittest.TestCase):
    def test_build_config_reads_runtime_resources_from_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with patch.dict(
                environ,
                {
                    "PEETSFEA_INPUT_QUEUE_DIR": str(root / "in"),
                    "PEETSFEA_OUTPUT_ROOT_DIR": str(root / "out"),
                    "PEETSFEA_DELETE_FAILED_DIR": str(root / "out" / "_delete_failed"),
                    "PEETSFEA_EXECUTE_REMOTE": "false",
                    "PEETSFEA_CPUS_PER_JOB": "24",
                    "PEETSFEA_MEM": "512G",
                    "PEETSFEA_TIME_LIMIT": "02:30:00",
                    "PEETSFEA_SLOTS_PER_JOB": "6",
                    "PEETSFEA_CORES_PER_SLOT": "4",
                    "PEETSFEA_BALANCE_METRIC": "license_max_520",
                },
                clear=False,
            ):
                config = runner._build_config(root)

            self.assertFalse(config.execute_remote)
            self.assertEqual(config.cpus_per_job, 24)
            self.assertEqual(config.mem, "512G")
            self.assertEqual(config.time_limit, "02:30:00")
            self.assertEqual(config.slots_per_job, 6)
            self.assertEqual(config.cores_per_slot, 4)
            self.assertEqual(config.balance_metric, "license_max_520")

    def test_package_version_uses_date_build_format(self) -> None:
        self.assertRegex(__version__, r"^\d{4}\.\d{2}\.\d{2}\.\d+$")


if __name__ == "__main__":
    unittest.main()
