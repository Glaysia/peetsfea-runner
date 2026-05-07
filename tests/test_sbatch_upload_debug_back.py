from __future__ import annotations

import unittest

from peetsfea_runner.remote_job import _build_pull_remote_sbatch_script_content


class TestPullRemoteSbatchUploadDebugBack(unittest.TestCase):
    def test_upload_debug_back_includes_pre_lease_logs(self) -> None:
        class _Cfg:
            account_id = "account_01"
            partition = "cpu2"
            slurm_partitions_allowlist = ("cpu2",)
            nodes = 1
            ntasks = 1
            cpus_per_job = 24
            mem = "96G"
            time_limit = "01:00:00"
            remote_root = "~/aedt_runs"
            host = "host.example"
            slurm_exclude_nodes = ()
            control_plane_host = "127.0.0.1"
            control_plane_port = 8765
            control_plane_ssh_target = "peetsmain@172.16.165.146"
            control_plane_return_host = "172.16.165.146"
            control_plane_return_port = 22
            control_plane_return_user = "peetsmain"
            tunnel_recovery_grace_seconds = 30

        content = _build_pull_remote_sbatch_script_content(
            config=_Cfg(),
            remote_job_dir="/tmp/peetsfea/run_01/worker_01",
            run_id="run_01",
            worker_id="worker_01",
        )

        self.assertIn("for path in launch_probe.txt worker.stdout worker.stderr control_tunnel_bootstrap.err slurm-%j.out slurm-%j.err enroot.create.stdout enroot.create.stderr; do", content)
        self.assertIn("for path in container*.stdout container*.stderr; do", content)
        self.assertIn("upload_debug_back() {", content)
