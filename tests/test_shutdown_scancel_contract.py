from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from peetsfea_runner import slurm_cleanup
from peetsfea_runner.state_store import StateStore

try:
    from peetsfea_runner import remote_job as _remote_job
except Exception:
    _remote_job = None

_build_pull_remote_sbatch_script_content = (
    getattr(_remote_job, "_build_pull_remote_sbatch_script_content", None) if _remote_job is not None else None
)


class _Account:
    def __init__(self, account_id: str, host_alias: str) -> None:
        self.account_id = account_id
        self.host_alias = host_alias


class TestShutdownScancelContract(unittest.TestCase):
    def test_active_state_rows_are_only_state_store_eligibility_candidates(self) -> None:
        expected_active_states = {"SUBMITTED", "PENDING", "RUNNING", "IDLE_DRAINING"}
        inactive_states = ("COMPLETED", "FAILED", "UNKNOWN")

        with TemporaryDirectory() as tmpdir:
            store = StateStore(Path(tmpdir) / "peetsfea_runner.state")
            store.initialize()
            run_id = "run_contract"

            store.upsert_slurm_worker(
                run_id=run_id,
                worker_id="worker_submitted",
                job_id="job_01",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-a",
                slurm_job_id="1001",
                worker_state="SUBMITTED",
                slots_configured=1,
                backend="sshfs",
            )
            store.upsert_slurm_worker(
                run_id=run_id,
                worker_id="worker_pending",
                job_id="job_02",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-a",
                slurm_job_id="1002",
                worker_state="PENDING",
                slots_configured=1,
                backend="sshfs",
            )
            store.upsert_slurm_worker(
                run_id=run_id,
                worker_id="worker_running",
                job_id="job_03",
                attempt_no=1,
                account_id="account_02",
                host_alias="gate1-b",
                slurm_job_id="1003",
                worker_state="RUNNING",
                slots_configured=1,
                backend="sshfs",
            )
            store.upsert_slurm_worker(
                run_id=run_id,
                worker_id="worker_draining",
                job_id="job_04",
                attempt_no=1,
                account_id="account_02",
                host_alias="gate1-b",
                slurm_job_id="1004",
                worker_state="IDLE_DRAINING",
                slots_configured=1,
                backend="sshfs",
            )
            for state in inactive_states:
                store.upsert_slurm_worker(
                    run_id=run_id,
                    worker_id=f"worker_{state.lower()}",
                    job_id=f"job_{state.lower()}",
                    attempt_no=1,
                    account_id="account_03",
                    host_alias="gate1-c",
                    slurm_job_id=state if state.isdigit() else "9001",
                    worker_state=state,
                    slots_configured=1,
                    backend="sshfs",
                )

            active_rows = store.list_active_slurm_workers(run_id=run_id)
            active_worker_ids = {str(row["worker_id"]) for row in active_rows}
            active_states = {str(row["worker_state"]) for row in active_rows}

            self.assertEqual(active_worker_ids, {"worker_submitted", "worker_pending", "worker_running", "worker_draining"})
            self.assertEqual(active_states, expected_active_states)

            deduped_job_ids = slurm_cleanup.dedupe_numeric_slurm_job_ids(active_worker_rows=active_rows)
            self.assertEqual(deduped_job_ids, ["1001", "1002", "1003", "1004"])

    def test_cleanup_uses_only_active_worker_rows_in_scancel_path(self) -> None:
        commands: list[list[str]] = []

        def run_command(command: list[str]) -> tuple[int, str, str]:
            commands.append(command)
            if command[0] == "ssh" and "squeue" in " ".join(command):
                return 0, "111 peetsfea-bootstrap\n", ""
            if "scancel" in " ".join(command):
                return 0, "", ""
            return 1, "", "unexpected command"

        with TemporaryDirectory() as tmpdir:
            store = StateStore(Path(tmpdir) / "peetsfea_runner.state")
            store.initialize()
            run_id = "run_contract"

            store.upsert_slurm_worker(
                run_id=run_id,
                worker_id="worker_run",
                job_id="job_active",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-a",
                slurm_job_id="1001",
                worker_state="RUNNING",
                slots_configured=1,
                backend="sshfs",
            )
            store.upsert_slurm_worker(
                run_id=run_id,
                worker_id="worker_done",
                job_id="job_done",
                attempt_no=1,
                account_id="account_01",
                host_alias="gate1-a",
                slurm_job_id="9001",
                worker_state="COMPLETED",
                slots_configured=1,
                backend="sshfs",
            )

            active_rows = store.list_active_slurm_workers(run_id=run_id)
            summary = slurm_cleanup.cleanup_slurm_workers(
                accounts=(_Account("account_01", "gate1-a"),),
                ssh_config_path="",
                active_worker_rows=active_rows,
                run_command=run_command,
            )

            self.assertEqual(summary["attempted_count"], 1)
            scancel_commands = [cmd for cmd in commands if cmd[:2] == ["ssh", "-o"] and "scancel" in " ".join(cmd)]
            self.assertTrue(any("1001" in " ".join(cmd) for cmd in scancel_commands))
            self.assertFalse(any("9001" in " ".join(cmd) for cmd in scancel_commands))

    def test_runner_owned_job_names_include_worker_and_legacy_remote_pull_name(self) -> None:
        output = "\n".join(
            [
                "1001 peetsfea-worker_01",
                "1002 remote_pull_sbatch.sh",
                "1003 unrelated",
                "abc not-numeric",
                "1004 peetsfea-bootstrap",
            ]
        )
        job_ids = slurm_cleanup.parse_runner_owned_job_ids(output=output)
        self.assertEqual(job_ids, ["1001", "1002"])

    def test_pull_sbatch_generation_uses_peetsfea_worker_job_name(self) -> None:
        if _build_pull_remote_sbatch_script_content is None:
            raise unittest.SkipTest("remote_job._build_pull_remote_sbatch_script_content helper unavailable")

        class _Cfg:
            nodes = 1
            ntasks = 1
            cpus_per_job = 40
            mem = "960G"
            time_limit = "00:45:00"
            partition = ""
            slurm_partitions_allowlist = ()
            slurm_exclude_nodes = ()
            remote_container_runtime = "enroot"
            remote_container_image = "~/runtime/enroot/aedt.sqsh"
            remote_container_ansys_root = "/opt/ohpc/pub/Electronics/v252"
            remote_ansys_executable = "/mnt/AnsysEM/ansysedt"
            control_plane_host = "127.0.0.1"
            control_plane_port = 8765
            control_plane_ssh_target = "user@host"
            control_plane_return_host = "172.16.165.146"
            control_plane_return_port = 22
            control_plane_return_user = "user"
            tunnel_recovery_grace_seconds = 30
            remote_job_dir = "/tmp/$USER/peetsfea-runner/test"

        script = _build_pull_remote_sbatch_script_content(  # type: ignore[call-arg]
            config=_Cfg(),
            remote_job_dir="/tmp/$USER/peetsfea-runner/test_run/job_01",
            run_id="run_01",
            worker_id="worker_01",
        )
        self.assertIn("#SBATCH --job-name=peetsfea-worker_01", script)
        self.assertIn("PEETS_CONTROL_WORKER_ID=worker_01", script)

    def test_systemd_unit_declares_scancel_shutdown_hook(self) -> None:
        # 컨트롤 플레인 유닛: 정상 SIGTERM 시엔 orchestrator.shutdown()이 잡을 scancel하고,
        # 하드 킬 대비로 ExecStopPost가 잔류 peetsfea-edt 잡을 정리한다.
        service_path = Path(__file__).resolve().parent.parent / "systemd" / "peetsfea-runner.service"
        content = service_path.read_text(encoding="utf-8")
        self.assertIn("ExecStopPost=", content)
        self.assertIn("scancel", content)
        self.assertIn("peetsfea-edt", content)


if __name__ == "__main__":
    unittest.main()
