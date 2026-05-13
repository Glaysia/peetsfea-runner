from __future__ import annotations

from typing import Callable

from peetsfea_runner import slurm_cleanup


class _Account:
    def __init__(self, account_id: str, host_alias: str) -> None:
        self.account_id = account_id
        self.host_alias = host_alias


def test_group_active_worker_rows_by_host() -> None:
    rows = [
        {"host_alias": "alpha", "slurm_job_id": "100"},
        {"host_alias": "beta", "slurm_job_id": "200"},
        {"host_alias": "alpha", "slurm_job_id": "300"},
        {"host_alias": "", "slurm_job_id": "400"},
        {"slurm_job_id": "500"},
    ]

    grouped = slurm_cleanup.group_active_worker_rows_by_host(active_worker_rows=rows)

    assert grouped.keys() == {"alpha", "beta"}
    assert grouped["alpha"] == [rows[0], rows[2]]
    assert grouped["beta"] == [rows[1]]


def test_dedupe_numeric_slurm_job_ids() -> None:
    rows = [
        {"slurm_job_id": "100"},
        {"slurm_job_id": "100"},
        {"slurm_job_id": "abc"},
        {"slurm_job_id": "200"},
        {"slurm_job_id": "200\n"},
        {"slurm_job_id": 300},
        {"slurm_job_id": None},
    ]

    deduped = slurm_cleanup.dedupe_numeric_slurm_job_ids(active_worker_rows=rows)

    assert deduped == ["100", "200", "300"]


def test_build_scancel_command_uses_ssh_config_path() -> None:
    command = slurm_cleanup.build_scancel_command(
        host_alias="gate1-harry261",
        slurm_job_ids=["123", "456"],
        ssh_config_path="/tmp/ssh.conf",
    )

    assert command[:2] == ["ssh", "-o"]
    assert "-F" in command
    idx = command.index("-F")
    assert command[idx + 1] == "/tmp/ssh.conf"
    assert command[-3:] == ["scancel", "123", "456"]


def test_parse_runner_owned_job_ids() -> None:
    output = "\n".join(
        [
            "111 peetsfea-bootstrap",
            "222 remote_pull_sbatch.sh",
            "333 other",
            "abc peetsfea-alpha",
            "444 peetsfea-worker_001",
            "222 remote_pull_sbatch.sh",
        ]
    )

    job_ids = slurm_cleanup.parse_runner_owned_job_ids(output=output)

    assert job_ids == ["222", "444"]


def test_cleanup_slurm_workers_runs_squeue_and_records_failures() -> None:
    commands: list[list[str]] = []

    def runner(command: list[str]) -> tuple[int, str, str]:
        commands.append(command)
        if command[0] == "ssh" and "squeue" in " ".join(command):
            if "gate1-harry261" in command:
                return 0, "777 peetsfea-worker_01\n888 remote_pull_sbatch.sh\n999 external", ""
            return 1, "", "ssh timeout"
        if "scancel" in command and "123" in command:
            return 0, "", ""
        if "scancel" in command and "777" in command:
            return 0, "", ""
        if "scancel" in command and "888" in command:
            return 0, "", ""
        if "scancel" in command and "200" in command:
            return 0, "", ""
        return 1, "", "unexpected"

    active_rows = [
        {"host_alias": "gate1-harry261", "slurm_job_id": "123"},
        {"host_alias": "gate1-jji0930", "slurm_job_id": "200"},
        {"host_alias": "gate1-jji0930", "slurm_job_id": "bad"},
    ]
    accounts = (_Account("a1", "gate1-harry261"), _Account("a2", "gate1-jji0930"))

    summary = slurm_cleanup.cleanup_slurm_workers(
        accounts=accounts,
        ssh_config_path="",
        active_worker_rows=active_rows,
        run_command=runner,
    )

    assert summary["attempted_count"] == 2
    assert summary["attempted_hosts"] == ["gate1-harry261", "gate1-jji0930"]
    assert summary["cancelled_hosts"] == ["gate1-harry261"]
    assert summary["failed_hosts"] == ["gate1-jji0930"]

    assert any("scancel" in cmd and "123" in cmd for cmd in commands)
    assert any("squeue" in " ".join(cmd) for cmd in commands if cmd[0] == "ssh")


def test_cleanup_slurm_workers_swallowing_exception() -> None:
    commands: list[list[str]] = []

    def runner(command: list[str]) -> tuple[int, str, str]:
        commands.append(command)
        if command[0] == "ssh" and "gate1-harry261" in command:
            raise RuntimeError("boom")
        return 0, "111 peetsfea-worker_01", ""

    summary = slurm_cleanup.cleanup_slurm_workers(
        accounts=(_Account("a1", "gate1-harry261"),),
        ssh_config_path="",
        active_worker_rows=[{"host_alias": "gate1-harry261", "slurm_job_id": "999"}],
        run_command=runner,
    )

    assert summary["attempted_hosts"] == ["gate1-harry261"]
    assert summary["failed_hosts"] == ["gate1-harry261"]
    assert summary["cancelled_hosts"] == []
    assert len(commands) == 2
