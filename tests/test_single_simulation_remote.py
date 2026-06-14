from __future__ import annotations

import subprocess
import tarfile
from pathlib import Path
from typing import Sequence

from peetsfea_runner import single_simulation_remote as remote
from peetsfea_runner.single_simulation_remote import (
    PEETSFEA_SOURCE_ARCHIVE_NAME,
    REMOTE_SERVER_FILENAME,
    SBATCH_FILENAME,
    SingleSimulationRemoteConfig,
    build_remote_single_simulation_server_script,
    build_single_simulation_sbatch_script,
    start_single_simulation_remote_api,
)


def test_remote_server_script_calls_peetsfea_031_high_level_api() -> None:
    script = build_remote_single_simulation_server_script()

    compile(script, REMOTE_SERVER_FILENAME, "exec")
    assert 'EXPECTED_PEETSFEA_VERSION = "0.3.1"' in script
    assert "from peetsfea.ssw_random_sample_reports import run_ssw_random_sample_reports_from_toml_text" in script
    assert "run_ssw_random_sample_reports_from_toml_text" in script
    assert 'os.environ.get("SLURM_JOB_ID", "") or os.environ.get("PEETS_SLURM_JOB_ID", "")' in script
    assert "candidate_toml_text" in script
    assert 'raw not in {"full", "semi_dry"}' in script


def test_single_api_sbatch_uses_one_enroot_container_reverse_tunnel_and_no_host_tmp() -> None:
    config = SingleSimulationRemoteConfig(
        control_return_user="peets",
        remote_work_root="~/peetsfea-single-api",
        remote_container_image="~/runtime/enroot/aedt.sqsh",
    )
    script = build_single_simulation_sbatch_script(
        config=config,
        session_id="session-001",
        remote_session_dir="~/peetsfea-single-api/sessions/session-001",
        local_api_port=45678,
    )

    assert "#SBATCH --job-name=peetsfea-single-api" in script
    assert "PEETS_ACCOUNT_ID=account_01" in script
    assert "PEETS_HOST_ALIAS=gate1-harry261" in script
    assert 'PEETS_REMOTE_SESSION_DIR="$HOME/peetsfea-single-api/sessions/session-001"' in script
    assert "export PEETS_SESSION_ID PEETS_ACCOUNT_ID PEETS_HOST_ALIAS PEETS_REMOTE_SESSION_DIR PEETS_LOCAL_API_PORT PEETS_REMOTE_API_PORT" in script
    assert 'JOB_DIR="$PEETS_REMOTE_SESSION_DIR/job-${SLURM_JOB_ID:-manual}"' in script
    assert 'ENROOT_BASE="$JOB_DIR/enroot"' in script
    assert 'ENROOT_RUNTIME_PATH="$ENROOT_BASE/runtime"' in script
    assert 'ENROOT_CACHE_PATH="$ENROOT_BASE/cache"' in script
    assert 'ENROOT_DATA_PATH="$ENROOT_BASE/data"' in script
    assert 'ENROOT_TEMP_PATH="$ENROOT_BASE/tmp"' in script
    assert script.count("enroot create -f -n \"$CONTAINER_NAME\"") == 1
    assert script.count("enroot start --root --rw") == 1
    assert '-R "127.0.0.1:${PEETS_LOCAL_API_PORT}:127.0.0.1:${PEETS_REMOTE_API_PORT}"' in script
    assert 'SOCKET_DIR="$HOME/.peetsfea-single-api-sockets"' in script
    assert 'TUNNEL_SOCKET="$SOCKET_DIR/t-${SLURM_JOB_ID:-manual}.sock"' in script
    assert '"$HOME/.ssh/id_ed25519" "$HOME/.ssh/id_ed25519_codex_to_pc" "$HOME/.ssh/id_rsa"' in script
    assert "export PYTHONPATH=/work/peetsfea/src:/work/peetsfea:${PYTHONPATH:-}" in script
    assert "source /work/container_env.sh" in script
    assert 'export PEETS_API_PORT="$PEETS_REMOTE_API_PORT"' in script
    assert 'export PEETS_SLURM_JOB_ID="${SLURM_JOB_ID:-}"' in script
    assert "export UV_CACHE_DIR=/work/uv_cache" in script
    assert "('cadquery', 'cadquery')" in script
    assert "('ocp_vscode', 'ocp-vscode>=3.1.2')" in script
    assert "[sys.executable, '-m', 'uv', 'pip', 'install', *missing]" in script
    assert "/opt/miniconda3/bin/python /work/remote_single_api_server.py" in script
    assert 'export TMPDIR=/work/container_tmp' in script
    assert "mktemp" not in script
    assert "/tmp/peetsfea" not in script
    assert 'TUNNEL_SOCKET="/tmp' not in script


def test_start_remote_api_stages_and_submits_with_repo_ssh_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_archive(*, source_path: Path, archive_path: Path, timeout_seconds: int) -> None:
        archive_path.write_bytes(b"archive")

    monkeypatch.setattr(remote, "_create_peetsfea_source_archive", fake_archive)
    commands: list[list[str]] = []

    def run_command(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        materialized = list(command)
        commands.append(materialized)
        if materialized[0] == "ssh" and "sbatch --parsable" in materialized[-1]:
            return subprocess.CompletedProcess(materialized, 0, stdout="12345\n", stderr="")
        return subprocess.CompletedProcess(materialized, 0, stdout="", stderr="")

    config = SingleSimulationRemoteConfig(
        ssh_config_path=tmp_path / "ssh_config",
        stage_root=tmp_path / "stage",
        peetsfea_source_path=tmp_path / "peetsfea",
        control_return_user="peets",
        local_api_port=45678,
    )

    session = start_single_simulation_remote_api(
        config=config,
        session_id="session-001",
        run_command=run_command,
    )

    assert session.session_id == "session-001"
    assert session.slurm_job_id == "12345"
    assert session.local_api_port == 45678
    assert session.remote_session_dir == "~/peetsfea-single-api/sessions/session-001"
    assert (session.stage_dir / REMOTE_SERVER_FILENAME).is_file()
    assert (session.stage_dir / SBATCH_FILENAME).is_file()
    assert (session.stage_dir / PEETSFEA_SOURCE_ARCHIVE_NAME).read_bytes() == b"archive"
    assert commands[0][:5] == ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
    assert "-F" in commands[0]
    assert commands[0][-2:] == [
        "gate1-harry261",
        "mkdir -p $HOME/peetsfea-single-api/sessions/session-001",
    ]
    assert commands[1][0] == "scp"
    assert commands[1][-1] == "gate1-harry261:~/peetsfea-single-api/sessions/session-001/"
    assert commands[2][0] == "ssh"
    assert commands[2][-2] == "gate1-harry261"
    assert "sbatch --parsable ./single_api_sbatch.sh" in commands[2][-1]


def test_peetsfea_source_archive_contains_only_runtime_members(tmp_path: Path) -> None:
    source = tmp_path / "peetsfea"
    for directory in (
        source / "src" / "peetsfea",
        source / "entry",
        source / "examples",
        source / "notebooks",
        source / "docs",
    ):
        directory.mkdir(parents=True)
    (source / "pyproject.toml").write_text("[project]\nname = \"peetsfea\"\n", encoding="utf-8")
    (source / "src" / "peetsfea" / "__init__.py").write_text("__version__ = '0.3.1'\n", encoding="utf-8")
    (source / "entry" / "debug_view_0_3_0_ssw.py").write_text("", encoding="utf-8")
    (source / "examples" / "0.3.0_sweep.toml").write_text("spec_version = \"0.3.1\"\n", encoding="utf-8")
    (source / "notebooks" / "mu_p.tab").write_text("", encoding="utf-8")
    (source / "docs" / "large.html").write_text("not needed", encoding="utf-8")

    archive = tmp_path / "peetsfea_source.tgz"
    remote._create_peetsfea_source_archive(source_path=source, archive_path=archive, timeout_seconds=10)

    with tarfile.open(archive, "r:gz") as handle:
        names = set(handle.getnames())

    assert "peetsfea/pyproject.toml" in names
    assert "peetsfea/src/peetsfea/__init__.py" in names
    assert "peetsfea/entry/debug_view_0_3_0_ssw.py" in names
    assert "peetsfea/examples/0.3.0_sweep.toml" in names
    assert "peetsfea/notebooks/mu_p.tab" in names
    assert "peetsfea/docs/large.html" not in names
