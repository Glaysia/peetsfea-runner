from __future__ import annotations

import getpass
import json
import shlex
import socket
import subprocess
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .single_simulation_api import DEFAULT_SINGLE_ACCOUNT_ID, DEFAULT_SINGLE_HOST_ALIAS, EXPECTED_PEETSFEA_VERSION
from .single_simulation_client import SingleSimulationApiClient


DEFAULT_REMOTE_WORK_ROOT = "~/peetsfea-single-api"
DEFAULT_REMOTE_CONTAINER_IMAGE = "~/runtime/enroot/aedt.sqsh"
DEFAULT_REMOTE_CONTAINER_ANSYS_ROOT = "/opt/ohpc/pub/Electronics/v252"
DEFAULT_PEETSFEA_SOURCE_PATH = "/home/peets/Projects/PythonProjects/peetsfea"
DEFAULT_CONTROL_RETURN_HOST = "172.16.165.146"
DEFAULT_REMOTE_API_PORT = 18888
DEFAULT_LOCAL_RESULT_DB = "build/single_simulation_results.duckdb"
DEFAULT_CONTAINER_SSHFS_MOUNT_ROOT = "/workspace"
REMOTE_SERVER_FILENAME = "remote_single_api_server.py"
SBATCH_FILENAME = "single_api_sbatch.sh"
PEETSFEA_SOURCE_ARCHIVE_NAME = "peetsfea_source.tgz"

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


@dataclass(slots=True, frozen=True)
class SingleSimulationRemoteConfig:
    account_id: str = DEFAULT_SINGLE_ACCOUNT_ID
    host_alias: str = DEFAULT_SINGLE_HOST_ALIAS
    ssh_config_path: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / ".ssh" / "config"
    )
    remote_work_root: str = DEFAULT_REMOTE_WORK_ROOT
    remote_container_image: str = DEFAULT_REMOTE_CONTAINER_IMAGE
    remote_container_ansys_root: str = DEFAULT_REMOTE_CONTAINER_ANSYS_ROOT
    peetsfea_source_path: Path = Path(DEFAULT_PEETSFEA_SOURCE_PATH)
    control_return_host: str = DEFAULT_CONTROL_RETURN_HOST
    control_return_port: int = 22
    control_return_user: str = field(default_factory=lambda: getpass.getuser().strip() or "peets")
    local_api_port: int = 0
    remote_api_port: int = DEFAULT_REMOTE_API_PORT
    partition: str = ""
    nodes: int = 1
    ntasks: int = 1
    cpus_per_job: int = 16
    mem: str = "64G"
    time_limit: str = "08:00:00"
    stage_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "build" / "single_simulation_remote"
    )
    local_sshfs_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "build" / "single_simulation_sshfs"
    )
    container_sshfs_mount_root: str = DEFAULT_CONTAINER_SSHFS_MOUNT_ROOT
    command_timeout_seconds: int = 120


@dataclass(slots=True, frozen=True)
class SingleSimulationRemoteSession:
    config: SingleSimulationRemoteConfig
    session_id: str
    remote_session_dir: str
    local_api_port: int
    remote_api_port: int
    slurm_job_id: str
    stage_dir: Path
    local_sshfs_session_root: Path

    @property
    def client(self) -> SingleSimulationApiClient:
        return SingleSimulationApiClient(f"http://127.0.0.1:{self.local_api_port}", timeout_seconds=30.0)


def start_single_simulation_remote_api(
    *,
    config: SingleSimulationRemoteConfig | None = None,
    session_id: str = "",
    run_command: CommandRunner | None = None,
) -> SingleSimulationRemoteSession:
    resolved_config = config or SingleSimulationRemoteConfig()
    resolved_session_id = session_id.strip() or _new_session_id()
    local_api_port = (
        int(resolved_config.local_api_port)
        if int(resolved_config.local_api_port) > 0
        else _allocate_local_loopback_port()
    )
    remote_session_dir = _join_remote_path(
        resolved_config.remote_work_root,
        "sessions",
        resolved_session_id,
    )
    local_sshfs_session_root = _prepare_local_sshfs_session_root(
        config=resolved_config,
        session_id=resolved_session_id,
    )
    stage_dir = _prepare_stage_dir(
        config=resolved_config,
        session_id=resolved_session_id,
        remote_session_dir=remote_session_dir,
        local_api_port=local_api_port,
    )
    runner = run_command or _default_run_command(resolved_config.command_timeout_seconds)
    _ensure_remote_session_dir(config=resolved_config, remote_session_dir=remote_session_dir, run_command=runner)
    _upload_stage_dir(
        config=resolved_config,
        remote_session_dir=remote_session_dir,
        stage_dir=stage_dir,
        run_command=runner,
    )
    slurm_job_id = _submit_remote_sbatch(
        config=resolved_config,
        remote_session_dir=remote_session_dir,
        run_command=runner,
    )
    return SingleSimulationRemoteSession(
        config=resolved_config,
        session_id=resolved_session_id,
        remote_session_dir=remote_session_dir,
        local_api_port=local_api_port,
        remote_api_port=resolved_config.remote_api_port,
        slurm_job_id=slurm_job_id,
        stage_dir=stage_dir,
        local_sshfs_session_root=local_sshfs_session_root,
    )


def wait_for_single_simulation_remote_health(
    session: SingleSimulationRemoteSession,
    *,
    timeout_seconds: float = 900.0,
    poll_seconds: float = 5.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            health = session.client.health()
            if health.get("status") == "ok":
                return health
            last_error = json.dumps(health, sort_keys=True)
        except Exception as exc:
            last_error = str(exc)
        time.sleep(poll_seconds)
    raise TimeoutError(
        "single simulation remote API did not become healthy "
        f"session={session.session_id} job={session.slurm_job_id} last_error={last_error}"
    )


def shutdown_single_simulation_remote_api(session: SingleSimulationRemoteSession) -> dict[str, Any]:
    return session.client.shutdown()


def cancel_single_simulation_remote_session(
    session: SingleSimulationRemoteSession,
    *,
    run_command: CommandRunner | None = None,
) -> None:
    runner = run_command or _default_run_command(session.config.command_timeout_seconds)
    command = [
        *_ssh_base_command(session.config),
        session.config.host_alias,
        f"scancel {shlex.quote(session.slurm_job_id)}",
    ]
    completed = runner(command)
    _raise_for_command_failure(completed, stage="remote scancel")


def build_remote_single_simulation_server_script() -> str:
    return r'''from __future__ import annotations

import hashlib
import json
import os
import threading
import traceback
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

EXPECTED_PEETSFEA_VERSION = "0.3.2"


class SimulationRequestError(ValueError):
    pass


class SimulationBusyError(RuntimeError):
    pass


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [jsonable(item) for item in value]
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def load_peetsfea_version() -> str:
    import peetsfea

    return str(peetsfea.__version__)


def load_primitive():
    from peetsfea.ssw_random_sample_reports import run_ssw_random_sample_reports_from_toml_text

    return run_ssw_random_sample_reports_from_toml_text


class SingleSimulationService:
    def __init__(
        self,
        *,
        output_root: Path,
        account_id: str,
        host_alias: str,
        remote_job_id: str,
        api_session_id: str,
    ) -> None:
        self.output_root = output_root
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.account_id = account_id
        self.host_alias = host_alias
        self.remote_job_id = remote_job_id
        self.api_session_id = api_session_id
        self._busy_lock = threading.Lock()
        self._busy = False

    @property
    def busy(self) -> bool:
        return self._busy

    def health(self) -> dict[str, Any]:
        peetsfea_version = load_peetsfea_version()
        return {
            "status": "ok",
            "busy": self.busy,
            "account_id": self.account_id,
            "host_alias": self.host_alias,
            "remote_job_id": self.remote_job_id,
            "api_session_id": self.api_session_id,
            "peetsfea_version": peetsfea_version,
            "expected_peetsfea_version": EXPECTED_PEETSFEA_VERSION,
            "peetsfea_version_ok": peetsfea_version == EXPECTED_PEETSFEA_VERSION,
        }

    def simulate(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        request_id = self._request_id(payload)
        candidate_toml_text = self._candidate_toml_text(payload)
        seed = self._seed(payload)
        mode = self._mode(payload)
        if not self._busy_lock.acquire(blocking=False):
            raise SimulationBusyError("single simulation API is busy")
        self._busy = True
        started_at = utc_now_iso()
        try:
            input_toml_hash = sha256_text(candidate_toml_text)
            job_output_dir = self.output_root / request_id
            result = load_primitive()(
                candidate_toml_text,
                output_dir=job_output_dir,
                seed=seed,
                mode=mode,
            )
            return {
                "request_id": request_id,
                "terminal_state": "success",
                "started_at": started_at,
                "finished_at": utc_now_iso(),
                "account_id": self.account_id,
                "host_alias": self.host_alias,
                "remote_job_id": self.remote_job_id,
                "api_session_id": self.api_session_id,
                "input_toml_hash": input_toml_hash,
                "peetsfea_version": load_peetsfea_version(),
                "mode": mode,
                "seed": seed,
                "output_dir": str(job_output_dir),
                "result": jsonable(result),
            }
        except Exception as exc:
            return {
                "request_id": request_id,
                "terminal_state": "failed",
                "started_at": started_at,
                "finished_at": utc_now_iso(),
                "account_id": self.account_id,
                "host_alias": self.host_alias,
                "remote_job_id": self.remote_job_id,
                "api_session_id": self.api_session_id,
                "input_toml_hash": sha256_text(candidate_toml_text),
                "peetsfea_version": self._safe_version(),
                "mode": mode,
                "seed": seed,
                "result": {},
                "error": {
                    "stage": "simulate",
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                },
            }
        finally:
            self._busy = False
            self._busy_lock.release()

    @staticmethod
    def _request_id(payload: Mapping[str, Any]) -> str:
        raw = payload.get("request_id")
        if raw is None or str(raw).strip() == "":
            return f"request-{uuid.uuid4().hex}"
        request_id = str(raw).strip()
        if "/" in request_id or request_id in {".", ".."}:
            raise SimulationRequestError("request_id must be a simple path segment")
        return request_id

    @staticmethod
    def _candidate_toml_text(payload: Mapping[str, Any]) -> str:
        raw = payload.get("candidate_toml_text")
        if not isinstance(raw, str) or raw.strip() == "":
            raise SimulationRequestError("candidate_toml_text must be a non-empty string")
        return raw

    @staticmethod
    def _seed(payload: Mapping[str, Any]) -> int:
        raw = payload.get("seed", 0)
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise SimulationRequestError("seed must be an integer")
        return raw

    @staticmethod
    def _mode(payload: Mapping[str, Any]) -> str:
        raw = str(payload.get("mode") or "full").strip()
        if raw not in {"full", "semi_dry"}:
            raise SimulationRequestError("mode must be 'full' or 'semi_dry'")
        return raw

    @staticmethod
    def _safe_version() -> str:
        try:
            return load_peetsfea_version()
        except Exception:
            return ""


def read_json(handler: BaseHTTPRequestHandler) -> Mapping[str, Any]:
    raw_length = handler.headers.get("Content-Length", "0")
    try:
        length = int(raw_length)
    except ValueError as exc:
        raise SimulationRequestError("invalid Content-Length") from exc
    body = handler.rfile.read(length) if length > 0 else b"{}"
    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise SimulationRequestError("request body must be valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise SimulationRequestError("request body must be a JSON object")
    return payload


def write_json(handler: BaseHTTPRequestHandler, status: int, payload: Mapping[str, Any]) -> None:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def error_payload(exc: Exception, *, stage: str) -> dict[str, Any]:
    return {"error": {"stage": stage, "type": type(exc).__name__, "message": str(exc)}}


def make_handler(service: SingleSimulationService):
    class Handler(BaseHTTPRequestHandler):
        server_version = "peetsfea-single-simulation-remote"

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            if urlparse(self.path).path != "/health":
                write_json(self, 404, {"error": "not_found"})
                return
            try:
                write_json(self, 200, service.health())
            except Exception as exc:
                write_json(self, 500, error_payload(exc, stage="health"))

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            if path == "/simulate":
                self.handle_simulate()
                return
            if path == "/shutdown":
                write_json(self, 200, {"status": "shutdown"})
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return
            write_json(self, 404, {"error": "not_found"})

        def handle_simulate(self) -> None:
            try:
                envelope = service.simulate(read_json(self))
                status = 200 if envelope.get("terminal_state") == "success" else 500
                write_json(self, status, envelope)
            except SimulationBusyError as exc:
                write_json(self, 409, error_payload(exc, stage="simulate"))
            except SimulationRequestError as exc:
                write_json(self, 400, error_payload(exc, stage="request"))
            except Exception as exc:
                write_json(self, 500, error_payload(exc, stage="simulate"))

    return Handler


def main() -> None:
    output_root = Path(os.environ["PEETS_OUTPUT_ROOT"]).resolve()
    service = SingleSimulationService(
        output_root=output_root,
        account_id=os.environ.get("PEETS_ACCOUNT_ID", "account_01"),
        host_alias=os.environ.get("PEETS_HOST_ALIAS", "gate1-harry261"),
        remote_job_id=os.environ.get("SLURM_JOB_ID", "") or os.environ.get("PEETS_SLURM_JOB_ID", ""),
        api_session_id=os.environ.get("PEETS_API_SESSION_ID", uuid.uuid4().hex),
    )
    host = os.environ.get("PEETS_API_HOST", "127.0.0.1")
    port = int(os.environ["PEETS_API_PORT"])
    server = ThreadingHTTPServer((host, port), make_handler(service))
    ready_path = Path(os.environ.get("PEETS_API_READY_PATH", "/work/api.ready"))
    ready_path.write_text(json.dumps(service.health(), sort_keys=True) + "\n", encoding="utf-8")
    server.serve_forever()
    server.server_close()


if __name__ == "__main__":
    main()
'''


def build_single_simulation_sbatch_script(
    *,
    config: SingleSimulationRemoteConfig,
    session_id: str,
    remote_session_dir: str,
    local_api_port: int,
) -> str:
    remote_session_shell = _remote_shell_path(remote_session_dir)
    image_path = _remote_shell_path(config.remote_container_image)
    ansys_base = _host_ansys_base_root(config.remote_container_ansys_root)
    ansys_root = _host_ansys_mount_root(config.remote_container_ansys_root)
    local_sshfs_session_root = _local_sshfs_session_root(config=config, session_id=session_id)
    workspace_mount_root = _container_sshfs_mount_root(config.container_sshfs_mount_root)
    partition_line = f"#SBATCH -p {config.partition}" if config.partition.strip() else ""
    lines = [
        "#!/usr/bin/env bash",
    ]
    if partition_line:
        lines.append(partition_line)
    lines.extend(
        [
            f"#SBATCH -N {int(config.nodes)}",
            f"#SBATCH -n {int(config.ntasks)}",
            f"#SBATCH -c {int(config.cpus_per_job)}",
            f"#SBATCH --mem={config.mem}",
            f"#SBATCH --time={config.time_limit}",
            "#SBATCH --job-name=peetsfea-single-api",
            "#SBATCH -o slurm-%j.out",
            "#SBATCH -e slurm-%j.err",
            "set -euo pipefail",
            "export PATH=/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}",
            f"PEETS_SESSION_ID={shlex.quote(session_id)}",
            f"PEETS_ACCOUNT_ID={shlex.quote(config.account_id)}",
            f"PEETS_HOST_ALIAS={shlex.quote(config.host_alias)}",
            f"PEETS_REMOTE_SESSION_DIR={_double_quoted_shell_value(remote_session_shell)}",
            f"PEETS_LOCAL_API_PORT={int(local_api_port)}",
            f"PEETS_REMOTE_API_PORT={int(config.remote_api_port)}",
            f"PEETS_CONTROL_RETURN_HOST={shlex.quote(config.control_return_host)}",
            f"PEETS_CONTROL_RETURN_PORT={int(config.control_return_port)}",
            f"PEETS_CONTROL_RETURN_USER={shlex.quote(config.control_return_user)}",
            f"PEETS_LOCAL_SSHFS_ROOT={_double_quoted_shell_value(str(local_sshfs_session_root))}",
            f"PEETS_WORKSPACE_MOUNT_ROOT={_double_quoted_shell_value(workspace_mount_root)}",
            f"REMOTE_CONTAINER_IMAGE={_double_quoted_shell_value(image_path)}",
            f"REMOTE_HOST_ANSYS_ROOT={_double_quoted_shell_value(ansys_root)}",
            f"REMOTE_HOST_ANSYS_BASE={_double_quoted_shell_value(ansys_base)}",
            "export PEETS_SESSION_ID PEETS_ACCOUNT_ID PEETS_HOST_ALIAS PEETS_REMOTE_SESSION_DIR PEETS_LOCAL_API_PORT PEETS_REMOTE_API_PORT",
            "export PEETS_CONTROL_RETURN_HOST PEETS_CONTROL_RETURN_PORT PEETS_CONTROL_RETURN_USER PEETS_LOCAL_SSHFS_ROOT PEETS_WORKSPACE_MOUNT_ROOT",
            "JOB_DIR=\"$PEETS_REMOTE_SESSION_DIR/job-${SLURM_JOB_ID:-manual}\"",
            "RAM_JOB_ROOT=\"/dev/shm/peetsfea-single-api-${SLURM_JOB_ID:-manual}\"",
            "ENROOT_BASE=\"$RAM_JOB_ROOT/enroot\"",
            "CONTAINER_NAME=\"peets-single-api-${SLURM_JOB_ID:-manual}\"",
            "SOCKET_DIR=\"$HOME/.peetsfea-single-api-sockets\"",
            "TUNNEL_SOCKET=\"$SOCKET_DIR/t-${SLURM_JOB_ID:-manual}.sock\"",
            "CONTAINER_PID=\"\"",
            "mkdir -p \"$SOCKET_DIR\"",
            "mkdir -p \"$JOB_DIR\" \"$JOB_DIR/home\" \"$JOB_DIR/container_tmp\" \"$JOB_DIR/ansys_work\" \"$RAM_JOB_ROOT\"",
            "mkdir -p \"$ENROOT_BASE/runtime\" \"$ENROOT_BASE/cache\" \"$ENROOT_BASE/data\" \"$ENROOT_BASE/tmp\"",
            "chmod 700 \"$ENROOT_BASE/runtime\" \"$ENROOT_BASE/cache\" \"$ENROOT_BASE/data\" \"$ENROOT_BASE/tmp\"",
            "select_control_identity() {",
            "  for candidate in \"$HOME/.ssh/id_ed25519\" \"$HOME/.ssh/id_ed25519_codex_to_pc\" \"$HOME/.ssh/id_rsa\"; do",
            "    if [ -r \"$candidate\" ]; then printf '%s\\n' \"$candidate\"; return 0; fi",
            "  done",
            "  return 1",
            "}",
            "cleanup() {",
            "  rc=$?",
            "  if [ -S \"$TUNNEL_SOCKET\" ]; then",
            "    ssh -F /dev/null -p \"$PEETS_CONTROL_RETURN_PORT\" -S \"$TUNNEL_SOCKET\" -O exit \"$PEETS_CONTROL_RETURN_USER@$PEETS_CONTROL_RETURN_HOST\" >/dev/null 2>&1 || true",
            "  fi",
            "  rm -f \"$TUNNEL_SOCKET\" >/dev/null 2>&1 || true",
            "  if [ -n \"$CONTAINER_PID\" ]; then kill \"$CONTAINER_PID\" >/dev/null 2>&1 || true; wait \"$CONTAINER_PID\" >/dev/null 2>&1 || true; fi",
            "  ENROOT_RUNTIME_PATH=\"$ENROOT_BASE/runtime\" ENROOT_CACHE_PATH=\"$ENROOT_BASE/cache\" ENROOT_DATA_PATH=\"$ENROOT_BASE/data\" ENROOT_TEMP_PATH=\"$ENROOT_BASE/tmp\" enroot remove -f \"$CONTAINER_NAME\" >/dev/null 2>&1 || true",
            "  rm -rf \"$RAM_JOB_ROOT\" >/dev/null 2>&1 || true",
            "  exit \"$rc\"",
            "}",
            "trap cleanup EXIT",
            "cd \"$JOB_DIR\"",
            "printf 'session_id=%s\\n' \"$PEETS_SESSION_ID\" > launch_probe.txt",
            "printf 'slurm_job_id=%s\\n' \"${SLURM_JOB_ID:-}\" >> launch_probe.txt",
            "printf 'hostname=%s\\n' \"$(hostname 2>/dev/null || true)\" >> launch_probe.txt",
            "printf 'job_dir=%s\\n' \"$JOB_DIR\" >> launch_probe.txt",
            "printf 'ram_job_root=%s\\n' \"$RAM_JOB_ROOT\" >> launch_probe.txt",
            "printf 'local_api_port=%s\\n' \"$PEETS_LOCAL_API_PORT\" >> launch_probe.txt",
            "printf 'remote_api_port=%s\\n' \"$PEETS_REMOTE_API_PORT\" >> launch_probe.txt",
            "printf 'local_sshfs_root=%s\\n' \"$PEETS_LOCAL_SSHFS_ROOT\" >> launch_probe.txt",
            "tar --no-same-owner -xzf \"$PEETS_REMOTE_SESSION_DIR/peetsfea_source.tgz\" -C \"$JOB_DIR\"",
            "cat > \"$JOB_DIR/container_run.sh\" <<'EOS'",
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "SSHFS_MOUNTED=0",
            "PEETS_RAM_ROOT=\"\"",
            "cleanup_container() {",
            "  rc=$?",
            "  if [ \"$SSHFS_MOUNTED\" = \"1\" ]; then",
            "    fusermount3 -u \"$PEETS_WORKSPACE_MOUNT_ROOT\" >/dev/null 2>&1 || fusermount -u \"$PEETS_WORKSPACE_MOUNT_ROOT\" >/dev/null 2>&1 || umount \"$PEETS_WORKSPACE_MOUNT_ROOT\" >/dev/null 2>&1 || true",
            "  fi",
            "  if [ -n \"$PEETS_RAM_ROOT\" ] && [ -d \"$PEETS_RAM_ROOT\" ]; then rm -rf \"$PEETS_RAM_ROOT\" >/dev/null 2>&1 || true; fi",
            "  exit \"$rc\"",
            "}",
            "trap cleanup_container EXIT",
            "mkdir -p /work/home /work/container_tmp /work/ansys_work",
            "export HOME=/work/home",
            "export XDG_CONFIG_HOME=/work/home/.config",
            "source /work/container_env.sh",
            "PEETS_RAM_ROOT=\"/dev/shm/peetsfea-single-api-${PEETS_API_SESSION_ID:-manual}\"",
            "if mkdir -p \"$PEETS_RAM_ROOT/tmp\" \"$PEETS_RAM_ROOT/ansys_work\" >/dev/null 2>&1; then",
            "  export TMP=\"$PEETS_RAM_ROOT/tmp\"",
            "  export TEMP=\"$PEETS_RAM_ROOT/tmp\"",
            "  export TMPDIR=\"$PEETS_RAM_ROOT/tmp\"",
            "  export ANSYS_WORK_DIR=\"$PEETS_RAM_ROOT/ansys_work\"",
            "else",
            "  export TMP=/work/container_tmp",
            "  export TEMP=/work/container_tmp",
            "  export TMPDIR=/work/container_tmp",
            "  export ANSYS_WORK_DIR=/work/ansys_work",
            "fi",
            "export UV_CACHE_DIR=\"$PEETS_RAM_ROOT/uv_cache\"",
            "export PIP_CACHE_DIR=\"$PEETS_RAM_ROOT/pip_cache\"",
            "export UV_LINK_MODE=copy",
            "mkdir -p \"$UV_CACHE_DIR\" \"$PIP_CACHE_DIR\"",
            "export ANSYSLMD_LICENSE_FILE=${ANSYSLMD_LICENSE_FILE:-1055@172.16.10.81}",
            "export ANSYSEM_ROOT252=/mnt/AnsysEM",
            "export ANS_IGNOREOS=1",
            "mkdir -p \"$PEETS_WORKSPACE_MOUNT_ROOT\" /etc/peetsfea_ssh",
            "test -f /etc/peetsfea_ssh/id_control || { echo \"[ERROR] missing container SSH identity: /etc/peetsfea_ssh/id_control\" >&2; exit 1; }",
            "chmod 700 /etc/peetsfea_ssh >/dev/null 2>&1 || true",
            "chmod 600 /etc/peetsfea_ssh/id_control >/dev/null 2>&1 || true",
            "sshfs -p \"$PEETS_CONTROL_RETURN_PORT\" -o reconnect,follow_symlinks,ServerAliveInterval=15,ServerAliveCountMax=3,idmap=user,uid=0,gid=0,umask=000,StrictHostKeyChecking=no,UserKnownHostsFile=/dev/null,IdentityFile=/etc/peetsfea_ssh/id_control \"$PEETS_WORKSPACE_REMOTE\" \"$PEETS_WORKSPACE_MOUNT_ROOT\"",
            "SSHFS_MOUNTED=1",
            "mkdir -p \"$PEETS_WORKSPACE_MOUNT_ROOT/output\"",
            "export PEETS_OUTPUT_ROOT=\"$PEETS_WORKSPACE_MOUNT_ROOT/output\"",
            "export PYTHONPATH=/work/peetsfea/src:/work/peetsfea:${PYTHONPATH:-}",
            "export PATH=/opt/miniconda3/bin:/mnt/AnsysEM:/ansys_inc/v252/AnsysEM:${PATH:-}",
            "BASE_PREFIX=/opt/miniconda3",
            "IMAGE_LD_LIBRARY_PATH=\"$BASE_PREFIX/lib:${LD_LIBRARY_PATH:-}\"",
            "cd /work",
            "LD_LIBRARY_PATH=\"$IMAGE_LD_LIBRARY_PATH\" /opt/miniconda3/bin/python - <<'PY'",
            "import importlib.util, subprocess, sys",
            "required = [('cadquery', 'cadquery'), ('ocp_vscode', 'ocp-vscode>=3.1.2'), ('psutil', 'psutil')]",
            "missing = [spec for module, spec in required if importlib.util.find_spec(module) is None]",
            "if missing:",
            "    try:",
            "        subprocess.check_call([sys.executable, '-m', 'uv', 'pip', 'install', *missing])",
            "    except Exception:",
            "        subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing])",
            "PY",
            "LD_LIBRARY_PATH=\"$IMAGE_LD_LIBRARY_PATH\" /opt/miniconda3/bin/python /work/remote_single_api_server.py",
            "EOS",
            "chmod +x \"$JOB_DIR/container_run.sh\"",
            "cat > \"$JOB_DIR/container_env.sh\" <<EOS",
            "export PEETS_API_SESSION_ID=\"$PEETS_SESSION_ID\"",
            "export PEETS_API_HOST=\"127.0.0.1\"",
            "export PEETS_API_PORT=\"$PEETS_REMOTE_API_PORT\"",
            "export PEETS_API_READY_PATH=\"/work/api.ready\"",
            "export PEETS_ACCOUNT_ID=\"$PEETS_ACCOUNT_ID\"",
            "export PEETS_HOST_ALIAS=\"$PEETS_HOST_ALIAS\"",
            "export PEETS_SLURM_JOB_ID=\"${SLURM_JOB_ID:-}\"",
            "export PEETS_CONTROL_RETURN_HOST=\"$PEETS_CONTROL_RETURN_HOST\"",
            "export PEETS_CONTROL_RETURN_PORT=\"$PEETS_CONTROL_RETURN_PORT\"",
            "export PEETS_CONTROL_RETURN_USER=\"$PEETS_CONTROL_RETURN_USER\"",
            "export PEETS_LOCAL_SSHFS_ROOT=\"$PEETS_LOCAL_SSHFS_ROOT\"",
            "export PEETS_WORKSPACE_MOUNT_ROOT=\"$PEETS_WORKSPACE_MOUNT_ROOT\"",
            "export PEETS_WORKSPACE_REMOTE=\"${PEETS_CONTROL_RETURN_USER}@${PEETS_CONTROL_RETURN_HOST}:${PEETS_LOCAL_SSHFS_ROOT}\"",
            "EOS",
            "CONTROL_IDENTITY=\"$(select_control_identity)\"",
            "ssh_mount_dir=\"$JOB_DIR/container_ssh\"",
            "mkdir -p \"$ssh_mount_dir\"",
            "cp -f \"$CONTROL_IDENTITY\" \"$ssh_mount_dir/id_control\"",
            "chmod 700 \"$ssh_mount_dir\"",
            "chmod 600 \"$ssh_mount_dir/id_control\"",
            "cp -f \"$PEETS_REMOTE_SESSION_DIR/remote_single_api_server.py\" \"$JOB_DIR/remote_single_api_server.py\"",
            "ENROOT_RUNTIME_PATH=\"$ENROOT_BASE/runtime\" ENROOT_CACHE_PATH=\"$ENROOT_BASE/cache\" ENROOT_DATA_PATH=\"$ENROOT_BASE/data\" ENROOT_TEMP_PATH=\"$ENROOT_BASE/tmp\" enroot create -f -n \"$CONTAINER_NAME\" \"$REMOTE_CONTAINER_IMAGE\" > enroot.create.stdout 2> enroot.create.stderr",
            "PEETS_API_SESSION_ID=\"$PEETS_SESSION_ID\" PEETS_API_HOST=127.0.0.1 PEETS_API_PORT=\"$PEETS_REMOTE_API_PORT\" PEETS_API_READY_PATH=/work/api.ready PEETS_ACCOUNT_ID=\"$PEETS_ACCOUNT_ID\" PEETS_HOST_ALIAS=\"$PEETS_HOST_ALIAS\" \\",
            "  ENROOT_RUNTIME_PATH=\"$ENROOT_BASE/runtime\" ENROOT_CACHE_PATH=\"$ENROOT_BASE/cache\" ENROOT_DATA_PATH=\"$ENROOT_BASE/data\" ENROOT_TEMP_PATH=\"$ENROOT_BASE/tmp\" \\",
            "  enroot start --root --rw --mount \"$REMOTE_HOST_ANSYS_ROOT:/mnt/AnsysEM\" --mount \"$REMOTE_HOST_ANSYS_BASE:/ansys_inc/v252\" --mount \"$REMOTE_HOST_ANSYS_BASE/licensingclient:/mnt/licensingclient\" --mount \"$JOB_DIR:/work\" --mount \"$ssh_mount_dir:/etc/peetsfea_ssh\" --mount \"/dev/fuse:/dev/fuse\" \"$CONTAINER_NAME\" /bin/bash /work/container_run.sh > container.stdout 2> container.stderr &",
            "CONTAINER_PID=$!",
            "wait_for_container_api() {",
            "  python3 - <<'PY'",
            "import os, sys, time, urllib.request",
            "url = f\"http://127.0.0.1:{os.environ['PEETS_REMOTE_API_PORT']}/health\"",
            "deadline = time.monotonic() + 300",
            "last = ''",
            "while time.monotonic() < deadline:",
            "    try:",
            "        with urllib.request.urlopen(url, timeout=5) as response:",
            "            body = response.read().decode('utf-8')",
            "            print(body)",
            "            sys.exit(0)",
            "    except Exception as exc:",
            "        last = str(exc)",
            "        time.sleep(2)",
            "print(last, file=sys.stderr)",
            "sys.exit(1)",
            "PY",
            "}",
            "wait_for_container_api > api.health.json",
            "ssh -F /dev/null -p \"$PEETS_CONTROL_RETURN_PORT\" -o BatchMode=yes -o ExitOnForwardFailure=yes -o IdentitiesOnly=yes -i \"$CONTROL_IDENTITY\" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -M -S \"$TUNNEL_SOCKET\" -fnNT -R \"127.0.0.1:${PEETS_LOCAL_API_PORT}:127.0.0.1:${PEETS_REMOTE_API_PORT}\" \"$PEETS_CONTROL_RETURN_USER@$PEETS_CONTROL_RETURN_HOST\"",
            "printf 'reverse_tunnel=ready\\n' >> launch_probe.txt",
            "printf '__PEETSFEA_SINGLE_API_READY__ session=%s job=%s local_port=%s remote_port=%s\\n' \"$PEETS_SESSION_ID\" \"${SLURM_JOB_ID:-}\" \"$PEETS_LOCAL_API_PORT\" \"$PEETS_REMOTE_API_PORT\"",
            "wait \"$CONTAINER_PID\"",
        ]
    )
    return "\n".join(lines) + "\n"


def _prepare_stage_dir(
    *,
    config: SingleSimulationRemoteConfig,
    session_id: str,
    remote_session_dir: str,
    local_api_port: int,
) -> Path:
    stage_dir = config.stage_root.expanduser().resolve() / session_id
    stage_dir.mkdir(parents=True, exist_ok=True)
    (stage_dir / REMOTE_SERVER_FILENAME).write_text(build_remote_single_simulation_server_script(), encoding="utf-8")
    (stage_dir / SBATCH_FILENAME).write_text(
        build_single_simulation_sbatch_script(
            config=config,
            session_id=session_id,
            remote_session_dir=remote_session_dir,
            local_api_port=local_api_port,
        ),
        encoding="utf-8",
    )
    _create_peetsfea_source_archive(
        source_path=config.peetsfea_source_path.expanduser().resolve(),
        archive_path=stage_dir / PEETSFEA_SOURCE_ARCHIVE_NAME,
        timeout_seconds=config.command_timeout_seconds,
    )
    return stage_dir


def _prepare_local_sshfs_session_root(*, config: SingleSimulationRemoteConfig, session_id: str) -> Path:
    session_root = _local_sshfs_session_root(config=config, session_id=session_id)
    (session_root / "output").mkdir(parents=True, exist_ok=True)
    return session_root


def _local_sshfs_session_root(*, config: SingleSimulationRemoteConfig, session_id: str) -> Path:
    return config.local_sshfs_root.expanduser().resolve() / session_id


def _create_peetsfea_source_archive(*, source_path: Path, archive_path: Path, timeout_seconds: int) -> None:
    if not (source_path / "pyproject.toml").is_file():
        raise FileNotFoundError(f"peetsfea source path is missing pyproject.toml: {source_path}")
    if not (source_path / "src" / "peetsfea").is_dir():
        raise FileNotFoundError(f"peetsfea source path is missing src/peetsfea: {source_path}")
    archive_members = [
        member
        for member in ("pyproject.toml", "src", "entry", "examples", "notebooks")
        if (source_path / member).exists()
    ]
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "tar",
        "--exclude=__pycache__",
        "-czf",
        str(archive_path),
        "-C",
        str(source_path),
        "--transform",
        "s,^,peetsfea/,",
        *archive_members,
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=timeout_seconds)
    _raise_for_command_failure(completed, stage="peetsfea source archive")


def _ensure_remote_session_dir(
    *,
    config: SingleSimulationRemoteConfig,
    remote_session_dir: str,
    run_command: CommandRunner,
) -> None:
    command = [
        *_ssh_base_command(config),
        config.host_alias,
        f"mkdir -p {_quote_remote_shell_path(remote_session_dir)}",
    ]
    completed = run_command(command)
    _raise_for_command_failure(completed, stage="remote session mkdir")


def _upload_stage_dir(
    *,
    config: SingleSimulationRemoteConfig,
    remote_session_dir: str,
    stage_dir: Path,
    run_command: CommandRunner,
) -> None:
    target = f"{config.host_alias}:{_scp_remote_path(remote_session_dir)}/"
    command = [
        "scp",
        * _scp_config_args(config),
        str(stage_dir / REMOTE_SERVER_FILENAME),
        str(stage_dir / SBATCH_FILENAME),
        str(stage_dir / PEETSFEA_SOURCE_ARCHIVE_NAME),
        target,
    ]
    completed = run_command(command)
    _raise_for_command_failure(completed, stage="remote stage upload")


def _submit_remote_sbatch(
    *,
    config: SingleSimulationRemoteConfig,
    remote_session_dir: str,
    run_command: CommandRunner,
) -> str:
    command = [
        *_ssh_base_command(config),
        config.host_alias,
        "cd "
        + _quote_remote_shell_path(remote_session_dir)
        + f" && sbatch --parsable ./{SBATCH_FILENAME}",
    ]
    completed = run_command(command)
    _raise_for_command_failure(completed, stage="remote sbatch submit")
    return _parse_sbatch_job_id(completed.stdout)


def _parse_sbatch_job_id(output: str) -> str:
    first_line = (output or "").strip().splitlines()[0] if (output or "").strip() else ""
    job_id = first_line.split(";", 1)[0].strip()
    if not job_id or not job_id[0].isdigit():
        raise RuntimeError(f"unable to parse sbatch job id from output: {output!r}")
    return job_id


def _default_run_command(timeout_seconds: int) -> CommandRunner:
    def _run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )

    return _run


def _raise_for_command_failure(completed: subprocess.CompletedProcess[str], *, stage: str) -> None:
    if completed.returncode == 0:
        return
    details = "\n".join(part.strip() for part in (completed.stdout, completed.stderr) if part and part.strip())
    if not details:
        details = f"returncode={completed.returncode}"
    raise RuntimeError(f"{stage} failed: {details}")


def _ssh_base_command(config: SingleSimulationRemoteConfig) -> list[str]:
    command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
    ssh_config_path = str(config.ssh_config_path).strip()
    if ssh_config_path:
        command.extend(["-F", ssh_config_path])
    return command


def _scp_config_args(config: SingleSimulationRemoteConfig) -> list[str]:
    ssh_config_path = str(config.ssh_config_path).strip()
    return ["-F", ssh_config_path] if ssh_config_path else []


def _join_remote_path(root: str, *parts: str) -> str:
    normalized = root.rstrip("/")
    suffix = "/".join(part.strip("/") for part in parts if part.strip("/"))
    return f"{normalized}/{suffix}" if suffix else normalized


def _remote_shell_path(path: str) -> str:
    normalized = str(path).strip()
    if normalized == "~":
        return "$HOME"
    if normalized.startswith("~/"):
        return f"$HOME/{normalized[2:]}"
    return normalized


def _scp_remote_path(path: str) -> str:
    normalized = str(path).strip()
    if normalized.startswith("$HOME/"):
        return "~/" + normalized[len("$HOME/") :]
    return normalized


def _quote_remote_shell_path(path: str) -> str:
    normalized = _remote_shell_path(path)
    if normalized == "$HOME":
        return "$HOME"
    if normalized.startswith("$HOME/"):
        quoted_parts = [shlex.quote(part) for part in normalized[len("$HOME/") :].split("/") if part]
        return "$HOME/" + "/".join(quoted_parts)
    return shlex.quote(normalized)


def _host_ansys_mount_root(path: str) -> str:
    normalized = _remote_shell_path(path).rstrip("/")
    if normalized.endswith("/AnsysEM"):
        return normalized
    return f"{normalized}/AnsysEM"


def _host_ansys_base_root(path: str) -> str:
    normalized = _remote_shell_path(path).rstrip("/")
    if normalized.endswith("/AnsysEM"):
        return str(Path(normalized).parent).rstrip("/")
    return normalized


def _container_sshfs_mount_root(path: str) -> str:
    normalized = str(path).strip() or DEFAULT_CONTAINER_SSHFS_MOUNT_ROOT
    if not normalized.startswith("/"):
        raise ValueError(f"container_sshfs_mount_root must be absolute: {path!r}")
    return normalized.rstrip("/") or "/"


def _double_quoted_shell_value(value: str) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def _new_session_id() -> str:
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"single-api-{timestamp}-{uuid.uuid4().hex[:8]}"


def _allocate_local_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


__all__ = [
    "SingleSimulationRemoteConfig",
    "SingleSimulationRemoteSession",
    "build_remote_single_simulation_server_script",
    "build_single_simulation_sbatch_script",
    "cancel_single_simulation_remote_session",
    "shutdown_single_simulation_remote_api",
    "start_single_simulation_remote_api",
    "wait_for_single_simulation_remote_health",
]
