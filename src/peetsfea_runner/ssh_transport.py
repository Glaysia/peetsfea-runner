from __future__ import annotations

import os
import tempfile
from pathlib import Path

_CONTROL_SOCKET_DIR = Path(tempfile.gettempdir()) / "peetsfea-runner-ssh-control"
_CONTROL_PERSIST_SECONDS = 600


def _ensure_control_socket_dir() -> str:
    _CONTROL_SOCKET_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(_CONTROL_SOCKET_DIR, 0o700)
    except OSError:
        # Best effort only: some platforms/filesystems ignore chmod.
        pass
    return str(_CONTROL_SOCKET_DIR / "%C")


def _connection_options() -> list[str]:
    control_path = _ensure_control_socket_dir()
    return [
        "-o",
        "ControlMaster=auto",
        "-o",
        f"ControlPersist={_CONTROL_PERSIST_SECONDS}s",
        "-o",
        f"ControlPath={control_path}",
    ]


def ssh_command(*, remote_host: str, remote_command: str) -> list[str]:
    return ["ssh", *_connection_options(), remote_host, remote_command]


def scp_to_remote_command(*, local_path: str, remote_host: str, remote_path: str) -> list[str]:
    return ["scp", *_connection_options(), local_path, f"{remote_host}:{remote_path}"]


def scp_from_remote_command(*, remote_host: str, remote_path: str, local_path: str) -> list[str]:
    return ["scp", *_connection_options(), f"{remote_host}:{remote_path}", local_path]
