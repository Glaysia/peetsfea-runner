from __future__ import annotations

from pathlib import Path

import pytest

from peetsfea_runner.job_workspace import (
    ForbiddenPathError,
    JobWorkspaceConfig,
    ensure_allowed_path,
    is_forbidden_path,
    job_workspace,
    job_workspace_dir,
)


def test_forbidden_paths() -> None:
    assert is_forbidden_path("/dev/shm")
    assert is_forbidden_path("/dev/shm/foo/bar")
    assert is_forbidden_path("/tmp")
    assert is_forbidden_path("/tmp/peets/x")
    assert not is_forbidden_path("/enroot/peets_123")
    assert not is_forbidden_path("/home/peets/work")


def test_ensure_allowed_raises_on_forbidden() -> None:
    with pytest.raises(ForbiddenPathError):
        ensure_allowed_path("/tmp/whatever")
    with pytest.raises(ForbiddenPathError):
        ensure_allowed_path("/dev/shm/x")
    # 허용 경로는 resolved Path 반환
    assert ensure_allowed_path("/enroot/peets_1").name == "peets_1"


def test_job_workspace_dir_naming() -> None:
    cfg = JobWorkspaceConfig(enroot_root=Path("/enroot"), user="harry261", job_id="680145")
    assert job_workspace_dir(cfg) == Path("/enroot/harry261_680145")


def test_job_workspace_create_and_cleanup(tmp_path: Path) -> None:
    cfg = JobWorkspaceConfig(enroot_root=tmp_path / "enroot", user="peets", job_id="42")
    expected = tmp_path / "enroot" / "peets_42"
    with job_workspace(cfg) as paths:
        assert paths.root == expected
        assert paths.root.is_dir()
        assert paths.job_tmpfs.is_dir()
        assert paths.job_disk.is_dir()
        # 작업물 흔적
        (paths.job_disk / "marker").write_text("x", encoding="utf-8")
    # 종료 시 통째로 삭제
    assert not expected.exists()


def test_job_workspace_cleans_up_on_exception(tmp_path: Path) -> None:
    cfg = JobWorkspaceConfig(enroot_root=tmp_path / "enroot", user="peets", job_id="99")
    expected = tmp_path / "enroot" / "peets_99"
    with pytest.raises(ValueError):
        with job_workspace(cfg) as paths:
            assert paths.root.is_dir()
            raise ValueError("boom")
    assert not expected.exists()
