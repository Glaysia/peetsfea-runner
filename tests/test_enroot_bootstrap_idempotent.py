"""부트스트랩 멱등성 검증 (비파괴적).

enroot_image_bootstrap.sh 는 `image_is_current`(이미지+메타데이터+contract_version 일치) 게이트로
멱등하다: 웜 캐시면 재빌드 없이 마커만 찍고 즉시 종료, 와이프되면 재빌드로 진행한다.
실제 `rm -rf ~/*` + 서비스 재시작은 파괴적이라 여기선 안 돌리고, 게이트 결정만 행동 검증한다.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "peetsfea_runner" / "enroot_image_bootstrap.sh"
MARKER = "__PEETSFEA_BOOTSTRAP__:ok"
CONTRACT = "test-contract-idempotent-001"


def _run(tmp_path: Path, env_overrides: dict[str, str]) -> subprocess.CompletedProcess[str]:
    # 재빌드를 시도하면 호출될 enroot 스텁(즉시 실패) — 빌드 진입을 탐지/차단.
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    stub = bindir / "enroot"
    stub.write_text("#!/usr/bin/env bash\necho STUB_ENROOT_CALLED >&2\nexit 1\n", encoding="utf-8")
    stub.chmod(0o755)
    env = {
        **os.environ,
        "HOME": str(tmp_path),  # $HOME 오염 방지
        "PATH": f"{bindir}:{os.environ.get('PATH', '')}",
        "BUILD_TMP_PARENT": str(tmp_path / "build_tmp"),
        "CONTRACT_VERSION": CONTRACT,
        **env_overrides,
    }
    return subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )


def test_warm_cache_skips_rebuild(tmp_path: Path) -> None:
    """이미지+메타(contract 일치) 존재 → 재빌드 없이 즉시 스킵(웜=빠름)."""
    image = tmp_path / "aedt.sqsh"
    image.write_text("fake-image", encoding="utf-8")
    meta = tmp_path / "aedt.sqsh.meta.json"
    meta.write_text(f'{{"contract_version":"{CONTRACT}"}}', encoding="utf-8")

    result = _run(tmp_path, {"TARGET_IMAGE": str(image), "METADATA_PATH": str(meta)})

    assert result.returncode == 0, result.stderr
    assert MARKER in result.stdout
    assert "STUB_ENROOT_CALLED" not in result.stderr  # 재빌드 안 함


def test_wiped_does_not_skip_and_attempts_rebuild(tmp_path: Path) -> None:
    """이미지 없음(= rm -rf 후) → 스킵 마커 없이 재빌드로 진행."""
    image = tmp_path / "aedt.sqsh"  # 생성 안 함
    meta = tmp_path / "aedt.sqsh.meta.json"

    result = _run(tmp_path, {"TARGET_IMAGE": str(image), "METADATA_PATH": str(meta)})

    # 웜 스킵 경로로 빠지지 않았다(= 재빌드 필요로 판정).
    assert MARKER not in result.stdout
    # 재빌드 진입 → enroot 스텁 호출(실제 빌드 대신 실패).
    assert "STUB_ENROOT_CALLED" in result.stderr
    assert result.returncode != 0


def test_stale_contract_does_not_skip(tmp_path: Path) -> None:
    """이미지는 있으나 contract_version 불일치 → 스킵 안 하고 재빌드."""
    image = tmp_path / "aedt.sqsh"
    image.write_text("fake-image", encoding="utf-8")
    meta = tmp_path / "aedt.sqsh.meta.json"
    meta.write_text('{"contract_version":"OLD-different-contract"}', encoding="utf-8")

    result = _run(tmp_path, {"TARGET_IMAGE": str(image), "METADATA_PATH": str(meta)})

    assert MARKER not in result.stdout
    assert "STUB_ENROOT_CALLED" in result.stderr
