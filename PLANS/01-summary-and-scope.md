# Plan 01: Summary and Scope

## Purpose

이 프로젝트는 AEDT 시뮬레이션을 Slurm 인터랙티브 잡과 `screen` 세션 안에서 자동 실행하고, 결과를 다시 로컬로 가져오는 파이프라인을 구현한다.

## Core Principles

1. 실행 경로는 Python 함수 호출 단일 경로만 허용한다.
2. CLI(`argparse`, `click`, `typer`, `python -m ...`)는 사용하지 않는다.
3. 공개 진입점은 `run_pipeline(config)` 하나만 허용한다.
4. 기존 `run.py`는 예시 파일로 간주하고 저장소에서 제거한다.

## Delivery Scope

1. 함수 기반 오케스트레이션 모듈 구현
2. 원격 `ssh -> srun --pty -> screen` 제어 자동화
3. `.aedt` 업로드, 실행, 결과 다운로드 자동화
4. 실패 코드/로그 표준화
5. GitHub 태그 기반 배포 및 원격 PC 설치 절차 문서화

## Out of Scope

1. 웹 UI 또는 서비스형 운영
2. 다중 사용자 작업 큐 관리
3. CLI 기반 실행 지원

## Repository and Release Target

1. 원격 저장소: `https://github.com/Glaysia/peetsfea-runner`
2. 릴리스 단위: Git tag
3. 원격 PC 설치: 태그 지정 `uv pip install git+...` 방식

