# Plan 04: Release, Tag, and Remote Installation

## Target Repository

1. `https://github.com/Glaysia/peetsfea-runner`

## Release Strategy

1. 변경 완료 후 원격 저장소에 push
2. 배포 단위는 Git tag로 고정
3. 원격 PC는 매 실행 주기마다 태그 지정 `uv pip install git+...`로 설치/업데이트

## Tagging Rules

1. 태그는 불변 아티팩트로 취급한다.
2. 새 배포는 반드시 새 태그를 발행한다.
3. 태그 패턴 예시: `v2026.03.04.1`

## Operator Commands

릴리스(개발 PC):

```bash
git push origin main
git tag v2026.03.04.1
git push origin v2026.03.04.1
```

원격 PC 설치/업데이트(매번 수행):

```bash
uv pip install "git+https://github.com/Glaysia/peetsfea-runner.git@v2026.03.04.1"
```

## Installation Policy

1. 항상 태그를 명시해 재현성을 보장한다.
2. 브랜치명(`main`) 직접 설치는 운영 경로에서 금지한다.
3. 설치 후 Python에서 `run_pipeline` import/호출로만 실행한다.

## Packaging Requirements

1. `pyproject.toml` 기반 패키징을 사용한다.
2. 함수 API 모듈이 import 가능해야 한다.
3. CLI entry point 정의(`scripts`, `console_scripts`)는 추가하지 않는다.

