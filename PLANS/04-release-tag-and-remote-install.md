# Plan 04: Release, Tag, and Remote Installation

## Target Repository

1. `https://github.com/Glaysia/peetsfea-runner`

## Release Strategy

1. 변경 완료 후 원격 저장소에 push
2. 배포 단위는 Git tag로 고정
3. 원격 PC는 매 실행 주기마다 태그 지정 설치/업데이트를 수행한다.
4. 원격 PC 표준 가상환경 경로는 `~/.peetsfea-runner-venv`로 고정한다.
5. 원격 PC는 항상 `~/miniconda3`를 기반으로 Python 3.12를 준비한다.

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
bash scripts/remote_bootstrap_install.sh v2026.03.04.4
```

수동 설치(필요 시):

```bash
~/.peetsfea-runner-venv/bin/python -m pip install --upgrade pip
~/.peetsfea-runner-venv/bin/python -m pip install "git+https://github.com/Glaysia/peetsfea-runner.git@v2026.03.04.4"
~/.peetsfea-runner-venv/bin/python -c "import peetsfea_runner; print('ok')"
```

## Installation Policy

1. 항상 태그를 명시해 재현성을 보장한다.
2. 브랜치명(`main`) 직접 설치는 운영 경로에서 금지한다.
3. 설치 후 Python에서 `run_pipeline` import/호출로만 실행한다.
4. 가상환경은 항상 `~/.peetsfea-runner-venv` 단일 경로만 사용한다.
5. `~/.peetsfea-runner-venv/bin/python -V`가 3.12가 아니면 venv를 재생성한다.
6. 원격 Python 3.12는 항상 `~/miniconda3/envs/peetsfea-runner-py312`에서 공급한다.

## Remote Bootstrap (Miniconda3 + Python 3.12)

고정 순서:

1. `~/miniconda3/bin/conda` 존재 확인
2. 없으면 Miniconda3를 사용자 홈(`~/miniconda3`)에 비대화형 설치
3. `conda create -y -n peetsfea-runner-py312 python=3.12`로 Python 3.12 환경 생성/보정
4. `~/miniconda3/envs/peetsfea-runner-py312/bin/python -m venv ~/.peetsfea-runner-venv`로 가상환경 생성/재생성
5. `~/.peetsfea-runner-venv/bin/python -m pip install --upgrade pip`
6. `~/.peetsfea-runner-venv/bin/python -m pip install "git+https://github.com/Glaysia/peetsfea-runner.git@<tag>"`
7. `~/.peetsfea-runner-venv/bin/python -c "import peetsfea_runner; print('ok')"` 검증

실패 처리:

1. Miniconda3 설치 실패: 권한/디스크/네트워크 점검 메시지 출력 후 중단
2. conda Python 3.12 환경 생성 실패: 네트워크/권한 점검 메시지 출력 후 중단
3. 태그 설치 실패: 태그 존재/접근 권한/프록시 점검 메시지 출력 후 중단

## Packaging Requirements

1. `pyproject.toml` 기반 패키징을 사용한다.
2. 함수 API 모듈이 import 가능해야 한다.
3. CLI entry point 정의(`scripts`, `console_scripts`)는 추가하지 않는다.
4. `requires-python`은 `==3.12`를 유지한다.
