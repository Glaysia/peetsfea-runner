# Plan 03: Remote Execution Workflow

## Fixed Workflow

1. 로컬 입력 검증(`input_aedt_path` 존재, 확장자, 읽기 권한)
2. 원격 실행 디렉토리 생성(`~/aedt_runs/<run_id>`)
3. 입력 `.aedt` 및 실행 스크립트 업로드
4. `ssh gate1-harry` 접속 (`pexpect`)
5. `srun --pty -p cpu2 -N 1 -n 1 -c 32 --mem=320G --time=05:00:00 bash`
6. 잡 내부에서 `screen` 세션 1개/윈도우 1개 생성 및 검증
7. `screen` 내부 최초 1회 환경 초기화
8. 원격 Python 환경 구성(venv + pip + pyaedt)
9. 시뮬레이션 실행 함수 호출
10. 원격 결과 폴더 압축
11. 로컬 다운로드 및 압축 해제
12. `PipelineResult` 반환

## Screen Internal One-Time Environment Setup

아래 명령은 `screen` 세션 안에서 최초 1회만 실행한다.

```bash
export ANSYSEM_ROOT252=/opt/ohpc/pub/Electronics/v252/AnsysEM
export SCREENDIR="$HOME/.screen"
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
unset LANGUAGE
export ANSYSLMD_LICENSE_FILE=1055@172.16.10.81
module load ansys-electronics/v252
```

운영 규칙:

1. 동일 세션 재진입 시 중복 실행하지 않는다.
2. 상태 파일(예: `.env_initialized`)로 1회 실행 여부를 기록한다.

## Retry Policy

1. 재시도 대상: `ssh`, `scp 업로드`, `srun`, `scp 다운로드`
2. 단계별 최대 1회 재시도
3. 재시도 간격: 10초
4. 2회 모두 실패 시 즉시 종료하고 로그 보존

## Artifact Policy

1. 로컬 산출물: `./artifacts/<run_id>/`
2. 원격 산출물: `~/aedt_runs/<run_id>/`
3. 최소 포함 파일: 실행 로그, 종료 코드 파일, 결과 아카이브

