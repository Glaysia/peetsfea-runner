# 09: systemd --user 서비스 설치/링크 가이드

## 목적

1. 저장소의 `systemd/peetsfea-runner.service`를 사용자 서비스로 링크한다.
2. 서비스는 함수 호출 경로(`run_pipeline(config)`)를 내부적으로 반복 실행한다.

## 서비스 파일 위치

1. 소스: `systemd/peetsfea-runner.service`
2. 사용자 링크 대상: `~/.config/systemd/user/peetsfea-runner.service`

## 설치 절차 (심볼릭 링크)

```bash
mkdir -p ~/.config/systemd/user
ln -sfn ~/Projects/PythonProjects/peetsfea-runner/systemd/peetsfea-runner.service ~/.config/systemd/user/peetsfea-runner.service
systemctl --user daemon-reload
systemctl --user enable --now peetsfea-runner.service
```

## 운영 확인

```bash
systemctl --user status peetsfea-runner.service
journalctl --user -u peetsfea-runner.service -f
```

## 주요 환경 변수

1. `PEETSFEA_INPUT_QUEUE_DIR`
2. `PEETSFEA_OUTPUT_ROOT_DIR`
3. `PEETSFEA_DELETE_FAILED_DIR`
4. `PEETSFEA_DB_PATH`
5. `PEETSFEA_ACCOUNT_ID`
6. `PEETSFEA_HOST_ALIAS`
7. `PEETSFEA_MAX_JOBS_PER_ACCOUNT`
8. `PEETSFEA_EXECUTE_REMOTE`
9. `PEETSFEA_POLL_SECONDS`

## 주의사항

1. 기본 설정은 단일 계정(`account_01`) 기준이다.
2. 라이선스 스로틀링은 앱에서 수행하지 않고 관측만 수행한다.
3. 폴더 큐가 비어 있으면 다음 폴링 주기까지 대기한다.
