# ROADMAP 09~10: systemd --user + DuckDB + 웹 상태판

## 목표/범위

목표:

1. 폴더 큐 기반 자동 워커를 `systemd --user`로 상시 운영
2. 상태 저장소를 DuckDB로 표준화
3. 최소 웹 상태판으로 실시간 가시성 제공

## 아키텍처 변경점

1. 수동 실행 방식 -> 데몬형 워커 서비스 전환
2. 파일 로그 중심 -> DuckDB 이벤트 소싱 중심 전환
3. CLI/로그 확인 중심 -> 웹 대시보드 기반 운영 전환

## 인터페이스/API/스키마 변경

1. DuckDB 스키마 도입:
   - `jobs(job_id, status, account_id, priority, created_at, updated_at)`
   - `attempts(attempt_id, job_id, node, started_at, ended_at, exit_code, error)`
   - `artifacts(job_id, local_path, size_bytes, checksum, created_at)`
   - `events(event_id, job_id, level, message, ts)`
2. 상태 조회 API 초안:
   - `/api/jobs`
   - `/api/jobs/{id}`
   - `/api/metrics/throughput`

## 실행 플로우

1. watcher가 지정 폴더(inbox) 감시
2. 신규 `.aedt`를 DuckDB에 `queued`로 등록
3. 스케줄러가 계정/슬롯/라이선스 기반 배정
4. 실행 상태를 DuckDB에 스트리밍 기록
5. 완료/실패 시 done/failed 폴더 이동 + 아티팩트 인덱싱
6. 웹 대시보드가 DuckDB 기반 API로 상태 표시

## 리소스 정책(코어/라이선스/계정/잡)

1. 08에서 확정된 스케줄링/스로틀링 정책을 그대로 사용
2. 워커 서비스는 단일 인스턴스 원칙, 내부에서 멀티 작업 관리

## 실패 처리 및 복구 절차

1. 워커 크래시 시 `systemd --user` 자동 재시작
2. 중간 상태 작업은 DuckDB 상태를 기준으로 재개/재큐잉
3. DuckDB 손상 대비 주기적 백업/체크포인트 수행

## 테스트/수용 기준

1. 워커 서비스 재시작 후 상태 복원 검증
2. DuckDB 스키마 무결성/마이그레이션 검증
3. 웹 상태판에서 큐/실행/완료/실패/처리량 정확 표시
4. 장시간 soak test에서 메모리/핸들 누수 없음

## 운영 체크리스트

1. `systemctl --user` unit 상태 정상
2. DuckDB 파일 크기/백업 상태 정상
3. 대시보드 health endpoint 정상
4. 경보(실패율/지연) 임계치 설정 완료

## 롤백 기준

1. 서비스 불안정 시 수동 실행(08 단계 운영 모드)로 즉시 전환
2. DB 문제 시 마지막 정상 백업으로 복구

## 다음 단계 진입 조건

1. 본 단계가 장기 운영 단계의 최종 형태이며 추가 단계는 운영 요구에 따라 별도 정의

