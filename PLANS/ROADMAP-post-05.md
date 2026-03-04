# ROADMAP post-05

## 목표/범위

05 완료 이후 확장 목표는 다음 4단계다.

1. 06: `1계정 + 1잡 + 1screen + 8window` 병렬
2. 07: `1계정 + 10잡 x 8window = 80` 병렬
3. 08: `최대 7계정`, 목표 처리량 550
4. 09~10: `systemd --user` + DuckDB + 웹 상태판

범위 외:

1. 기존 05 단일 실행 플로우 재설계
2. Python 런타임 정책 변경(Miniconda py3.12 유지)

## 아키텍처 변경점

1. 단일 워커 구조를 단계별로 멀티 window, 멀티 잡, 멀티 계정 구조로 확장
2. 큐/상태 관리를 파일 기반에서 DuckDB 중심으로 승격
3. 운영 관리를 수동 실행에서 `systemd --user` 서비스 기반으로 전환

## 인터페이스/API/스키마 변경

1. `PipelineConfig`, `run_pipeline` 공개 API는 유지
2. 내부 스케줄링 계층(작업 분배기, 계정 셰어더, 라이선스 스로틀러) 추가
3. DuckDB 스키마(작업/시도/아티팩트/이벤트) 도입

## 실행 플로우

1. 단계 06~08에서는 배치 단위 병렬 확장과 안정성 검증
2. 단계 09~10에서는 장기 운영 자동화와 가시화 구현
3. 각 단계는 이전 단계 게이트 통과 시에만 진입

## 리소스 정책(코어/라이선스/계정/잡)

1. 코어: `4core x pyaedt process` 기준 유지
2. 라이선스: 최대 550 한도 내 동시성 제어
3. 계정: 1계정 -> 최대 7계정 확장
4. 잡: 1잡 -> 10잡/계정 이상 확장

## 실패 처리 및 복구 절차

1. 단계별 실패를 작업 단위로 격리
2. 재시도 정책은 단계별 상한/백오프 포함
3. 원격 잔여 파일은 단계와 무관하게 정리 강제

## 테스트/수용 기준

1. 각 단계 문서는 구현자가 추가 질문 없이 바로 착수 가능해야 한다.
2. 단계별 성능/안정성 지표와 통과 기준이 명시되어야 한다.
3. 실패 시 복구 경로가 테스트 케이스에 포함되어야 한다.

## 운영 체크리스트

1. 태그 고정 설치 경로 유지
2. Miniconda py3.12 환경 유지
3. 원격 정리 정책(잔여 파일 0) 유지
4. 실행 로그/증적 아카이브 유지

## 롤백 기준

1. 단계별 수용 기준 미달 시 직전 안정 단계로 복귀
2. 복귀 시 데이터 호환성(DuckDB 스키마 버전) 보장

## 다음 단계 진입 조건

1. [ROADMAP-06-8window.md](./ROADMAP-06-8window.md) 구현/검증 완료

## 단계 링크/의존성/게이트

1. Step 06: [ROADMAP-06-8window.md](./ROADMAP-06-8window.md)
2. Step 07: [ROADMAP-07-80parallel-single-account.md](./ROADMAP-07-80parallel-single-account.md)
3. Step 08: [ROADMAP-08-550parallel-multi-account.md](./ROADMAP-08-550parallel-multi-account.md)
4. Step 09~10: [ROADMAP-09-10-ops-duckdb-systemd-web.md](./ROADMAP-09-10-ops-duckdb-systemd-web.md)
5. 게이트 규칙: 각 단계의 수용 기준 100% 통과 후 다음 단계 진입

