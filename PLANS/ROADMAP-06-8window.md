# ROADMAP 06: 1계정 1잡 1screen 8window

## 목표/범위

목표:

1. 단일 Slurm 잡 내부 `screen` 1세션에 window 8개를 생성
2. window당 `pyaedt` 1개, 코어 4개씩 총 8병렬 실행
3. 결과 전량 다운로드 + 원격 정리(잔여 파일 0)

## 아키텍처 변경점

1. 단일 window 실행기를 multi-window 오케스트레이터로 확장
2. window별 독립 작업 디렉토리(`case_01`~`case_08`) 도입

## 인터페이스/API/스키마 변경

1. 내부 설정 추가:
   - `parallel_windows=8`
   - `cores_per_window=4`
2. 공개 API는 유지

## 실행 플로우

1. 원격 run 디렉토리 생성
2. case 디렉토리 8개 생성 및 입력 분배
3. `screen -dmS <session>` 생성 후 window 8개 추가
4. 각 window에서 독립 `runner` 실행
5. window별 `exit.code` 집계
6. 전체 결과 tar 생성/다운로드
7. 원격 정리

## 리소스 정책(코어/라이선스/계정/잡)

1. 코어: `8 * 4 = 32` 고정
2. 라이선스: 8동시 사용
3. 계정: 1
4. 잡: 1

## 실패 처리 및 복구 절차

1. 개별 window 실패는 case 단위로 기록
2. 실패 case 로그는 다운로드 후 `failed_cases.json`에 기록
3. 모든 case 완료/실패 여부와 관계없이 원격 정리 수행

## 테스트/수용 기준

1. 8 window 생성 검증
2. case별 경로 충돌 없음 검증
3. 성공 케이스 결과 누락 없음
4. 실패 케이스 로그 수집 완료
5. 원격 잔여 파일 0

## 운영 체크리스트

1. Slurm `-c 32` 확인
2. 라이선스 여유 확인
3. case별 입력/출력 경로 고유성 확인

## 롤백 기준

1. 8병렬 성공률이 목표 미달이면 1window 모드로 즉시 롤백

## 다음 단계 진입 조건

1. 8병렬 E2E 3회 연속 성공
2. 원격 정리 누락 0건
3. [ROADMAP-07-80parallel-single-account.md](./ROADMAP-07-80parallel-single-account.md) 착수 승인

