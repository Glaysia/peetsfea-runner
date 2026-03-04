# 06-01: Screen 8-Window Orchestration 세부 계획

## 목적

1. `screen` 1세션 안에서 window 8개를 표준 방식으로 생성/실행한다.
2. 기존 1-window 강제 검증 로직을 제거하고 8-window 검증으로 교체한다.

## 구현 범위

1. `run_pipeline(config)` 단일 진입 유지.
2. 내부 오케스트레이션 로직만 수정.

## 실행 설계

1. 세션명: `aedt_<run_id>`.
2. window명: `case_01`~`case_08`.
3. 실행 순서:
   1. `screen -dmS <session> -t case_01 bash -lc '<case_01_cmd>'`
   2. `screen -S <session> -X screen -t case_0N bash -lc '<case_0N_cmd>'` 반복
4. 각 window는 자기 디렉토리에서 독립 실행하고 `exit.code`를 기록.

## 검증 설계

1. 기존 `_looks_like_single_window` 삭제 또는 대체.
2. `screen -S <session> -Q windows` 출력에서 window 토큰 8개를 확인.
3. window명이 `case_01`~`case_08`으로 존재하는지 확인.

## 실패 처리

1. window 생성 실패는 `EXIT_CODE_SCREEN_FAILURE`로 매핑.
2. 생성 중 일부 실패 시 세션 정리 후 예외 반환.

## 산출물

1. 구현 변경 체크리스트.
2. window 생성/검증 커맨드 목록.
3. 장애 시 진단 로그 포인트.
