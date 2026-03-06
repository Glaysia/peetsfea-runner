# Roadmap 4A: Ready Automation And Ingest State

## Summary
- `.ready`를 내부 자동 생성물로 바꾸려면 ingest state 모델이 필요하다.
- 현재 파일 존재 여부만 보는 방식은 운영 UX와 내부 상태를 섞는다.
- 사용자는 `.aedt`만 넣고, 서비스는 내부적으로 ingest readiness를 관리해야 한다.

## Implementation Decisions
- 새 `.aedt`가 보이면 서비스가 `.ready`를 생성하거나 동등한 internal ready state를 기록한다.
- `.ready`는 사용자 API가 아니라 내부 진행 상태를 표현하는 부속물로만 쓴다.
- ingest index는 파일 경로, 크기, mtime 기반으로 중복을 막는다.
- 준비 중, 큐잉됨, 업로드됨, 삭제됨 상태를 ingest 단에서 따로 관리한다.

## Acceptance Signals
- 사용자는 `.aedt`만 넣어도 서비스가 자동 ingest한다.
- `.ready` 생성 실패가 있어도 내부 상태로 대체 가능하다.
- ingest와 execute 상태를 분리해 중복 ingest를 막는다.

## Assumptions
- 장기적으로 `.ready` 파일 자체를 외부 contract에서 감춘다.
- ingest state는 DB에 남아야 한다.
