# Docs

`PLANS/`는 tonight 운영과 cutover 판단을 위한 active 실행 문서다. `docs/`는 현재 저장소 구현 상태를 설명하는 참조 문서이며, 운영 판단의 우선순위는 항상 `PLANS/`가 가진다.

## 문서 인덱스

- [architecture/current-project-overview.md](./architecture/current-project-overview.md): 저장소 구조, `run_pipeline(config)` 런타임 흐름, continuous service와 validation lane 경계를 Mermaid로 정리한 현재 구조 문서

## 현재 문서 원칙

- 파이프라인 공식 시작점은 계속 `run_pipeline(config)` 하나다.
- 예시는 CLI가 아니라 함수 호출 경로 또는 systemd service 경로만 사용한다.
- canary 판정은 active `PLANS/`의 CSV schema gate 기준을 따른다.
