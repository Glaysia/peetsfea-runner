# Docs

문서 단일 기준(SSOT)은 `PLANS/MASTER_PLAN.md`다. 충돌 시 항상 MASTER_PLAN이 우선한다.

## 문서 지도
- `PLANS/MASTER_PLAN.md` — 전체 목표·아키텍처·6단계 (SSOT)
- `GOAL.md` — 지금 구현할 범위(Phase 1, 1/6)
- `PLANS/peetsfea_main.md` — peetsfea 0.3.2에 추가할 것 + 일차 이유
- `PLANS/peetsfea_runner.md` — runner 쪽 peetsfea 연동 책임
- `AGENTS.md` — 저장소 작업 규칙(실행=systemctl, 타입체킹, 의존성 등)
- `docs/architecture/new-architecture.html` — 신규 아키텍처 시각화

## 비고
- 옛 50-worker lease/worker 아키텍처를 설명하던 `SPECS/*`, roadmap, legacy docs는 혼동 방지를 위해 제거했다.
- 인프라/환경 사실(호스트·경로·라이선스·파티션 등)은 MASTER_PLAN **부록 A**로 보존했다.
