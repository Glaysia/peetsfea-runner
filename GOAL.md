# GOAL — 스루풋 상시 유지 (2026-06-19 13:00 KST 까지)

## 미션
**내일(2026-06-19) 13:00 KST까지 모니터링/디버깅하며 솔브 스루풋을 계속 유지**한다.
keeper·web 두 서비스를 자유롭게 올리고 내려도 된다(사용자 승인). 컨테이너 재램프 허용.

## 목표 지표
- 전역 동시 솔브(elec_solve_hfss, = effective AEDT)를 **목표 밴드 100~150**으로 유지.
- 1.5h 주기로 봉우리가 무너지는(엔벨로프 붕괴) 현상을 **탐지→원인규명→회복**.

## 관측된 문제 (열린 항목)
- 컨테이너 재램프 후 ~143까지 잘 올라가다 ~1.5h 뒤 fleet 전체가 동시 하강
  (solve 143→13). **제어기 throttle 아님**(effective<target라 자유발급, active_permits≈1).
  워커가 죽지도/에러도 없는데 새 솔브를 안 시작 → 공급/워커 stuck 또는 노드 자원(디스크/메모리) 누적 의심.
- 유력 가설: 노드-로컬 스크래치(/tmp·/enroot) 누적 포화 또는 장시간 워커 누수
  (시간/볼륨 비례 → fleet 동시성). keeper 재시작(safe_scancel)이 /enroot 청소 + 재램프로 회복.

## 회복 플레이북
1. 솔브가 밴드 밑(예: <80)으로 지속 하강하고 컨테이너가 alive면 → **keeper 재시작**
   (9잡 safe_scancel + 재제출 → 재램프, /enroot 청소). 재램프 ~10분.
2. web 문제(대시보드/ingest/lease 응답불가)면 → **web만 재시작**(컨테이너 무영향).
3. 회복 후 ~1.5h 뒤 재발 감시. 근본원인 확정 시 여기 갱신.

## 모니터링 절차 (주기 ~20~30분)
- `curl localhost:8080/api/resources` → solve_mine/desktop, 노드별 solve/load/mem.
- `curl localhost:7879/health` → active_permits, effective, lic_mine (제어기 내부).
- PG: 10분 버킷 완료율 + avg_solve_min 추세 (UTC; finished_at는 UTC 문자열).
- `curl localhost:8080/api/summary` (이제 ~0.02s) → success/throughput_1h.

## 진행 로그
- (작성 시작) 2026-06-19 ~00:40 KST: 미션 수립. 현재 solve ~13으로 붕괴 상태 → 회복 착수.
