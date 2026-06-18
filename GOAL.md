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

## 근본 원인 (확정)
스루풋 붕괴 = 워커 idle이 아니라 **AEDT 세션 손상 실패 폭주**. fail율 5%→73%(190/10분).
실패: project_name=None·STEP import 빈 바디(peetsfea ssw_ports.py). 실패 솔브는 라이선스 짧게 쥐어
동시 solve 급락 = "100 밑". **메커니즘: EdtManager.recover()가 실패 시 살아있는 손상 데스크톱을 재부착
→ 다음 솔브도 같은 세션서 실패 → 폭주.** 제어기 throttle 아님(effective<target, active_permits≈1).

## 처방 (배포됨)
- 러너 `edt_dispatcher`: 연속 실패 N회(2)면 recover 대신 **force_restart**(새 AEDT 세션)로 자가치유.
  da0d15a, 145 passed. 게이트 venv에 배포(백업 .bak) + keeper 재램프로 적용.
- peetsfea 본체 수정 불요(1차). 잔여 의심: 공유 $HOME/Ansoft 격리는 2차 관찰 후 판단.

## 진행 로그
- 2026-06-19 ~00:40 KST: 미션 수립. solve 붕괴(~13) — 원인 분석(재램프 보류).
- ~00:50: 데이터로 실패 폭주 확정. srun --overlap로 노드 내부 검사(RAM/디스크 무관 확인).
- ~01:03: recover() 재부착이 폭주 원인 확정. force_restart fix 구현·테스트·커밋·게이트 배포.
- ~01:10: keeper 재램프로 fix 적용(직전 자체회복 119였으나 OLD코드라 재램프 불가피).
  → **검증 과제: 다음 ~1.5~2h 동안 폭주 없이 100+ 유지되는지 관찰.**
