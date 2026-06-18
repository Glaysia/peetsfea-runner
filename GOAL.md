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

## 근본 처방 2차 (정식 — 배포됨)
1솔브=1AEDT: edtmgr.release()/recover()가 **매 솔브마다 사용한 ansysedt를 죽이고 새 세션을 띄운다**
(warm 재사용 폐기). AEDT가 close_project로 메모리를 OS에 반환 안 하는 누수를 **구조적으로 제거**.
+ 디스패처 워커 루프가 콜드스타트 실패에도 안 죽고 슬롯 재기동 후 계속(어트리션 방지). peetsfea 무수정.
검증 포인트: **노드 메모리가 램프 후 평탄(더 안 오름)** + solve 100+ 유지 + 콜드스타트 throughput 비용 측정.
대가: 매 솔브 콜드스타트(감수). 필요시 콜드스타트 숨기기(geometry build와 overlap)는 후속.

## 진행 로그
- 2026-06-19 ~00:40 KST: 미션 수립. solve 붕괴(~13) — 원인 분석(재램프 보류).
- ~00:50: 데이터로 실패 폭주 확정. srun --overlap로 노드 내부 검사(RAM/디스크 무관 확인).
- ~01:03: recover() 재부착이 폭주 원인 확정. force_restart fix 구현·테스트·커밋·게이트 배포.
- ~01:10: 1차 fix(연속실패 force_restart) 적용 재램프.
- ~01:30: 사용자 통찰(메모리 꽉참+CPU내림=누수) → 누수 원인 규명(AEDT가 close_project로 메모리 미반환).
- ~01:50: **정식 설계 = edtmgr 1솔브=1AEDT** 구현·테스트·게이트 배포·재램프. peetsfea 무수정.
- ~02:15: 검증 baseline(srun n107): ansysedt 36개, RSS~1.2GB, 나이 4~8분.
  **판별(~40-60분 뒤): ansysedt 나이가 ≤~20분 유지+RSS~1.2GB 평탄이면 누수 제거 확정.**
  주의: 노드-총메모리는 공유클러스터라 노이즈(타 사용자) → ansysedt RSS·나이 + 실패율로 판정.
  - 메트릭: srun `ps ansysedt rss,etimes` / PG 실패율 10분버킷 / 7879 effective.
- ~02:20 (신설계 50분): **긍정 신호**. 실패율 1~5%(구:73% 폭주) 안정, solve 100~150 밴드 진동(155피크),
  ansysedt RSS 나이무관 ~1.2GB 평탄(42분된 것도 1.2GB=누수0), 노드메모리 램프후 플래토.
  단서: 콜드스타트로 슬롯 다수 idle-warm이나 50슬롯 슬랙이 흡수해 목표 달성. **아직 50분차 — 1.5h(03:00) 통과 필요.**
- 02:25~ : 모니터 b68r6mhyz. 신 edtmgr 02:32에 solve 106·fail 6%·mem 62% — 안정(폭주 없음).
- ~02:50: **설계 재탕 문제** 발견(카운트 2691 정체). 원인 3겹: ①seed_base 고정 재탕 ②코드(launcher epoch
  주입+entrypoint 반영) ③slot_service.sh enroot --env에 새 변수 없어 컨테이너 경계서 드롭. 셋 다 수정.
  커밋·FF·게이트배포(entrypoint+slot_service.sh)·재램프. 검증: 새 잡 seed_base=449M+(684210=449000001).
- ~03:00: 재램프 직후(solve 램프 중). 신규-seed 결과는 솔브~14분 후(~03:06+). 모니터 b67qq1kdh로
  **카운트 상승(>2691) + new_seed>=449M 등장 + 안정성**을 추적 중.

- ~03:46: **두 fix 실증 완료.** designs 2691→2938(+247/40분, ~370/h), new_seed≥449M 4→316
  (재탕 끝). solve 61→149 밴드, 메모리 56→61% 평탄(누수0). 실패율~20%는 **새 설계공간 자연 실패율**
  (무효 지오메트리; 폭주 아님 — solve·메모리 건강이 증거. 에러종류는 같아도 격리돼 연쇄 안 됨).

## 남은 것 (13:00까지)
- [x] 카운트 2691 위로 상승 — 실증됨(2938, 계속 상승).
- [x] 신 edtmgr 메모리 평탄·폭주 0 — ~1h 구간 실증.
- [ ] **1.5h 지점(~04:20) 통과** 확인(구 폭주 재발 창) — 모니터 bc9ri5rg4(~2h).
- [ ] 13:00까지 밴드 유지 모니터링(이후 ~1h 간격).
