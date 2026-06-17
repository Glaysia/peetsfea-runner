# ArchiveStore pending orphan 누수 — 수정 계획서

- **작성일:** 2026-06-17
- **상태:** 계획(미구현) — 코드 수정 보류, 문서만
- **영향 범위:** `peetsfea_runner/edt_archive.py` (web/데이터 플레인 프로세스). 컨테이너·솔버 무관.
- **긴급도:** 낮음(데이터 유실 아님). 단 장기 디스크 누수라 방치 시 누적.

## 증상

- `~/mnt/8tb/edt-archive/pending/`에 **76G / 247개 project_dir**가 압축되지 않고 누적.
  그중 **0.3.6 잔존 139개** + 초기 0.3.7 일부 = 이미 끝난 옛 결과인데 batch로 안 묶임.
- pending이 batch 임계(`ARCHIVE_BATCH_BYTES` ≈ 21GB)를 한참 넘었는데도 **마지막 batch가 05:16 이후 없음**
  (그날 web 재시작 여러 번: 버전필터·keeper/web 분리·우선순위 배포로 08:58·09:17 등).

## 근본 원인

`ArchiveStore`는 묶음 상태를 **프로세스 메모리에만** 들고 있고, 재시작 시 디스크의 `pending/`를 재흡수하지 않는다.

- `__post_init__`은 기존 batch 파일에서 `_seq`(배치 번호)만 복원한다. `_pending`/`_pending_bytes`는
  `field(default_factory=list)` / `0`으로 **빈 상태에서 시작**한다.
- `add()`는 새로 staging한 dir만 `_pending`에 넣고, 누계가 임계를 넘으면 `_flush_locked()`로 묶는다.
- `_flush_locked()` / `flush()`는 **인메모리 `self._pending` 리스트만** 압축·삭제한다.

→ 재시작 직전 `_pending`에 있던(아직 21GB 미달이라 안 묶인) dir들과, 그 이전 런들이 남긴 dir들은
   새 프로세스의 `_pending`에 없으므로 **영원히 묶이지 않는 orphan**이 된다.
   batch로만 FIFO(`_evict_locked`)가 도므로, orphan은 **FIFO 관리 밖에서 raw로 누적**된다.

재시작이 잦을수록(이번처럼 하루 3~4회 배포) orphan 누수가 커진다.

### 관련 코드 위치
- `edt_archive.py` `ArchiveStore.__post_init__` — `_seq`만 복원, `pending/` 재스캔 없음.
- `edt_archive.py` `_flush_locked` / `flush` — `self._pending`만 처리.
- `edt_archive.py` `_stage` — 이름 충돌 시 `<name>.{seq}.{len(pending)}`로 staging.

## 수정안

### A. 재시작 시 `pending/` 재흡수 (핵심)
`__post_init__`에서 `_seq` 복원 직후, `pending/` 하위의 기존 project_dir들을 `_pending`에 재적재하고
`_pending_bytes`를 재계산한다. 그러면 다음 `add()`/`flush()` 때 자연히 batch로 묶이고 FIFO 관리에 든다.

고려사항:
- **부분 전송(in-flight) 배제:** bulk 수신이 원자적인지 확인 필요. 비원자적이면 전송 완료 마커(예:
  `.complete` sentinel)나 임시 디렉토리(`pending/.incoming/` → 완료 후 rename) 도입을 검토. 재흡수가
  전송 중 dir을 잡으면 깨진 tar가 생긴다.
- **이름 충돌/`.{seq}.{n}` 잔재:** `_stage`의 충돌 회피 네이밍과 일관되게 재흡수(중복 이름 그대로 둬도
  무방, batch arcname은 `d.name`이라 tar 내 충돌만 주의).
- **대량 흡수 시 첫 flush 폭주:** 76G를 한 번에 묶으면 큰 tar.gz 1개 + CPU 스파이크. `batch_threshold`를
  넘는 만큼 **여러 batch로 분할 flush**하거나, 부팅 시 1회 분할 정리.

### B. (선택) 부팅 시 정리 flush
재흡수 후 임계 무관 1회 `flush()`로 orphan을 즉시 batch화. 또는 자연 누적에 맡김(다음 신규분과 함께).

### C. 현재 76G orphan 일회성 처리
- 옵션1: A 배포 후 자연 흡수(가장 안전, 코드 일관).
- 옵션2: 수동 `tar`로 `pending/` 묶어 `batch_*`로 편입(즉시 회수, 단 수동).

## 테스트 계획
- 재시작 시뮬: `pending/`에 dir 몇 개 둔 채 새 `ArchiveStore` 인스턴스 생성 → `_pending`/`_pending_bytes`가
  재흡수됐는지, 다음 `add()`에서 임계 넘으면 전부(기존+신규) 묶이는지.
- 중복 이름/`.{seq}.{n}` staging 케이스에서 깨지지 않는지.
- (마커 도입 시) 미완성 dir이 재흡수에서 제외되는지.
- 회귀: 기존 `tests/test_edt_archive.py` 전부 통과 유지.

## 롤아웃
- ArchiveStore는 **web 프로세스**에만 있으므로 `systemctl --user restart peetsfea-web` 한 번으로 반영.
  **컨테이너·keeper 무관**(잡 안 죽음).
- 재흡수가 첫 부팅에 큰 flush를 유발할 수 있으니, 한가한 시점에 배포 권장.

## 비목표 / 메모
- 디스크 전체 압박과는 별개 사안. 현재 `~/mnt`(7.3T) 사용 3.6T(52%) 중 큰 덩어리는
  `peetsfea-runner-old.tar.zstd`(1.67T)·`backup_nas`(1.9T)이고 edt-archive는 134G로 FIFO 상한(1TB)
  한참 아래. orphan 누수는 "당장 가득 참" 문제가 아니라 **장기적으로 FIFO를 우회하며 쌓이는** 게 핵심.
- 관련 후속(별도): 우선순위 lease의 at-most-once 유실 → lease-TTL/재배달(현재 best-effort).
