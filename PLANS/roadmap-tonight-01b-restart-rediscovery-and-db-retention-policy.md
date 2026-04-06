# Roadmap Tonight 01B: Restart and Cold-Start State Policy

## 목적

이 문서는 restart-safe 복구 대신 콜드 스타트 정책을 고정한다.
현재 기준에서 durable truth는 DB가 아니라 입력/출력 파일시스템이다.

## 현재 상태

- service restart는 old worker/state를 이어받지 않는다.
- old worker는 cancel 후 새 worker pool을 채운다.
- 입력의 진실은 `input_queue/**/*.aedt`, `*.ready`, `*.done`이다.
- 결과의 진실은 `output/**/*.aedt.out`이다.
- 런타임 상태는 프로세스 메모리와 `*.state` 네임스페이스로만 유지한다.

## 핵심 변경

- DB 보존/삭제 정책을 폐기한다.
- restart 정책은 아래로 고정한다.
  - service stop
  - old worker cancel
  - 새 service start
  - input tree 전체 rescan
  - `.done`이 아닌 입력 재큐잉
- stale lease/token은 restart 후 전부 무효다.
- validation lane은 live output과 섞지 않는 별도 input/output 경로를 사용한다.

## 운영 절차

1. restart 전 현재 runner worker를 정리한다.
2. service를 내린다.
3. old worker가 남아 있으면 모두 cancel한다.
4. service를 다시 올린다.
5. `input_queue`를 처음부터 다시 스캔한다.
6. `.done`이 아닌 입력이 queue에 다시 올라오는지 확인한다.
7. 새 worker pool이 목표 수까지 채워지는지 확인한다.

## 테스트/수용 기준

- restart 후 old worker가 남지 않는다.
- restart 후 `.done`이 아닌 입력이 다시 pending으로 인식된다.
- restart 후 새 worker pool이 정상 제출된다.
- 런타임 상태 저장소를 운영 진실로 참조하지 않는다.

## 참고 문서

- [roadmap-tonight-01-drain-and-restart.md](./roadmap-tonight-01-drain-and-restart.md)
- [roadmap-tonight-01a-drain-bootstrap-and-control-toggle.md](./roadmap-tonight-01a-drain-bootstrap-and-control-toggle.md)
- [roadmap-tonight-master-plan.md](./roadmap-tonight-master-plan.md)
