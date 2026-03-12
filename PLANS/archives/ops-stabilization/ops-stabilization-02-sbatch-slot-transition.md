# OPS Stabilization 02: sbatch and Slot Transition

## 역할

이 문서는 실행 구조 전환 workstream의 상위 index다.
실제 구현 단위는 아래 세 문서로 분리한다.

- [ops-stabilization-02a-persistent-worker-job-model.md](./ops-stabilization-02a-persistent-worker-job-model.md)
- [ops-stabilization-02b-slot-pool-and-worker-owned-refill.md](./ops-stabilization-02b-slot-pool-and-worker-owned-refill.md)
- [ops-stabilization-02c-ssh-tunneled-control-plane-and-recovery.md](./ops-stabilization-02c-ssh-tunneled-control-plane-and-recovery.md)

## 핵심 방향

- `foreground ssh` 중심 실행에서 벗어난다.
- `sbatch`로 장수 worker job을 제출한다.
- worker job 내부에서 `slot pool`을 유지하고 worker가 스스로 refill한다.
- main PC는 steady-state slot refill이 아니라 worker replacement와 lease 부여를 담당한다.
- steady-state 통신은 `worker별 SSH 터널 + localhost HTTP control API`로 고정한다.

## 세 문서의 분담

### 02a: Persistent Worker Job Model

- `sbatch` 제출 모델
- `worker_id` / `slurm_job_id`
- worker lifecycle
- detached execution과 결과 수집 분리

### 02b: Slot Pool and Worker-Owned Refill

- `slot_pool_size = 4` 기본값
- slot lifecycle
- worker-owned refill
- drain 정책과 lease timeout

### 02c: SSH-Tunneled Control Plane and Recovery

- localhost-only control API
- worker별 SSH 터널
- restart recovery
- tunnel loss / worker death 대응

## 완료 기준

- service restart 후에도 worker job은 계속 살고 상태 복구가 가능하다.
- worker job 내부 slot pool이 유지되고, slot 종료 시 worker가 같은 job 안에서 refill한다.
- main PC가 실행 중 worker job 안에 다시 붙어 slot을 주입하는 모델을 쓰지 않는다.
- 외부 포트 개방 없이 steady-state 제어가 유지된다.
