# PLANS Archive

## 목적

이 디렉터리는 기존 `ops-stabilization-*` 문서를 실행 기준에서 분리해 보존하는 archive다.
현재 active 운영 문서 체계는 `PLANS/roadmap-tonight-master-plan.md`와 그 하위 `4`개 세부 문서다.

## Active 진입점

- [roadmap-tonight-master-plan.md](../roadmap-tonight-master-plan.md)
- [roadmap-tonight-01-drain-and-restart.md](../roadmap-tonight-01-drain-and-restart.md)
- [roadmap-tonight-02-bundle-throughput-and-prefetch.md](../roadmap-tonight-02-bundle-throughput-and-prefetch.md)
- [roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md](../roadmap-tonight-03-resource-aware-dispatch-and-bad-node-control.md)
- [roadmap-tonight-04-csv-integrity-and-output-contract.md](../roadmap-tonight-04-csv-integrity-and-output-contract.md)

## Archive 기준

- 기존 `ops-stabilization-*` 문서는 내용 보존을 위해 파일명 그대로 archive로 이동했다.
- archive 문서는 참고 기록이며, tonight 실행 기준은 아니다.
- active 문서에서 필요한 경우에만 archive 문서를 참고 링크로 사용한다.

## 보관 문서

아래 문서는 `ops-stabilization/` 하위에 보관한다.

- `ops-stabilization-master-plan.md`
- `ops-stabilization-01-current-path-guardrails.md`
- `ops-stabilization-01a-capacity-hard-cap-and-backlog.md`
- `ops-stabilization-01b-slurm-truth-and-cutover-guardrails.md`
- `ops-stabilization-02-sbatch-slot-transition.md`
- `ops-stabilization-02a-persistent-worker-job-model.md`
- `ops-stabilization-02b-slot-pool-and-worker-owned-refill.md`
- `ops-stabilization-02c-ssh-tunneled-control-plane-and-recovery.md`
- `ops-stabilization-03-operations-gui-and-service-readiness.md`
- `ops-stabilization-03a-resource-telemetry-and-snapshots.md`
- `ops-stabilization-03b-overview-gui-and-operator-flow.md`
- `ops-stabilization-03c-night-operations-rollout-and-service-boundary.md`
- `ops-stabilization-04-full-4x10x10-saturation.md`

아래 문서는 enroot 도입 전 설계를 위한 archive 참고 문서다.

- `enroot-canary-repo-ssh-handover.md`
- `enroot-canary-01-ssh-and-account-topology.md`
- `enroot-canary-02-worker-runtime-and-container-contract.md`
- `enroot-canary-03-canary-rollout-and-270-slot-target.md`

## 구조

- active tree: `1 + 4`
- leaf docs: `11`
- archive index: `1`
- archive enroot docs: `4`
- total tracked docs: `21`
