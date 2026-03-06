# Roadmap 4B: Output Materialization And Mirroring

## Summary
- output은 사용자가 보는 최종 인터페이스이므로 input 구조를 그대로 따라야 한다.
- 성공/실패 여부와 별개로, 존재하는 산출물은 최대한 `.aedt.out` 아래로 모아야 한다.
- DB 성공보다 실제 materialize 성공이 더 중요하다.

## Implementation Decisions
- `output/<relative>/<file>.aedt.out/` 구조를 최종 계약으로 고정한다.
- `project.aedt*` 기반 원격 산출물을 원래 입력 파일명 기준으로 rename한다.
- `.aedt`, `.aedtresults`, `.aedt.q.complete`, `run.log`, `exit.code`를 우선 회수 대상으로 둔다.
- case별 output 회수 성공 여부를 per-window completion 판단에 반영한다.

## Acceptance Signals
- input 상대 경로가 output에 그대로 재현된다.
- 실패한 case도 가능한 만큼 `.aedt.out` 아래에서 확인 가능하다.
- materialize 실패 시 성공 처리되지 않는다.

## Assumptions
- 원격 임시는 `/tmp`, 최종 보존은 `output`으로 분리한다.
- `tmp/` 같은 내부 scratch는 최종 output에서 제외해도 된다.
