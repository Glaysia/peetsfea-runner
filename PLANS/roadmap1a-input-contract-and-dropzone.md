# Roadmap 1A: Input Contract And Drop Zone

## Summary
- 24/7 서비스의 첫 계약은 사용자가 `input_queue`에 `.aedt`만 넣는다는 점이다.
- `.ready`는 외부 입력물이 아니라 서비스 내부 상태 전이용 파일로 축소한다.
- drop zone 계약이 흔들리면 이후 ingest, delete, output 계약도 전부 흔들린다.

## Implementation Decisions
- 사용자 입력 contract는 `.aedt only`로 고정한다.
- `.ready`는 서비스가 자동 생성하거나 동등한 내부 상태로 대체한다.
- 입력 후보는 `input_queue` 아래 `.aedt`만 본다.
- 사용자가 수동 `.ready`를 만들지 않아도 ingest가 시작되어야 한다.
- 같은 경로의 입력은 `input_path + size + mtime` 기준으로 중복 ingest를 막는다.

## Acceptance Signals
- 새 `.aedt`를 넣으면 다음 서비스 poll 안에 ingest 후보로 잡힌다.
- `.ready`가 없어서 입력이 무시되는 상태가 없어야 한다.
- 동일 파일이 변화 없이 남아 있어도 중복 ingest되지 않는다.

## Assumptions
- 입력 경로는 계속 `input_queue` 하나로 유지한다.
- 사용자는 운영적으로 `.ready`를 몰라도 된다.
