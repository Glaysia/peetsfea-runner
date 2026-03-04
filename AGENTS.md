# Repository Agent Rules

이 저장소는 AEDT Slurm 자동화를 **함수 호출 기반 단일 경로**로 유지한다.

## Mandatory Rules

1. 본 저장소 파이프라인은 CLI를 사용하지 않는다.
2. 파이프라인 시작점은 `run_pipeline(config)` 단 하나만 허용한다.
3. 신규 코드/리팩터링 시 CLI 옵션/서브커맨드/엔트리포인트 추가를 금지한다.
4. 문서와 테스트도 함수 호출 경로만 예시로 사용한다.

