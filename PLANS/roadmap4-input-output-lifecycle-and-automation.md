# Roadmap 4: Input Output Lifecycle And Automation

## Summary
- 장기 운영 모델에서 사용자는 `input_queue`에 `.aedt`만 넣어야 하고, 서비스는 입력 감지부터 output 생성, 입력 정리까지 파일 lifecycle 전체를 책임져야 한다.
- output은 input 구조를 그대로 따라야 하며, 성공/실패와 무관하게 가능한 결과 파일과 로그를 최대한 회수해야 한다.
- 처리 완료 입력이 queue에 무기한 남아 재실행 후보처럼 보이지 않도록 입력 삭제 또는 이동 정책이 필요하다.

## Must-Have Improvements
- `.ready`는 서비스가 자동 생성하고, 외부 사용자는 `.aedt`만 넣도록 UX를 고정한다.
- output 구조는 input 상대 경로를 그대로 따라가고, 각 `.aedt`는 대응하는 `.aedt.out` 디렉터리 안에 정리한다.
- `.aedt`, `.aedtresults`, `.aedt.q.complete`, `run.log`, `exit.code`, 기타 case 옆 파일들을 가능한 한 회수한다.
- 성공/실패와 무관하게 존재하는 산출물은 무조건 output으로 가져오도록 한다.
- 실제 output materialize가 이루어졌는지를 완료 판정에 반영한다.
- 입력 삭제 정책을 추가한다.
  - 최소 기준은 실행 또는 업로드가 시작된 입력이 중복 실행되지 않도록 하는 것
  - 최종 기준은 성공 또는 정책 만족 후 입력을 삭제하거나 별도 위치로 이동하는 것
- 삭제 실패 시 quarantine 또는 retry 정책을 둔다.

## Acceptance Signals
- 사용자는 `.aedt`만 넣으면 되고 별도 `.ready` 조작이 필요 없다.
- output 아래에 input 구조를 그대로 반영한 `.aedt.out` 디렉터리가 자동 생성된다.
- 실패한 case도 가능한 한 `run.log`, `exit.code`, 결과 디렉터리를 output에서 볼 수 있다.
- 처리 완료 입력이 queue에 무기한 남지 않고 정책대로 삭제 또는 이동된다.
- 같은 입력이 내용 변화 없이 다시 자동 실행되지 않는다.

## Assumptions
- 최종 결과 sink는 계속 `output` 디렉터리다.
- 원격 대용량 임시 파일은 `/tmp`를 사용하고, 최종 보존 대상만 `output`으로 가져온다.
- 입력 정리 정책은 장기적으로 자동화 대상이며, 사람 수동 정리를 전제하지 않는다.
