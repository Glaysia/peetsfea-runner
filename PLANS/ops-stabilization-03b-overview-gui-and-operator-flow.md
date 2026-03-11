# OPS Stabilization 03B: Overview GUI and Operator Flow

## 목적

이 문서는 현재 느린 GUI를 운영 콘솔 형태로 다시 정리하는 계획서다.
핵심은 첫 화면에서 필요한 것만 빠르게 보여주고, 상세는 drill-down으로 분리하는 것이다.

## 현재 병목

- single-threaded status API
- 5초 polling으로 overview와 대형 목록을 함께 재조회
- 큰 테이블 전체 재조회와 전체 렌더링
- 자원 정보 부재로 인한 낮은 설명력

## 목표 화면

### Overview

- 전체 health
- worker alive
- slot configured
- slot active
- slot idle
- refill lag
- tunnel status
- account별 target / running / pending
- 핵심 경보

### Resource Panel

- 할당 메모리
- 실사용 메모리
- CPU
- 부하
- process count
- worker별 idle / active mix

### Detail Pages

- account detail
- worker detail
- slot detail
- node detail
- recent events

## 운영자 확인 흐름

1. overview에서 health와 경보를 본다.
2. account capacity를 확인한다.
3. resource panel에서 memory/load를 본다.
4. refill lag와 tunnel 상태를 본다.
5. 필요한 경우에만 detail로 들어간다.

## 구현 체크리스트

1. overview summary API와 detail API를 분리한다.
2. 첫 화면에서 긴 테이블을 제거하거나 접어 둔다.
3. resource panel은 느린 주기 snapshot을 사용한다.
4. 경보를 첫 화면에 노출한다.
5. worker / slot / node drill-down 경로를 정리한다.

## 테스트 계획

- overview API가 summary snapshot 기반으로 빠르게 응답하는 테스트
- detail 화면 조회가 overview 성능에 영향을 덜 주는 테스트
- `할당 메모리`, `실사용 메모리`, `부하`가 overview 또는 resource panel에 노출되는 테스트
- stale heartbeat, underfill, oversubmission 경보가 첫 화면에서 보이는 테스트

## 수용 기준

- GUI 첫 화면이 summary 중심으로 가볍게 동작한다.
- 운영자가 첫 화면만 보고 현재 병목과 상태를 빠르게 판단할 수 있다.
- detail이 필요한 경우에만 깊게 들어가면 된다.
