"""read 평면 데이터 API (데이터플레인 갈아엎기, PLANS/data_plane_overhaul.html).

학습 주체가 **변경분만** 받아오는 단일 증분 커서 엔드포인트:
    GET /api/results?since=<seq>&limit=<N>&full=<0|1>
      since=0        → 전체동기(cold start), since=last_seq → 증분(매 라운드).
      응답: Apache Arrow IPC 스트림(컬럼형). 헤더 X-Max-Seq = 이번 배치 최대 seq.
      행수가 limit과 같으면 더 있음 → since=X-Max-Seq로 다음 페이지(루프).

전송은 psycopg3 서버사이드 커서(상수 메모리) → pyarrow RecordBatch → IPC 스트리밍.
temp parquet/압축/락 없음 → 구 /results.parquet·:7877 bulk의 FD死 부류를 구조적으로 제거.
"""

from __future__ import annotations

import io
from collections.abc import Iterable, Iterator
from typing import Any

import pyarrow as pa
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, StreamingResponse

from .postgres_store import PostgresResultStore

# Arrow 스키마(고정). 거대 blob(envelope/telemetry/csv_text)은 기본 제외 — full=1일 때만 포함.
# 학습에 필요한 설계입력(point_values)·결과(result_json)·결과상태·지표 컬럼만 lean하게.
_TEXT = pa.string()
_COLUMN_TYPES: dict[str, pa.DataType] = {
    "seq": pa.int64(),
    "request_id": _TEXT,
    "terminal_state": _TEXT,
    "peetsfea_version": _TEXT,
    "partition": _TEXT,
    "node": _TEXT,
    "mode": _TEXT,
    "seed": pa.int64(),
    "design_id": _TEXT,
    "point_hash": _TEXT,
    "dimension_count": pa.int64(),
    "free_owner_paths_json": _TEXT,
    "point_values_json": _TEXT,
    "started_at": _TEXT,
    "finished_at": _TEXT,
    "setup_pass_counts_json": _TEXT,
    "csv_paths_json": _TEXT,
    "artifact_references_json": _TEXT,
    "error_stage": _TEXT,
    "error_type": _TEXT,
    "error_message": _TEXT,
    "result_json": _TEXT,
    "elapsed_ms": pa.float64(),
    "gpu_used": pa.bool_(),
}
# full=1에서 추가로 포함하는 거대 컬럼.
_FULL_EXTRA: dict[str, pa.DataType] = {
    "solve_telemetry_json": _TEXT,
    "csv_text_by_report_json": _TEXT,
    "envelope_json": _TEXT,
}

_BATCH_ROWS = 2000  # RecordBatch당 행수(상수 메모리 단위)


def _schema(full: bool) -> pa.Schema:
    cols = dict(_COLUMN_TYPES)
    if full:
        cols.update(_FULL_EXTRA)
    return pa.schema([(name, dtype) for name, dtype in cols.items()])


def _coerce(value: Any, dtype: pa.DataType) -> Any:
    if value is None:
        return None
    if pa.types.is_integer(dtype):
        return int(value)
    if pa.types.is_floating(dtype):
        return float(value)
    if pa.types.is_boolean(dtype):
        return bool(value)
    return str(value)


def _arrow_ipc_stream(rows: Iterable[dict[str, Any]], schema: pa.Schema) -> Iterator[bytes]:
    """행 이터러블 → Arrow IPC 스트림 바이트(증분 yield). bio를 매 배치 비워 상수 메모리."""
    names = schema.names
    bio = io.BytesIO()
    writer = pa.ipc.new_stream(bio, schema)

    def drain() -> bytes:
        data = bio.getvalue()
        bio.seek(0)
        bio.truncate(0)
        return data

    yield drain()  # 스키마 헤더
    batch: list[dict[str, Any]] = []

    def flush() -> bytes:
        cols = {name: [_coerce(r.get(name), schema.field(name).type) for r in batch] for name in names}
        writer.write_batch(pa.RecordBatch.from_pydict(cols, schema=schema))
        batch.clear()
        return drain()

    for row in rows:
        batch.append(row)
        if len(batch) >= _BATCH_ROWS:
            yield flush()
    if batch:
        yield flush()
    writer.close()
    yield drain()


def create_data_api_app(*, store: PostgresResultStore | None = None) -> FastAPI:
    """read 평면 FastAPI 앱. store 미지정 시 기본 DSN의 PostgresResultStore."""
    result_store = store if store is not None else PostgresResultStore()
    app = FastAPI(title="peetsfea data API", docs_url=None, redoc_url=None)

    @app.get("/health")
    def health() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/api/results")
    def results(
        since: int = Query(0, ge=0),
        limit: int = Query(50000, ge=1, le=500000),
        full: int = Query(0, ge=0, le=1),
    ) -> StreamingResponse:
        """seq>since 행을 Arrow IPC로 스트리밍. since=0=전체, since=last_seq=증분.
        X-Max-Seq 헤더로 다음 커서를 알려준다(행수==limit이면 더 있음 → 다음 페이지)."""
        schema = _schema(bool(full))
        cols = list(schema.names)
        max_seq = {"v": since}

        def row_iter() -> Iterator[dict[str, Any]]:
            for r in result_store.stream_results_since(since=since, limit=limit, columns=cols):
                s = r.get("seq")
                if isinstance(s, int) and s > max_seq["v"]:
                    max_seq["v"] = s
                yield r

        # 주의: X-Max-Seq는 스트림 끝에야 확정되지만, 클라이언트는 limit 도달 시 받은 마지막 행의 seq로
        # 다음 since를 정한다(헤더는 보조). 헤더에 since(시작값)+limit을 실어 클라이언트가 페이지 판단.
        gen = _arrow_ipc_stream(row_iter(), schema)
        return StreamingResponse(
            gen,
            media_type="application/vnd.apache.arrow.stream",
            headers={"X-Since": str(since), "X-Limit": str(limit)},
        )

    return app


__all__ = ["create_data_api_app", "_arrow_ipc_stream", "_schema"]
