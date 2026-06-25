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
import json
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
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


# ======================================================================================
# v2 데이터플레인 계약 (PLANS/runner_dataplane_reform.html). 기존 /api/results 는 P1 병행 유지.
# 규범: Arrow IPC stream + ZSTD body compression, 타입 고정(timestamp/map/dictionary/float64/int64),
# seq 커서(X-Next-Since/X-Has-More), 컬럼 프로젝션, 핫 경로에 CSV/측정값 텍스트 0.
# ======================================================================================
_TS_UTC = pa.timestamp("us", tz="UTC")
_DICT = pa.dictionary(pa.int32(), pa.string())
_F64 = pa.float64()
_I64 = pa.int64()
_STR = pa.string()

# (출력 필드, dtype) — 계약 §2 results 스키마. point_values 는 point_values_json 에서 파생.
_V2_RESULTS_FIELDS: list[tuple[str, pa.DataType]] = [
    ("seq", _I64),
    ("request_id", _STR),
    ("design_id", _STR),
    ("point_hash", _STR),
    ("peetsfea_version", _DICT),
    ("terminal_state", _DICT),
    ("node", _DICT),
    ("started_at", _TS_UTC),
    ("finished_at", _TS_UTC),
    ("elapsed_ms", _I64),
    ("error_stage", _STR),
    ("error_type", _STR),
    ("error_message", _STR),
    ("point_values", pa.map_(_STR, _F64)),
    ("op_freq_hz", _F64),
    ("op_re_z11", _F64), ("op_im_z11", _F64),
    ("op_re_z22", _F64), ("op_im_z22", _F64),
    ("op_re_z12", _F64), ("op_im_z12", _F64),
    ("max_mag_delta_s", _F64),
    ("loss_w_copper_tx", _F64), ("loss_w_fr4_tx", _F64), ("loss_w_ferrite_tx", _F64),
    ("loss_w_copper_rx", _F64), ("loss_w_fr4_rx", _F64), ("loss_w_ferrite_rx", _F64),
]
# DB에서 읽을 컬럼(point_values 대신 point_values_json). 전부 실DB 컬럼명.
_V2_RESULTS_DB_COLS = [n for n, _ in _V2_RESULTS_FIELDS if n != "point_values"] + ["point_values_json"]

# 계약 §3 sweeps 스키마(long-format).
_V2_SWEEPS_FIELDS: list[tuple[str, pa.DataType]] = [
    ("result_seq", _I64),
    ("freq_hz", _F64),
    ("re_z11", _F64), ("im_z11", _F64),
    ("re_z22", _F64), ("im_z22", _F64),
    ("re_z12", _F64), ("im_z12", _F64),
]

_ZSTD_OPTS = pa.ipc.IpcWriteOptions(compression="zstd")


def _to_f(x: Any) -> float | None:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v else None  # NaN -> None


def _to_i(x: Any) -> int | None:
    f = _to_f(x)
    return None if f is None else int(round(f))


def _parse_ts(v: Any) -> datetime | None:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(v).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _v2_build_array(name: str, dtype: pa.DataType, rows: list[dict[str, Any]]) -> pa.Array:
    if name == "point_values":
        out: list[list[tuple[str, float]]] = []
        for r in rows:
            pairs: list[tuple[str, float]] = []
            raw = r.get("point_values_json")
            if raw:
                try:
                    obj = json.loads(raw)
                except (TypeError, ValueError):
                    obj = {}
                if isinstance(obj, dict):
                    for k, val in obj.items():
                        fv = _to_f(val)
                        if fv is not None:
                            pairs.append((str(k), fv))
            out.append(pairs)
        return pa.array(out, type=dtype)
    if pa.types.is_timestamp(dtype):
        return pa.array([_parse_ts(r.get(name)) for r in rows], type=dtype)
    if pa.types.is_integer(dtype):
        return pa.array([_to_i(r.get(name)) for r in rows], type=dtype)
    if pa.types.is_floating(dtype):
        return pa.array([_to_f(r.get(name)) for r in rows], type=dtype)
    # utf8 / dictionary<utf8>: 빈 문자열은 null(위장 금지)
    vals = [None if (r.get(name) is None or r.get(name) == "") else str(r.get(name)) for r in rows]
    return pa.array(vals, type=dtype)


def _v2_arrow_stream(rows: list[dict[str, Any]], fields: list[tuple[str, pa.DataType]]) -> Iterator[bytes]:
    """타입 고정 + ZSTD Arrow IPC stream. rows는 이미 페이지 경계로 확정된 리스트(헤더 선확정 위해)."""
    schema = pa.schema(fields)
    bio = io.BytesIO()
    writer = pa.ipc.new_stream(bio, schema, options=_ZSTD_OPTS)

    def drain() -> bytes:
        data = bio.getvalue()
        bio.seek(0)
        bio.truncate(0)
        return data

    yield drain()  # 스키마 헤더(0행이어도 자기서술 스트림)
    for start in range(0, len(rows), _BATCH_ROWS):
        chunk = rows[start:start + _BATCH_ROWS]
        arrays = [_v2_build_array(n, t, chunk) for n, t in fields]
        writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
        yield drain()
    writer.close()
    yield drain()


def _v2_project(all_fields: list[tuple[str, pa.DataType]], columns: str | None, always: tuple[str, ...]) -> list[tuple[str, pa.DataType]]:
    if not columns:
        return list(all_fields)
    want = {c.strip() for c in columns.split(",") if c.strip()} | set(always)
    return [(n, t) for n, t in all_fields if n in want]


def _page_sweeps(it: Iterator[dict[str, Any]], limit: int, since: int) -> tuple[list[dict[str, Any]], int, bool]:
    """설계 경계를 깨지 않고 limit행까지 모은다. 반환(rows, next_since=마지막 완성 result_seq, has_more)."""
    rows: list[dict[str, Any]] = []
    last: int | None = None
    next_since = since
    has_more = False
    for r in it:
        rs = int(r["result_seq"])
        if len(rows) >= limit and rs != last:
            has_more = True
            break
        rows.append(r)
        last = rs
        if rs > next_since:
            next_since = rs
    return rows, next_since, has_more


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

    # ------------------------------------------------------------------ v2 계약 엔드포인트
    _ARROW_MT = "application/vnd.apache.arrow.stream"

    def _err(status: int, message: str) -> JSONResponse:
        return JSONResponse({"error": "bad_request" if status < 500 else "server_error", "message": message}, status_code=status)

    @app.get("/api/v2/health")
    def v2_health() -> JSONResponse:
        try:
            h = result_store.dataplane_health()
        except Exception as e:  # noqa: BLE001
            return _err(500, f"{type(e).__name__}: {e}")
        return JSONResponse({"status": "ok", "schema_version": "2.0", **h})

    @app.get("/api/v2/results")
    def v2_results(
        since: int = Query(0, ge=0),
        limit: int = Query(50000, ge=1, le=200000),
        version: str | None = Query(None),
        state: str = Query("success"),
        columns: str | None = Query(None),
    ) -> Any:
        """seq>since 행을 §2 스키마 Arrow IPC(ZSTD)로. 커서는 X-Next-Since(스캔 최대 seq, 필터 무관)."""
        fields = _v2_project(_V2_RESULTS_FIELDS, columns, always=("seq",))
        try:
            # 필터를 SQL에 넣지 않는다 → X-Next-Since가 '스캔한' 최대 seq를 가리켜 필터 0행 페이지도 커서 전진.
            page = list(result_store.stream_results_since(since=since, limit=limit, columns=_V2_RESULTS_DB_COLS))
        except Exception as e:  # noqa: BLE001
            return _err(500, f"{type(e).__name__}: {e}")
        next_since = max((int(r["seq"]) for r in page), default=since)
        has_more = len(page) == limit
        st = state.lower()
        if st not in ("success", "failed", "any"):
            return _err(400, "state must be one of: success | failed | any")
        if st != "any":
            page = [r for r in page if str(r.get("terminal_state") or "") == st]
        if version:
            page = [r for r in page if str(r.get("peetsfea_version") or "") == version]
        return StreamingResponse(
            _v2_arrow_stream(page, fields),
            media_type=_ARROW_MT,
            headers={"X-Next-Since": str(next_since), "X-Has-More": "true" if has_more else "false", "X-Schema-Version": "2.0"},
        )

    @app.get("/api/v2/sweeps")
    def v2_sweeps(
        since: int = Query(0, ge=0),
        result_seqs: str | None = Query(None),
        limit: int = Query(2000000, ge=1, le=20000000),
        columns: str | None = Query(None),
    ) -> Any:
        """§3 sweeps. since(증분) 와 result_seqs(지정) 상호배타. (result_seq,freq_hz) 오름차순, 설계 분할 금지."""
        fields = _v2_project(_V2_SWEEPS_FIELDS, columns, always=("result_seq", "freq_hz"))
        try:
            if result_seqs is not None:
                if since:
                    return _err(400, "since 와 result_seqs 는 상호배타입니다")
                try:
                    seqs = [int(x) for x in result_seqs.split(",") if x.strip()]
                except ValueError:
                    return _err(400, "result_seqs 는 콤마구분 int64 여야 합니다")
                it = result_store.stream_sweeps_since(result_seqs=seqs)
            else:
                it = result_store.stream_sweeps_since(since=since)
            rows, next_since, has_more = _page_sweeps(it, limit, since)
        except Exception as e:  # noqa: BLE001
            return _err(500, f"{type(e).__name__}: {e}")
        return StreamingResponse(
            _v2_arrow_stream(rows, fields),
            media_type=_ARROW_MT,
            headers={"X-Next-Since": str(next_since), "X-Has-More": "true" if has_more else "false", "X-Schema-Version": "2.0"},
        )

    return app


__all__ = ["create_data_api_app", "_arrow_ipc_stream", "_schema"]
