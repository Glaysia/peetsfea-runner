from __future__ import annotations

import json
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping

import psycopg


_DEFAULT_DSN = "host=127.0.0.1 port=5433 dbname=peetsfea user=peets"


# psycopg3 connection은 스레드 안전하지 않다(동일 연결을 두 스레드가 동시에 쓰면 깨진다).
# DuckDB store와 동일하게 DSN별 단일 영속 연결 + RLock으로 모든 접근을 직렬화한다.
_PG_LOCKS: dict[str, threading.RLock] = {}
_PG_CONNS: dict[str, "psycopg.Connection"] = {}
_PG_LOCKS_GUARD = threading.Lock()


def _pg_lock(dsn: str) -> threading.RLock:
    with _PG_LOCKS_GUARD:
        lock = _PG_LOCKS.get(dsn)
        if lock is None:
            lock = threading.RLock()
            _PG_LOCKS[dsn] = lock
        return lock


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


@dataclass(slots=True)
class PostgresResultStore:
    """`SingleSimulationResultStore`(DuckDB)와 동일한 공개 메서드 시그니처/반환형을 가진 Postgres 백엔드.

    psycopg3 기반. DSN별 단일 영속 연결 + RLock으로 직렬화(연결은 스레드 안전하지 않음).
    autocommit=True로 단순화하고, 트랜잭션이 필요한 곳(record_envelope/lease)은 명시적 BEGIN/COMMIT 또는
    FOR UPDATE 행잠금으로 keep-best/원자성을 보장한다.
    """

    dsn: str = field(default_factory=lambda: os.environ.get("EDT_PG_DSN", _DEFAULT_DSN))

    @contextmanager
    def _locked_connect(self) -> "Iterator[psycopg.Connection]":
        with _pg_lock(self.dsn):
            conn = _PG_CONNS.get(self.dsn)
            if conn is None or conn.closed:
                conn = psycopg.connect(self.dsn, autocommit=True)
                _PG_CONNS[self.dsn] = conn
            yield conn

    def initialize(self) -> None:
        with self._locked_connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS single_simulation_results (
                    request_id text PRIMARY KEY,
                    account_id text,
                    host_alias text,
                    partition text,
                    node text,
                    remote_job_id text,
                    remote_api_session_id text,
                    input_toml_hash text,
                    peetsfea_version text,
                    mode text,
                    seed bigint,
                    design_id text,
                    point_hash text,
                    dimension_count bigint,
                    free_owner_paths_json text,
                    point_values_json text,
                    terminal_state text,
                    started_at text,
                    finished_at text,
                    setup_pass_counts_json text,
                    solve_telemetry_json text,
                    csv_text_by_report_json text,
                    csv_paths_json text,
                    artifact_references_json text,
                    error_stage text,
                    error_type text,
                    error_message text,
                    result_json text,
                    envelope_json text
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS ssr_started_at_idx ON single_simulation_results (started_at)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS ssr_terminal_state_idx ON single_simulation_results (terminal_state)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS ssr_peetsfea_version_idx ON single_simulation_results (peetsfea_version)"
            )
            # elapsed_ms·gpu_used 전용 컬럼: build_summary가 거대 solve_telemetry_json(평균 342KB)을 매 로드
            # 통째로 끌어와 파싱하던 걸 제거(로드당 ~880MB → 인덱스 집계). ingest가 텔레메트리에서 추출해 채운다.
            connection.execute(
                "ALTER TABLE single_simulation_results ADD COLUMN IF NOT EXISTS elapsed_ms double precision"
            )
            connection.execute(
                "ALTER TABLE single_simulation_results ADD COLUMN IF NOT EXISTS gpu_used boolean"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS resource_snapshots (
                    ts double precision,
                    running bigint,
                    pending bigint,
                    lic_mine bigint,
                    lic_inuse bigint,
                    load double precision,
                    cpus bigint,
                    mem_used_mb bigint,
                    mem_total_mb bigint,
                    nominal_aedt bigint,
                    effective_aedt bigint
                )
                """
            )
            for _col in ("nominal_aedt", "effective_aedt"):
                connection.execute(
                    f"ALTER TABLE resource_snapshots ADD COLUMN IF NOT EXISTS {_col} bigint"
                )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS priority_sweeps (
                    request_id text PRIMARY KEY,
                    sweep_toml_text text,
                    seed bigint,
                    mode text,
                    total_count bigint,
                    remaining_count bigint,
                    created_at double precision
                )
                """
            )

    def record_envelope(self, envelope: Mapping[str, Any]) -> None:
        self.initialize()
        result = _mapping(envelope.get("result"))
        error = _mapping(envelope.get("error"))
        request_id = str(envelope.get("request_id") or "")
        if not request_id:
            raise ValueError("simulation envelope is missing request_id")
        tel = _mapping(result.get("solve_telemetry"))
        _elapsed = tel.get("elapsed_ms")
        row = {
            "request_id": request_id,
            "account_id": str(envelope.get("account_id") or ""),
            "host_alias": str(envelope.get("host_alias") or ""),
            "partition": str(envelope.get("partition") or ""),
            "node": str(envelope.get("node") or ""),
            "remote_job_id": str(envelope.get("remote_job_id") or ""),
            "remote_api_session_id": str(envelope.get("api_session_id") or ""),
            "input_toml_hash": str(envelope.get("input_toml_hash") or ""),
            "peetsfea_version": str(envelope.get("peetsfea_version") or ""),
            "mode": str(envelope.get("mode") or result.get("mode") or ""),
            "seed": int(envelope.get("seed") or result.get("seed") or 0),
            "design_id": str(result.get("design_id") or ""),
            "point_hash": str(result.get("point_hash") or ""),
            "dimension_count": int(result.get("dimension_count") or 0),
            "free_owner_paths_json": _json_dumps(result.get("free_owner_paths") or []),
            "point_values_json": _json_dumps(result.get("point_values") or {}),
            "terminal_state": str(envelope.get("terminal_state") or "failed"),
            "started_at": str(envelope.get("started_at") or ""),
            "finished_at": str(envelope.get("finished_at") or ""),
            "setup_pass_counts_json": _json_dumps(result.get("setup_pass_counts") or {}),
            "solve_telemetry_json": _json_dumps(tel),
            "csv_text_by_report_json": _json_dumps(result.get("csv_text_by_report") or {}),
            "csv_paths_json": _json_dumps(result.get("csv_paths") or {}),
            "artifact_references_json": _json_dumps(result.get("artifact_references") or {}),
            "error_stage": str(error.get("stage") or ""),
            "error_type": str(error.get("type") or ""),
            "error_message": str(error.get("message") or ""),
            "result_json": _json_dumps(result),
            "envelope_json": _json_dumps(envelope),
            # build_summary 집계용 추출 컬럼(거대 JSON 재파싱 회피).
            "elapsed_ms": float(_elapsed) if isinstance(_elapsed, (int, float)) and not isinstance(_elapsed, bool) else None,
            "gpu_used": bool(tel.get("gpu_used")),
        }
        columns = tuple(row)
        placeholders = ", ".join("%s" for _ in columns)
        # keep-best: 이미 'success'가 있으면 비-success로 덮어쓰지 않는다.
        # ON CONFLICT DO UPDATE ... WHERE (기존이 success가 아니거나 신규가 success일 때만 갱신).
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "request_id")
        sql = (
            f"INSERT INTO single_simulation_results ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT (request_id) DO UPDATE SET {update_set} "
            "WHERE single_simulation_results.terminal_state <> 'success' "
            "OR EXCLUDED.terminal_state = 'success'"
        )
        with self._locked_connect() as connection:
            connection.execute(sql, [row[column] for column in columns])

    def fetch_rows(
        self,
        *,
        since: str | None = None,
        terminal_state: str | None = None,
        peetsfea_version: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        self.initialize()
        clauses: list[str] = []
        params: list[Any] = []
        if since:
            clauses.append("started_at >= %s")
            params.append(since)
        if terminal_state:
            clauses.append("terminal_state = %s")
            params.append(terminal_state)
        if peetsfea_version:
            clauses.append("peetsfea_version = %s")
            params.append(peetsfea_version)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT * FROM single_simulation_results{where} ORDER BY finished_at"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._locked_connect() as connection:
            cur = connection.execute(sql, params)
            columns = [c.name for c in cur.description]
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    def fetch_result(self, request_id: str, *, peetsfea_version: str | None = None) -> dict[str, Any] | None:
        self.initialize()
        sql = "SELECT * FROM single_simulation_results WHERE request_id = %s"
        params: list[Any] = [request_id]
        if peetsfea_version:
            sql += " AND peetsfea_version = %s"
            params.append(peetsfea_version)
        with self._locked_connect() as connection:
            cur = connection.execute(sql, params)
            row = cur.fetchone()
            if row is None:
                return None
            columns = [c.name for c in cur.description]
            return dict(zip(columns, row, strict=True))

    def state_counts(self, *, peetsfea_version: str | None = None) -> dict[str, int]:
        self.initialize()
        where = " WHERE peetsfea_version = %s" if peetsfea_version else ""
        params: list[Any] = [peetsfea_version] if peetsfea_version else []
        with self._locked_connect() as connection:
            rows = connection.execute(
                "SELECT terminal_state, count(*) FROM single_simulation_results" + where + " GROUP BY 1",
                params,
            ).fetchall()
        return {str(state): int(n) for state, n in rows}

    def version_counts(self) -> dict[str, int]:
        self.initialize()
        with self._locked_connect() as connection:
            rows = connection.execute(
                "SELECT COALESCE(NULLIF(peetsfea_version,''),'(unknown)'), count(*) "
                "FROM single_simulation_results GROUP BY 1 ORDER BY 1"
            ).fetchall()
        return {str(v): int(n) for v, n in rows}

    def count_since(self, since: str, *, terminal_state: str | None = None, peetsfea_version: str | None = None) -> int:
        self.initialize()
        clauses = ["finished_at >= %s"]
        params: list[Any] = [since]
        if terminal_state:
            clauses.append("terminal_state = %s")
            params.append(terminal_state)
        if peetsfea_version:
            clauses.append("peetsfea_version = %s")
            params.append(peetsfea_version)
        sql = "SELECT count(*) FROM single_simulation_results WHERE " + " AND ".join(clauses)
        with self._locked_connect() as connection:
            row = connection.execute(sql, params).fetchone()
        return int(row[0]) if row else 0

    def timeseries(self, *, bucket_minutes: int = 15, since: str | None = None, peetsfea_version: str | None = None) -> list[dict[str, Any]]:
        """`finished_at` 시간버킷 집계 — DuckDB time_bucket과 동일한 버킷 라벨(UTC ISO 분)/카운트.

        PG에는 time_bucket이 없으므로 `to_timestamp(floor(epoch/secs)*secs)`로 동일 버킷을 만든 뒤
        `to_char(... , 'YYYY-MM-DD"T"HH24:MI')`로 DuckDB strftime('%Y-%m-%dT%H:%M')와 같은 라벨을 만든다.
        """
        self.initialize()
        bm = max(1, int(bucket_minutes))
        secs = bm * 60
        clauses = ["finished_at <> ''"]
        params: list[Any] = []
        if since:
            clauses.append("finished_at >= %s")
            params.append(since)
        if peetsfea_version:
            clauses.append("peetsfea_version = %s")
            params.append(peetsfea_version)
        where = " AND ".join(clauses)
        # finished_at(텍스트 ISO) → timestamp → epoch → 버킷 floor → 라벨. DuckDB와 동일하게 naive(UTC) 처리.
        bucket_expr = (
            "to_char("
            "to_timestamp(floor(extract(epoch FROM finished_at::timestamp) / " + str(secs) + ") * " + str(secs) + ")"
            " AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI')"
        )
        sql = (
            "SELECT " + bucket_expr + " AS b, "
            "sum(CASE WHEN terminal_state='success' THEN 1 ELSE 0 END), "
            "sum(CASE WHEN terminal_state<>'success' THEN 1 ELSE 0 END), "
            "sum(CASE WHEN solve_telemetry_json LIKE '%%\"gpu_used\": true%%' THEN 1 ELSE 0 END), "
            "count(*) FROM single_simulation_results WHERE " + where + " GROUP BY 1 ORDER BY 1"
        )
        with self._locked_connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [
            {"t": r[0], "success": int(r[1]), "failed": int(r[2]), "gpu": int(r[3]), "total": int(r[4])}
            for r in rows
        ]

    def fetch_solve_telemetry(
        self, *, terminal_state: str = "success", peetsfea_version: str | None = None, limit: int | None = 5000
    ) -> list[dict[str, Any]]:
        self.initialize()
        clauses: list[str] = []
        params: list[Any] = []
        if terminal_state:
            clauses.append("terminal_state = %s")
            params.append(terminal_state)
        if peetsfea_version:
            clauses.append("peetsfea_version = %s")
            params.append(peetsfea_version)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        # 거대 solve_telemetry_json 대신 추출 컬럼만(로드당 ~880MB → ~수십KB). ingest/백필이 채움.
        sql = f"SELECT partition, elapsed_ms, gpu_used FROM single_simulation_results{where}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._locked_connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [{"partition": r[0], "elapsed_ms": r[1], "gpu_used": bool(r[2])} for r in rows]

    def export_parquet(
        self,
        dest: Path,
        *,
        since: str | None = None,
        terminal_state: str | None = None,
        peetsfea_version: str | None = None,
        limit: int | None = None,
    ) -> None:
        """Postgres에는 native parquet이 없어 DuckDB의 postgres 확장으로 ATTACH 후 COPY로 내보낸다.

        DuckDB store와 동일하게 envelope_json/result_json 제외, finished_at 정렬. 확장 설치 실패(오프라인 등)
        시에는 예외를 잡아 로그만 남기고 조용히 반환한다(우회 불가시 graceful fallback).
        """
        self.initialize()
        clauses: list[str] = []
        params: list[Any] = []
        if since:
            clauses.append("started_at >= %s")
            params.append(since)
        if terminal_state:
            clauses.append("terminal_state = %s")
            params.append(terminal_state)
        if peetsfea_version:
            clauses.append("peetsfea_version = %s")
            params.append(peetsfea_version)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
        dest_sql = str(dest).replace("'", "''")
        attach_dsn = self.dsn.replace("'", "''")
        # 파라미터 바인딩을 DuckDB COPY에 그대로 쓰기 위해 리터럴로 안전 이스케이프(작은따옴표 방어).
        lit_clauses: list[str] = []
        if since:
            lit_clauses.append("started_at >= '" + str(since).replace("'", "''") + "'")
        if terminal_state:
            lit_clauses.append("terminal_state = '" + str(terminal_state).replace("'", "''") + "'")
        if peetsfea_version:
            lit_clauses.append("peetsfea_version = '" + str(peetsfea_version).replace("'", "''") + "'")
        lit_where = (" WHERE " + " AND ".join(lit_clauses)) if lit_clauses else ""
        try:
            import duckdb

            con = duckdb.connect()
            try:
                con.execute("INSTALL postgres; LOAD postgres;")
                con.execute(f"ATTACH '{attach_dsn}' AS pg (TYPE postgres, READ_ONLY)")
                con.execute(
                    "COPY (SELECT * EXCLUDE (envelope_json, result_json) FROM pg.single_simulation_results"
                    f"{lit_where} ORDER BY finished_at{limit_sql}) TO '{dest_sql}' "
                    "(FORMAT PARQUET, COMPRESSION 'zstd')"
                )
            finally:
                con.close()
        except Exception as exc:  # noqa: BLE001 — 오프라인 등으로 postgres 확장 불가시 graceful fallback.
            import logging

            logging.getLogger(__name__).warning("export_parquet via duckdb postgres failed: %s", exc)

    # --- 라이선스/자원 시계열 영속 --------------------------------------------------------------

    _RESOURCE_COLS = (
        "ts", "running", "pending", "lic_mine", "lic_inuse", "load", "cpus", "mem_used_mb", "mem_total_mb",
        "nominal_aedt", "effective_aedt",
    )

    def record_resource_snapshot(self, point: Mapping[str, Any]) -> None:
        self.initialize()
        row = [float(point.get("ts") or 0.0)] + [int(point.get(c) or 0) for c in self._RESOURCE_COLS[1:5]]
        row += [float(point.get("load") or 0.0)] + [int(point.get(c) or 0) for c in self._RESOURCE_COLS[6:]]
        placeholders = ", ".join("%s" for _ in self._RESOURCE_COLS)
        with self._locked_connect() as connection:
            connection.execute(
                f"INSERT INTO resource_snapshots ({', '.join(self._RESOURCE_COLS)}) VALUES ({placeholders})", row
            )

    def fetch_resource_history(self, *, since_ts: float | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        self.initialize()
        where = " WHERE ts >= %s" if since_ts is not None else ""
        params: list[Any] = [float(since_ts)] if since_ts is not None else []
        sql = f"SELECT {', '.join(self._RESOURCE_COLS)} FROM resource_snapshots{where} ORDER BY ts"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._locked_connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(zip(self._RESOURCE_COLS, r, strict=True)) for r in rows]

    def prune_resource_snapshots(self, *, before_ts: float) -> int:
        self.initialize()
        with self._locked_connect() as connection:
            n = connection.execute(
                "SELECT count(*) FROM resource_snapshots WHERE ts < %s", [float(before_ts)]
            ).fetchone()
            connection.execute("DELETE FROM resource_snapshots WHERE ts < %s", [float(before_ts)])
        return int(n[0]) if n else 0

    # --- 우선순위 큐 영속 ----------------------------------------------------------------------

    def priority_enqueue_sweep(
        self, *, request_id: str, sweep_toml_text: str, seed: int, count: int, mode: str, now: float
    ) -> None:
        self.initialize()
        with self._locked_connect() as connection:
            connection.execute(
                "INSERT INTO priority_sweeps "
                "(request_id, sweep_toml_text, seed, mode, total_count, remaining_count, created_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (request_id) DO NOTHING",
                [request_id, sweep_toml_text, int(seed), mode, int(count), int(count), float(now)],
            )

    def priority_lease_chunk(self, k: int) -> dict[str, Any] | None:
        self.initialize()
        if k <= 0:
            return None
        with self._locked_connect() as connection:
            with connection.transaction():
                cur = connection.execute(
                    "SELECT request_id, sweep_toml_text, seed, mode, total_count, remaining_count "
                    "FROM priority_sweeps WHERE remaining_count > 0 ORDER BY created_at LIMIT 1 FOR UPDATE"
                )
                row = cur.fetchone()
                if row is None:
                    return None
                rid, sweep, seed, mode, total, remaining = row
                take = min(int(k), int(remaining))
                offset = int(total) - int(remaining)
                new_remaining = int(remaining) - take
                # 다 빨려도 행을 삭제하지 않고 remaining=0으로 남긴다 → lineage가 total/leased/inflight를
                # 영속 추적(다 빨린 직후~첫 결과 도착 전 sweep도 큐 탭에 inflight로 보이게). lease는
                # remaining>0만 집는다(아래 SELECT 조건)므로 0행은 재배분되지 않는다.
                connection.execute(
                    "UPDATE priority_sweeps SET remaining_count = %s WHERE request_id = %s",
                    [new_remaining, rid],
                )
        return {
            "request_id": rid,
            "sweep_toml_text": sweep,
            "seed_base": int(seed) + offset,
            "mode": mode or "full",
            "count": take,
        }

    def priority_depth(self) -> int:
        self.initialize()
        with self._locked_connect() as connection:
            row = connection.execute("SELECT COALESCE(sum(remaining_count), 0) FROM priority_sweeps").fetchone()
        return int(row[0]) if row else 0

    def priority_list(self, *, limit: int | None = 200) -> list[dict[str, Any]]:
        self.initialize()
        sql = (
            "SELECT request_id, total_count, remaining_count, created_at FROM priority_sweeps "
            "WHERE remaining_count > 0 ORDER BY created_at"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._locked_connect() as connection:
            rows = connection.execute(sql).fetchall()
        return [
            {"request_id": r[0], "total_count": int(r[1] or 0), "remaining_count": int(r[2] or 0), "created_at": float(r[3] or 0.0)}
            for r in rows
        ]

    def priority_lineage(self, *, limit: int | None = 200) -> dict[str, Any]:
        """sweep별 들어온/대기/분배 + 파생 출력(성공/실패) 집계. lease가 행을 삭제하지 않고 remaining=0으로
        남기므로(priority_lease_chunk 참고), 다 빨려 진행 중인 sweep도 큐 탭에 inflight로 영속 표시된다."""
        self.initialize()
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        with self._locked_connect() as connection:
            tot = connection.execute(
                "SELECT count(*), COALESCE(sum(total_count),0), COALESCE(sum(remaining_count),0) FROM priority_sweeps"
            ).fetchone()
            rows = connection.execute(
                "SELECT ps.request_id, ps.total_count, ps.remaining_count, ps.created_at, "
                "  COALESCE(sum(CASE WHEN r.terminal_state='success' THEN 1 ELSE 0 END),0), "
                "  COALESCE(sum(CASE WHEN r.terminal_state IS NOT NULL AND r.terminal_state<>'success' THEN 1 ELSE 0 END),0) "
                "FROM priority_sweeps ps "
                "LEFT JOIN single_simulation_results r "
                "  ON r.request_id >= ps.request_id || '-' AND r.request_id < ps.request_id || '.' "
                "GROUP BY ps.request_id, ps.total_count, ps.remaining_count, ps.created_at "
                "ORDER BY ps.created_at DESC" + lim
            ).fetchall()
        items: list[dict[str, Any]] = []
        ok_sum = fail_sum = 0
        for rid, total, remaining, created, ok, fail in rows:
            total_i, rem_i, ok_i, fail_i = int(total or 0), int(remaining or 0), int(ok or 0), int(fail or 0)
            leased = total_i - rem_i
            done = ok_i + fail_i
            ok_sum += ok_i
            fail_sum += fail_i
            items.append({
                "request_id": rid, "total": total_i, "remaining": rem_i, "leased": leased,
                "ok": ok_i, "fail": fail_i, "done": done, "inflight": max(0, leased - done),
                "created_at": float(created or 0.0),
            })
        submitted, waiting = int(tot[1]), int(tot[2])
        leased_all = submitted - waiting
        done_all = ok_sum + fail_sum
        return {
            "sweeps": int(tot[0]), "submitted": submitted, "waiting": waiting, "leased": leased_all,
            "ok": ok_sum, "fail": fail_sum, "done": done_all, "inflight": max(0, leased_all - done_all),
            "items": items,
        }


__all__ = ["PostgresResultStore"]
