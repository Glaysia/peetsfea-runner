from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping

import duckdb


# DuckDB는 한 프로세스에서 같은 파일을 동시에 두 번 connect하면 "Unique file handle conflict"로 죽는다.
# web은 멀티스레드(ingest 쓰기 + 대시보드 읽기 + lease + 자원기록)라 파일별 lock으로 모든 접근을 직렬화한다.
_DB_LOCKS: dict[str, threading.RLock] = {}
_DB_CONNS: dict[str, "duckdb.DuckDBPyConnection"] = {}  # 경로별 단일 영속 연결(프로세스당 파일당 1개)
_DB_LOCKS_GUARD = threading.Lock()


def _db_lock(db_path: Path) -> threading.RLock:
    key = str(db_path)
    with _DB_LOCKS_GUARD:
        lock = _DB_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _DB_LOCKS[key] = lock
        return lock


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


@dataclass(slots=True)
class SingleSimulationResultStore:
    db_path: Path

    def __post_init__(self) -> None:
        self.db_path = Path(self.db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _locked_connect(self) -> "Iterator[duckdb.DuckDBPyConnection]":
        """파일별 lock으로 직렬화된 **경로별 단일 영속 연결**. 매 호출 open/close churn은 고부하에서
        DuckDB를 크래시(NULL unique_ptr)시키므로, 프로세스당 파일당 연결 1개를 열어 재사용한다
        (직렬 접근은 lock이 보장; 같은 경로 store 다중 인스턴스도 연결을 공유해 'already attached' 방지)."""
        key = str(self.db_path)
        with _db_lock(self.db_path):
            conn = _DB_CONNS.get(key)
            if conn is None:
                conn = duckdb.connect(key)
                _DB_CONNS[key] = conn
            yield conn

    def initialize(self) -> None:
        with self._locked_connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS single_simulation_results (
                    request_id VARCHAR PRIMARY KEY,
                    account_id VARCHAR,
                    host_alias VARCHAR,
                    partition VARCHAR,
                    node VARCHAR,
                    remote_job_id VARCHAR,
                    remote_api_session_id VARCHAR,
                    input_toml_hash VARCHAR,
                    peetsfea_version VARCHAR,
                    mode VARCHAR,
                    seed BIGINT,
                    design_id VARCHAR,
                    point_hash VARCHAR,
                    dimension_count BIGINT,
                    free_owner_paths_json VARCHAR,
                    point_values_json VARCHAR,
                    terminal_state VARCHAR,
                    started_at VARCHAR,
                    finished_at VARCHAR,
                    setup_pass_counts_json VARCHAR,
                    solve_telemetry_json VARCHAR,
                    csv_text_by_report_json VARCHAR,
                    csv_paths_json VARCHAR,
                    artifact_references_json VARCHAR,
                    error_stage VARCHAR,
                    error_type VARCHAR,
                    error_message VARCHAR,
                    result_json VARCHAR,
                    envelope_json VARCHAR
                )
                """
            )
            # 라이선스/자원 시계열(대시보드 추세 탭). web 재시작·12h ring buffer 한계를 넘어 영속.
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS resource_snapshots (
                    ts DOUBLE,
                    running BIGINT,
                    pending BIGINT,
                    lic_mine BIGINT,
                    lic_inuse BIGINT,
                    load DOUBLE,
                    cpus BIGINT,
                    mem_used_mb BIGINT,
                    mem_total_mb BIGINT
                )
                """
            )
            # 우선순위 큐(intake :7875 적재 / lease :7878 분배). **sweep 요청** 단위로 저장 —
            # 무거운 cadquery 샘플링은 web이 아니라 컨테이너 워커가 lease 후 수행(baseline과 대칭).
            # row 1개 = "이 sweep을 count개 시뮬", remaining_count를 chunk lease가 차감.
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS priority_sweeps (
                    request_id VARCHAR PRIMARY KEY,
                    sweep_toml_text VARCHAR,
                    seed BIGINT,
                    mode VARCHAR,
                    total_count BIGINT,
                    remaining_count BIGINT,
                    created_at DOUBLE
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
            "solve_telemetry_json": _json_dumps(result.get("solve_telemetry") or {}),
            "csv_text_by_report_json": _json_dumps(result.get("csv_text_by_report") or {}),
            "csv_paths_json": _json_dumps(result.get("csv_paths") or {}),
            "artifact_references_json": _json_dumps(result.get("artifact_references") or {}),
            "error_stage": str(error.get("stage") or ""),
            "error_type": str(error.get("type") or ""),
            "error_message": str(error.get("message") or ""),
            "result_json": _json_dumps(result),
            "envelope_json": _json_dumps(envelope),
        }
        columns = tuple(row)
        placeholders = ", ".join("?" for _ in columns)
        with self._locked_connect() as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                # keep-best: 같은 request_id에 이미 'success'가 있으면, 비-success로 덮어쓰지 않는다.
                # (잡 재시작 시 같은 seed를 재탐색하다 실패하면 누적된 성공 데이터가 유실되는 고질적 버그 방지.)
                if row["terminal_state"] != "success":
                    existing = connection.execute(
                        "SELECT terminal_state FROM single_simulation_results WHERE request_id = ?",
                        [request_id],
                    ).fetchone()
                    if existing is not None and existing[0] == "success":
                        connection.execute("ROLLBACK")
                        return
                connection.execute("DELETE FROM single_simulation_results WHERE request_id = ?", [request_id])
                connection.execute(
                    f"INSERT INTO single_simulation_results ({', '.join(columns)}) VALUES ({placeholders})",
                    [row[column] for column in columns],
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def fetch_rows(
        self,
        *,
        since: str | None = None,
        terminal_state: str | None = None,
        peetsfea_version: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """결과 행 조회(대시보드/CSV용, 읽기 전용). `peetsfea_version`은 표시용 버전 필터."""
        self.initialize()
        clauses: list[str] = []
        params: list[Any] = []
        if since:
            clauses.append("started_at >= ?")
            params.append(since)
        if terminal_state:
            clauses.append("terminal_state = ?")
            params.append(terminal_state)
        if peetsfea_version:
            clauses.append("peetsfea_version = ?")
            params.append(peetsfea_version)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT * FROM single_simulation_results{where} ORDER BY finished_at"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._locked_connect() as connection:
            result = connection.execute(sql, params)
            columns = [column[0] for column in result.description]
            return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    def fetch_result(self, request_id: str, *, peetsfea_version: str | None = None) -> dict[str, Any] | None:
        self.initialize()
        sql = "SELECT * FROM single_simulation_results WHERE request_id = ?"
        params: list[Any] = [request_id]
        if peetsfea_version:
            sql += " AND peetsfea_version = ?"
            params.append(peetsfea_version)
        with self._locked_connect() as connection:
            result = connection.execute(sql, params)
            row = result.fetchone()
            if row is None:
                return None
            columns = [column[0] for column in result.description]
            return dict(zip(columns, row, strict=True))

    def state_counts(self, *, peetsfea_version: str | None = None) -> dict[str, int]:
        """terminal_state별 건수(경량 집계 — 전체 행을 끌어오지 않음). 대시보드 요약용."""
        self.initialize()
        where = " WHERE peetsfea_version = ?" if peetsfea_version else ""
        params: list[Any] = [peetsfea_version] if peetsfea_version else []
        with self._locked_connect() as connection:
            rows = connection.execute(
                "SELECT terminal_state, count(*) FROM single_simulation_results" + where + " GROUP BY 1",
                params,
            ).fetchall()
        return {str(state): int(n) for state, n in rows}

    def version_counts(self) -> dict[str, int]:
        """peetsfea_version별 건수(경량 집계). 대시보드 `/api/versions`용 — 전 버전 분포 노출."""
        self.initialize()
        with self._locked_connect() as connection:
            rows = connection.execute(
                "SELECT COALESCE(NULLIF(peetsfea_version,''),'(unknown)'), count(*) "
                "FROM single_simulation_results GROUP BY 1 ORDER BY 1"
            ).fetchall()
        return {str(v): int(n) for v, n in rows}

    def count_since(self, since: str, *, terminal_state: str | None = None, peetsfea_version: str | None = None) -> int:
        """`finished_at >= since` 건수(처리량 추정용). 선택적으로 상태/버전 필터."""
        self.initialize()
        clauses = ["finished_at >= ?"]
        params: list[Any] = [since]
        if terminal_state:
            clauses.append("terminal_state = ?")
            params.append(terminal_state)
        if peetsfea_version:
            clauses.append("peetsfea_version = ?")
            params.append(peetsfea_version)
        sql = "SELECT count(*) FROM single_simulation_results WHERE " + " AND ".join(clauses)
        with self._locked_connect() as connection:
            row = connection.execute(sql, params).fetchone()
        return int(row[0]) if row else 0

    def timeseries(self, *, bucket_minutes: int = 15, since: str | None = None, peetsfea_version: str | None = None) -> list[dict[str, Any]]:
        """`finished_at` 시간버킷 집계 — 처리량/성공/실패/GPU(대시보드 시계열용, 서버측 집계).

        무거운 행 전송 없이 DuckDB `time_bucket`으로 버킷별 카운트만 반환. 버킷 라벨은 UTC ISO(분).
        """
        self.initialize()
        bm = max(1, int(bucket_minutes))  # SQL 인터폴레이션 전 정수화(인젝션 방지)
        clauses = ["finished_at != ''"]
        params: list[Any] = []
        if since:
            clauses.append("finished_at >= ?")
            params.append(since)
        if peetsfea_version:
            clauses.append("peetsfea_version = ?")
            params.append(peetsfea_version)
        where = " AND ".join(clauses)
        sql = (
            "SELECT strftime(time_bucket(INTERVAL '" + str(bm) + " minutes', finished_at::TIMESTAMP), '%Y-%m-%dT%H:%M') AS b, "
            "sum(CASE WHEN terminal_state='success' THEN 1 ELSE 0 END), "
            "sum(CASE WHEN terminal_state!='success' THEN 1 ELSE 0 END), "
            "sum(CASE WHEN solve_telemetry_json LIKE '%\"gpu_used\": true%' THEN 1 ELSE 0 END), "
            "count(*) FROM single_simulation_results WHERE " + where + " GROUP BY 1 ORDER BY 1"
        )
        with self._locked_connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [
            {"t": r[0], "success": int(r[1]), "failed": int(r[2]), "gpu": int(r[3]), "total": int(r[4])}
            for r in rows
        ]

    def export_parquet(
        self,
        dest: Path,
        *,
        since: str | None = None,
        terminal_state: str | None = None,
        peetsfea_version: str | None = None,
        limit: int | None = None,
    ) -> None:
        """결과를 **DuckDB COPY로 parquet(zstd) 직접 내보내기**(서버측; web 메모리에 전 행 적재 안 함).

        무거운 중복 컬럼(envelope_json·result_json)은 제외. JSON 컬럼(point_values_json 등)은 그대로 둔다.
        """
        self.initialize()
        clauses: list[str] = []
        params: list[Any] = []
        if since:
            clauses.append("started_at >= ?")
            params.append(since)
        if terminal_state:
            clauses.append("terminal_state = ?")
            params.append(terminal_state)
        if peetsfea_version:
            clauses.append("peetsfea_version = ?")
            params.append(peetsfea_version)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
        dest_sql = str(dest).replace("'", "''")  # 경로는 서버 임시파일(사용자 입력 아님); 따옴표만 방어 이스케이프
        sql = (
            "COPY (SELECT * EXCLUDE (envelope_json, result_json) FROM single_simulation_results"
            f"{where} ORDER BY finished_at{limit_sql}) TO '{dest_sql}' (FORMAT PARQUET, COMPRESSION 'zstd')"
        )
        with self._locked_connect() as connection:
            connection.execute(sql, params)

    # --- 라이선스/자원 시계열 영속 (대시보드 추세 탭; web 재시작·12h 넘어 보존) ----------------

    _RESOURCE_COLS = ("ts", "running", "pending", "lic_mine", "lic_inuse", "load", "cpus", "mem_used_mb", "mem_total_mb")

    def record_resource_snapshot(self, point: Mapping[str, Any]) -> None:
        """`ResourcePoller`의 history 포인트 1개를 영속(폴링마다 호출)."""
        self.initialize()
        row = [float(point.get("ts") or 0.0)] + [int(point.get(c) or 0) for c in self._RESOURCE_COLS[1:5]]
        row += [float(point.get("load") or 0.0)] + [int(point.get(c) or 0) for c in self._RESOURCE_COLS[6:]]
        placeholders = ", ".join("?" for _ in self._RESOURCE_COLS)
        with self._locked_connect() as connection:
            connection.execute(
                f"INSERT INTO resource_snapshots ({', '.join(self._RESOURCE_COLS)}) VALUES ({placeholders})", row
            )

    def fetch_resource_history(self, *, since_ts: float | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        """자원 시계열(오래된→최신). `since_ts`(epoch)로 범위 제한. 대시보드 `/api/resources/history`용."""
        self.initialize()
        where = " WHERE ts >= ?" if since_ts is not None else ""
        params: list[Any] = [float(since_ts)] if since_ts is not None else []
        sql = f"SELECT {', '.join(self._RESOURCE_COLS)} FROM resource_snapshots{where} ORDER BY ts"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._locked_connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(zip(self._RESOURCE_COLS, r, strict=True)) for r in rows]

    def prune_resource_snapshots(self, *, before_ts: float) -> int:
        """`before_ts`(epoch)보다 오래된 자원 스냅샷 삭제(무한 성장 방지). 삭제 건수 반환."""
        self.initialize()
        with self._locked_connect() as connection:
            n = connection.execute("SELECT count(*) FROM resource_snapshots WHERE ts < ?", [float(before_ts)]).fetchone()
            connection.execute("DELETE FROM resource_snapshots WHERE ts < ?", [float(before_ts)])
        return int(n[0]) if n else 0

    # --- 우선순위 큐 영속 (intake :7875 sweep 적재 / lease :7878 chunk 분배) ------------------

    def priority_enqueue_sweep(
        self, *, request_id: str, sweep_toml_text: str, seed: int, count: int, mode: str, now: float
    ) -> None:
        """sweep 요청 1줄 적재(intake; 샘플링 없음). 같은 request_id는 무시."""
        self.initialize()
        with self._locked_connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO priority_sweeps "
                "(request_id, sweep_toml_text, seed, mode, total_count, remaining_count, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [request_id, sweep_toml_text, int(seed), mode, int(count), int(count), float(now)],
            )

    def priority_lease_chunk(self, k: int) -> dict[str, Any] | None:
        """가장 오래된 미완료 sweep에서 최대 k개 분배(원자적). 컨테이너 워커가 받아 k개 샘플링·솔브.

        반환 `{request_id, sweep_toml_text, seed_base, mode, count}` 또는 None. `seed_base`는 이미 분배된
        오프셋(total-remaining)을 더해 chunk마다 다른 후보가 나오게 한다. remaining=0이면 행 삭제.
        """
        self.initialize()
        if k <= 0:
            return None
        with self._locked_connect() as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                row = connection.execute(
                    "SELECT request_id, sweep_toml_text, seed, mode, total_count, remaining_count "
                    "FROM priority_sweeps WHERE remaining_count > 0 ORDER BY created_at LIMIT 1"
                ).fetchone()
                if row is None:
                    connection.execute("COMMIT")
                    return None
                rid, sweep, seed, mode, total, remaining = row
                take = min(int(k), int(remaining))
                offset = int(total) - int(remaining)  # 이미 분배된 수 = seed 오프셋
                new_remaining = int(remaining) - take
                if new_remaining <= 0:
                    connection.execute("DELETE FROM priority_sweeps WHERE request_id = ?", [rid])
                else:
                    connection.execute(
                        "UPDATE priority_sweeps SET remaining_count = ? WHERE request_id = ?", [new_remaining, rid]
                    )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        return {
            "request_id": rid,
            "sweep_toml_text": sweep,
            "seed_base": int(seed) + offset,
            "mode": mode or "full",
            "count": take,
        }

    def priority_depth(self) -> int:
        """대기 중 후보 총수(= 미완료 sweep들의 remaining_count 합)."""
        self.initialize()
        with self._locked_connect() as connection:
            row = connection.execute("SELECT COALESCE(sum(remaining_count), 0) FROM priority_sweeps").fetchone()
        return int(row[0]) if row else 0

    def priority_list(self, *, limit: int | None = 200) -> list[dict[str, Any]]:
        """대기 중 sweep 요청 조회(pop 안 함; toml 본문 제외). 대시보드 입력큐 탭용."""
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


@dataclass
class DbPriorityQueue:
    """DB 영속 우선순위 큐(sweep 요청 단위). IntakeService가 sweep을 적재, lease 서버가 chunk를 분배.

    무거운 샘플링은 web이 아니라 컨테이너 워커가 lease 후 수행. web 재시작에도 미처리 요청이 남는다.
    """

    store: SingleSimulationResultStore
    clock: Any = None  # () -> float; None이면 time.time
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def _now(self) -> float:
        import time

        return (self.clock or time.time)()

    def enqueue_sweep(self, *, request_id: str, sweep_toml_text: str, seed: int, count: int, mode: str = "full") -> None:
        self.store.priority_enqueue_sweep(
            request_id=request_id, sweep_toml_text=sweep_toml_text, seed=seed, count=count, mode=mode, now=self._now()
        )

    def lease_chunk(self, k: int) -> dict[str, Any] | None:
        with self._lock:  # 한 web 프로세스 내 동시 lease 직렬화(중복 분배 방지)
            return self.store.priority_lease_chunk(k)

    def depth(self) -> int:
        return self.store.priority_depth()

    def list_requests(self, *, limit: int | None = 200) -> list[dict[str, Any]]:
        return self.store.priority_list(limit=limit)


__all__ = ["DbPriorityQueue", "SingleSimulationResultStore"]
