from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping

import duckdb

from .edt_queue import QueueItem


# DuckDB는 한 프로세스에서 같은 파일을 동시에 두 번 connect하면 "Unique file handle conflict"로 죽는다.
# web은 멀티스레드(ingest 쓰기 + 대시보드 읽기 + lease + 자원기록)라 파일별 lock으로 모든 접근을 직렬화한다.
_DB_LOCKS: dict[str, threading.RLock] = {}
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
        """파일별 lock으로 직렬화된 연결(동시 connect 시 DuckDB file-handle 충돌 방지)."""
        with _db_lock(self.db_path):
            with duckdb.connect(str(self.db_path)) as connection:
                yield connection

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
            # 우선순위 큐(intake :7875가 채우고 lease :7878이 분배). 인메모리 deque였던 것을 영속화.
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS priority_queue (
                    request_id VARCHAR PRIMARY KEY,
                    candidate_toml_text VARCHAR,
                    seed BIGINT,
                    mode VARCHAR,
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

    # --- 우선순위 큐 영속 (intake :7875 적재 / lease :7878 분배) -----------------------------

    def priority_enqueue(self, items: "list[QueueItem]", *, now: float) -> int:
        """우선순위 항목 적재(intake). 같은 request_id는 무시. 적재 시도 건수 반환."""
        self.initialize()
        if not items:
            return 0
        with self._locked_connect() as connection:
            for idx, item in enumerate(items):
                connection.execute(
                    "INSERT OR IGNORE INTO priority_queue (request_id, candidate_toml_text, seed, mode, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    [item.request_id, item.candidate_toml_text, int(item.seed), item.mode, float(now) + idx * 1e-6],
                )
        return len(items)

    def priority_lease(self, n: int) -> "list[QueueItem]":
        """오래된 순으로 최대 n건 pop(원자적: SELECT→DELETE 한 트랜잭션). lease 서버가 호출."""
        self.initialize()
        if n <= 0:
            return []
        with self._locked_connect() as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                rows = connection.execute(
                    "SELECT request_id, candidate_toml_text, seed, mode FROM priority_queue ORDER BY created_at LIMIT ?",
                    [int(n)],
                ).fetchall()
                if rows:
                    ids = [r[0] for r in rows]
                    placeholders = ", ".join("?" for _ in ids)
                    connection.execute(f"DELETE FROM priority_queue WHERE request_id IN ({placeholders})", ids)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        return [QueueItem(request_id=r[0], candidate_toml_text=r[1], seed=int(r[2] or 0), mode=r[3] or "full") for r in rows]

    def priority_depth(self) -> int:
        self.initialize()
        with self._locked_connect() as connection:
            row = connection.execute("SELECT count(*) FROM priority_queue").fetchone()
        return int(row[0]) if row else 0

    def priority_list(self, *, limit: int | None = 200) -> list[dict[str, Any]]:
        """대기 중인 우선순위 항목 조회(pop 안 함; 무거운 toml 본문 제외). 대시보드 입력큐 탭용."""
        self.initialize()
        sql = "SELECT request_id, seed, mode, created_at FROM priority_queue ORDER BY created_at"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._locked_connect() as connection:
            rows = connection.execute(sql).fetchall()
        return [{"request_id": r[0], "seed": int(r[1] or 0), "mode": r[2], "created_at": float(r[3] or 0.0)} for r in rows]


@dataclass
class DbPriorityQueue:
    """DB 영속 우선순위 큐 — `TwoLaneQueue`의 우선순위 메서드를 드롭인 대체(IntakeService/lease 서버용).

    baseline은 컨테이너 자기공급이라 여기선 우선순위 레인만 다룬다. web 재시작에도 미처리 항목이 남는다.
    """

    store: SingleSimulationResultStore
    clock: Any = None  # () -> float; None이면 time.time
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def _now(self) -> float:
        import time

        return (self.clock or time.time)()

    def extend_priority(self, items: "list[QueueItem]") -> None:
        self.store.priority_enqueue(list(items), now=self._now())

    def put_priority(self, item: "QueueItem") -> None:
        self.store.priority_enqueue([item], now=self._now())

    def lease_priority(self, n: int) -> "list[QueueItem]":
        with self._lock:  # 한 web 프로세스 내 동시 lease 직렬화(중복 분배 방지)
            return self.store.priority_lease(n)

    def depths(self) -> tuple[int, int]:
        return (self.store.priority_depth(), 0)


__all__ = ["DbPriorityQueue", "SingleSimulationResultStore"]
