"""MySQL -> parquet 동기화. 하루 단위 멱등 처리, 중단·재시작 안전."""
from __future__ import annotations

import datetime as dt
import shutil
import threading
import time
import traceback
from pathlib import Path

import duckdb

from . import config, store

MARKET_CLOSE = dt.time(15, 40)
MIN1_COLS = """
       CAST(instrument_id AS INTEGER) AS iid,
       CAST(bar_timestamp AS TIMESTAMP) AS ts,
       CAST(open AS INTEGER) AS o,
       CAST(high AS INTEGER) AS h,
       CAST(low AS INTEGER) AS l,
       CAST(close AS INTEGER) AS c,
       CAST(volume AS BIGINT) AS v
"""


class SyncState:
    def __init__(self):
        self.running = False
        self.mode = ""
        self.phase = "idle"
        self.total = 0
        self.done = 0
        self.current = ""
        self.rows_added = 0
        self.started_at = None
        self.finished_at = None
        self.error = None
        self.cancel = threading.Event()
        self.log: list[str] = []
        self._lock = threading.Lock()

    def say(self, msg: str) -> None:
        line = f"[{dt.datetime.now():%H:%M:%S}] {msg}"
        with self._lock:
            self.log.append(line)
            if len(self.log) > 500:
                self.log = self.log[-400:]
        print(line, flush=True)

    def snapshot(self) -> dict:
        with self._lock:
            tail = self.log[-120:]
        eta = None
        if self.running and self.done and self.started_at:
            per = (time.time() - self.started_at) / self.done
            eta = int(per * max(self.total - self.done, 0))
        return {
            "running": self.running,
            "mode": self.mode,
            "phase": self.phase,
            "total": self.total,
            "done": self.done,
            "current": self.current,
            "rows_added": self.rows_added,
            "eta_sec": eta,
            "error": self.error,
            "cancelled": self.cancel.is_set(),
            "log": tail,
        }


STATE = SyncState()
_THREAD: threading.Thread | None = None
_START_LOCK = threading.Lock()


def _tmp_dir() -> Path:
    path = config.PQ_DIR / "tmp"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _con():
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{config.DUCK_MEMORY}'")
    con.execute(f"SET threads={config.DUCK_THREADS}")
    con.execute(f"SET temp_directory='{_tmp_dir().as_posix()}'")
    con.execute("INSTALL mysql")
    con.execute("LOAD mysql")
    con.execute(config.mysql_attach())
    return con


def mysql_rows(con, sql: str):
    return con.execute(f"SELECT * FROM mysql_query('my', $${sql}$$)").fetchall()


def _dir_bytes(path: Path) -> int:
    return sum(file.stat().st_size for file in path.rglob("*.parquet"))


def sync_daily(con, state: SyncState) -> None:
    state.phase = "daily"
    state.current = "daily.parquet"
    temp = _tmp_dir() / "daily.tmp.parquet"
    if temp.exists():
        temp.unlink()
    started = time.time()
    con.execute(f"""
    COPY (
      SELECT CAST(instrument_id AS INTEGER) AS iid,
             CAST(trading_date AS DATE) AS d,
             CAST(open AS INTEGER) AS o, CAST(high AS INTEGER) AS h,
             CAST(low AS INTEGER) AS l, CAST(close AS INTEGER) AS c,
             CAST(volume AS BIGINT) AS v, CAST(amount AS BIGINT) AS amt
      FROM my.korean_equity_daily
      ORDER BY instrument_id, trading_date
    ) TO '{temp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
    """)
    config.DAILY_PQ.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(temp), str(config.DAILY_PQ))
    count, last_day = con.execute(
        f"SELECT COUNT(*), MAX(d) FROM read_parquet('{config.DAILY_PQ.as_posix()}')"
    ).fetchone()
    state.say(f"일봉 갱신 완료 {count:,}행, 최종 {last_day} ({time.time() - started:.1f}s)")

    state.current = "instrument.parquet"
    temp_inst = _tmp_dir() / "inst.tmp.parquet"
    if temp_inst.exists():
        temp_inst.unlink()
    con.execute(f"""
    COPY (
      SELECT CAST(instrument_id AS INTEGER) AS iid,
             code, name, market, sec_class,
             CAST(first_seen_date AS DATE) AS d_first,
             CAST(last_seen_date AS DATE) AS d_last
      FROM my.market_instrument ORDER BY instrument_id
    ) TO '{temp_inst.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
    """)
    shutil.move(str(temp_inst), str(config.INST_PQ))
    state.say("종목마스터 갱신 완료")


def sync_ref(con, state: SyncState) -> None:
    """지수 일봉 + 시가총액(월말 스냅샷) 동기화."""
    state.phase = "ref"
    jobs = (
        ("index.parquet", config.INDEX_PQ, """
            SELECT index_code AS code, CAST(trading_date AS DATE) AS d,
                   CAST(open AS DOUBLE) AS o, CAST(high AS DOUBLE) AS h,
                   CAST(low AS DOUBLE) AS l, CAST(close AS DOUBLE) AS c,
                   CAST(volume AS BIGINT) AS v
            FROM my.market_index_daily ORDER BY index_code, trading_date"""),
        ("mcap.parquet", config.MCAP_PQ, """
            SELECT CAST(instrument_id AS INTEGER) AS iid,
                   CAST(trading_date AS DATE) AS d,
                   CAST(listed_shares AS BIGINT) AS listed_shares,
                   CAST(market_cap AS BIGINT) AS market_cap
            FROM my.korean_equity_market_cap ORDER BY instrument_id, trading_date"""),
    )
    for label, dest, query in jobs:
        state.current = label
        temp = _tmp_dir() / (label + ".tmp")
        if temp.exists():
            temp.unlink()
        con.execute(f"COPY ({query}) TO '{temp.as_posix()}' "
                    "(FORMAT parquet, COMPRESSION zstd)")
        shutil.move(str(temp), str(dest))
        count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{dest.as_posix()}')").fetchone()[0]
        state.say(f"{label} 갱신 완료 {count:,}행")


def min1_extent(con) -> tuple:
    return mysql_rows(con, """
      SELECT MIN(trading_date), MAX(trading_date)
      FROM market_data.korean_equity_minute_1m
    """)[0]


def calendar_days(con, lo, hi) -> list:
    rows = con.execute(
        f"SELECT DISTINCT d FROM read_parquet('{config.DAILY_PQ.as_posix()}') "
        "WHERE d BETWEEN ? AND ? ORDER BY d", [lo, hi]
    ).fetchall()
    return [row[0] for row in rows]


def _skip_today(day) -> bool:
    now = dt.datetime.now()
    return day == now.date() and now.time() < MARKET_CLOSE


def sync_min1_day(con, day) -> tuple[int, int]:
    stage = _tmp_dir() / f"stage_d={day}"
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)
    source = f"""
      SELECT instrument_id, bar_timestamp, open, high, low, close, volume
      FROM market_data.korean_equity_minute_1m
           FORCE INDEX (idx_korean_equity_minute_1m_date)
      WHERE trading_date = '{day}'
    """
    output = stage / "part.parquet"
    con.execute(f"""
    COPY (
      SELECT {MIN1_COLS}
      FROM mysql_query('my', $${source}$$)
      ORDER BY iid, ts
    ) TO '{output.as_posix()}'
      (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
    """)
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output.as_posix()}')").fetchone()[0]
    size = output.stat().st_size
    destination = config.MIN1_DIR / f"d={day}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    shutil.move(str(stage), str(destination))
    return int(count), int(size)


def reconcile(state: SyncState) -> dict:
    state.phase = "reconcile"
    con = duckdb.connect()
    found = 0
    try:
        if not config.MIN1_DIR.exists():
            state.say("분봉 폴더 없음 - 매니페스트 비움")
            store.forget_days("min1")
            return {"days": 0}
        directories = sorted(path for path in config.MIN1_DIR.glob("d=*") if path.is_dir())
        state.total, state.done = len(directories), 0
        store.forget_days("min1")
        for path in directories:
            if state.cancel.is_set():
                state.say("정합성 검사 취소")
                break
            day = path.name.split("=", 1)[1]
            files = list(path.glob("*.parquet"))
            if not files:
                state.say(f"{day} 빈 폴더 - 미완료 처리")
                state.done += 1
                continue
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path.as_posix()}/*.parquet')").fetchone()[0]
            store.mark_day("min1", day, int(count), _dir_bytes(path))
            found += 1
            state.done += 1
            state.current = day
        state.say(f"정합성 검사 완료: {found}일 등록")
        return {"days": found}
    finally:
        con.close()


def _run(mode: str) -> None:
    state = STATE
    run_id = store.start_run(mode)
    days_done = 0
    ok = False
    note = ""
    con = None
    try:
        state.say(f"=== 동기화 시작 (mode={mode}) ===")
        if mode == "reconcile":
            reconcile(state)
            ok = True
            return
        con = _con()
        state.say("MySQL 연결 OK")
        if mode in ("auto", "full", "daily"):
            sync_daily(con, state)
            sync_ref(con, state)
        if mode in ("auto", "full", "min1"):
            low, high = min1_extent(con)
            state.say(f"MySQL 분봉 범위 {low} ~ {high}")
            calendar = calendar_days(con, low, high)
            have = {str(day) for day in store.done_days("min1")}
            todo = [day for day in calendar if str(day) not in have and not _skip_today(day)]
            if mode == "full":
                todo = [day for day in calendar if not _skip_today(day)]
            state.phase = "min1"
            state.total, state.done = len(todo), 0
            state.say(f"대상 {len(todo)}일 (보유 {len(have)}일 건너뜀)")
            for day in todo:
                if state.cancel.is_set():
                    note = f"사용자 취소 ({days_done}일 완료)"
                    state.say(note)
                    break
                started = time.time()
                state.current = str(day)
                count, size = sync_min1_day(con, day)
                store.mark_day("min1", day, count, size)
                state.rows_added += count
                days_done += 1
                state.done += 1
                state.say(f"{day}  {count:,}행  {size / 1048576:.1f}MB  {time.time() - started:.1f}s")
            else:
                ok = True
        else:
            ok = True
        if ok and mode in ("auto", "full", "daily", "min1"):
            store.set_meta("last_sync_date", dt.date.today().isoformat())
            store.set_meta("last_sync_at", dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        state.say("=== 동기화 종료 ===")
    except Exception as error:
        state.error = f"{type(error).__name__}: {error}"
        note = state.error
        state.say(f"[오류] {state.error}")
        state.say(traceback.format_exc()[-1500:])
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        store.finish_run(run_id, ok, days_done, state.rows_added, note)
        state.running = False
        state.phase = "done" if ok else "error"
        state.finished_at = time.time()
        state.current = ""
        try:
            from . import api
            api.reset()
        except Exception:
            pass


def start(mode: str = "auto") -> dict:
    global _THREAD
    with _START_LOCK:
        if STATE.running:
            return {"ok": False, "msg": "이미 실행 중입니다"}
        STATE.running = True
        STATE.mode = mode
        STATE.phase = "starting"
        STATE.total = STATE.done = STATE.rows_added = 0
        STATE.current = ""
        STATE.error = None
        STATE.cancel.clear()
        STATE.started_at = time.time()
        STATE.finished_at = None
        STATE.log = []
        _THREAD = threading.Thread(target=_run, args=(mode,), daemon=True)
        _THREAD.start()
        return {"ok": True, "msg": f"{mode} 동기화 시작"}


def cancel() -> dict:
    if not STATE.running:
        return {"ok": False, "msg": "실행 중이 아닙니다"}
    STATE.cancel.set()
    return {"ok": True, "msg": "현재 날짜 처리 후 안전하게 중단합니다"}


def needs_today() -> bool:
    return store.get_meta("last_sync_date") != dt.date.today().isoformat()


def status() -> dict:
    coverage = store.coverage("min1")
    output = {
        "sync": STATE.snapshot(),
        "min1_coverage": coverage,
        "needs_today_sync": needs_today(),
        "last_sync_at": store.get_meta("last_sync_at"),
        "daily_pq": config.health(),
        "market_close": MARKET_CLOSE.strftime("%H:%M"),
    }
    try:
        con = duckdb.connect()
        row = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT iid), MAX(d) FROM read_parquet('{config.DAILY_PQ.as_posix()}')"
        ).fetchone()
        output["daily"] = {
            "rows": row[0], "codes": row[1],
            "d_max": row[2].isoformat() if row[2] else None,
        }
        con.close()
    except Exception as error:
        output["daily"] = {"error": str(error)}
    return output
