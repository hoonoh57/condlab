"""실행 이력·동기화 매니페스트 저장소. DuckDB 파일 1개."""
from __future__ import annotations

import datetime as dt
import json
import threading

import duckdb

from . import config

_LOCK = threading.Lock()

_DDL = [
	"""CREATE TABLE IF NOT EXISTS sync_day (
		kind VARCHAR, d DATE, rows BIGINT, bytes BIGINT, done_at TIMESTAMP
	)""",
	"""CREATE TABLE IF NOT EXISTS sync_run (
		run_id BIGINT, mode VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP,
		ok BOOLEAN, days_done INTEGER, rows_added BIGINT, note VARCHAR
	)""",
	"""CREATE TABLE IF NOT EXISTS meta (k VARCHAR, v VARCHAR)""",
	"""CREATE TABLE IF NOT EXISTS cond_set (
		name VARCHAR, ver INTEGER, params VARCHAR, bt VARCHAR,
		note VARCHAR, created_at TIMESTAMP
	)""",
	"""CREATE TABLE IF NOT EXISTS bt_result (
		run_at TIMESTAMP, name VARCHAR, ver INTEGER, d_from DATE, d_to DATE,
		n_days INTEGER, n_trades INTEGER, up_rate DOUBLE, win_rate DOUBLE,
		avg_ret DOUBLE, avg_eod DOUBLE, avg_mfe DOUBLE, avg_mae DOUBLE,
		avg_alpha DOUBLE, params VARCHAR, bt VARCHAR, summary VARCHAR
	)""",
]


def _open():
	config.STORE_DB.parent.mkdir(parents=True, exist_ok=True)
	con = duckdb.connect(str(config.STORE_DB))
	for query in _DDL:
		con.execute(query)
	return con


def init() -> None:
	with _LOCK:
		_open().close()


def get_meta(key: str, default=None):
	with _LOCK:
		con = _open()
		try:
			row = con.execute("SELECT v FROM meta WHERE k = ?", [key]).fetchone()
			return row[0] if row else default
		finally:
			con.close()


def set_meta(key: str, value: str) -> None:
	with _LOCK:
		con = _open()
		try:
			con.execute("DELETE FROM meta WHERE k = ?", [key])
			con.execute("INSERT INTO meta VALUES (?, ?)", [key, str(value)])
		finally:
			con.close()


def done_days(kind: str = "min1") -> set:
	with _LOCK:
		con = _open()
		try:
			rows = con.execute(
				"SELECT d FROM sync_day WHERE kind = ? AND rows > 0", [kind]
			).fetchall()
			return {row[0] for row in rows}
		finally:
			con.close()


def mark_day(kind: str, day, rows: int, nbytes: int) -> None:
	with _LOCK:
		con = _open()
		try:
			con.execute("DELETE FROM sync_day WHERE kind = ? AND d = ?", [kind, day])
			con.execute(
				"INSERT INTO sync_day VALUES (?, ?, ?, ?, ?)",
				[kind, day, rows, nbytes, dt.datetime.now()],
			)
		finally:
			con.close()


def forget_days(kind: str = "min1") -> None:
	with _LOCK:
		con = _open()
		try:
			con.execute("DELETE FROM sync_day WHERE kind = ?", [kind])
		finally:
			con.close()


def coverage(kind: str = "min1") -> dict:
	with _LOCK:
		con = _open()
		try:
			row = con.execute(
				"SELECT COUNT(*), COALESCE(SUM(rows), 0), COALESCE(SUM(bytes), 0), "
				"MIN(d), MAX(d) FROM sync_day WHERE kind = ?",
				[kind],
			).fetchone()
			return {
				"days": row[0],
				"rows": int(row[1]),
				"bytes": int(row[2]),
				"d_min": row[3].isoformat() if row[3] else None,
				"d_max": row[4].isoformat() if row[4] else None,
			}
		finally:
			con.close()


def start_run(mode: str) -> int:
	run_id = int(dt.datetime.now().timestamp() * 1000)
	with _LOCK:
		con = _open()
		try:
			con.execute(
				"INSERT INTO sync_run VALUES (?, ?, ?, NULL, NULL, 0, 0, NULL)",
				[run_id, mode, dt.datetime.now()],
			)
		finally:
			con.close()
	return run_id


def finish_run(run_id: int, ok: bool, days: int, rows: int, note: str = "") -> None:
	with _LOCK:
		con = _open()
		try:
			con.execute(
				"UPDATE sync_run SET finished_at = ?, ok = ?, days_done = ?, "
				"rows_added = ?, note = ? WHERE run_id = ?",
				[dt.datetime.now(), ok, days, rows, note[:500], run_id],
			)
		finally:
			con.close()


def recent_runs(limit: int = 30) -> list[dict]:
	with _LOCK:
		con = _open()
		try:
			cur = con.execute(
				"SELECT run_id, mode, started_at, finished_at, ok, days_done, "
				"rows_added, note FROM sync_run ORDER BY started_at DESC LIMIT ?",
				[limit],
			)
			columns = [column[0] for column in cur.description]
			result = []
			for row in cur.fetchall():
				record = dict(zip(columns, row))
				for key in ("started_at", "finished_at"):
					if record[key] is not None:
						record[key] = record[key].strftime("%Y-%m-%d %H:%M:%S")
				result.append(record)
			return result
		finally:
			con.close()


def save_cond(name: str, params: dict, bt: dict, note: str = "") -> dict:
	with _LOCK:
		con = _open()
		try:
			ver = int(con.execute(
				"SELECT COALESCE(MAX(ver), 0) FROM cond_set WHERE name = ?", [name]
			).fetchone()[0]) + 1
			con.execute("INSERT INTO cond_set VALUES (?, ?, ?, ?, ?, ?)", [
				name, ver, json.dumps(params, ensure_ascii=False),
				json.dumps(bt, ensure_ascii=False), note, dt.datetime.now()])
			return {"name": name, "ver": ver}
		finally:
			con.close()


def list_conds() -> list[dict]:
	with _LOCK:
		con = _open()
		try:
			rows = con.execute(
				"SELECT name, ver, params, bt, note, created_at FROM cond_set "
				"ORDER BY name, ver DESC").fetchall()
			return [{"name": row[0], "ver": row[1], "params": json.loads(row[2]),
					 "bt": json.loads(row[3]), "note": row[4],
					 "created_at": row[5].strftime("%Y-%m-%d %H:%M:%S")} for row in rows]
		finally:
			con.close()


def get_cond(name: str, ver: int | None = None) -> dict | None:
	with _LOCK:
		con = _open()
		try:
			if ver:
				row = con.execute(
					"SELECT ver, params, bt, note FROM cond_set "
					"WHERE name = ? AND ver = ?", [name, int(ver)]).fetchone()
			else:
				row = con.execute(
					"SELECT ver, params, bt, note FROM cond_set "
					"WHERE name = ? ORDER BY ver DESC LIMIT 1", [name]).fetchone()
			if not row:
				return None
			return {"name": name, "ver": row[0], "params": json.loads(row[1]),
					"bt": json.loads(row[2]), "note": row[3]}
		finally:
			con.close()


def delete_cond(name: str, ver: int | None = None) -> None:
	with _LOCK:
		con = _open()
		try:
			if ver:
				con.execute("DELETE FROM cond_set WHERE name = ? AND ver = ?", [name, int(ver)])
			else:
				con.execute("DELETE FROM cond_set WHERE name = ?", [name])
		finally:
			con.close()


def save_bt_result(name: str, ver: int, result: dict) -> None:
	summary = result.get("summary", {})
	with _LOCK:
		con = _open()
		try:
			con.execute("INSERT INTO bt_result VALUES "
						"(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
				dt.datetime.now(), name, int(ver), result["d_from"], result["d_to"],
				result.get("n_days"), summary.get("n_trades"), summary.get("up_rate"),
				summary.get("win_rate"), summary.get("avg_ret"), summary.get("avg_eod"),
				summary.get("avg_mfe"), summary.get("avg_mae"), summary.get("avg_alpha_eod"),
				json.dumps(result.get("params"), ensure_ascii=False),
				json.dumps(result.get("bt"), ensure_ascii=False),
				json.dumps(summary, ensure_ascii=False)])
		finally:
			con.close()


def recent_bt(limit: int = 50) -> list[dict]:
	with _LOCK:
		con = _open()
		try:
			cur = con.execute(
				"SELECT run_at, name, ver, d_from, d_to, n_days, n_trades, up_rate, "
				"win_rate, avg_ret, avg_eod, avg_mfe, avg_mae, avg_alpha FROM bt_result "
				"ORDER BY run_at DESC LIMIT ?", [limit])
			columns = [column[0] for column in cur.description]
			result = []
			for row in cur.fetchall():
				record = dict(zip(columns, row))
				record["run_at"] = record["run_at"].strftime("%m-%d %H:%M:%S")
				for key in ("d_from", "d_to"):
					record[key] = str(record[key])
				result.append(record)
			return result
		finally:
			con.close()