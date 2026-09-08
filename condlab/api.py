"""외부 공개 API. 호출자는 이 모듈(또는 cli.py)만 사용한다."""
from __future__ import annotations

import time
import datetime as _dt
from decimal import Decimal as _Dec
from pathlib import Path

import duckdb

from . import params as P

try:
	from . import config as _cfg
except Exception:
	_cfg = None


def _cv(name, default):
	return getattr(_cfg, name, default) if _cfg is not None else default


DAILY_PQ = _cv("DAILY_PQ", "E:/pq/daily.parquet")
INST_PQ = _cv("INST_PQ", "E:/pq/instrument.parquet")
MEM = _cv("DUCK_MEMORY", "16GB")
THREADS = _cv("DUCK_THREADS", 8)

SQL_DIR = Path(__file__).parent / "sql"

_CON = None
_CTX_MA = None


def _sqlfile(name: str) -> str:
	return (SQL_DIR / name).read_text(encoding="utf-8")


def _con():
	global _CON
	if _CON is None:
		_CON = duckdb.connect()
		_CON.execute(f"SET memory_limit='{MEM}'")
		_CON.execute(f"SET threads={int(THREADS)}")
	return _CON


def _ensure_ctx(ma_period: int):
	"""ma_period가 바뀔 때만 ctx 재생성."""
	global _CTX_MA
	con = _con()
	if _CTX_MA != ma_period:
		sql = _sqlfile("ctx_daily.sql").format(
			DAILY=Path(DAILY_PQ).as_posix(),
			MA=int(ma_period),
			MA_M1=int(ma_period) - 1,
		)
		con.execute(sql)
		_CTX_MA = ma_period
	return con


def scan(date: str, params: dict | None = None) -> dict:
	p = P.merge_cond(params)

	t0 = time.time()
	con = _ensure_ctx(p["ma_period"])
	ctx_sec = time.time() - t0

	sql = _sqlfile("scan.sql").format(INST=Path(INST_PQ).as_posix())
	binds = {"d": str(date)}
	binds.update({k: p[k] for k in P.SQL_KEYS})

	t1 = time.time()
	cur = con.execute(sql, binds)
	cols = [c[0] for c in cur.description]
	hits = [dict(zip(cols, row)) for row in cur.fetchall()]
	scan_sec = time.time() - t1

	return {
		"ok": True,
		"api": "scan",
		"date": str(date),
		"params": p,
		"n_hits": len(hits),
		"hits": hits,
		"elapsed": {
			"ctx_build_sec": round(ctx_sec, 3),
			"scan_sec": round(scan_sec, 4),
		},
	}

def backtest(d_from: str, d_to: str | None = None, params: dict | None = None,
			 bt: dict | None = None, include_trades: bool = True) -> dict:
	from . import backtest as _bt
	return _bt.run(d_from, d_to, params, bt, include_trades)


def _jsonable(v):
	if isinstance(v, _dt.datetime):
		return v.isoformat()
	if isinstance(v, _dt.date):
		return v.isoformat()
	if isinstance(v, _Dec):
		return float(v)
	return v


def _rows(cur):
	cols = [c[0] for c in cur.description]
	return [{c: _jsonable(v) for c, v in zip(cols, row)} for row in cur.fetchall()]


def scan_range(
	d_from: str,
	d_to: str,
	params: dict | None = None,
	include_hits: bool = True,
) -> dict:
	p = P.merge_cond(params)

	t0 = time.time()
	con = _ensure_ctx(p["ma_period"])
	ctx_sec = time.time() - t0

	sql = _sqlfile("scan_range.sql").format(INST=Path(INST_PQ).as_posix())
	binds = {"d_from": str(d_from), "d_to": str(d_to)}
	binds.update({k: p[k] for k in P.SQL_KEYS})

	t1 = time.time()
	hits = _rows(con.execute(sql, binds))
	scan_sec = time.time() - t1

	by_date = {}
	for hit in hits:
		by_date[hit["d"]] = by_date.get(hit["d"], 0) + 1

	out = {
		"ok": True,
		"api": "scan_range",
		"d_from": str(d_from),
		"d_to": str(d_to),
		"params": p,
		"n_hits": len(hits),
		"n_days": len(by_date),
		"by_date": dict(sorted(by_date.items())),
		"elapsed": {
			"ctx_build_sec": round(ctx_sec, 3),
			"scan_sec": round(scan_sec, 4),
		},
	}
	if include_hits:
		out["hits"] = hits
	return out


def health() -> dict:
	con = _con()
	daily = con.execute(
		"SELECT COUNT(*), COUNT(DISTINCT iid), MIN(d), MAX(d) "
		"FROM read_parquet(?)",
		[str(DAILY_PQ)],
	).fetchone()
	return {
		"ok": True,
		"api": "health",
		"daily_parquet": str(DAILY_PQ),
		"inst_parquet": str(INST_PQ),
		"daily_rows": daily[0],
		"daily_codes": daily[1],
		"d_min": _jsonable(daily[2]),
		"d_max": _jsonable(daily[3]),
		"duckdb": duckdb.__version__,
	}


def reset() -> None:
	"""parquet 갱신 후 캐시 무효화."""
	global _CON, _CTX_MA
	if _CON is not None:
		try:
			_CON.close()
		except Exception:
			pass
	_CON = None
	_CTX_MA = None
	try:
		from . import backtest as _bt
		_bt.reset()
	except Exception:
		pass