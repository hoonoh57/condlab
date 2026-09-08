"""조건식 정의·실행·일봉 성과검증. feat.parquet 단일 참조."""
from __future__ import annotations

import datetime as dt
import math
import time

from . import api, config

BETA_COLS = ("beta60", "beta120", "beta250", "beta360")
_FWD = None

SPECS = {
	"base": {
		"label": "기준봉돌파",
		"note": "지정기간 내 급등 기준봉 발생 → 60이평 위에서 JMA(7,50,2) 상승돌파",
		"params": {
			"base_chg_min": 10.0, "base_days_min": 3, "base_days_max": 60,
			"base_hi_over_min": -15.0, "base_hi_over_max": 5.0,
			"need_ma60": 1, "need_jma_cross": 1, "need_jma_dir": 1,
			"min_prev_amt": 3000000000, "min_amt20": 3000000000,
			"min_price": 1000, "max_price": 0, "min_vola": 0.0,
			"chg_min": 0.0, "chg_max": 20.0,
			"market": "ALL", "sec_class": "COMMON",
		},
	},
	"beta": {
		"label": "고베타·중저가",
		"note": "KOSPI/KOSDAQ 지수 대비 베타 상위 + 중저가 고변동",
		"params": {
			"beta_col": "beta250", "beta_min": 1.0, "beta_max": 10.0,
			"beta_col2": "beta60", "beta2_min": 0.0,
			"min_prev_amt": 10000000000, "min_amt20": 0,
			"min_price": 1000, "max_price": 0,
			"min_vola": 0.0, "max_mcap": 0,
			"chg_min": -30.0, "chg_max": 30.0,
			"market": "ALL", "sec_class": "COMMON",
		},
	},
	"sector": {
		"label": "섹터주도주",
		"note": "미구현: 종목-업종 매핑 테이블 필요 (ka10099/ka10101 적재 후 활성화)",
		"params": {},
	},
}


def _ensure(con):
	if not config.FEAT_PQ.exists():
		raise RuntimeError("feat.parquet 없음. features.build() 먼저 실행")
	con.execute("CREATE OR REPLACE VIEW screen_feat AS SELECT * FROM "
				f"read_parquet('{config.FEAT_PQ.as_posix()}')")
	return con


def reset() -> None:
	global _FWD
	_FWD = None


def merge(name: str, user: dict | None = None) -> dict:
	if name not in SPECS:
		raise ValueError(f"unknown screen: {name} (있는 것: {sorted(SPECS)})")
	if name == "sector":
		raise RuntimeError(SPECS["sector"]["note"])
	out = dict(SPECS[name]["params"])
	unknown = set(user or {}) - set(out)
	if unknown:
		raise ValueError(f"unknown param: {sorted(unknown)}")
	for key, value in (user or {}).items():
		default = out[key]
		out[key] = str(value) if isinstance(default, str) else (
			int(value) if isinstance(default, int) else float(value))
	for key in ("beta_col", "beta_col2"):
		if key in out and out[key] not in BETA_COLS:
			raise ValueError(f"{key} must be one of {BETA_COLS}")
	for key, value in out.items():
		if isinstance(value, float) and not math.isfinite(value):
			raise ValueError(f"{key} must be finite")
	return out


def _common(p: dict) -> list[str]:
	p = {key: value.replace("'", "''") if isinstance(value, str) else value
		 for key, value in p.items()}
	clauses = [
		f"c >= {p['min_price']}",
		f"(({p['max_price']} = 0) OR c <= {p['max_price']})",
		f"prev_amt >= {p['min_prev_amt']}",
		f"amt20 >= {p['min_amt20']}",
		f"vola20 >= {p['min_vola']}",
		f"chg_pct BETWEEN {p['chg_min']} AND {p['chg_max']}",
	]
	if p["market"] != "ALL":
		clauses.append(f"market = '{p['market']}'")
	if p["sec_class"] != "ALL":
		clauses.append(f"sec_class = '{p['sec_class']}'")
	return clauses


def _where(name: str, p: dict) -> list[str]:
	clauses = _common(p)
	if name == "base":
		clauses += [
			f"base_days BETWEEN {p['base_days_min']} AND {p['base_days_max']}",
			f"base_chg >= {p['base_chg_min']}",
			f"base_hi_over BETWEEN {p['base_hi_over_min']} AND {p['base_hi_over_max']}",
			f"(({p['need_ma60']} = 0) OR c > ma60)",
			f"(({p['need_jma_cross']} = 0) OR (prev_c <= jma_p AND c > jma))",
			f"(({p['need_jma_dir']} = 0) OR jma_dir = 1)",
		]
	else:
		clauses += [
			f"{p['beta_col']} BETWEEN {p['beta_min']} AND {p['beta_max']}",
			f"{p['beta_col2']} >= {p['beta2_min']}",
			f"(({p['max_mcap']} = 0) OR mcap <= {p['max_mcap']})",
		]
	return clauses


COLS = ("d, code, name, market, c, chg_pct, round(prev_amt / 100000000.0, 0) AS prev_amt_억, "
		"round(amt20 / 100000000.0, 0) AS amt20_억, vola20, ma60_over, "
		"beta60, beta250, jma, jma_slope, base_days, base_chg, base_hi_over, "
		"CAST(mcap / 100000000 AS BIGINT) AS mcap_억")


def run(name: str, d_from: str, d_to: str | None = None,
		overrides: dict | None = None, limit: int = 500) -> dict:
	started = time.time()
	p = merge(name, overrides)
	con = _ensure(api._con())
	d_from = dt.date.fromisoformat(d_from).isoformat()
	d_to = dt.date.fromisoformat(d_to or d_from).isoformat()
	if d_from > d_to or int(limit) < 0:
		raise ValueError("invalid date range or limit")
	where = " AND ".join(_where(name, p))
	scope = f"d BETWEEN DATE '{d_from}' AND DATE '{d_to}'"
	con.execute(f"CREATE OR REPLACE TABLE hits AS "
				f"SELECT * FROM screen_feat WHERE {scope} AND {where}")
	total, days = con.execute("SELECT count(*), count(DISTINCT d) FROM hits").fetchone()
	pool = con.execute(f"SELECT count(DISTINCT d), count(*) FROM screen_feat WHERE {scope} "
					   "AND sec_class = 'COMMON'").fetchone()
	by_date = con.execute(
		"SELECT CAST(d AS VARCHAR) AS d, count(*) AS n FROM hits GROUP BY 1 ORDER BY 1"
	).fetchall()
	rows = api._rows(con.execute(f"SELECT {COLS} FROM hits ORDER BY d DESC, prev_amt DESC "
					   f"LIMIT {int(limit)}"))
	return {
		"ok": True, "screen": name, "label": SPECS[name]["label"],
		"d_from": d_from, "d_to": d_to, "params": p,
		"n_hits": total, "n_days_hit": days, "n_days_pool": pool[0],
		"per_day": round(total / pool[0], 2) if pool[0] else None,
		"pool_rows": pool[1],
		"pct_of_pool": round(100.0 * total / pool[1], 2) if pool[1] else None,
		"by_date": [{"d": d, "n": n} for d, n in by_date],
		"rows": rows, "elapsed_sec": round(time.time() - started, 2),
	}


def _ensure_fwd(con, marks: tuple) -> None:
	global _FWD
	stamp = (con, marks, config.DAILY_PQ, config.DAILY_PQ.stat().st_mtime_ns)
	if _FWD == stamp:
		return
	leads = ", ".join(f"lead(c, {m}) OVER pw AS c{m}" for m in marks)
	horizon = max(marks)
	con.execute(f"""
	CREATE OR REPLACE TABLE fwd AS
	SELECT iid, d, c AS c0, {leads},
		   max(h) OVER wf AS hmax, min(l) OVER wf AS lmin
	FROM read_parquet('{config.DAILY_PQ.as_posix()}')
	WINDOW pw AS (PARTITION BY iid ORDER BY d),
		   wf AS (PARTITION BY iid ORDER BY d
				  ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING)""")
	_FWD = stamp


def _stats(con, source: str, marks: tuple) -> dict:
	cols = ", ".join(
		f"round(avg(f.c{m} / f.c0 - 1) * 100, 3) AS r{m}, "
		f"round(100.0 * avg(CASE WHEN f.c{m} IS NULL THEN NULL WHEN f.c{m} > f.c0 THEN 1.0 ELSE 0.0 END), 2) AS win{m}"
		for m in marks)
	row = con.execute(f"""
		SELECT count(*) AS n, {cols},
			   round(avg(f.hmax / f.c0 - 1) * 100, 3) AS mfe,
			   round(avg(f.lmin / f.c0 - 1) * 100, 3) AS mae
		FROM {source} s JOIN fwd f ON f.iid = s.iid AND f.d = s.d
	""").fetchdf().to_dict("records")[0]
	return {key: (None if value != value else value) for key, value in row.items()}


def verify(name: str, d_from: str, d_to: str | None = None,
		   overrides: dict | None = None, marks=(1, 3, 5, 10, 20)) -> dict:
	marks = tuple(sorted({int(m) for m in marks if int(m) > 0}))
	if not marks:
		raise ValueError("marks must contain a positive horizon")
	result = run(name, d_from, d_to, overrides, limit=0)
	con = api._con()
	_ensure_fwd(con, marks)
	con.execute(f"""CREATE OR REPLACE TABLE poolday AS
		SELECT iid, d FROM screen_feat
		WHERE d IN (SELECT DISTINCT d FROM hits)
		  AND sec_class = 'COMMON' AND prev_amt >= 1000000000""")
	hit, base = _stats(con, "hits", marks), _stats(con, "poolday", marks)
	edge = {f"edge_r{m}": (None if hit[f"r{m}"] is None or base[f"r{m}"] is None
						   else round(hit[f"r{m}"] - base[f"r{m}"], 3)) for m in marks}
	result.pop("rows", None)
	result["marks"] = list(marks)
	result["hit"], result["base"], result["edge"] = hit, base, edge
	return result
