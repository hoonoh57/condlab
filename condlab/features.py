"""일봉 파생지표 테이블. 조건식·성과검증이 참조하는 단일 진실원.

이동평균·베타·기준봉과 JMA의 _p 컬럼은 D-1까지의 데이터로 계산한다.
OHLCV·거래대금·등락률·이격도·접미사 없는 JMA는 당일 값을 포함한다.
장중 검증에서는 당일 일봉 대신 해당 시점 분봉과 _p 지표를 사용해야 한다.
"""
from __future__ import annotations

import time
from pathlib import Path

import duckdb

from . import api, config
from .jma import step as jma_step

BASE_CHG = 10.0     # 기준봉 최소 등락률(%)
BASE_LOOK = 60      # 기준봉 탐색 최대 경과 거래일
JMA_PARAM = (7, 50, 2)
KS_CANDIDATES = ("U001", "KOSPI", "KS11", "KOSPI200", "0001")
KQ_CANDIDATES = ("U201", "KOSDAQ", "KQ11", "1001")


def _pq(path) -> str:
	return Path(path).as_posix()


def _index_codes(con) -> dict:
	if not config.INDEX_PQ.exists():
		return {}
	have = {row[0] for row in con.execute(
		f"SELECT DISTINCT code FROM read_parquet('{_pq(config.INDEX_PQ)}')").fetchall()}
	found = {}
	for market, candidates in (("KOSPI", KS_CANDIDATES), ("KOSDAQ", KQ_CANDIDATES)):
		for code in candidates:
			if code in have:
				found[market] = code
				break
	return found if len(found) == 2 else {}


def _mret_sql(con) -> tuple[str, str]:
	codes = _index_codes(con)
	if codes:
		sql = f"""
		SELECT market, d, c / nullif(lag(c) OVER (PARTITION BY market ORDER BY d), 0) - 1 AS rm
		FROM (
			SELECT CASE code WHEN '{codes['KOSPI']}' THEN 'KOSPI'
							 WHEN '{codes['KOSDAQ']}' THEN 'KOSDAQ' END AS market,
				   d, c
			FROM read_parquet('{_pq(config.INDEX_PQ)}')
			WHERE code IN ('{codes['KOSPI']}', '{codes['KOSDAQ']}')
		)"""
		return sql, f"index:{codes['KOSPI']}/{codes['KOSDAQ']}"
	sql = f"""
	SELECT market, d, avg(c / nullif(prev_c, 0) - 1) AS rm
	FROM (
		SELECT i.market, p.d, p.c,
			   lag(p.c) OVER (PARTITION BY p.iid ORDER BY p.d) AS prev_c
		FROM read_parquet('{_pq(config.DAILY_PQ)}') p
		JOIN read_parquet('{_pq(config.INST_PQ)}') i USING (iid)
	) GROUP BY 1, 2"""
	return sql, "eqw-fallback"


def _build_jma(con) -> int:
	period, phase, power = JMA_PARAM
	rows = con.execute(
		f"SELECT iid, d, c FROM read_parquet('{_pq(config.DAILY_PQ)}') ORDER BY iid, d"
	).fetchall()
	iids, days, values, dirs, slopes = [], [], [], [], []
	current, state = None, None
	for iid, day, close in rows:
		if iid != current:
			current, state = iid, None
		state, value, direction, slope = jma_step(state, float(close), period, phase, power)
		iids.append(iid)
		days.append(day)
		values.append(value)
		dirs.append(direction)
		slopes.append(slope)
	try:
		import pyarrow as pa
		table = pa.table({"iid": iids, "d": days, "jma": values,
						  "jdir": dirs, "jslope": slopes})
		con.register("jma_raw", table)
	except ImportError:
		import pandas as pd
		con.register("jma_raw", pd.DataFrame({
			"iid": iids, "d": days, "jma": values, "jdir": dirs, "jslope": slopes}))
	con.execute(f"""
	CREATE OR REPLACE TABLE jmat AS
	SELECT iid, d, jma, jdir, jslope,
		   lag(jma) OVER pw AS jma_p,
		   lag(jdir) OVER pw AS jdir_p,
		   lag(jslope) OVER pw AS jslope_p
	FROM jma_raw
	WINDOW pw AS (PARTITION BY iid ORDER BY d)""")
	con.execute(f"COPY jmat TO '{_pq(config.JMA_PQ)}' (FORMAT parquet, COMPRESSION zstd)")
	return len(rows)


def _beta_windows() -> tuple[str, str, str]:
	sums, wins, cols = [], [], []
	for size in (20, 60, 120, 250, 360):
		sums.append(f"""
		count(rx) OVER b{size} AS n{size}b, sum(rx * rm) OVER b{size} AS sxy{size},
		sum(rx) OVER b{size} AS sx{size}, sum(rm) OVER b{size} AS sy{size},
		sum(rm * rm) OVER b{size} AS syy{size}""")
		wins.append(f"b{size} AS (PARTITION BY iid ORDER BY d "
					f"ROWS BETWEEN {size} PRECEDING AND 1 PRECEDING)")
		cols.append(f"""
		CASE WHEN w.n{size}b >= {int(size * 0.8)} THEN round(
			(w.n{size}b * w.sxy{size} - w.sx{size} * w.sy{size}) /
			nullif(w.n{size}b * w.syy{size} - w.sy{size} * w.sy{size}, 0), 3)
		END AS beta{size}""")
	return ",".join(sums), ",\n\t\t".join(wins), ",".join(cols)


def build() -> dict:
	started = time.time()
	con = api._con()
	con.execute("SET preserve_insertion_order=false")
	jma_rows = _build_jma(con)
	mret_sql, mret_src = _mret_sql(con)
	beta_sums, beta_wins, beta_cols = _beta_windows()

	con.execute(f"""
	CREATE OR REPLACE TABLE feat AS
	WITH px AS (
		SELECT p.iid, p.d, p.o, p.h, p.l, p.c, p.v, p.amt,
			   i.code, i.name, i.market, i.sec_class,
			   lag(p.c) OVER pw AS prev_c, lag(p.v) OVER pw AS prev_v,
			   lag(p.amt) OVER pw AS prev_amt, row_number() OVER pw AS rn
		FROM read_parquet('{_pq(config.DAILY_PQ)}') p
		JOIN read_parquet('{_pq(config.INST_PQ)}') i USING (iid)
		WHERE i.d_first <= p.d AND i.d_last >= p.d
		WINDOW pw AS (PARTITION BY p.iid ORDER BY p.d)
	),
	mr AS ({mret_sql}),
	j AS (
		SELECT px.*, px.c / nullif(px.prev_c, 0) - 1 AS rx, mr.rm,
			   CASE WHEN px.prev_c > 0
					 AND (px.c / px.prev_c - 1) * 100 >= {BASE_CHG} THEN 1 ELSE 0 END AS is_base
		FROM px LEFT JOIN mr ON mr.market = px.market AND mr.d = px.d
	),
	w AS (
		SELECT j.*,
			avg(c) OVER w5 AS ma5_p, avg(c) OVER w20 AS ma20_p,
			avg(c) OVER w60 AS ma60_p, avg(c) OVER w120 AS ma120_p,
			count(c) OVER w60 AS n60,
			avg(amt) OVER w20 AS amt20_p,
			avg((h - l) / nullif(prev_c, 0)) OVER w20 * 100 AS vola20_p,
			max(h) OVER w20 AS hi20_p, min(l) OVER w20 AS lo20_p,
			{beta_sums},
			last_value(CASE WHEN is_base = 1 THEN d END IGNORE NULLS) OVER wb AS base_d,
			last_value(CASE WHEN is_base = 1 THEN rn END IGNORE NULLS) OVER wb AS base_rn,
			last_value(CASE WHEN is_base = 1 THEN h END IGNORE NULLS) OVER wb AS base_hi,
			last_value(CASE WHEN is_base = 1 THEN l END IGNORE NULLS) OVER wb AS base_lo,
			last_value(CASE WHEN is_base = 1 THEN c END IGNORE NULLS) OVER wb AS base_c,
			last_value(CASE WHEN is_base = 1
				THEN round((c / nullif(prev_c, 0) - 1) * 100, 2) END IGNORE NULLS)
				OVER wb AS base_chg,
			sum(is_base) OVER wb AS base_cnt
		FROM j
		WINDOW
			w5   AS (PARTITION BY iid ORDER BY d ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),
			w20  AS (PARTITION BY iid ORDER BY d ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),
			w60  AS (PARTITION BY iid ORDER BY d ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING),
			w120 AS (PARTITION BY iid ORDER BY d ROWS BETWEEN 120 PRECEDING AND 1 PRECEDING),
			wb   AS (PARTITION BY iid ORDER BY d ROWS BETWEEN {BASE_LOOK} PRECEDING AND 1 PRECEDING),
			{beta_wins}
	)
	SELECT w.iid, w.d, w.code, w.name, w.market, w.sec_class,
		w.o, w.h, w.l, w.c, w.v, w.amt, w.prev_c, w.prev_v, w.prev_amt,
		round((w.c / nullif(w.prev_c, 0) - 1) * 100, 2) AS chg_pct,
		round((w.o / nullif(w.prev_c, 0) - 1) * 100, 2) AS gap_pct,
		round(w.ma5_p, 1) AS ma5, round(w.ma20_p, 1) AS ma20,
		round(w.ma60_p, 1) AS ma60, round(w.ma120_p, 1) AS ma120,
		round((w.c / nullif(w.ma60_p, 0) - 1) * 100, 2) AS ma60_over,
		round((w.c / nullif(w.ma20_p, 0) - 1) * 100, 2) AS ma20_over,
		CAST(w.amt20_p AS BIGINT) AS amt20,
		round(w.vola20_p, 2) AS vola20,
		round((w.c / nullif(w.hi20_p, 0) - 1) * 100, 2) AS hi20_over,
		round((w.c / nullif(w.lo20_p, 0) - 1) * 100, 2) AS lo20_over,
		{beta_cols},
		w.base_d, w.base_cnt,
		CASE WHEN w.base_rn IS NULL THEN NULL ELSE w.rn - w.base_rn END AS base_days,
		w.base_hi, w.base_lo, w.base_c, w.base_chg,
		round((w.c / nullif(w.base_hi, 0) - 1) * 100, 2) AS base_hi_over,
		round((w.c / nullif(w.base_lo, 0) - 1) * 100, 2) AS base_lo_over,
		jm.jma, jm.jdir AS jma_dir, jm.jslope AS jma_slope,
		jm.jma_p, jm.jdir_p AS jma_dir_p, jm.jslope_p AS jma_slope_p,
		m.market_cap AS mcap, m.listed_shares
	FROM w
	LEFT JOIN jmat jm ON jm.iid = w.iid AND jm.d = w.d
	ASOF LEFT JOIN read_parquet('{_pq(config.MCAP_PQ)}') m
		ON m.iid = w.iid AND m.d <= w.d
	WHERE w.n60 = 60 AND w.prev_c > 0
	""")
	con.execute(f"COPY feat TO '{_pq(config.FEAT_PQ)}' (FORMAT parquet, COMPRESSION zstd)")
	row = con.execute("""
		SELECT count(*), count(DISTINCT iid), min(d), max(d),
			   count(beta360), count(base_days), count(jma_p), count(mcap)
		FROM feat""").fetchone()
	return {
		"ok": True, "rows": row[0], "codes": row[1],
		"d_min": str(row[2]), "d_max": str(row[3]),
		"beta360_filled": row[4], "base_filled": row[5],
		"jma_filled": row[6], "mcap_filled": row[7],
		"market_return_source": mret_src, "jma_input_rows": jma_rows,
		"jma_param": list(JMA_PARAM),
		"elapsed_sec": round(time.time() - started, 1),
	}
