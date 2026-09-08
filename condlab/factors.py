"""요인 스캔. 기준시각 모집단을 요인별 분위로 나눠 고MFE 달성률을 측정한다."""
from __future__ import annotations

import math
import time
from pathlib import Path

from . import api, config
from . import backtest as bt
from . import params as P

FACTORS = {
    "gap": ("시가갭%", "(day_open / prev_c - 1) * 100"),
    "chg": ("전일대비%", "(c / prev_c - 1) * 100"),
    "run": ("시가대비%", "(c / day_open - 1) * 100"),
    "pace": ("거래량배속", "cum_v / nullif(prev_v * el / 380.0, 0)"),
    "turn": ("전일대금대비", "cum_amt / nullif(prev_amt, 0)"),
    "amt": ("전일거래대금", "prev_amt * 1.0"),
    "px": ("주가", "c * 1.0"),
    "ext": ("60일선이격%", "(c / nullif(base_prev, 0) - 1) * 100"),
    "pos": ("장중위치", "(c - lo) / nullif(hi - lo, 0)"),
    "vola": ("장중변동%", "(hi / nullif(lo, 0) - 1) * 100"),
}

_POOL = """
CREATE OR REPLACE TABLE pool AS
WITH ref AS (
    SELECT iid, any_value(market) AS market,
           arg_max(c, t) AS c, arg_max(day_open, t) AS day_open,
           arg_max(prev_c, t) AS prev_c, arg_max(prev_v, t) AS prev_v,
           arg_max(prev_amt, t) AS prev_amt, arg_max(base_prev, t) AS base_prev,
           arg_max(cum_v, t) AS cum_v, arg_max(cum_amt, t) AS cum_amt,
           max(h) AS hi, min(l) AS lo, max(t) AS rt,
           date_diff('minute', TIME '09:00:00', max(t)) AS el
    FROM bars
    WHERE t <= CAST($t_from AS TIME)
    GROUP BY iid
    HAVING arg_max(prev_amt, t) >= $min_amt AND arg_max(c, t) >= $min_price
),
fwd AS (
    SELECT r.iid, max(b.h) AS mh, min(b.l) AS ml, arg_max(b.c, b.t) AS eod_px,
           min(b.t) FILTER (WHERE b.h >= r.c * {UP}) AS up_t,
           min(b.t) FILTER (WHERE b.l <= r.c * {DN}) AS dn_t
    FROM ref r JOIN bars b ON b.iid = r.iid AND b.t > r.rt
    GROUP BY r.iid, r.c
)
SELECT DATE '{day}' AS d, r.iid, r.market, {cols},
       (f.mh / r.c - 1) * 100 AS mfe,
       (f.ml / r.c - 1) * 100 AS mae,
       (f.eod_px / r.c - 1) * 100 AS eod,
       CASE WHEN f.up_t IS NOT NULL AND (f.dn_t IS NULL OR f.up_t < f.dn_t)
            THEN 1 ELSE 0 END AS ok
FROM ref r JOIN fwd f USING (iid)
WHERE r.c > 0 AND r.el > 0
"""


HIT = {"mfe": "mfe >= {edge}", "first": "ok = 1"}
MLBL = {"mfe": "MFE 도달", "first": "선착(먼저 도달)"}


def build_pool(con, d_from: str, d_to: str, cond: dict, opt: dict,
               edge: float, stop: float) -> list:
    bt._ensure_btctx(con, cond["ma_period"])
    days = [row[0] for row in con.execute(
        f"SELECT DISTINCT d FROM read_parquet('{Path(api.DAILY_PQ).as_posix()}') "
        "WHERE d BETWEEN ? AND ? ORDER BY d", [d_from, d_to]).fetchall()]
    have = [day for day in days
            if (config.MIN1_DIR / f"d={day}").is_dir()
            and any((config.MIN1_DIR / f"d={day}").glob("*.parquet"))]
    if not have:
        return []
    cols = ", ".join(f"{expr} AS {key}" for key, (_, expr) in FACTORS.items())
    args = dict(cond)
    args.update({"t_from": opt["t_from"], "t_until": opt["t_until"],
                 "t_eod": opt["t_eod"], "rpb": opt["require_prev_below"],
                 "slow": opt["slow"], "orb_pad": opt["orb_pad"]})
    for index, day in enumerate(have):
        bars_sql = bt._bars_sql(day, opt)
        con.execute(bars_sql, bt._bind(bars_sql, args))
        pool_sql = _POOL.format(day=day, cols=cols,
                                UP=1 + edge / 100.0, DN=1 - stop / 100.0)
        con.execute(pool_sql, bt._bind(pool_sql, args))
        con.execute("CREATE OR REPLACE TABLE pf AS SELECT * FROM pool" if index == 0
                    else "INSERT INTO pf SELECT * FROM pool")
    return have


def run(d_from: str, d_to: str | None = None, params: dict | None = None,
        bt_opt: dict | None = None, edge: float = 10.0, bins: int = 10,
        stop: float = 5.0, metric: str = "first") -> dict:
    cond = P.merge_cond(params)
    opt = bt.merge_bt(bt_opt)
    edge, stop = float(edge), float(stop)
    if not all(math.isfinite(v) and v > 0 for v in (edge, stop)):
        raise ValueError("edge and stop must be finite and > 0")
    bins = max(2, min(20, int(bins)))
    if metric not in HIT:
        raise ValueError(f"metric must be one of {tuple(HIT)}")
    hit_expr = HIT[metric].format(edge=edge)
    started = time.time()
    con = api._con()
    have = build_pool(con, d_from, d_to or d_from, cond, opt, edge, stop)
    if not have:
        return {"ok": False, "api": "factor", "reason": "분봉 데이터가 없습니다"}
    total, hits = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE {hit_expr}) FROM pf").fetchone()
    base = (100.0 * hits / total) if total else None
    neutral = round(100.0 * stop / (edge + stop), 2) if metric == "first" else None
    out = {}
    for key, (label, _) in FACTORS.items():
        rows = api._rows(con.execute(f"""
            WITH q AS (
                SELECT ntile({bins}) OVER (PARTITION BY d ORDER BY {key}) AS b,
                       {key} AS fv, mfe, mae, eod, ok
                FROM pf WHERE {key} IS NOT NULL AND isfinite({key})
            )
            SELECT b AS bin, count(*) AS n,
                   round(min(fv), 3) AS lo, round(max(fv), 3) AS hi,
                   round(100.0 * count(*) FILTER (WHERE {hit_expr}) / count(*), 2) AS hit,
                   round(avg(CASE WHEN ok = 1 THEN {edge} ELSE -{stop} END), 3) AS ev,
                   round(avg(mfe), 3) AS avg_mfe, round(avg(mae), 3) AS avg_mae,
                   round(avg(eod), 3) AS avg_eod
            FROM q GROUP BY b ORDER BY b
        """))
        for row in rows:
            row["lift"] = round(row["hit"] / base, 2) if base else None
        ic = con.execute(
            f"SELECT round(corr({key}, ok), 4) FROM pf "
            f"WHERE {key} IS NOT NULL AND isfinite({key})").fetchone()[0]
        ic = ic if ic is not None and math.isfinite(ic) else None
        top, bottom = (rows[-1], rows[0]) if rows else ({}, {})
        out[key] = {"label": label, "ic": ic, "bins": rows,
                    "top_hit": top.get("hit"), "top_lift": top.get("lift"),
                    "top_ev": top.get("ev"), "bot_hit": bottom.get("hit"),
                    "bot_lift": bottom.get("lift"), "bot_ev": bottom.get("ev")}
    rank = sorted(out.items(),
                  key=lambda item: -max(item[1]["top_hit"] or 0, item[1]["bot_hit"] or 0))
    return {
        "ok": True, "api": "factor", "d_from": d_from, "d_to": str(d_to or d_from),
        "n_days": len(have), "n_pool": int(total), "edge": edge, "stop": stop,
        "metric": metric, "metric_label": MLBL[metric], "neutral_rate": neutral,
        "base_rate": round(base, 3) if base is not None else None,
        "rank": [{"key": key, "label": value["label"], "ic": value["ic"],
                  "top_hit": value["top_hit"], "top_lift": value["top_lift"],
                  "top_ev": value["top_ev"], "bot_hit": value["bot_hit"],
                  "bot_lift": value["bot_lift"], "bot_ev": value["bot_ev"]}
                 for key, value in rank],
        "factors": out, "elapsed_sec": round(time.time() - started, 1),
    }
