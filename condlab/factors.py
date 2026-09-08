"""요인 스캔. 기준시각 모집단을 요인별 분위로 나눠 고MFE 달성률을 측정한다."""
from __future__ import annotations

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
    SELECT r.iid, max(b.h) AS mh, arg_max(b.c, b.t) AS eod_px
    FROM ref r JOIN bars b ON b.iid = r.iid AND b.t > r.rt
    GROUP BY r.iid
)
SELECT DATE '{day}' AS d, r.iid, r.market, {cols},
       (f.mh / r.c - 1) * 100 AS mfe,
       (f.eod_px / r.c - 1) * 100 AS eod
FROM ref r JOIN fwd f USING (iid)
WHERE r.c > 0 AND r.el > 0
"""


def build_pool(con, d_from: str, d_to: str, cond: dict, opt: dict) -> list:
    """기준시각 모집단 + 이후 MFE/종료수익 테이블 pf 생성."""
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
        pool_sql = _POOL.format(day=day, cols=cols)
        con.execute(pool_sql, bt._bind(pool_sql, args))
        con.execute("CREATE OR REPLACE TABLE pf AS SELECT * FROM pool" if index == 0
                    else "INSERT INTO pf SELECT * FROM pool")
    return have


def run(d_from: str, d_to: str | None = None, params: dict | None = None,
        bt_opt: dict | None = None, edge: float = 10.0, bins: int = 10) -> dict:
    cond = P.merge_cond(params)
    opt = bt.merge_bt(bt_opt)
    edge = float(edge)
    bins = max(2, min(20, int(bins)))
    started = time.time()
    con = api._con()
    have = build_pool(con, d_from, d_to or d_from, cond, opt)
    if not have:
        return {"ok": False, "api": "factor", "reason": "분봉 데이터가 없습니다"}
    total, hits = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE mfe >= ?) FROM pf", [edge]).fetchone()
    base = (100.0 * hits / total) if total else None
    out = {}
    for key, (label, _) in FACTORS.items():
        rows = api._rows(con.execute(f"""
            WITH q AS (
                SELECT ntile({bins}) OVER (PARTITION BY d ORDER BY {key}) AS b,
                       {key} AS fv, mfe, eod
                FROM pf WHERE {key} IS NOT NULL AND isfinite({key})
            )
            SELECT b AS bin, count(*) AS n,
                   round(min(fv), 3) AS lo, round(max(fv), 3) AS hi,
                   round(100.0 * count(*) FILTER (WHERE mfe >= ?) / count(*), 2) AS hit,
                   round(avg(mfe), 3) AS avg_mfe, round(avg(eod), 3) AS avg_eod
            FROM q GROUP BY b ORDER BY b
        """, [edge]))
        for row in rows:
            row["lift"] = round(row["hit"] / base, 2) if base else None
        ic = con.execute(
            f"SELECT round(corr({key}, mfe), 4) FROM pf "
            f"WHERE {key} IS NOT NULL AND isfinite({key})").fetchone()[0]
        top, bottom = (rows[-1], rows[0]) if rows else ({}, {})
        out[key] = {"label": label, "ic": ic, "bins": rows,
                    "top_hit": top.get("hit"), "top_lift": top.get("lift"),
                    "bot_lift": bottom.get("lift")}
    rank = sorted(out.items(), key=lambda item: -(item[1]["top_lift"] or 0))
    return {
        "ok": True, "api": "factor", "d_from": d_from, "d_to": str(d_to or d_from),
        "n_days": len(have), "n_pool": int(total), "edge": edge,
        "base_rate": round(base, 3) if base else None,
        "rank": [{"key": key, "label": value["label"], "ic": value["ic"],
                  "top_hit": value["top_hit"], "top_lift": value["top_lift"],
                  "bot_lift": value["bot_lift"]} for key, value in rank],
        "factors": out, "elapsed_sec": round(time.time() - started, 1),
    }
