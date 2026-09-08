"""장중 시점 성과검증 (키움 1516 대체). 당일 일봉을 참조하지 않는다."""
from __future__ import annotations

import datetime as dt
import time
from pathlib import Path

from . import api, config
from . import params as P

DEFAULT_BT = {
    "strat": "base59",
    "t_from": "09:05:00",
    "t_until": "14:30:00",
    "t_eod": "15:30:00",
    "amt_mode": "hloc4",
    "liq_basis": "prev_amt",
    "vol_basis": "pace",
    "chg_basis": "prev_c",
    "entry": "signal_close",
    "require_prev_below": 0,
    "max_amt": 0,
    "max_price": 0,
    "orb_pad": 0.005,
    "fast": 5,
    "slow": 20,
    "hold_min": 30,
    "tp_pct": 0.03,
    "sl_pct": 0.02,
    "fee_pct": 0.0015,
    "bench": "eqw",
    "marks": [1, 3, 5, 10, 15, 30, 60],
    "mfe_bins": [3, 5, 10, 20],
    "base_pool": 1,
}

_INT = ("require_prev_below", "fast", "slow", "hold_min",
        "max_amt", "max_price", "base_pool")
_FLT = ("tp_pct", "sl_pct", "fee_pct", "orb_pad")
_ENUM = {
    "strat": ("base59", "ma_cross", "hod"),
    "amt_mode": ("hloc4", "close"),
    "liq_basis": ("prev_amt", "cum_amt", "none"),
    "vol_basis": ("cum", "pace", "none"),
    "chg_basis": ("prev_c", "day_open"),
    "entry": ("signal_close", "next_open"),
    "bench": ("eqw", "none"),
}
_AMT = {"hloc4": "(o + h + l + c) / 4.0 * v", "close": "c * 1.0 * v"}
_CTX_MA = None


def merge_bt(user: dict | None) -> dict:
    out = dict(DEFAULT_BT)
    for key, value in (user or {}).items():
        if key not in out:
            raise ValueError(f"unknown bt key: {key}")
        out[key] = value
    for key in _INT:
        out[key] = int(out[key])
    for key in _FLT:
        out[key] = float(out[key])
    for key, allowed in _ENUM.items():
        if out[key] not in allowed:
            raise ValueError(f"{key} must be one of {allowed}")
    out["marks"] = sorted({int(mark) for mark in out["marks"] if int(mark) > 0})
    out["mfe_bins"] = sorted({float(b) for b in out["mfe_bins"] if float(b) > 0})
    if out["slow"] <= out["fast"]:
        raise ValueError("slow must be > fast")
    if out["hold_min"] < 1:
        raise ValueError("hold_min must be >= 1")
    return out


def _ensure_btctx(con, ma_period: int):
    global _CTX_MA
    if _CTX_MA != ma_period:
        con.execute(api._sqlfile("bt_ctx.sql").format(
            DAILY=Path(api.DAILY_PQ).as_posix(),
            MA=int(ma_period), MA_M1=int(ma_period) - 1))
        _CTX_MA = ma_period


def reset() -> None:
    global _CTX_MA
    _CTX_MA = None


def _bars_sql(day, opt: dict) -> str:
    return f"""
CREATE OR REPLACE TABLE bars AS
WITH raw AS (
    SELECT iid, CAST(ts AS TIME) AS t, o, h, l, c, v, {_AMT[opt['amt_mode']]} AS bamt
    FROM read_parquet('{(config.MIN1_DIR / ('d=' + str(day))).as_posix()}/*.parquet')
    WHERE CAST(ts AS TIME) <= CAST($t_eod AS TIME) AND v > 0
),
cum AS (
    SELECT iid, t, o, h, l, c, v,
           first_value(o) OVER w AS day_open,
           sum(v) OVER w AS cum_v,
           sum(bamt) OVER w AS cum_amt,
           lag(c) OVER pw AS pc,
           max(h) OVER wp AS pre_h,
           lead(t) OVER pw AS next_t,
           lead(o) OVER pw AS next_o,
           avg(c) OVER wf AS ma_f,
           avg(c) OVER ws AS ma_s,
           count(*) OVER ws AS n_slow
    FROM raw
    WINDOW
        pw AS (PARTITION BY iid ORDER BY t),
        wp AS (PARTITION BY iid ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        w AS (PARTITION BY iid ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        wf AS (PARTITION BY iid ORDER BY t ROWS BETWEEN {opt['fast'] - 1} PRECEDING AND CURRENT ROW),
        ws AS (PARTITION BY iid ORDER BY t ROWS BETWEEN {opt['slow'] - 1} PRECEDING AND CURRENT ROW)
)
SELECT k.*,
       lag(k.ma_f) OVER pw2 AS pma_f,
       lag(k.ma_s) OVER pw2 AS pma_s,
       x.base_prev, x.prev_ma, x.prev_c, x.prev_v, x.prev_amt,
       i.code, i.name, i.market
FROM cum k
JOIN btctx x ON x.iid = k.iid AND x.d = DATE '{day}'
JOIN read_parquet('{Path(api.INST_PQ).as_posix()}') i ON i.iid = k.iid
WHERE x.n_base = $ma_period - 1 AND x.n_full = $ma_period
  AND x.prev_c > 0 AND x.prev_v > 0
  AND ($sec_class = 'ALL' OR i.sec_class = $sec_class)
  AND ($market = 'ALL' OR i.market = $market)
  AND i.d_first <= x.d AND i.d_last >= x.d
WINDOW pw2 AS (PARTITION BY k.iid ORDER BY k.t);
"""


def _pred(opt: dict) -> str:
    cores = {
        "base59": ("pc IS NOT NULL AND pc < base_prev AND c >= base_prev"
                   " AND ($rpb = 0 OR prev_c < prev_ma)"),
        "ma_cross": ("n_slow = $slow AND pma_f IS NOT NULL"
                     " AND pma_f <= pma_s AND ma_f > ma_s"),
        "hod": "pre_h IS NOT NULL AND c > pre_h * (1 + $orb_pad)",
    }
    core = cores[opt["strat"]]
    change = "c / prev_c - 1" if opt["chg_basis"] == "prev_c" else "c / day_open - 1"
    volume = {
        "cum": "cum_v > prev_v * $vol_mult",
        "pace": ("cum_v > prev_v * $vol_mult"
                 " * (date_diff('minute', TIME '09:00:00', t) / 380.0)"),
        "none": "TRUE",
    }[opt["vol_basis"]]
    liquidity = {
        "prev_amt": "prev_amt >= $min_amt",
        "cum_amt": "cum_amt >= $min_amt",
        "none": "TRUE",
    }[opt["liq_basis"]]
    caps = ""
    if opt["max_amt"]:
        caps += "    AND prev_amt <= $max_amt\n"
    if opt["max_price"]:
        caps += "    AND c <= $max_price\n"
    return f"""
    t >= CAST($t_from AS TIME) AND t <= CAST($t_until AS TIME)
    AND {core}
    AND c >= $min_price
    AND ($bull = 0 OR c > day_open)
    AND {change} BETWEEN $chg_min AND $chg_max
    AND {volume}
    AND {liquidity}
{caps}    AND (c / base_prev - 1) * 100 <= $over_max
"""


def _trades_sql(opt: dict) -> str:
    price = "q.next_o" if opt["entry"] == "next_open" else "q.c"
    timestamp = "q.next_t" if opt["entry"] == "next_open" else "q.t"
    marks = ",\n       ".join(
        f"arg_max(b.c, b.t) FILTER (WHERE b.t <= e.e_t + to_minutes({mark})) AS m{mark}"
        for mark in opt["marks"])
    return f"""
CREATE OR REPLACE TABLE tr AS
WITH ent AS (
    SELECT iid, code, name, market, base_prev, prev_c, day_open, t AS sig_t,
           {timestamp} AS e_t, {price} AS e_px
    FROM (SELECT *, row_number() OVER (PARTITION BY iid ORDER BY t) AS rn
          FROM bars WHERE {_pred(opt)}) q
    WHERE rn = 1 AND {price} IS NOT NULL AND {price} > 0
)
SELECT e.iid, e.code, e.name, e.market, e.base_prev, e.prev_c, e.day_open,
       e.sig_t, e.e_t, e.e_px,
    coalesce(max(b.h) FILTER (WHERE b.t > e.e_t), max(e.e_px)) AS max_h,
    coalesce(min(b.l) FILTER (WHERE b.t > e.e_t), max(e.e_px)) AS min_l,
       arg_max(b.c, b.t) AS eod_px,
       arg_max(b.c, b.t) FILTER (WHERE b.t <= e.e_t + to_minutes({opt['hold_min']})) AS hold_px,
       {marks},
    min(b.t) FILTER (WHERE b.t > e.e_t AND b.h >= e.e_px * (1 + $tp_pct)) AS tp_t,
    min(b.t) FILTER (WHERE b.t > e.e_t AND b.l <= e.e_px * (1 - $sl_pct)) AS sl_t,
       count(*) AS n_bars
FROM ent e
JOIN bars b ON b.iid = e.iid AND b.t >= e.e_t
GROUP BY ALL;
"""


def _bench_series(con) -> dict:
    rows = con.execute("""
        SELECT market, t, avg(c * 1.0 / prev_c) - 1 AS r
        FROM bars GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    result = {}
    for market, timestamp, ret in rows:
        result.setdefault(market, []).append((timestamp, float(ret)))
    return result


def _bench_at(series, market, when):
    rows = series.get(market)
    if not rows:
        return None
    best = None
    for timestamp, ret in rows:
        if timestamp <= when:
            best = ret
        else:
            break
    return best


def _pct(now, base):
    return None if now is None or not base else round((now / base - 1) * 100, 3)


def _tag(bin_pct) -> str:
    return ("%g" % float(bin_pct)).replace(".", "_")


def _pool_sql(opt: dict) -> str:
    """모집단(유동성 통과 전체) 기준 MFE 달성 종목 수 = 선별력의 분모."""
    bins = ",\n       ".join(
        f"count(*) FILTER (WHERE mh / px - 1 >= {float(b)} / 100.0) AS p{_tag(b)}"
        for b in opt["mfe_bins"])
    return f"""
WITH ref AS (
    SELECT iid, arg_max(c, t) AS px, max(t) AS rt
    FROM bars
    WHERE t <= CAST($t_from AS TIME) AND prev_amt >= $min_amt AND c >= $min_price
    GROUP BY iid
),
mv AS (
    SELECT r.iid, r.px, max(b.h) AS mh, arg_max(b.c, b.t) AS eod
    FROM ref r JOIN bars b ON b.iid = r.iid AND b.t > r.rt
    WHERE r.px > 0
    GROUP BY r.iid, r.px
)
SELECT count(*) AS n_pool,
       {bins},
       round(avg(mh / px - 1) * 100, 3) AS pool_avg_mfe,
       round(avg(eod / px - 1) * 100, 3) AS pool_avg_eod
FROM mv
"""


def _bind(sql: str, pool: dict) -> dict:
    """SQL 본문에 실제로 등장하는 명명 파라미터만 골라 넘긴다."""
    return {key: value for key, value in pool.items() if f"${key}" in sql}


def _one_day(con, day, cond: dict, opt: dict) -> tuple[list, dict]:
    args = dict(cond)
    args.update({
        "t_from": opt["t_from"], "t_until": opt["t_until"], "t_eod": opt["t_eod"],
        "rpb": opt["require_prev_below"], "slow": opt["slow"],
        "tp_pct": opt["tp_pct"], "sl_pct": opt["sl_pct"],
        "max_amt": opt["max_amt"], "max_price": opt["max_price"],
        "orb_pad": opt["orb_pad"],
    })
    bars_sql = _bars_sql(day, opt)
    con.execute(bars_sql, _bind(bars_sql, args))
    universe = con.execute("SELECT count(DISTINCT iid) FROM bars").fetchone()[0]
    trades_sql = _trades_sql(opt)
    con.execute(trades_sql, _bind(trades_sql, args))
    rows = api._rows(con.execute("SELECT * FROM tr ORDER BY e_t, code"))
    series = _bench_series(con) if opt["bench"] == "eqw" else {}
    fee = opt["fee_pct"] * 100
    trades = []
    for row in rows:
        entry = row["e_px"]
        eod = _pct(row["eod_px"], entry)
        exit_kind = ("SL" if row["sl_t"] and (not row["tp_t"] or row["sl_t"] <= row["tp_t"])
                     else "TP" if row["tp_t"] else "EOD")
        ret = (-opt["sl_pct"] * 100 if exit_kind == "SL"
               else opt["tp_pct"] * 100 if exit_kind == "TP" else eod)
        record = {
            "d": str(day), "code": row["code"], "name": row["name"], "market": row["market"],
            "sig_at": str(row["sig_t"]), "entry_at": str(row["e_t"]), "entry_px": entry,
            "breakout_px": round(row["base_prev"], 1) if row["base_prev"] else None,
            "mfe_pct": _pct(row["max_h"], entry), "mae_pct": _pct(row["min_l"], entry),
            "hold_pct": _pct(row["hold_px"] or row["eod_px"], entry), "eod_pct": eod,
            "exit_kind": exit_kind, "ret_pct": round(ret - fee, 3) if ret is not None else None,
            "n_bars": row["n_bars"],
        }
        for mark in opt["marks"]:
            record[f"r{mark}m"] = _pct(row[f"m{mark}"], entry)
        if opt["bench"] == "eqw":
            bench_start = _bench_at(series, row["market"], row["e_t"])
            bench_end = _bench_at(
                series, row["market"], dt.time.fromisoformat(opt["t_eod"])
            )
            if bench_start is not None and bench_end is not None:
                record["bench_eod_pct"] = round((bench_end - bench_start) * 100, 3)
                if eod is not None:
                    record["alpha_eod_pct"] = round(eod - record["bench_eod_pct"], 3)
        trades.append(record)
    stat = {"d": str(day), "universe": int(universe), "n": len(trades)}
    if opt["base_pool"]:
        pool_sql = _pool_sql(opt)
        cur = con.execute(pool_sql, _bind(pool_sql, args))
        stat.update(dict(zip([col[0] for col in cur.description], cur.fetchone())))
    for edge in opt["mfe_bins"]:
        stat[f"n_mfe{_tag(edge)}"] = sum(
            1 for trade in trades if (trade["mfe_pct"] or -999) >= edge)
    return trades, stat


def _avg(values):
    clean = [value for value in values if value is not None]
    return round(sum(clean) / len(clean), 3) if clean else None


def run(d_from: str, d_to: str | None = None, params: dict | None = None,
        bt: dict | None = None, include_trades: bool = True) -> dict:
    cond = P.merge_cond(params)
    options = merge_bt(bt)
    d_to = d_to or d_from
    started = time.time()
    con = api._con()
    _ensure_btctx(con, cond["ma_period"])
    days = [row[0] for row in con.execute(
        f"SELECT DISTINCT d FROM read_parquet('{Path(api.DAILY_PQ).as_posix()}') "
        "WHERE d BETWEEN ? AND ? ORDER BY d", [d_from, d_to]).fetchall()]
    have = [day for day in days if (config.MIN1_DIR / f"d={day}").is_dir()
            and any((config.MIN1_DIR / f"d={day}").glob("*.parquet"))]
    missing = [str(day) for day in days if day not in have]
    if not have:
        return {"ok": False, "api": "backtest", "reason": "분봉 데이터가 없습니다",
                "missing_min1_days": missing}
    trades, by_date = [], []
    for day in have:
        day_trades, stat = _one_day(con, day, cond, options)
        trades.extend(day_trades)
        stat["avg_ret"] = _avg([trade["ret_pct"] for trade in day_trades])
        by_date.append(stat)
    elapsed = time.time() - started
    count = len(trades)
    up = sum(1 for trade in trades if (trade["eod_pct"] or 0) > 0)
    flat = sum(1 for trade in trades if trade["eod_pct"] == 0)
    summary = {
        "n_trades": count, "n_up": up, "n_flat": flat, "n_down": count - up - flat,
        "up_rate": round(100.0 * up / count, 2) if count else None,
        "avg_ret": _avg([trade["ret_pct"] for trade in trades]),
        "avg_eod": _avg([trade["eod_pct"] for trade in trades]),
        "avg_mfe": _avg([trade["mfe_pct"] for trade in trades]),
        "avg_mae": _avg([trade["mae_pct"] for trade in trades]),
        "avg_hold": _avg([trade["hold_pct"] for trade in trades]),
        "win_rate": round(100.0 * sum(1 for trade in trades if (trade["ret_pct"] or 0) > 0) / count, 2) if count else None,
        "n_tp": sum(1 for trade in trades if trade["exit_kind"] == "TP"),
        "n_sl": sum(1 for trade in trades if trade["exit_kind"] == "SL"),
        "n_eod": sum(1 for trade in trades if trade["exit_kind"] == "EOD"),
    }
    pool_n = sum(stat.get("n_pool") or 0 for stat in by_date)
    summary["n_pool"] = pool_n
    for edge in options["mfe_bins"]:
        tag = _tag(edge)
        hit = sum(1 for trade in trades if (trade["mfe_pct"] or -999) >= edge)
        pool_hit = sum(stat.get(f"p{tag}") or 0 for stat in by_date)
        base = (100.0 * pool_hit / pool_n) if pool_n else None
        summary[f"n_mfe{tag}"] = hit
        summary[f"per_day_mfe{tag}"] = round(hit / len(have), 2)
        summary[f"rate_mfe{tag}"] = round(100.0 * hit / count, 2) if count else None
        summary[f"pool_mfe{tag}"] = pool_hit
        summary[f"recall_mfe{tag}"] = round(100.0 * hit / pool_hit, 2) if pool_hit else None
        summary[f"base_mfe{tag}"] = round(base, 3) if base else None
        summary[f"lift_mfe{tag}"] = (round((100.0 * hit / count) / base, 2)
                                     if count and base else None)
    for mark in options["marks"]:
        summary[f"avg_r{mark}m"] = _avg([trade[f"r{mark}m"] for trade in trades])
    if options["bench"] == "eqw":
        summary["avg_bench_eod"] = _avg([trade.get("bench_eod_pct") for trade in trades])
        summary["avg_alpha_eod"] = _avg([trade.get("alpha_eod_pct") for trade in trades])
    result = {
        "ok": True, "api": "backtest", "d_from": d_from, "d_to": d_to,
        "params": cond, "bt": options, "n_days": len(have), "missing_min1_days": missing,
        "summary": summary, "by_date": by_date,
        "elapsed": {"total_sec": round(elapsed, 2), "per_day_sec": round(elapsed / len(have), 3)},
    }
    if include_trades:
        result["trades"] = trades
    return result
