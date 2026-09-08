"""요인 조합 탐색. 일별 분위 컷을 탐욕적으로 쌓아 고MFE 적중률을 최대화한다."""
from __future__ import annotations

import math
import time

from . import api
from . import backtest as bt
from . import factors as F
from . import params as P

QS = (0.5, 0.3, 0.2, 0.1)


def _cuts(keys):
    for key in keys:
        for q in QS:
            yield key, "상위", q, f"r_{key} >= {round(1 - q, 3)}"
            yield key, "하위", q, f"r_{key} <= {q}"


def run(d_from: str, d_to: str | None = None, params: dict | None = None,
        bt_opt: dict | None = None, edge: float = 10.0,
        min_keep: float = 8.0, depth: int = 3, keys: list | None = None,
        stop: float = 5.0, metric: str = "first") -> dict:
    cond = P.merge_cond(params)
    opt = bt.merge_bt(bt_opt)
    edge, stop, min_keep = float(edge), float(stop), float(min_keep)
    if not all(math.isfinite(v) and v > 0 for v in (edge, stop)):
        raise ValueError("edge and stop must be finite and > 0")
    if metric not in F.HIT:
        raise ValueError(f"metric must be one of {tuple(F.HIT)}")
    hit_expr = F.HIT[metric].format(edge=edge)
    started = time.time()
    con = api._con()
    have = F.build_pool(con, d_from, d_to or d_from, cond, opt, edge, stop)
    if not have:
        return {"ok": False, "api": "combo", "reason": "분봉 데이터가 없습니다"}
    keys = [key for key in (keys or list(F.FACTORS)) if key in F.FACTORS]
    if not keys:
        raise ValueError("유효한 요인을 하나 이상 지정하세요")
    ranks = ", ".join(
        f"percent_rank() OVER (PARTITION BY d ORDER BY {key}) AS r_{key}" for key in keys)
    con.execute(f"CREATE OR REPLACE TABLE pr AS SELECT *, {ranks} FROM pf")
    n_days = len(have)
    total, hits = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE {hit_expr}) FROM pr").fetchone()
    base = 100.0 * hits / total if total else None

    def measure(clause):
        return con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE {hit_expr}), "
            f"round(avg(CASE WHEN ok = 1 THEN {edge} ELSE -{stop} END), 3) "
            f"FROM pr WHERE {clause}").fetchone()

    path, used, where = [], set(), []
    for _ in range(max(1, min(4, int(depth)))):
        best = None
        best_rate = -1.0
        for key, side, q, expr in _cuts([k for k in keys if k not in used]):
            count, hit, ev = measure(" AND ".join(where + [expr]))
            if not count or count / n_days < min_keep:
                continue
            rate = 100.0 * hit / count
            if rate > best_rate:
                best_rate = rate
                best = {"key": key, "label": F.FACTORS[key][0], "side": side,
                        "q": q, "expr": expr, "n": int(count),
                        "n_day": round(count / n_days, 1), "hit": round(rate, 2),
                        "lift": round(rate / base, 2) if base else None, "ev": ev}
        if best is None:
            break
        used.add(best["key"])
        where.append(best["expr"])
        quantile = 1 - best["q"] if best["side"] == "상위" else best["q"]
        best["cut"] = con.execute(
            f"SELECT round(quantile_cont({best['key']}, {quantile}), 3) FROM pr"
        ).fetchone()[0]
        best["abs_expr"] = (f"{best['key']} >= {best['cut']}" if best["side"] == "상위"
                            else f"{best['key']} <= {best['cut']}")
        count, hit, ev = measure(" AND ".join(step["abs_expr"] for step in path + [best]))
        best.update({"abs_n_day": round(count / n_days, 1) if count else 0,
                     "abs_hit": round(100.0 * hit / count, 2) if count else None,
                     "abs_ev": ev})
        path.append(best)
    return {"ok": True, "api": "combo", "d_from": d_from, "d_to": str(d_to or d_from),
            "n_days": n_days, "n_pool": int(total), "edge": edge, "stop": stop,
            "metric": metric, "base_rate": round(base, 3) if base is not None else None,
            "neutral_rate": round(100.0 * stop / (edge + stop), 2),
            "min_keep": min_keep, "path": path,
            "elapsed_sec": round(time.time() - started, 1)}
