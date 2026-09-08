"""요인 조합 탐색. 일별 분위 컷을 탐욕적으로 쌓아 고MFE 적중률을 최대화한다."""
from __future__ import annotations

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
        min_keep: float = 8.0, depth: int = 3, keys: list | None = None) -> dict:
    cond = P.merge_cond(params)
    opt = bt.merge_bt(bt_opt)
    edge, min_keep = float(edge), float(min_keep)
    started = time.time()
    con = api._con()
    have = F.build_pool(con, d_from, d_to or d_from, cond, opt)
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
        "SELECT count(*), count(*) FILTER (WHERE mfe >= ?) FROM pr", [edge]).fetchone()
    base = 100.0 * hits / total if total else None
    path, used, where = [], set(), []
    for _ in range(max(1, min(4, int(depth)))):
        best = None
        best_rate = -1.0
        for key, side, q, expr in _cuts([k for k in keys if k not in used]):
            clause = " AND ".join(where + [expr])
            count, hit = con.execute(
                f"SELECT count(*), count(*) FILTER (WHERE mfe >= ?) FROM pr WHERE {clause}",
                [edge]).fetchone()
            if not count or count / n_days < min_keep:
                continue
            rate = 100.0 * hit / count
            if rate > best_rate:
                best_rate = rate
                best = {"key": key, "label": F.FACTORS[key][0], "side": side,
                        "q": q, "expr": expr, "n": int(count),
                        "n_day": round(count / n_days, 1), "hit": round(rate, 2),
                        "lift": round(rate / base, 2) if base else None}
        if best is None:
            break
        used.add(best["key"])
        where.append(best["expr"])
        stat = con.execute(
            f"SELECT round(min({best['key']}), 3), round(max({best['key']}), 3), "
            f"round(avg(mfe), 3), round(avg(eod), 3) FROM pr WHERE {' AND '.join(where)}"
        ).fetchone()
        best["cut"] = stat[0] if best["side"] == "상위" else stat[1]
        best.update({"avg_mfe": stat[2], "avg_eod": stat[3]})
        path.append(best)
    return {"ok": True, "api": "combo", "d_from": d_from, "d_to": str(d_to or d_from),
            "n_days": n_days, "n_pool": int(total), "edge": edge,
            "base_rate": round(base, 3) if base is not None else None, "min_keep": min_keep, "path": path,
            "elapsed_sec": round(time.time() - started, 1)}
