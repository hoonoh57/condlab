"""조건식 그리드 탐색. 모든 변형을 cond_set 버전으로 남기고 결과를 bt_result에 기록."""
from __future__ import annotations

import itertools
import time

from . import backtest as bt
from . import params as P
from . import store

MAX_VARIANTS = 64
KPI = "n_mfe10"
_COLS = ("n_mfe5", "n_mfe10", "n_mfe20", "rate_mfe10", "recall_mfe10",
         "base_mfe10", "lift_mfe10", "per_day_mfe10", "avg_mfe", "avg_eod")


def _split(over: dict) -> tuple[dict, dict]:
    cond, opts = {}, {}
    for key, value in over.items():
        if key in P.DEFAULT_COND:
            cond[key] = value
        elif key in bt.DEFAULT_BT:
            opts[key] = value
        else:
            raise ValueError(f"unknown grid key: {key}")
    return cond, opts


def expand(grid: dict) -> list[dict]:
    keys = list(grid)
    combos = [dict(zip(keys, values))
              for values in itertools.product(*(grid[key] for key in keys))]
    if not combos:
        raise ValueError("grid가 비었습니다")
    if len(combos) > MAX_VARIANTS:
        raise ValueError(f"조합 {len(combos)}개는 너무 많습니다 (최대 {MAX_VARIANTS})")
    return combos


def run(name: str, d_from: str, d_to: str | None, grid: dict,
        base_params: dict | None = None, base_bt: dict | None = None,
        kpi: str = KPI, note: str = "") -> dict:
    started = time.time()
    rows = []
    for over in expand(grid):
        cond_over, bt_over = _split(over)
        cond = P.merge_cond({**(base_params or {}), **cond_over})
        opts = bt.merge_bt({**(base_bt or {}), **bt_over})
        label = ", ".join(f"{key}={value}" for key, value in over.items())
        tail = f" | {note}" if note else ""
        record = store.save_cond(name, cond, opts, f"[sweep] {label}{tail}")
        result = bt.run(d_from, d_to, cond, opts, include_trades=False)
        if not result.get("ok"):
            rows.append({"ver": record["ver"], "label": label,
                         "error": result.get("reason")})
            continue
        store.save_bt_result(name, record["ver"], result)
        summary = result["summary"]
        rows.append({"ver": record["ver"], "label": label,
                     "n_days": result["n_days"], "n_trades": summary["n_trades"],
                     **{key: summary.get(key) for key in _COLS}})
    rows.sort(key=lambda row: (row.get(kpi) is None, -(row.get(kpi) or 0)))
    return {"ok": True, "api": "sweep", "name": name, "d_from": d_from,
            "d_to": d_to, "kpi": kpi, "n_variants": len(rows),
            "elapsed_sec": round(time.time() - started, 1), "rows": rows}
