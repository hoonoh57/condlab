"""condlab 웹 UI 서버."""
from __future__ import annotations

import os
import threading
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from . import api, config, params, store, sync

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"
_SCAN_LOCK = threading.Lock()

app = FastAPI(title="condlab")


@app.on_event("startup")
def _startup():
    store.init()
    (ROOT / ".pid").write_text(str(os.getpid()), encoding="utf-8")
    if sync.needs_today() and os.getenv("CONDLAB_AUTOSYNC", "1") == "1":
        sync.start("auto")


@app.get("/")
def index():
    return FileResponse(WEB / "index.html")


@app.get("/api/status")
def status():
    return sync.status()


@app.get("/api/defaults")
def defaults():
    return {"cond": params.DEFAULT_COND}


@app.post("/api/sync/start")
def sync_start(body: dict):
    mode = body.get("mode", "auto")
    if mode not in ("auto", "full", "daily", "min1", "reconcile"):
        raise HTTPException(400, "unknown mode")
    result = sync.start(mode)
    return JSONResponse(result, status_code=200 if result["ok"] else 409)


@app.post("/api/sync/cancel")
def sync_cancel():
    return sync.cancel()


@app.get("/api/sync/progress")
def sync_progress():
    return sync.STATE.snapshot()


@app.get("/api/runs")
def runs():
    return {"runs": store.recent_runs(30)}


@app.post("/api/scan")
def scan(body: dict):
    if sync.STATE.running:
        raise HTTPException(409, "동기화 중에는 조건검색을 실행할 수 없습니다")
    with _SCAN_LOCK:
        try:
            if body.get("d_to"):
                return api.scan_range(
                    body["date"], body["d_to"], body.get("params"),
                    include_hits=not body.get("summary", False),
                )
            return api.scan(body["date"], body.get("params"))
        except Exception as error:
            raise HTTPException(400, f"{type(error).__name__}: {error}")


@app.get("/api/bt_defaults")
def bt_defaults():
    from . import backtest as bt
    return {"bt": bt.DEFAULT_BT}


@app.post("/api/backtest")
def run_backtest(body: dict):
    if sync.STATE.running:
        raise HTTPException(409, "동기화 중에는 성과검증을 실행할 수 없습니다")
    name, ver = body.get("name"), body.get("ver")
    cond, options = body.get("params"), body.get("bt")
    if name:
        record = store.get_cond(name, ver)
        if not record:
            raise HTTPException(404, f"조건식 없음: {name}")
        cond, options, ver = record["params"], record["bt"], record["ver"]
    with _SCAN_LOCK:
        try:
            result = api.backtest(body["d_from"], body.get("d_to"), cond, options,
                include_trades=not body.get("summary", False))
        except Exception as error:
            raise HTTPException(400, f"{type(error).__name__}: {error}")
    if result.get("ok") and name and body.get("save", True):
        store.save_bt_result(name, ver, result)
        result["saved_as"] = {"name": name, "ver": ver}
    return result


@app.get("/api/conds")
def conds():
    return {"conds": store.list_conds()}


@app.post("/api/conds")
def cond_save(body: dict):
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "이름이 필요합니다")
    try:
        cond = params.merge_cond(body.get("params"))
        from . import backtest as bt
        options = bt.merge_bt(body.get("bt"))
    except Exception as error:
        raise HTTPException(400, f"{type(error).__name__}: {error}")
    return store.save_cond(name, cond, options, body.get("note", ""))


@app.post("/api/conds/delete")
def cond_delete(body: dict):
    store.delete_cond(body["name"], body.get("ver"))
    return {"ok": True}


@app.get("/api/bt_results")
def bt_results():
    return {"results": store.recent_bt(50)}

@app.post("/api/sweep")
def sweep(body: dict):
    if sync.STATE.running:
        raise HTTPException(409, "동기화 중에는 탐색을 실행할 수 없습니다")
    from . import sweep as sw
    with _SCAN_LOCK:
        try:
            return sw.run(
                (body.get("name") or "sweep").strip(), body["d_from"], body.get("d_to"),
                body["grid"], body.get("params"), body.get("bt"),
                body.get("kpi") or sw.KPI, body.get("note", ""))
        except Exception as error:
            raise HTTPException(400, f"{type(error).__name__}: {error}")


@app.post("/api/factor")
def factor(body: dict):
    if sync.STATE.running:
        raise HTTPException(409, "동기화 중에는 요인분석을 실행할 수 없습니다")
    from . import factors
    with _SCAN_LOCK:
        try:
            return factors.run(body["d_from"], body.get("d_to"), body.get("params"),
                               body.get("bt"), body.get("edge", 10.0),
                               body.get("bins", 10), body.get("stop", 5.0),
                               body.get("metric", "first"))
        except Exception as error:
            raise HTTPException(400, f"{type(error).__name__}: {error}")


@app.post("/api/combo")
def combo(body: dict):
    if sync.STATE.running:
        raise HTTPException(409, "동기화 중에는 조합탐색을 실행할 수 없습니다")
    from . import combo as cb
    with _SCAN_LOCK:
        try:
            return cb.run(body["d_from"], body.get("d_to"), body.get("params"),
                          body.get("bt"), body.get("edge", 10.0),
                          body.get("min_keep", 8.0), body.get("depth", 3),
                          body.get("keys"), body.get("stop", 5.0),
                          body.get("metric", "first"))
        except Exception as error:
            raise HTTPException(400, f"{type(error).__name__}: {error}")
