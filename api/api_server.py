from fastapi import FastAPI, Query, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Optional
import os
import requests
from pathlib import Path

# ---------------- Config ----------------

# Allowed rates (match TopBar dropdown)
ALLOWED_RATES = {200, 1000, 10000, 50000, 500000}
DEFAULT_RATE = int(os.getenv("DEFAULT_RATE", "1000"))

# Streamer-control base URL
# Local dev:  http://127.0.0.1:7000
# Docker:     http://streamer-control:7000  (service name in docker-compose)
STREAMER_CTRL_URL = os.getenv("STREAMER_CTRL_URL", "http://127.0.0.1:7000").rstrip("/")


def default_pg_conninfo():
    # docker 內會有這個檔案
    in_docker = os.path.exists("/.dockerenv")
    host = "db" if in_docker else "127.0.0.1"
    return f"host={host} port=5432 dbname=batonic user=postgres password=postgres"


def default_feed_path() -> str:
    """
    Dual-environment default:
    - In Docker: engine writes into shared volume: /shared/snapshots_feed.jsonl
    - Local:     default to repo_root/frontend/public/snapshots_feed.jsonl
                (computed robustly from this file location, NOT from cwd)
    """
    in_docker = os.path.exists("/.dockerenv")
    if in_docker:
        return "/shared/snapshots_feed.jsonl"

    here = Path(__file__).resolve()
    # If api_server.py is in repo root, root=parent
    # If api_server.py is in api/ subfolder, root=parent.parent
    # We'll detect by presence of "frontend" folder.
    cand1 = here.parent  # repo root candidate
    cand2 = here.parent.parent  # repo root candidate if in api/

    if (cand1 / "frontend").exists():
        repo_root = cand1
    elif (cand2 / "frontend").exists():
        repo_root = cand2
    else:
        # fallback to current working directory (last resort)
        repo_root = Path(os.getcwd())

    return str(repo_root / "frontend" / "public" / "snapshots_feed.jsonl")


CONNINFO = os.getenv("PG_CONNINFO", default_pg_conninfo())

# Feed output path (engine writes here; api serves it)
FEED_PATH = os.getenv("FEED_PATH", default_feed_path())

# CORS origins (dev + docker/nginx)
CORS_ORIGINS = [
    # Vite dev
    "http://127.0.0.1:5173",
    "http://localhost:5173",

    # nginx (docker)
    "http://127.0.0.1",
    "http://localhost",
    "http://127.0.0.1:80",
    "http://localhost:80",
    "http://127.0.0.1:8080",
    "http://localhost:8080",

    # (optional) future domain
    # "https://batonic.dev",
]

# ---------------- Models ----------------


class ReplayStartReq(BaseModel):
    rate: int = DEFAULT_RATE


# ---------------- App ----------------

app = FastAPI(title="Batonic Snapshot API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- Helpers ----------------


def get_conn():
    return psycopg2.connect(CONNINFO)


def parse_dt(s: str) -> datetime:
    """
    Accept ISO8601 like:
      2025-09-24T19:30:00Z
      2025-09-24T19:30:00.123Z
      2025-09-24T19:30:00+00:00
    """
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad datetime: {s}")


def _sc_get(path: str, timeout_s: float = 2.0):
    """GET streamer-control helper."""
    url = f"{STREAMER_CTRL_URL}{path}"
    try:
        r = requests.get(url, timeout=timeout_s)
        return r
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Cannot reach streamer-control ({url}): {e}")


def _sc_post(path: str, payload: dict, timeout_s: float = 3.0):
    """POST streamer-control helper."""
    url = f"{STREAMER_CTRL_URL}{path}"
    try:
        r = requests.post(url, json=payload, timeout=timeout_s)
        return r
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Cannot reach streamer-control ({url}): {e}")


def _feed_file() -> Path:
    """
    Normalize FEED_PATH into an absolute path for consistent behavior.
    - If FEED_PATH is absolute: use it.
    - If relative: resolve relative to repo root (best-effort), not cwd.
    """
    p = Path(FEED_PATH)

    if p.is_absolute():
        return p

    # Try to resolve relative to detected repo root (same logic as default_feed_path)
    here = Path(__file__).resolve()
    cand1 = here.parent
    cand2 = here.parent.parent
    if (cand1 / "frontend").exists():
        repo_root = cand1
    elif (cand2 / "frontend").exists():
        repo_root = cand2
    else:
        repo_root = Path(os.getcwd())

    return (repo_root / p).resolve()


# ---------------- Routes (Health) ----------------


@app.get("/health")
def health():
    # include streamer-control reachability to help debugging in docker
    sc_ok = False
    try:
        r = requests.get(f"{STREAMER_CTRL_URL}/health", timeout=1.0)
        sc_ok = (r.status_code == 200)
    except Exception:
        sc_ok = False

    p = _feed_file()
    feed_ok = p.exists() and p.is_file()
    feed_size = p.stat().st_size if feed_ok else None

    return {
        "ok": True,
        "streamer_control_ok": sc_ok,
        "feed_ok": feed_ok,
        "feed_path": str(p),
        "feed_size_bytes": feed_size,
    }


# ---------------- Routes (Feed download) ----------------


@app.head("/feed")
def feed_head():
    p = _feed_file()
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail=f"Feed not found: {str(p)}")
    return Response(status_code=200)


@app.get("/feed")
def download_feed():
    """
    Download NDJSON feed generated by engine.
    Frontend should hit ONLY this API route (works in local + docker).
    """
    p = _feed_file()
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail=f"Feed not found: {str(p)}")

    return FileResponse(
        path=str(p),
        media_type="application/x-ndjson",
        filename="snapshots_feed.jsonl",
    )


# ---------------- Routes (DB) ----------------


@app.get("/symbols")
def symbols():
    sql = "SELECT DISTINCT symbol FROM snapshots ORDER BY symbol;"
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        return [r["symbol"] for r in cur.fetchall()]


@app.get("/latest")
def latest(symbol: str = Query(..., min_length=1)):
    sql = """
    SELECT ts, symbol, best_bid_px, best_bid_sz, best_ask_px, best_ask_sz, mid, spread
    FROM snapshots
    WHERE symbol = %s
    ORDER BY ts DESC
    LIMIT 1;
    """
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (symbol,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No data for symbol")
        return row


@app.get("/range")
def time_range(symbol: str = Query(..., min_length=1)):
    sql = """
    SELECT
      min(ts) AS start_ts,
      max(ts) AS end_ts,
      count(*) AS rows
    FROM snapshots
    WHERE symbol = %s;
    """
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (symbol,))
        row = cur.fetchone()
        if not row or row["start_ts"] is None or row["end_ts"] is None:
            raise HTTPException(status_code=404, detail="No data for symbol")
        return row


@app.get("/history")
def history(
    symbol: str = Query(..., min_length=1),
    start: str = Query(..., description="ISO8601, e.g. 2025-09-24T19:30:00Z"),
    end: str = Query(..., description="ISO8601"),
    limit: int = Query(2000, ge=1, le=20000),
    bucket: Optional[str] = Query(None, description="(disabled) Timescale bucket"),
):
    dt_start = parse_dt(start)
    dt_end = parse_dt(end)
    if dt_end <= dt_start:
        raise HTTPException(status_code=400, detail="end must be > start")

    if bucket:
        raise HTTPException(
            status_code=400,
            detail="bucket/time_bucket requires TimescaleDB extension (not installed). Omit bucket.",
        )

    sql = """
    SELECT ts, symbol, best_bid_px, best_bid_sz, best_ask_px, best_ask_sz, mid, spread
    FROM snapshots
    WHERE symbol = %s
      AND ts >= %s AND ts <= %s
    ORDER BY ts ASC
    LIMIT %s;
    """
    params = (symbol, dt_start, dt_end, limit)

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


# ---------------- Routes (Replay control via streamer-control) ----------------


@app.post("/control/replay/start")
def start_replay(req: ReplayStartReq):
    """
    Idempotent replay start:
    - If replay already running -> return success (no scary error for other clients)
    - Otherwise start via streamer-control
    """
    rate = int(req.rate)
    if rate not in ALLOWED_RATES:
        raise HTTPException(
            status_code=400, detail=f"Invalid rate {rate}. Allowed: {sorted(ALLOWED_RATES)}"
        )

    # 1) check status
    st_resp = _sc_get("/control/status", timeout_s=2.0)
    if st_resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"streamer-control status failed: {st_resp.text}")

    st = st_resp.json()
    if st.get("state") == "RUNNING":
        # already running -> treat as success
        return {"state": "REPLAYING", "note": "already running", "rate": rate, "upstream": st}

    # 2) start
    start_resp = _sc_post("/control/start", payload={"rate": rate, "loop": 0}, timeout_s=3.0)

    if start_resp.status_code == 409:
        # race condition (another client started it between status/start)
        return {"state": "REPLAYING", "note": "already running (race)", "rate": rate}

    if start_resp.status_code != 200:
        raise HTTPException(status_code=start_resp.status_code, detail=start_resp.text)

    return {"state": "REPLAYING", "rate": rate, "upstream": start_resp.json()}


@app.get("/control/replay/status")
def replay_status():
    """
    Proxy streamer-control status so frontend still hits this API only.
    """
    st_resp = _sc_get("/control/status", timeout_s=2.0)
    if st_resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"streamer-control status failed: {st_resp.text}")
    return st_resp.json()
