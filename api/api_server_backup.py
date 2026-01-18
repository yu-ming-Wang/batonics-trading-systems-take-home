from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Optional
import subprocess
import threading
from pathlib import Path
import os 
import requests



# ---------------- Streamer control (LOCAL DEMO ONLY) ----------------
streamer_process = None
streamer_lock = threading.Lock()

BASE_DIR = Path(__file__).resolve().parent   # BATONIC/api
ROOT_DIR = BASE_DIR.parent                  # BATONIC

STREAMER_BIN = ROOT_DIR / "mbo-stream" / "src" / "streamer"
CSV_PATH = ROOT_DIR / "mbo-stream" / "data" / "CLX5_mbo.csv"

# llowed rates (match TopBar dropdown)
ALLOWED_RATES = {200, 1000, 10000, 50000, 500000}
DEFAULT_RATE = 1000


def build_streamer_cmd(rate: int):
    # streamer <csv> <port> <rate> <start_offset>
    return [
        str(STREAMER_BIN),
        str(CSV_PATH),
        "9000",
        str(rate),
        "0",
    ]


class ReplayStartReq(BaseModel):
    rate: int = DEFAULT_RATE


app = FastAPI(title="Batonic Snapshot API")

# let frontend（Vite 5173）可以打 API
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
    "http://127.0.0.1:5173", "http://localhost:5173",     # dev
    "http://127.0.0.1:3000", "http://localhost:3000",],   # docker/nginx
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
CONNINFO = os.getenv(
    "PG_CONNINFO",
    "host=db port=5432 dbname=batonic user=postgres password=postgres"
)

# CONNINFO = "host=127.0.0.1 port=5432 dbname=batonic user=postgres password=postgres"


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
        # handle trailing Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad datetime: {s}")


@app.get("/health")
def health():
    return {"ok": True}


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
    """
    Return min/max timestamp available for a symbol.
    Useful for frontend auto-fill of From/To.
    """
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
            detail="bucket/time_bucket requires TimescaleDB extension (not installed). Omit bucket."
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


@app.post("/control/replay/start")
def start_replay(req: ReplayStartReq):
    """
    Start streamer once with selected TCP rate.
    Frontend sends JSON: { "rate": 200|1000|10000|50000|500000 }
    """
    global streamer_process

    rate = int(req.rate)
    if rate not in ALLOWED_RATES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid rate {rate}. Allowed: {sorted(ALLOWED_RATES)}"
        )

    cmd = build_streamer_cmd(rate)

    with streamer_lock:
        if streamer_process and streamer_process.poll() is None:
            raise HTTPException(status_code=409, detail="Replay already running")

        if not STREAMER_BIN.exists():
            raise HTTPException(
                status_code=500,
                detail=f"Streamer binary not found: {STREAMER_BIN}"
            )

        if not CSV_PATH.exists():
            raise HTTPException(
                status_code=500,
                detail=f"CSV not found: {CSV_PATH}"
            )

        try:
            streamer_process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as e:
            streamer_process = None
            raise HTTPException(status_code=500, detail=str(e))

        return {
            "state": "REPLAYING",
            "pid": streamer_process.pid,
            "rate": rate,
            "cmd": cmd,
        }


@app.get("/control/replay/status")
def replay_status():
    if not streamer_process:
        return {"state": "IDLE"}

    rc = streamer_process.poll()
    if rc is None:
        return {"state": "REPLAYING", "pid": streamer_process.pid}

    return {"state": "DONE", "exit_code": rc}
