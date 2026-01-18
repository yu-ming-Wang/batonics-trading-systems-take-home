from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess
import threading
import os
from pathlib import Path
from typing import Optional

app = FastAPI(title="Streamer Control")

# ---- process state ----
_proc: Optional[subprocess.Popen] = None
_lock = threading.Lock()

# ---- config (local defaults; docker later can override via env) ----
BASE_DIR = Path(__file__).resolve().parent          # .../streamer
STREAMER_BIN = Path(os.getenv("STREAMER_BIN", str(BASE_DIR / "streamer"))).resolve()
CSV_PATH = Path(os.getenv("CSV_PATH", str(BASE_DIR / "data" / "CLX5_mbo.csv"))).resolve()
STREAMER_PORT = int(os.getenv("STREAMER_PORT", "9000"))

# Allowed rates (match your dropdown)
ALLOWED_RATES = {200, 1000, 10000, 50000, 500000}
DEFAULT_RATE = int(os.getenv("DEFAULT_RATE", "1000"))

class StartReq(BaseModel):
    rate: int = DEFAULT_RATE
    loop: int = 1                   # 1 = loop forever, 0 = once
    max_msgs: Optional[int] = None  # optional

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/config")
def config():
    # helpful for debugging in docker / reviewer environment
    return {
        "streamer_bin": str(STREAMER_BIN),
        "csv_path": str(CSV_PATH),
        "streamer_port": STREAMER_PORT,
        "allowed_rates": sorted(ALLOWED_RATES),
        "default_rate": DEFAULT_RATE,
    }

def _is_running() -> bool:
    global _proc
    return _proc is not None and _proc.poll() is None

@app.get("/control/status")
def status():
    global _proc
    if _proc is None:
        return {"state": "IDLE"}

    rc = _proc.poll()
    if rc is None:
        return {"state": "RUNNING", "pid": _proc.pid}

    return {"state": "DONE", "exit_code": rc}

@app.post("/control/start")
def start(req: StartReq):
    """
    Start streamer with chosen rate.
    streamer usage:
      streamer <csv_path> <port> <rate_msgs_per_sec> <loop:0|1> [max_msgs]
    """
    global _proc

    rate = int(req.rate)
    loop = 1 if int(req.loop) != 0 else 0

    if rate not in ALLOWED_RATES:
        raise HTTPException(400, f"Invalid rate {rate}. Allowed: {sorted(ALLOWED_RATES)}")

    if not STREAMER_BIN.exists():
        raise HTTPException(500, f"Streamer binary not found: {STREAMER_BIN}")
    if not CSV_PATH.exists():
        raise HTTPException(500, f"CSV not found: {CSV_PATH}")

    cmd = [str(STREAMER_BIN), str(CSV_PATH), str(STREAMER_PORT), str(rate), str(loop)]
    if req.max_msgs is not None:
        cmd.append(str(int(req.max_msgs)))

    with _lock:
        # Make it idempotent: if already running, return success (not 409 scary)
        if _is_running():
            return {
                "state": "RUNNING",
                "already_running": True,
                "pid": _proc.pid,
                "cmd": cmd,
            }

        try:
            # inherit stdout/stderr for local dev visibility
            _proc = subprocess.Popen(cmd)
        except Exception as e:
            _proc = None
            raise HTTPException(500, f"Failed to start streamer: {e}")

        return {"state": "RUNNING", "pid": _proc.pid, "cmd": cmd}

@app.post("/control/stop")
def stop():
    global _proc
    with _lock:
        if not _is_running():
            return {"state": "IDLE"}

        pid = _proc.pid
        _proc.terminate()
        try:
            _proc.wait(timeout=2.0)
        except Exception:
            # if it won't die, we go full John Wick (SIGKILL)
            try:
                _proc.kill()
                _proc.wait(timeout=2.0)
            except Exception:
                pass

        return {"state": "IDLE", "stopped_pid": pid}

@app.post("/control/restart")
def restart(req: StartReq):
    """
    Convenience: stop then start (useful if you want to change rate).
    """
    stop()
    return start(req)
