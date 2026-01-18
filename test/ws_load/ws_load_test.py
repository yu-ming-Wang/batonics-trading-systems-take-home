import argparse
import asyncio
import json
import time
from dataclasses import dataclass, asdict
from typing import Optional, List
from pathlib import Path

import websockets


@dataclass
class ClientResult:
    ok: bool
    msgs: int
    bytes: int
    err: Optional[str]
    t_connect_ms: float
    t_run_s: float


async def one_client(i: int, url: str, symbol: str, push_ms: int, duration: float) -> ClientResult:
    t0 = time.perf_counter()
    msgs = 0
    nbytes = 0
    try:
        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=2,
            max_size=None,
        ) as ws:
            t1 = time.perf_counter()

            sub = {"type": "subscribe", "symbol": symbol, "push_ms": push_ms}
            await ws.send(json.dumps(sub))

            deadline = time.perf_counter() + duration
            while time.perf_counter() < deadline:
                timeout = max(0.0, deadline - time.perf_counter())
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    break

                if isinstance(msg, str):
                    nbytes += len(msg.encode("utf-8"))
                else:
                    nbytes += len(msg)

                msgs += 1

        t2 = time.perf_counter()
        return ClientResult(True, msgs, nbytes, None, (t1 - t0) * 1000.0, (t2 - t1))
    except Exception as e:
        t1 = time.perf_counter()
        return ClientResult(False, msgs, nbytes, str(e), (t1 - t0) * 1000.0, 0.0)


def pct(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = (len(xs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return xs[f]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


async def run_round(n: int, url: str, symbol: str, push_ms: int, duration: float, ramp_s: float):
    results: List[ClientResult] = []

    async def launch(i):
        if ramp_s > 0:
            await asyncio.sleep((i / max(1, n - 1)) * ramp_s)
        return await one_client(i, url, symbol, push_ms, duration)

    tasks = [asyncio.create_task(launch(i)) for i in range(n)]
    for t in asyncio.as_completed(tasks):
        results.append(await t)

    ok = [r for r in results if r.ok]
    bad = [r for r in results if not r.ok]

    total_msgs = sum(r.msgs for r in ok)
    total_bytes = sum(r.bytes for r in ok)

    connect_ms = [r.t_connect_ms for r in ok]

    return {
        "ts_wall_s": time.time(),
        "clients": n,
        "ok": len(ok),
        "fail": len(bad),
        "duration_s": duration,
        "push_ms": push_ms,
        "total_msgs": total_msgs,
        "total_mps": total_msgs / duration if duration > 0 else 0.0,
        "total_mbps": (total_bytes * 8) / (duration * 1e6) if duration > 0 else 0.0,
        "avg_msgs_per_client": (total_msgs / len(ok)) if ok else 0.0,
        "connect_p50_ms": pct(connect_ms, 50),
        "connect_p95_ms": pct(connect_ms, 95),
        "sample_error": bad[0].err if bad else None,
    }


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="ws://127.0.0.1:8080")
    ap.add_argument("--path", default="")
    ap.add_argument("--symbol", default="CLX5")
    ap.add_argument("--push_ms", type=int, default=50)
    ap.add_argument("--duration", type=float, default=20.0)
    ap.add_argument("--ramp", type=float, default=8.0)
    ap.add_argument("--clients", default="10,50,100,1000")
    ap.add_argument(
        "--out",
        default="test/ws_load/ws_load_results.jsonl",
        help="output JSONL file",
    )
    args = ap.parse_args()

    url = args.url.rstrip("/")
    if args.path:
        url = url + "/" + args.path.lstrip("/")

    rounds = [int(x.strip()) for x in args.clients.split(",") if x.strip()]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nWS load test → {url}")
    print(f"Results will be written to: {out_path.resolve()}")
    print("-" * 96)

    with out_path.open("a") as f:
        for n in rounds:
            r = await run_round(n, url, args.symbol, args.push_ms, args.duration, args.ramp)

            # write JSONL
            f.write(json.dumps(r) + "\n")
            f.flush()

            print(
                f"clients={r['clients']:>5}  ok={r['ok']:>5}  fail={r['fail']:>4}  "
                f"total_mps={r['total_mps']:.1f}  "
                f"throughput={r['total_mbps']:.2f} Mbps  "
                f"connect_p50={r['connect_p50_ms']:.1f}ms  p95={r['connect_p95_ms']:.1f}ms"
            )
            print("-" * 96)


if __name__ == "__main__":
    asyncio.run(main())
