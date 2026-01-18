import React, { useEffect, useMemo, useRef, useState } from "react";
import "./App.css";

import LeftPanel from "./components/LeftPanel";
import RightPanel from "./components/RightPanel";
import TopBar from "./components/TopBar";

/**
 * Live WS snapshot shape (from your C++ ws server):
 * { "symbol":"CLX5", "bids":[...], "asks":[...] }
 *
 * REST /history row shape (from FastAPI):
 * { "ts": "...", "symbol":"CLX5", "best_bid_px":..., ... }
 *
 * REST /range shape:
 * { "start_ts": "...", "end_ts":"...", "rows": 191 }
 */

function fmtPx(level) {
  if (level?.px_f != null) return Number(level.px_f).toFixed(4);
  if (level?.px != null) return String(level.px);
  return "-";
}

function fmtSz(level) {
  if (level?.sz == null) return "-";
  return String(level.sz);
}

function maxSz(levels) {
  let m = 1;
  for (const l of levels || []) m = Math.max(m, l?.sz ?? 0);
  return m || 1;
}

function computeBBO(bids, asks) {
  const bestBid = bids?.[0] || null;
  const bestAsk = asks?.[0] || null;
  return { bestBid, bestAsk };
}

// ----- datetime-local helpers -----
function isoToDatetimeLocal(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  const pad = (n) => String(n).padStart(2, "0");
  return (
    d.getFullYear() +
    "-" +
    pad(d.getMonth() + 1) +
    "-" +
    pad(d.getDate()) +
    "T" +
    pad(d.getHours()) +
    ":" +
    pad(d.getMinutes())
  );
}
function datetimeLocalToIso(dtLocal) {
  return new Date(dtLocal).toISOString();
}

// ----- Tiny chart helpers -----
function toNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function normalize01(v, vmin, vmax) {
  if (v == null || vmin == null || vmax == null) return 0;
  if (vmax === vmin) return 0.5;
  return (v - vmin) / (vmax - vmin);
}

function buildPolyline(points, width, height, pad = 6) {
  const w = Math.max(1, width - pad * 2);
  const h = Math.max(1, height - pad * 2);
  return points
    .map((p) => {
      const x = pad + p.x01 * w;
      const y = pad + (1 - p.y01) * h;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
}

function MiniLineChart({ title, data, height = 110 }) {
  const w = 520;
  const h = height;

  const vals = data.map((d) => d.v).filter((v) => v != null);
  const t0 = data[0]?.t ?? 0;
  const t1 = data[data.length - 1]?.t ?? t0;

  const vmin = vals.length ? Math.min(...vals) : 0;
  const vmax = vals.length ? Math.max(...vals) : 1;

  const pts = data.map((d) => {
    const x01 = t1 === t0 ? 0 : (d.t - t0) / (t1 - t0);
    const y01 = normalize01(d.v, vmin, vmax);
    return { x01, y01 };
  });

  const poly = pts.length >= 2 ? buildPolyline(pts, w, h, 8) : "";

  return (
    <div className="chartCard">
      <div className="chartTitle">{title}</div>

      {data.length < 2 ? (
        <div className="chartPlaceholder">Not enough points (need 2+)</div>
      ) : (
        <>
          <svg
            viewBox={`0 0 ${w} ${h}`}
            width="100%"
            height={h}
            style={{ display: "block", opacity: 0.95 }}
          >
            <polyline
              points={poly}
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinejoin="round"
              strokeLinecap="round"
            />
          </svg>

          <div className="footerNote" style={{ marginTop: 6 }}>
            min {vmin.toFixed(4)} · max {vmax.toFixed(4)} · n={data.length}
          </div>
        </>
      )}
    </div>
  );
}

export default function App() {
  const WS_URL = (window.location.protocol === "https:" ? "wss://" : "ws://") + window.location.host + "/ws";
  const API_BASE = "/api";

  const [conn, setConn] = useState("disconnected");
  const [symbol, setSymbol] = useState("—");
  const [bids, setBids] = useState([]);
  const [asks, setAsks] = useState([]);
  const [lastTs, setLastTs] = useState(null);
  const [depth, setDepth] = useState(10);

  // ✅ NEW: streamer tcp rate (msg/sec)
  const [tcpRate, setTcpRate] = useState(1000);

  // Replay UI states (visual only for now)
  const [paused, setPaused] = useState(false);
  const [speed, setSpeed] = useState(1.0);

  // ✅ Replay control state (TopBar)
  // IDLE | REPLAYING | DONE | ERROR
  const [replayState, setReplayState] = useState("IDLE");
  const [replayErr, setReplayErr] = useState(null);

  // ✅ Final full-book
  const [finalBook, setFinalBook] = useState(null);
  const [finalErr, setFinalErr] = useState(null);
  const [finalLoading, setFinalLoading] = useState(false);

  // ✅ Historical query controls
  const [qSymbol, setQSymbol] = useState("CLX5");
  const [fromDT, setFromDT] = useState("");
  const [toDT, setToDT] = useState("");

  // ✅ Historical query result
  const [histRows, setHistRows] = useState([]);
  const [histErr, setHistErr] = useState(null);
  const [histLoading, setHistLoading] = useState(false);

  // ✅ Range auto-fill state
  const [rangeLoading, setRangeLoading] = useState(false);
  const [rangeErr, setRangeErr] = useState("");
  const rangeAbortRef = useRef(null);

  // Keep only latest snapshot
  const latestRef = useRef(null);
  const rafRef = useRef(0);

  // user-touched datetime fields
  const userTouchedRef = useRef({ from: false, to: false });

  // ----------------------- WS subscribe -----------------------
  useEffect(() => {
    setConn("connecting");
    const ws = new WebSocket(WS_URL);

    ws.onopen = () => {
      setConn("connected");
      ws.send(
        JSON.stringify({
          type: "subscribe",
          symbol: "CLX5",
          push_ms: 50,
        })
      );
    };

    ws.onclose = () => setConn("disconnected");
    ws.onerror = () => setConn("disconnected");

    ws.onmessage = (e) => {
      latestRef.current = e.data;
      if (!rafRef.current) {
        rafRef.current = requestAnimationFrame(() => {
          rafRef.current = 0;
          try {
            const obj = JSON.parse(latestRef.current);
            setSymbol(obj.symbol || "—");
            setBids(Array.isArray(obj.bids) ? obj.bids : []);
            setAsks(Array.isArray(obj.asks) ? obj.asks : []);
            setLastTs(Date.now());
          } catch {}
        });
      }
    };

    return () => {
      try {
        ws.close();
      } catch {}
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      rafRef.current = 0;
    };
  }, []);

  // ✅ one-shot load final book JSON (from /public)
  async function loadFinalBook() {
    try {
      setFinalLoading(true);
      setFinalErr(null);

      const fname =
        symbol && symbol !== "—"
          ? `/final_book_${symbol}.json`
          : `/final_book.json`;

      const res = await fetch(fname, { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${fname}`);

      const obj = await res.json();
      setFinalBook(obj);
    } catch (e) {
      setFinalErr(String(e?.message ?? e));
      setFinalBook(null);
    } finally {
      setFinalLoading(false);
    }
  }

  // ---------------- ✅ Auto Fill button handler: /range ----------------
  async function onAutoFillRange() {
    const sym = qSymbol.trim();
    if (!sym) return;

    // cancel previous request if still running
    if (rangeAbortRef.current) rangeAbortRef.current.abort();
    const ac = new AbortController();
    rangeAbortRef.current = ac;

    setRangeLoading(true);
    setRangeErr("");

    try {
      const url = `${API_BASE}/range?symbol=${encodeURIComponent(sym)}`;
      const res = await fetch(url, { cache: "no-store", signal: ac.signal });

      if (res.status === 404) {
        setRangeErr("No data for symbol (404).");
        return;
      }
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status}: ${txt || res.statusText}`);
      }

      const row = await res.json();
      const startStr = isoToDatetimeLocal(row.start_ts);
      const endStr = isoToDatetimeLocal(row.end_ts);

      if (!startStr || !endStr) throw new Error("Bad /range payload");

      // system-filled: mark not user-touched
      userTouchedRef.current = userTouchedRef.current || { from: false, to: false };
      userTouchedRef.current.from = false;
      userTouchedRef.current.to = false;

      setFromDT(startStr);
      setToDT(endStr);
    } catch (e) {
      if (e?.name === "AbortError") return;
      setRangeErr(`Auto-fill failed: ${String(e?.message ?? e)}`);
    } finally {
      setRangeLoading(false);
    }
  }

  // ✅ Query /history
  async function onQuery() {
    const sym = qSymbol.trim();
    if (!sym) {
      setHistErr("Missing symbol");
      return;
    }
    if (!fromDT || !toDT) {
      setHistErr("Missing from/to datetime");
      return;
    }

    const startIso = datetimeLocalToIso(fromDT);
    const endIso = datetimeLocalToIso(toDT);

    const url =
      `${API_BASE}/history?` +
      `symbol=${encodeURIComponent(sym)}` +
      `&start=${encodeURIComponent(startIso)}` +
      `&end=${encodeURIComponent(endIso)}` +
      `&limit=20000`;

    try {
      setHistLoading(true);
      setHistErr(null);
      setHistRows([]);

      const res = await fetch(url, { cache: "no-store" });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(`HTTP ${res.status}: ${txt}`);
      }

      const rows = await res.json();
      if (!Array.isArray(rows)) throw new Error("Bad response: expected array");
      setHistRows(rows);
    } catch (e) {
      setHistErr(String(e?.message ?? e));
      setHistRows([]);
    } finally {
      setHistLoading(false);
    }
  }

  // ---------------- ✅ Replay controls (REAL API) ----------------
  async function fetchReplayStatus() {
    const res = await fetch(`${API_BASE}/control/replay/status`, {
      cache: "no-store",
    });
    if (!res.ok) throw new Error(`status HTTP ${res.status}`);
    return await res.json(); // {state: IDLE/REPLAYING/DONE, ...}
  }

  async function onStartReplay() {
    try {
      setReplayErr(null);

      // optimistic UI
      setReplayState("REPLAYING");

      // ✅ send selected rate to API
      const res = await fetch(`${API_BASE}/control/replay/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rate: tcpRate }),
      });

      if (!res.ok) {
        const txt = await res.text();
        // 409 = already running (not fatal; just reflect status)
        if (res.status === 409) {
          const s = await fetchReplayStatus();
          setReplayState(s.state || "REPLAYING");
          return;
        }
        throw new Error(`start HTTP ${res.status}: ${txt}`);
      }

      // start ok; status polling will update DONE
      await res.json();
    } catch (e) {
      setReplayErr(String(e?.message ?? e));
      setReplayState("ERROR");
    }
  }

  // Poll status (only while REPLAYING)
  useEffect(() => {
    if (replayState !== "REPLAYING") return;

    let alive = true;
    const interval = setInterval(async () => {
      try {
        const s = await fetchReplayStatus();
        if (!alive) return;

        const st = s?.state || "IDLE";
        if (st === "REPLAYING") {
          setReplayState("REPLAYING");
        } else if (st === "DONE") {
          setReplayState("DONE");
        } else {
          setReplayState("IDLE");
        }
      } catch (e) {
        if (!alive) return;
        setReplayErr(String(e?.message ?? e));
        setReplayState("ERROR");
      }
    }, 500);

    return () => {
      alive = false;
      clearInterval(interval);
    };
  }, [replayState, API_BASE]);

  // Optional: on first load, fetch initial status once
  useEffect(() => {
    (async () => {
      try {
        const s = await fetchReplayStatus();
        setReplayState(s?.state || "IDLE");
      } catch {
        // ignore
      }
    })();
  }, [API_BASE]);

  // ✅ Build chart series from /history rows
  const chartSeries = useMemo(() => {
    if (!histRows?.length) return { mid: [], spread: [], size: [] };

    const pts = histRows
      .map((r) => {
        const t = Date.parse(r.ts);
        if (!Number.isFinite(t)) return null;

        const midV = toNum(r.mid);
        const sprV = toNum(r.spread);

        const bsz = toNum(r.best_bid_sz);
        const asz = toNum(r.best_ask_sz);
        const sizeV = (bsz == null ? 0 : bsz) + (asz == null ? 0 : asz);

        return { t, midV, sprV, sizeV };
      })
      .filter(Boolean);

    return {
      mid: pts.map((p) => ({ t: p.t, v: p.midV })),
      spread: pts.map((p) => ({ t: p.t, v: p.sprV })),
      size: pts.map((p) => ({ t: p.t, v: p.sizeV })),
    };
  }, [histRows]);

  return (
    <div className="app">
      <TopBar
        symbol={symbol}
        conn={conn}
        replayState={replayState}
        replayErr={replayErr}
        onStartReplay={onStartReplay}
        // ✅ NEW
        tcpRate={tcpRate}
        onTcpRateChange={setTcpRate}
      />

      <div className="grid">
        <LeftPanel
          symbol={symbol}
          conn={conn}
          bids={bids}
          asks={asks}
          lastTs={lastTs}
          depth={depth}
          setDepth={setDepth}
          paused={paused}
          setPaused={setPaused}
          speed={speed}
          setSpeed={setSpeed}
          fmtPx={fmtPx}
          fmtSz={fmtSz}
          maxSz={maxSz}
          computeBBO={computeBBO}
          finalBook={finalBook}
          finalErr={finalErr}
          finalLoading={finalLoading}
          loadFinalBook={loadFinalBook}
        />

        <RightPanel
          qSymbol={qSymbol}
          setQSymbol={setQSymbol}
          fromDT={fromDT}
          setFromDT={setFromDT}
          toDT={toDT}
          setToDT={setToDT}
          onQuery={onQuery}
          // ✅ NEW props for range
          onAutoFillRange={onAutoFillRange}
          rangeLoading={rangeLoading}
          rangeErr={rangeErr}
          // existing props
          histErr={histErr}
          histRows={histRows}
          histLoading={histLoading}
          chartSeries={chartSeries}
          MiniLineChart={MiniLineChart}
          userTouchedRef={userTouchedRef}
        />
      </div>
    </div>
  );
}
