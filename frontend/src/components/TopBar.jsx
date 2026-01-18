import React from "react";

export default function TopBar({
  symbol,
  conn,
  replayState, // "IDLE" | "REPLAYING" | "DONE" | "ERROR"
  replayErr,   // string | null
  onStartReplay,

  // ✅ NEW: tcp rate
  tcpRate,
  onTcpRateChange,
}) {
  const statusText =
    conn === "connected"
      ? "CONNECTED"
      : conn === "connecting"
      ? "CONNECTING"
      : "DISCONNECTED";

  const canStart = conn === "connected" && replayState !== "REPLAYING";

  const replayLabel =
    replayState === "IDLE"
      ? "IDLE"
      : replayState === "REPLAYING"
      ? "REPLAYING"
      : replayState === "DONE"
      ? "DONE"
      : "ERROR";

  const buttonText =
    replayState === "REPLAYING" ? "⏳ Running..." : "▶ Replay";

  const buttonTitle =
    conn !== "connected"
      ? "WebSocket not connected"
      : replayState === "REPLAYING"
      ? "Replay is running"
      : "Start replay (run streamer once)";

  return (
    <header className="topbar">
      <div className="brand">
        <div className="logo">≋</div>
        <div>
          <div className="title">Order Book Dashboard</div>
          <div className="subtitle">C++ MBO Replay → WebSocket → React UI</div>
        </div>
      </div>

      <div className="rightMeta">
        {/* ✅ Replay State */}
        <div className="pill" title={replayErr ? replayErr : "Replay state"}>
          <span className="pillLabel">REPLAY</span>
          <span className="pillValue">{replayLabel}</span>
        </div>

        {/* ✅ NEW: TCP Rate Select */}
        <div className="pill" title="Streamer TCP send rate (msg/sec)">
          <span className="pillLabel">TCP RATE</span>
          <select
            className="topSelect"
            value={tcpRate}
            onChange={(e) => onTcpRateChange(Number(e.target.value))}
            disabled={replayState === "REPLAYING"} // 避免你改了但其實跑的是舊的
          >
            <option value={200}>200</option>
            <option value={1000}>1000</option>
            <option value={10000}>10000</option>
            <option value={50000}>50000</option>
            <option value={500000}>500000</option>
          </select>
        </div>

        {/* ✅ Start Replay (no stop) */}
        <button
          className={replayState === "REPLAYING" ? "btn ghost" : "btn"}
          onClick={onStartReplay}
          disabled={!canStart}
          title={buttonTitle}
        >
          {buttonText}
        </button>

        {/* Symbol */}
        <div className="pill">
          <span className="pillLabel">SYMBOL</span>
          <span className="pillValue">{symbol}</span>
        </div>

        {/* Connection Status */}
        <div className={`status ${conn}`}>
          <span className="dot" />
          <span className="statusText">{statusText}</span>
        </div>
      </div>
    </header>
  );
}
