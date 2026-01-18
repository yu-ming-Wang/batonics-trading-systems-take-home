import React from "react";

export default function RightPanel({
  qSymbol,
  setQSymbol,
  fromDT,
  setFromDT,
  toDT,
  setToDT,
  onQuery,

  // NEW
  onAutoFillRange,
  rangeLoading,
  rangeErr,

  histErr,
  histRows,
  histLoading,
  chartSeries,

  MiniLineChart,

  userTouchedRef,
}) {
  return (
    <section className="card">
      <div className="cardHeader">
        <div className="cardTitle">HISTORICAL QUERY</div>
        <div className="cardHint">Data retrieval + analysis (REST), not stream</div>
      </div>

      <div className="queryRow">
        <div className="field">
          <label>Symbol</label>
          <input
            value={qSymbol}
            onChange={(e) => {
              setQSymbol(e.target.value);
              userTouchedRef.current = { from: false, to: false };
              setFromDT("");
              setToDT("");
            }}
            placeholder="e.g. CLX5"
          />
        </div>

        <div className="field">
          <label>From</label>
          <input
            type="datetime-local"
            value={fromDT}
            onChange={(e) => {
              // save
              userTouchedRef.current = userTouchedRef.current || { from: false, to: false };
              userTouchedRef.current.from = true;
              setFromDT(e.target.value);
            }}
          />
        </div>

        <div className="field">
          <label>To</label>
          <input
            type="datetime-local"
            value={toDT}
            onChange={(e) => {
              userTouchedRef.current = userTouchedRef.current || { from: false, to: false };
              userTouchedRef.current.to = true;
              setToDT(e.target.value);
            }}
          />
        </div>

        {/* NEW: Auto Fill */}
        <button
          className="btn"
          onClick={onAutoFillRange}
          disabled={histLoading || rangeLoading || !qSymbol.trim()}
          title="Fetch min/max ts from DB"
        >
          {rangeLoading ? "Filling..." : "Auto Fill"}
        </button>

        <button className="btn queryBtn" onClick={onQuery} disabled={histLoading}>
          {histLoading ? "Querying..." : "Query"}
        </button>
      </div>

      {/* NEW: range error */}
      {rangeErr && (
        <div className="hint" style={{ color: "#ff6b6b", marginTop: 10 }}>
          {rangeErr}
        </div>
      )}

      {histErr && (
        <div className="hint" style={{ color: "#ff6b6b", marginTop: 10 }}>
          {histErr}
        </div>
      )}

      {!histErr && histRows?.length > 0 && (
        <div className="hint" style={{ marginTop: 10 }}>
          Loaded {histRows.length} rows for {qSymbol.trim()}
        </div>
      )}

      <div className="chartStack">
        <MiniLineChart title="SPREAD VS TIME" data={chartSeries.spread} />
        <MiniLineChart title="MID PRICE VS TIME" data={chartSeries.mid} />
        <MiniLineChart title="TOP-OF-BOOK SIZE VS TIME" data={chartSeries.size} />

        <div className="chartCard">
          <div className="chartTitle">RAW SAMPLE (FIRST 5)</div>

          {histRows.length === 0 ? (
            <div className="chartPlaceholder">Query first</div>
          ) : (
            <div className="rawBox">
              <pre className="rawPre">{JSON.stringify(histRows.slice(0, 5), null, 2)}</pre>
            </div>
          )}
        </div>
      </div>

      <div className="footerNote">
        Uses <code>/range</code> to auto-fill From/To (DB truth) and <code>/history</code> to fetch points.
      </div>
    </section>
  );
}
