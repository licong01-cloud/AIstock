"use client";

import { useMemo, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface IndicatorRow {
  [key: string]: any;
}

interface IndicatorScreeningResponse {
  success: boolean;
  error: string | null;
  filters_applied: string[];
  filters_skipped: string[];
  trade_date: string;
  total_candidates: number;
  selected_count: number;
  rows: IndicatorRow[];
}

function formatNumber(v: number | null | undefined, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return v.toFixed(digits);
}

function formatAmountWan(v: number | null | undefined) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return `${(v / 1e4).toFixed(2)}万`;
}

function formatAmountYi(v: number | null | undefined) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return `${(v / 1e8).toFixed(2)}亿`;
}

export default function IndicatorScreeningPage() {
  const [tradeDate, setTradeDate] = useState<string>(() =>
    new Date().toISOString().slice(0, 10),
  );
  const [topN, setTopN] = useState(100);
  const [pctChgMin, setPctChgMin] = useState(-1.5);
  const [pctChgMax, setPctChgMax] = useState(2.5);
  const [turnoverMin, setTurnoverMin] = useState(3.0);
  const [volMin, setVolMin] = useState(50000);
  const [floatShareMaxE, setFloatShareMaxE] = useState(150.0);
  const [floatMvMaxE, setFloatMvMaxE] = useState(500.0);
  const [netTodayMinW, setNetTodayMinW] = useState(2000.0);
  const [net10dMinW, setNet10dMinW] = useState(2000.0);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<IndicatorScreeningResponse | null>(
    null,
  );

  async function handleRun() {
    if (!tradeDate) {
      setError("请先选择交易日");
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const body = {
        trade_date: tradeDate,
        top_n: topN,
        pct_chg_min: pctChgMin,
        pct_chg_max: pctChgMax,
        turnover_min: turnoverMin,
        volume_hand_min: volMin,
        float_share_max: floatShareMaxE * 1_0000_0000,
        float_mv_max: floatMvMaxE * 1_0000_0000,
        net_inflow_today_min: netTodayMinW * 10_000,
        net_inflow_10d_min: net10dMinW * 10_000,
      };

      const res = await fetch(`${API_BASE}/indicator-screening/open-0935`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        throw new Error(`请求失败: ${res.status}`);
      }

      const data: IndicatorScreeningResponse = await res.json();
      setResult(data);
      if (!data.success && data.error) {
        setError(data.error);
      }
    } catch (e: any) {
      setError(e?.message || "未知错误");
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  const rows = useMemo(() => result?.rows || [], [result]);

  const has0935Cols = useMemo(() => {
    return rows.some(
      (r) =>
        r.pct_chg_0935 != null ||
        r.vol_0935 != null ||
        r.volume_ratio_0935 != null,
    );
  }, [rows]);

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 24,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 22 }}>📊 指标选股（开盘 9:35 策略）</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          基于 Tushare 日线、换手率、流通市值与资金流的开盘选股策略，当前版本使用日线
          pct_chg 近似 9:35 前涨跌幅。
        </p>
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
          marginBottom: 20,
          fontSize: 13,
        }}
      >
        <h2 style={{ marginTop: 0, fontSize: 16 }}>参数设置</h2>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
            gap: 12,
            alignItems: "flex-end",
          }}
        >
          <div>
            <label style={{ display: "block", marginBottom: 4 }}>交易日</label>
            <input
              type="date"
              value={tradeDate}
              onChange={(e) => setTradeDate(e.target.value)}
              style={{ width: "100%" }}
            />
          </div>
          <div>
            <label style={{ display: "block", marginBottom: 4 }}>
              保留前 N 名
            </label>
            <input
              type="number"
              min={10}
              max={2000}
              step={10}
              value={topN}
              onChange={(e) => setTopN(Number(e.target.value) || 0)}
              style={{ width: "100%" }}
            />
          </div>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
            gap: 12,
            marginTop: 12,
          }}
        >
          <div>
            <label style={{ display: "block", marginBottom: 4 }}>
              9:35 涨跌幅下限(%)
            </label>
            <input
              type="number"
              step={0.1}
              value={pctChgMin}
              onChange={(e) => setPctChgMin(Number(e.target.value))}
              style={{ width: "100%" }}
            />
            <label style={{ display: "block", margin: "8px 0 4px" }}>
              换手率下限(%)
            </label>
            <input
              type="number"
              step={0.5}
              value={turnoverMin}
              onChange={(e) => setTurnoverMin(Number(e.target.value))}
              style={{ width: "100%" }}
            />
            <label style={{ display: "block", margin: "8px 0 4px" }}>
              9:35 成交量下限(手)
            </label>
            <input
              type="number"
              step={5000}
              value={volMin}
              onChange={(e) => setVolMin(Number(e.target.value))}
              style={{ width: "100%" }}
            />
          </div>

          <div>
            <label style={{ display: "block", marginBottom: 4 }}>
              9:35 涨跌幅上限(%)
            </label>
            <input
              type="number"
              step={0.1}
              value={pctChgMax}
              onChange={(e) => setPctChgMax(Number(e.target.value))}
              style={{ width: "100%" }}
            />
            <label style={{ display: "block", margin: "8px 0 4px" }}>
              流通股本上限(亿股)
            </label>
            <input
              type="number"
              step={10}
              value={floatShareMaxE}
              onChange={(e) => setFloatShareMaxE(Number(e.target.value))}
              style={{ width: "100%" }}
            />
            <label style={{ display: "block", margin: "8px 0 4px" }}>
              流通市值上限(亿元)
            </label>
            <input
              type="number"
              step={50}
              value={floatMvMaxE}
              onChange={(e) => setFloatMvMaxE(Number(e.target.value))}
              style={{ width: "100%" }}
            />
          </div>

          <div>
            <label style={{ display: "block", marginBottom: 4 }}>
              当日净流入下限(万元)
            </label>
            <input
              type="number"
              step={500}
              value={netTodayMinW}
              onChange={(e) => setNetTodayMinW(Number(e.target.value))}
              style={{ width: "100%" }}
            />
            <label style={{ display: "block", margin: "8px 0 4px" }}>
              近10日净流入下限(万元)
            </label>
            <input
              type="number"
              step={500}
              value={net10dMinW}
              onChange={(e) => setNet10dMinW(Number(e.target.value))}
              style={{ width: "100%" }}
            />
            <button
              onClick={handleRun}
              disabled={loading}
              style={{
                marginTop: 16,
                padding: "8px 16px",
                borderRadius: 999,
                border: "none",
                background: "#4f46e5",
                color: "#fff",
                fontSize: 14,
                cursor: "pointer",
              }}
            >
              {loading ? "执行中..." : "🚀 执行选股"}
            </button>
          </div>
        </div>

        {error && (
          <p style={{ color: "#b00020", marginTop: 12, fontSize: 13 }}>
            错误：{error}
          </p>
        )}
      </section>

      {result && (
        <>
          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
              marginBottom: 16,
              fontSize: 13,
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 16 }}>策略执行结果</h2>
            <p style={{ marginBottom: 8 }}>
              交易日：{result.trade_date} · 候选股票：{result.total_candidates} 只 ·
              最终筛选：{result.selected_count} 只
            </p>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                gap: 16,
              }}
            >
              <div>
                <h3 style={{ fontSize: 14, marginBottom: 4 }}>已应用条件</h3>
                {result.filters_applied.length ? (
                  <ul style={{ paddingLeft: 18, marginTop: 4 }}>
                    {result.filters_applied.map((f, idx) => (
                      <li key={idx}>{f}</li>
                    ))}
                  </ul>
                ) : (
                  <p style={{ color: "#777" }}>（无）</p>
                )}
              </div>
              <div>
                <h3 style={{ fontSize: 14, marginBottom: 4 }}>
                  未应用条件 / 当前未实现部分
                </h3>
                {result.filters_skipped.length ? (
                  <ul style={{ paddingLeft: 18, marginTop: 4 }}>
                    {result.filters_skipped.map((f, idx) => (
                      <li key={idx}>{f}</li>
                    ))}
                  </ul>
                ) : (
                  <p style={{ color: "#777" }}>（无）</p>
                )}
              </div>
            </div>
          </section>

          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 12,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
              fontSize: 12,
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                marginBottom: 8,
              }}
            >
              <h2 style={{ margin: 0, fontSize: 16 }}>筛选结果列表</h2>
              <span style={{ color: "#777" }}>
                列表为后端计算结果的直接展开，可在后续版本补充 CSV 导出与批量操作。
              </span>
            </div>

            {rows.length === 0 ? (
              <p style={{ color: "#777" }}>过滤后没有满足条件的股票。</p>
            ) : (
              <div
                style={{
                  maxHeight: 520,
                  overflow: "auto",
                  borderRadius: 6,
                  border: "1px solid #eee",
                }}
              >
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                  }}
                >
                  <thead>
                    <tr
                      style={{
                        background: "#fafafa",
                        position: "sticky",
                        top: 0,
                      }}
                    >
                      <th style={{ padding: 6, textAlign: "left" }}>代码</th>
                      <th style={{ padding: 6, textAlign: "left" }}>名称</th>
                      <th style={{ padding: 6, textAlign: "right" }}>
                        涨跌幅%
                      </th>
                      <th style={{ padding: 6, textAlign: "right" }}>
                        换手率%
                      </th>
                      <th style={{ padding: 6, textAlign: "right" }}>
                        成交量(手)
                      </th>
                      <th style={{ padding: 6, textAlign: "right" }}>
                        流通市值(亿元)
                      </th>
                      <th style={{ padding: 6, textAlign: "right" }}>
                        当日主力净流入(万元)
                      </th>
                      <th style={{ padding: 6, textAlign: "right" }}>
                        近10日净流入(万元)
                      </th>
                      {has0935Cols && (
                        <>
                          <th style={{ padding: 6, textAlign: "right" }}>
                            9:35涨跌幅%
                          </th>
                          <th style={{ padding: 6, textAlign: "right" }}>
                            9:35成交量(手)
                          </th>
                          <th style={{ padding: 6, textAlign: "right" }}>
                            9:35量比
                          </th>
                        </>
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row, idx) => (
                      <tr
                        key={idx}
                        style={{
                          borderTop: "1px solid #f0f0f0",
                          background: idx % 2 === 0 ? "#fff" : "#fcfcfc",
                        }}
                      >
                        <td style={{ padding: 6, fontFamily: "monospace" }}>
                          {String(row.ts_code || "")}
                        </td>
                        <td style={{ padding: 6 }}>{row.name || ""}</td>
                        <td
                          style={{
                            padding: 6,
                            textAlign: "right",
                            color:
                              (row.pct_chg ?? 0) > 0
                                ? "#e53935"
                                : (row.pct_chg ?? 0) < 0
                                  ? "#1e88e5"
                                  : "#333",
                          }}
                        >
                          {formatNumber(row.pct_chg, 2)}
                        </td>
                        <td style={{ padding: 6, textAlign: "right" }}>
                          {formatNumber(row.turnover_rate, 2)}
                        </td>
                        <td style={{ padding: 6, textAlign: "right" }}>
                          {formatNumber(row.vol, 0)}
                        </td>
                        <td style={{ padding: 6, textAlign: "right" }}>
                          {formatAmountYi(
                            row.circ_mv != null ? row.circ_mv * 10_000 : null,
                          )}
                        </td>
                        <td style={{ padding: 6, textAlign: "right" }}>
                          {formatAmountWan(row.net_mf_today)}
                        </td>
                        <td style={{ padding: 6, textAlign: "right" }}>
                          {formatAmountWan(row.net_mf_10d)}
                        </td>
                        {has0935Cols && (
                          <>
                            <td style={{ padding: 6, textAlign: "right" }}>
                              {formatNumber(row.pct_chg_0935, 2)}
                            </td>
                            <td style={{ padding: 6, textAlign: "right" }}>
                              {formatNumber(row.vol_0935, 0)}
                            </td>
                            <td style={{ padding: 6, textAlign: "right" }}>
                              {formatNumber(row.volume_ratio_0935, 2)}
                            </td>
                          </>
                        )}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </>
      )}
    </main>
  );
}
