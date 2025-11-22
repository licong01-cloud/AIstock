"use client";

import { useEffect, useMemo, useState } from "react";
import dynamic from "next/dynamic";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any;

type MappingKey = "涨幅着色 · 流入定尺" | "流入着色 · 涨幅定尺" | "复合着色(α) · 流入定尺";

const MAPPING_SCHEMES: Record<MappingKey, { color: "chg" | "flow" | "combo"; size: "chg" | "flow" }> = {
  "涨幅着色 · 流入定尺": { color: "chg", size: "flow" },
  "流入着色 · 涨幅定尺": { color: "flow", size: "chg" },
  "复合着色(α) · 流入定尺": { color: "combo", size: "flow" },
};

const COLOR_SCALE = ["#d73027", "#ffffff", "#1a9850"];

interface HotboardItem {
  cate_type: number;
  board_code: string;
  board_name: string;
  pct_chg: number | null;
  amount: number | null;
  net_inflow: number | null;
  turnover: number | null;
  ratioamount: number | null;
  score?: number | null;
}

interface HotboardRealtimeResponse {
  ts: string | null;
  items: HotboardItem[];
}

function formatNumber(v: number | null | undefined, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return v.toFixed(digits);
}

export default function HotboardPage() {
  const [schemeLabel, setSchemeLabel] = useState<MappingKey>(
    "复合着色(α) · 流入定尺",
  );
  const [alpha, setAlpha] = useState(0.5);
  const [cateLabel, setCateLabel] = useState<"行业" | "概念" | "证监会行业" | "全部">(
    "全部",
  );
  const [activeTab, setActiveTab] = useState<"realtime" | "history">(
    "realtime",
  );
  const [realtimeData, setRealtimeData] = useState<HotboardRealtimeResponse | null>(
    null,
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedBoard, setSelectedBoard] = useState<string | null>(null);
  const [topStocks, setTopStocks] = useState<any[] | null>(null);
  const [historySource, setHistorySource] = useState<"sina" | "tdx">("sina");
  const [historyDate, setHistoryDate] = useState<string>(() =>
    new Date().toISOString().slice(0, 10),
  );
  const [historyData, setHistoryData] = useState<
    { date: string; items: any[] } | null
  >(null);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [historyTdxTypes, setHistoryTdxTypes] = useState<string[]>([]);
  const [historyIdxType, setHistoryIdxType] = useState<string>("");

  const cateMap: Record<string, number | null> = {
    行业: 0,
    概念: 1,
    证监会行业: 2,
    全部: null,
  };

  const scheme = useMemo(() => MAPPING_SCHEMES[schemeLabel], [schemeLabel]);

  async function loadRealtime() {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("metric", scheme.color);
      params.set("alpha", String(alpha));
      const ct = cateMap[cateLabel];
      if (ct !== null && ct !== undefined) params.set("cate_type", String(ct));
      const res = await fetch(`${API_BASE}/hotboard/realtime?${params.toString()}`);
      if (!res.ok) throw new Error(`请求失败: ${res.status}`);
      const data: HotboardRealtimeResponse = await res.json();
      setRealtimeData(data);
    } catch (e: any) {
      setError(e?.message || "未知错误");
      setRealtimeData(null);
    } finally {
      setLoading(false);
    }
  }

  async function loadTopRealtime(boardCode: string) {
    try {
      const params = new URLSearchParams();
      params.set("board_code", boardCode);
      params.set("metric", "chg");
      params.set("limit", "20");
      const res = await fetch(
        `${API_BASE}/hotboard/top-stocks/realtime?${params.toString()}`,
      );
      if (!res.ok) throw new Error(`Top20 请求失败: ${res.status}`);
      const data = await res.json();
      setTopStocks(data?.items || []);
    } catch (e) {
      setTopStocks(null);
    }
  }

  async function loadHistory() {
    setHistoryLoading(true);
    setHistoryError(null);
    try {
      if (historySource === "sina") {
        const params = new URLSearchParams();
        params.set("date", historyDate);
        const ct = cateMap[cateLabel];
        if (ct !== null && ct !== undefined) {
          params.set("cate_type", String(ct));
        }
        const res = await fetch(
          `${API_BASE}/hotboard/daily?${params.toString()}`,
        );
        if (!res.ok) throw new Error(`历史请求失败: ${res.status}`);
        const data = await res.json();
        setHistoryData(data);
      } else {
        const params = new URLSearchParams();
        params.set("date", historyDate);
        if (historyIdxType) {
          params.set("idx_type", historyIdxType);
        }
        const res = await fetch(
          `${API_BASE}/hotboard/tdx/daily?${params.toString()}`,
        );
        if (!res.ok) throw new Error(`TDX 历史请求失败: ${res.status}`);
        const data = await res.json();
        setHistoryData(data);
      }
    } catch (e: any) {
      setHistoryError(e?.message || "未知错误");
      setHistoryData(null);
    } finally {
      setHistoryLoading(false);
    }
  }

  async function ensureTdxTypesLoaded() {
    if (historyTdxTypes.length > 0) return;
    try {
      const res = await fetch(`${API_BASE}/hotboard/tdx/types`);
      if (!res.ok) return;
      const data = await res.json();
      const items = (data?.items || []) as string[];
      setHistoryTdxTypes(items);
      if (!historyIdxType && items.length > 0) {
        setHistoryIdxType(items[0]);
      }
    } catch {
      // 忽略，保持为空
    }
  }

  async function loadTopHistory(boardCode: string) {
    try {
      if (historySource === "tdx") {
        const params = new URLSearchParams();
        params.set("board_code", boardCode);
        params.set("date", historyDate);
        params.set("metric", "chg");
        params.set("limit", "20");
        const res = await fetch(
          `${API_BASE}/hotboard/top-stocks/tdx?${params.toString()}`,
        );
        if (!res.ok) throw new Error(`历史 Top20 请求失败: ${res.status}`);
        const data = await res.json();
        setTopStocks(data?.items || []);
      } else {
        // 新浪历史暂时复用实时成分股接口
        await loadTopRealtime(boardCode);
      }
    } catch {
      setTopStocks(null);
    }
  }

  useEffect(() => {
    loadRealtime();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (activeTab === "history") {
      if (historySource === "tdx") {
        ensureTdxTypesLoaded();
      }
      loadHistory();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, historySource, historyDate, cateLabel, historyIdxType]);

  const treemapData = useMemo(() => {
    if (!realtimeData?.items?.length) return null;
    const items = realtimeData.items;
    const sizeMetric = scheme.size;
    const area: number[] = [];
    const colorVals: number[] = [];
    for (const it of items) {
      const net = Number(it.net_inflow ?? 0) || 0;
      const pct = Number(it.pct_chg ?? 0) || 0;
      if (sizeMetric === "flow") {
        area.push(Math.abs(net) + 1e-6);
      } else {
        area.push(Math.abs(pct) + 1e-6);
      }
      if (scheme.color === "combo") {
        const score = Number(it.score ?? 0) || 0;
        colorVals.push(score);
      } else if (scheme.color === "flow") {
        colorVals.push(net);
      } else {
        colorVals.push(pct);
      }
    }
    const maxAbs =
      colorVals.length > 0
        ? Math.max(...colorVals.map((v) => Math.abs(v) || 0), 1)
        : 1;

    return {
      labels: items.map((it) => it.board_name || it.board_code),
      parents: items.map((it) => String(it.cate_type ?? "-")),
      values: area,
      colors: colorVals,
      maxAbs,
      codes: items.map((it) => it.board_code),
    };
  }, [realtimeData, scheme]);

  const historyTreemapData = useMemo(() => {
    if (!historyData?.items?.length) return null;
    const items = historyData.items as any[];
    const area: number[] = [];
    const colorVals: number[] = [];
    for (const it of items) {
      const pct = Number(it.pct_chg ?? 0) || 0;
      const amt = Number(it.amount ?? 0) || 0;
      area.push(Math.abs(amt) + 1e-6);
      colorVals.push(pct);
    }
    const maxAbs =
      colorVals.length > 0
        ? Math.max(...colorVals.map((v) => Math.abs(v) || 0), 1)
        : 1;
    return {
      labels: items.map((it) => it.board_name || it.board_code),
      parents: items.map((it) =>
        historySource === "sina"
          ? String(it.cate_type ?? "-")
          : String(it.idx_type ?? "TDX"),
      ),
      values: area,
      colors: colorVals,
      maxAbs,
      codes: items.map((it) => it.board_code),
    };
  }, [historyData, historySource]);

  return (
    <main style={{ padding: 24 }}>
      <section style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>🔥 热点板块跟踪</h1>
        <p style={{ marginTop: 4, fontSize: 13, color: "#666" }}>
          复刻原应用热点板块功能，展示实时与历史板块热力图及成分股 Top20。
        </p>
      </section>

      <section
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
          gap: 12,
          marginBottom: 16,
        }}
      >
        <div className="card">
          <div style={{ fontSize: 13, fontWeight: 600 }}>映射方案</div>
          <select
            style={{ marginTop: 6, width: "100%" }}
            value={schemeLabel}
            onChange={(e) => setSchemeLabel(e.target.value as MappingKey)}
          >
            {Object.keys(MAPPING_SCHEMES).map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </select>
        </div>

        <div className="card">
          <div style={{ fontSize: 13, fontWeight: 600 }}>复合权重 α</div>
          <input
            type="range"
            min={0}
            max={1}
            step={0.05}
            value={alpha}
            onChange={(e) => setAlpha(parseFloat(e.target.value))}
            style={{ width: "100%", marginTop: 6 }}
          />
          <div style={{ fontSize: 12, marginTop: 4 }}>{alpha.toFixed(2)}</div>
        </div>

        <div className="card">
          <div style={{ fontSize: 13, fontWeight: 600 }}>板块分类</div>
          <select
            style={{ marginTop: 6, width: "100%" }}
            value={cateLabel}
            onChange={(e) => setCateLabel(e.target.value as any)}
          >
            {Object.keys(cateMap).map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </select>
        </div>
      </section>

      <section style={{ marginBottom: 12 }}>
        <div style={{ display: "flex", gap: 12, fontSize: 13 }}>
          <button
            onClick={() => setActiveTab("realtime")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "realtime" ? "#eef2ff" : "#fff",
            }}
          >
            实时热点板块
          </button>
          <button
            onClick={() => setActiveTab("history")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "history" ? "#eef2ff" : "#fff",
            }}
          >
            历史热点板块
          </button>
        </div>
      </section>

      {activeTab === "realtime" && (
        <section
          style={{
            display: "grid",
            gridTemplateColumns: "minmax(0, 3fr) minmax(0, 2fr)",
            gap: 16,
          }}
        >
          <div
            style={{
              background: "#fff",
              padding: 12,
              borderRadius: 10,
              boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                marginBottom: 8,
                fontSize: 13,
              }}
            >
              <div>
                <strong>实时热点板块</strong>
                <span style={{ marginLeft: 8, color: "#777" }}>
                  {realtimeData?.ts ? `数据时刻: ${realtimeData.ts}` : ""}
                </span>
              </div>
              <button
                onClick={loadRealtime}
                disabled={loading}
                style={{
                  padding: "4px 10px",
                  borderRadius: 999,
                  border: "1px solid #ccc",
                  background: "#fafafa",
                  fontSize: 12,
                }}
              >
                {loading ? "刷新中..." : "刷新"}
              </button>
            </div>

            {error && (
              <p style={{ color: "#b00020", fontSize: 13 }}>错误：{error}</p>
            )}

            {treemapData ? (
              <Plot
                data={[
                  {
                    type: "treemap",
                    labels: treemapData.labels,
                    parents: treemapData.parents,
                    values: treemapData.values,
                    marker: {
                      colors: treemapData.colors,
                      colorscale: COLOR_SCALE,
                      cmin: -treemapData.maxAbs,
                      cmax: treemapData.maxAbs,
                    },
                    hovertemplate:
                      "%{label}<br>score=%{color:.2f}<extra></extra>",
                  },
                ]}
                layout={{
                  margin: { l: 0, r: 0, t: 0, b: 0 },
                  height: 420,
                  coloraxis: {
                    cmin: -treemapData.maxAbs,
                    cmax: treemapData.maxAbs,
                    colorscale: COLOR_SCALE,
                  },
                }}
                style={{ width: "100%", height: "100%" }}
                config={{ displayModeBar: false, responsive: true }}
                onClick={(ev: any) => {
                  const p = ev?.points?.[0];
                  if (!p) return;
                  const idx = p.pointIndex as number;
                  const code = treemapData.codes[idx];
                  setSelectedBoard(code);
                  loadTopRealtime(code);
                }}
              />
            ) : (
              <p style={{ fontSize: 13, color: "#777" }}>暂无实时数据</p>
            )}
          </div>

          <div
            style={{
              background: "#fff",
              padding: 12,
              borderRadius: 10,
              boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
            }}
          >
            <div style={{ fontSize: 13, marginBottom: 8 }}>
              <strong>成分股 Top20</strong>
              <span style={{ marginLeft: 8, color: "#777" }}>
                {selectedBoard ? `板块代码：${selectedBoard}` : "(点击左侧板块以查看)"}
              </span>
            </div>
            {topStocks && topStocks.length > 0 ? (
              <div
                style={{
                  maxHeight: 420,
                  overflow: "auto",
                  borderRadius: 6,
                  border: "1px solid #eee",
                }}
              >
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    fontSize: 12,
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
                      <th style={{ padding: 6, textAlign: "right" }}>涨幅%</th>
                      <th style={{ padding: 6, textAlign: "right" }}>成交额</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topStocks.map((row, idx) => (
                      <tr
                        key={idx}
                        style={{
                          borderTop: "1px solid #f0f0f0",
                          background: idx % 2 === 0 ? "#fff" : "#fcfcfc",
                        }}
                      >
                        <td style={{ padding: 6 }}>{row.code}</td>
                        <td style={{ padding: 6 }}>{row.name}</td>
                        <td
                          style={{
                            padding: 6,
                            textAlign: "right",
                            color:
                              (row.pct_change ?? 0) > 0
                                ? "#e53935"
                                : (row.pct_change ?? 0) < 0
                                  ? "#1e88e5"
                                  : "#333",
                          }}
                        >
                          {formatNumber(row.pct_change, 2)}
                        </td>
                        <td style={{ padding: 6, textAlign: "right" }}>
                          {row.amount != null
                            ? `${(row.amount / 1e8).toFixed(2)}亿`
                            : "-"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p style={{ fontSize: 12, color: "#777" }}>暂无成分股数据</p>
            )}
          </div>
        </section>
      )}

      {activeTab === "history" && (
        <section
          style={{
            display: "grid",
            gridTemplateColumns: "minmax(0, 3fr) minmax(0, 2fr)",
            gap: 16,
          }}
        >
          <div
            style={{
              background: "#fff",
              padding: 12,
              borderRadius: 10,
              boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                marginBottom: 8,
                fontSize: 13,
              }}
            >
              <div>
                <strong>历史热点板块</strong>
                <span style={{ marginLeft: 8, color: "#777" }}>
                  日期：{historyDate}
                </span>
              </div>
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <input
                  type="date"
                  value={historyDate}
                  onChange={(e) => setHistoryDate(e.target.value)}
                  style={{ fontSize: 12 }}
                />
                <select
                  value={historySource}
                  onChange={(e) =>
                    setHistorySource(e.target.value as "sina" | "tdx")
                  }
                  style={{ fontSize: 12 }}
                >
                  <option value="sina">新浪历史</option>
                  <option value="tdx">TDX 历史</option>
                </select>
                {historySource === "tdx" && (
                  <select
                    value={historyIdxType}
                    onChange={(e) => setHistoryIdxType(e.target.value)}
                    style={{ fontSize: 12 }}
                  >
                    {historyTdxTypes.map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                  </select>
                )}
                <button
                  onClick={loadHistory}
                  disabled={historyLoading}
                  style={{
                    padding: "4px 10px",
                    borderRadius: 999,
                    border: "1px solid #ccc",
                    background: "#fafafa",
                    fontSize: 12,
                  }}
                >
                  {historyLoading ? "刷新中..." : "刷新"}
                </button>
              </div>
            </div>

            {historyError && (
              <p style={{ color: "#b00020", fontSize: 13 }}>错误：{historyError}</p>
            )}

            {historyTreemapData ? (
              <Plot
                data={[
                  {
                    type: "treemap",
                    labels: historyTreemapData.labels,
                    parents: historyTreemapData.parents,
                    values: historyTreemapData.values,
                    marker: {
                      colors: historyTreemapData.colors,
                      colorscale: COLOR_SCALE,
                      cmin: -historyTreemapData.maxAbs,
                      cmax: historyTreemapData.maxAbs,
                    },
                    hovertemplate:
                      "%{label}<br>pct=%{color:.2f}%<extra></extra>",
                  },
                ]}
                layout={{
                  margin: { l: 0, r: 0, t: 0, b: 0 },
                  height: 420,
                  coloraxis: {
                    cmin: -historyTreemapData.maxAbs,
                    cmax: historyTreemapData.maxAbs,
                    colorscale: COLOR_SCALE,
                  },
                }}
                style={{ width: "100%,", height: "100%" }}
                config={{ displayModeBar: false, responsive: true }}
                onClick={(ev: any) => {
                  const p = ev?.points?.[0];
                  if (!p) return;
                  const idx = p.pointIndex as number;
                  const code = historyTreemapData.codes[idx];
                  setSelectedBoard(code);
                  loadTopHistory(code);
                }}
              />
            ) : (
              <p style={{ fontSize: 13, color: "#777" }}>暂无历史数据</p>
            )}
          </div>

          <div
            style={{
              background: "#fff",
              padding: 12,
              borderRadius: 10,
              boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
            }}
          >
            <div style={{ fontSize: 13, marginBottom: 8 }}>
              <strong>成分股 Top20</strong>
              <span style={{ marginLeft: 8, color: "#777" }}>
                {selectedBoard
                  ? `板块代码：${selectedBoard}`
                  : "(点击左侧板块以查看)"}
              </span>
            </div>
            {topStocks && topStocks.length > 0 ? (
              <div
                style={{
                  maxHeight: 420,
                  overflow: "auto",
                  borderRadius: 6,
                  border: "1px solid #eee",
                }}
              >
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    fontSize: 12,
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
                      <th style={{ padding: 6, textAlign: "right" }}>涨幅%</th>
                      <th style={{ padding: 6, textAlign: "right" }}>成交额</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topStocks.map((row, idx) => (
                      <tr
                        key={idx}
                        style={{
                          borderTop: "1px solid #f0f0f0",
                          background: idx % 2 === 0 ? "#fff" : "#fcfcfc",
                        }}
                      >
                        <td style={{ padding: 6 }}>{row.code}</td>
                        <td style={{ padding: 6 }}>{row.name}</td>
                        <td
                          style={{
                            padding: 6,
                            textAlign: "right",
                            color:
                              (row.pct_change ?? row.pct_chg ?? 0) > 0
                                ? "#e53935"
                                : (row.pct_change ?? row.pct_chg ?? 0) < 0
                                  ? "#1e88e5"
                                  : "#333",
                          }}
                        >
                          {formatNumber(row.pct_change ?? row.pct_chg ?? null, 2)}
                        </td>
                        <td style={{ padding: 6, textAlign: "right" }}>
                          {row.amount != null
                            ? `${(row.amount / 1e8).toFixed(2)}亿`
                            : "-"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p style={{ fontSize: 12, color: "#777" }}>暂无成分股数据</p>
            )}
          </div>
        </section>
      )}
    </main>
  );
}
