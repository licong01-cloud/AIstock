"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface StrategyCatalogItem {
  strategy_id: string;
  display_name?: string | null;
  scenario: string | null;
  step_name: string | null;
  action: string | null;
  workspace_example?: {
    task_run_id: string | null;
    loop_id: number | null;
    workspace_id: string | null;
    workspace_path: string | null;
  } | null;
  template_files?: Record<string, any> | null;
  data_config?: Record<string, any> | null;
  dataset_config?: Record<string, any> | null;
  portfolio_config?: Record<string, any> | null;
  backtest_config?: Record<string, any> | null;
  model_config?: Record<string, any> | null;
  feature_list?: any | null;
  market?: string | null;
  instruments?: any | null;
  freq?: string | null;
  python_implementation?: {
    module: string;
    func: string;
  } | null;
  asset_bundle_id?: string | null;
  metrics?: {
    annualized_return?: number | null;
    max_drawdown?: number | null;
    sharpe?: number | null;
    ic?: number | null;
    ic_ir?: number | null;
    win_rate?: number | null;
  } | null;
}

interface StrategyCatalogResponse {
  total: number;
  items: StrategyCatalogItem[];
}

export default function RDagentStrategyCatalogPage() {
  const [items, setItems] = useState<StrategyCatalogItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<StrategyCatalogItem | null>(null);
  const [inferenceLoading, setInferenceLoading] = useState(false);
  const [inferenceResults, setInferenceResults] = useState<any[]>([]);
  const [activeDetailTab, setActiveDetailTab] = useState<"meta" | "results">("meta");

  const [stepName, setStepName] = useState<string>("");
  const [action, setAction] = useState<string>("");
  const [strategyIdFilter, setStrategyIdFilter] = useState<string>("");
  
  // 选股中心状态
  const [selectedForInference, setSelectedForInference] = useState<string[]>([]);

  useEffect(() => {
    const sp = new URLSearchParams(window.location.search);
    const sid = sp.get("strategy_id");
    if (sid) {
      setStrategyIdFilter(sid);
    }
    loadData();
    
    // 加载已选中的策略
    const stored = localStorage.getItem("aistock_inference_strategies");
    if (stored) {
      try {
        setSelectedForInference(JSON.parse(stored));
      } catch (e) {
        console.error("加载选股中心策略失败", e);
      }
    }
  }, []);

  function toggleForInference(strategyId: string) {
    const next = selectedForInference.includes(strategyId)
      ? selectedForInference.filter(id => id !== strategyId)
      : [...selectedForInference, strategyId];
    
    setSelectedForInference(next);
    localStorage.setItem("aistock_inference_strategies", JSON.stringify(next));
  }

  async function loadData() {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("limit", "200");
      params.set("offset", "0");
      if (stepName) params.set("step_name", stepName);
      if (action) params.set("action", action);
      const res = await fetch(`${API_BASE}/rdagent/catalogs/strategies?${params.toString()}`);
      if (!res.ok) throw new Error(`加载策略目录失败: ${res.status}`);
      const data: StrategyCatalogResponse = await res.json();
      setItems(data.items || []);
      setTotal(data.total || 0);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }

  async function handleRunInference(item: StrategyCatalogItem) {
    if (!confirm(`确定要触发策略 ${item.strategy_id} 的实时推理预览吗？\n这将调用 QMT 获取最新行情并计算因子。`)) return;
    
    setInferenceLoading(true);
    try {
      const res = await fetch(`${API_BASE}/rdagent/strategies/${item.strategy_id}/inference`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          version_tag: "v1", // 默认推理 v1 版本
        }),
      });
      if (!res.ok) throw new Error("推理任务启动失败");
      const data = await res.json();
      alert(`推理完成！成功处理 ${data.symbols_count} 只标的。`);
      
      // 推理成功后，尝试加载最新信号
      await loadInferenceSignals(item.strategy_id);
      setActiveDetailTab("results");
    } catch (e: any) {
      alert(`推理失败: ${e.message}`);
    } finally {
      setInferenceLoading(false);
    }
  }

  async function loadInferenceSignals(strategyId: string) {
    try {
      // 首先获取最新版本 ID
      const vRes = await fetch(`${API_BASE}/rdagent/strategies/${strategyId}/versions`);
      const vData = await vRes.json();
      if (!vData.items || vData.items.length === 0) return;
      
      const latestVersionId = vData.items[0].strategy_version_id;
      const today = new Date().toISOString().split("T")[0];
      
      const sRes = await fetch(`${API_BASE}/rdagent/signals/by_date?strategy_version_id=${latestVersionId}&trade_date=${today}&k=50`);
      const sData = await sRes.json();
      setInferenceResults(sData.rows || []);
    } catch (e) {
      console.error("加载信号失败", e);
    }
  }

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #6366f1 0%, #ec4899 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>RD-Agent 策略目录</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          浏览从 RD-Agent 导出的策略模板 catalog（aistock_strategy_catalog）
        </p>
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          marginBottom: 16,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
          <label style={{ fontSize: 12 }}>
            strategy_id:
            <input
              value={strategyIdFilter}
              onChange={(e) => setStrategyIdFilter(e.target.value)}
              placeholder="来自已导入策略的 ID 过滤"
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 180 }}
            />
          </label>
          <label style={{ fontSize: 12 }}>
            step_name:
            <input
              value={stepName}
              onChange={(e) => setStepName(e.target.value)}
              placeholder="如 running/feedback"
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 140 }}
            />
          </label>
          <label style={{ fontSize: 12 }}>
            action:
            <input
              value={action}
              onChange={(e) => setAction(e.target.value)}
              placeholder="如 factor/model"
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 140 }}
            />
          </label>
          <button
            type="button"
            onClick={loadData}
            style={{
              padding: "6px 10px",
              borderRadius: 6,
              border: "1px solid #e5e7eb",
              background: "#f9fafb",
              fontSize: 12,
              cursor: "pointer",
            }}
          >
            重新加载
          </button>
          <span style={{ fontSize: 12, color: "#6b7280" }}>总计 {total} 条策略模板</span>
          <span style={{ marginLeft: "auto", fontSize: 12, color: "#6b7280" }}>
            选股中心已下线（当前已勾选 {selectedForInference.length} 条）
          </span>
        </div>
      </section>

      {error && (
        <div
          style={{
            padding: 12,
            background: "#fee2e2",
            border: "1px solid #fecaca",
            borderRadius: 8,
            marginBottom: 16,
            color: "#b91c1c",
            fontSize: 13,
          }}
        >
          {error}
        </div>
      )}

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 20,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        {loading ? (
          <div>加载中...</div>
        ) : items.length === 0 ? (
          <div style={{ padding: 40, textAlign: "center", color: "#6b7280" }}>
            暂无策略目录数据，请先在后台执行 catalog 导入任务。
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ background: "#f9fafb" }}>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    strategy_id
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    场景 / 步骤 / 动作
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    收益指标
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    资产包状态
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    示例 workspace
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    操作
                  </th>
                </tr>
              </thead>
              <tbody>
                {items
                  .filter((s) =>
                    strategyIdFilter
                      ? s.strategy_id.toLowerCase().includes(strategyIdFilter.toLowerCase())
                      : true,
                  )
                  .map((s) => (
                  <tr
                    key={s.strategy_id}
                    style={{
                      borderBottom: "1px solid #e5e7eb",
                      cursor: "pointer",
                      backgroundColor: strategyIdFilter &&
                        s.strategy_id.toLowerCase().includes(strategyIdFilter.toLowerCase())
                        ? "#ecfdf5"
                        : undefined,
                    }}
                    onClick={() => setSelected(s)}
                  >
                    <td style={{ padding: 8, fontSize: 11 }}>
                      <div style={{ fontWeight: 700, color: "#111827" }}>
                        {s.display_name || s.strategy_id}
                      </div>
                      <div style={{ marginTop: 2, color: "#6b7280" }}>{s.strategy_id}</div>
                    </td>
                    <td style={{ padding: 8, fontSize: 12 }}>
                      <div>{s.scenario || "-"}</div>
                      <div style={{ marginTop: 2, color: "#4b5563" }}>
                        {s.step_name || "-"} / {s.action || "-"}
                      </div>
                    </td>
                    <td style={{ padding: 8, fontSize: 11 }}>
                      {s.metrics ? (
                        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                          <div style={{ color: s.metrics.annualized_return && s.metrics.annualized_return > 0 ? "#059669" : "#dc2626" }}>
                            年化收益: {s.metrics.annualized_return != null ? `${(s.metrics.annualized_return * 100).toFixed(2)}%` : "-"}
                          </div>
                          <div>IC: {s.metrics.ic != null ? s.metrics.ic.toFixed(4) : "-"}</div>
                          <div>Sharpe: {s.metrics.sharpe != null ? s.metrics.sharpe.toFixed(2) : "-"}</div>
                        </div>
                      ) : (
                        <span style={{ color: "#9ca3af" }}>-</span>
                      )}
                    </td>
                    <td style={{ padding: 8, fontSize: 11 }}>
                      {s.asset_bundle_id ? (
                        <span style={{ 
                          padding: "2px 8px", 
                          borderRadius: 4, 
                          background: "#ecfdf5", 
                          color: "#059669", 
                          fontSize: 11, 
                          fontWeight: 500 
                        }}>
                          ✅ 已关联
                        </span>
                      ) : (
                        <span style={{ 
                          padding: "2px 8px", 
                          borderRadius: 4, 
                          background: "#fef3c7", 
                          color: "#d97706", 
                          fontSize: 11 
                        }}>
                          ⚠️ 未关联
                        </span>
                      )}
                    </td>
                    <td style={{ padding: 8, fontSize: 11, color: "#6b7280" }}>
                      <div>task_run: {s.workspace_example?.task_run_id || "-"}</div>
                      <div>loop: {s.workspace_example?.loop_id ?? "-"}</div>
                    </td>
                    <td style={{ padding: 8, fontSize: 12 }}>
                      <div style={{ display: "flex", gap: 8 }}>
                        <button
                          type="button"
                          onClick={(e) => { e.stopPropagation(); toggleForInference(s.strategy_id); }}
                          style={{
                            padding: "4px 8px",
                            borderRadius: 6,
                            border: "none",
                            background: selectedForInference.includes(s.strategy_id) ? "#ef4444" : "#10b981",
                            color: "#fff",
                            fontSize: 12,
                            cursor: "pointer",
                            minWidth: 100
                          }}
                        >
                          {selectedForInference.includes(s.strategy_id) ? "移除选股" : "添加到选股"}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {selected && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.5)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 24,
              width: "94%",
              maxWidth: 1000,
              maxHeight: "90vh",
              overflow: "auto",
              boxShadow: "0 20px 50px rgba(0,0,0,0.3)",
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                marginBottom: 20,
                borderBottom: "1px solid #f3f4f6",
                paddingBottom: 12,
              }}
            >
              <div style={{ display: "flex", gap: 16, alignItems: "baseline" }}>
                <h2 style={{ margin: 0, fontSize: 18, color: "#111827" }}>
                  策略详情: {selected.display_name || selected.strategy_id}
                </h2>
                <div style={{ fontSize: 12, color: "#6b7280" }}>{selected.strategy_id}</div>
                <div style={{ display: "flex", background: "#f3f4f6", borderRadius: 6, padding: 2 }}>
                  <button 
                    onClick={() => setActiveDetailTab("meta")}
                    style={{
                      padding: "4px 12px", borderRadius: 4, fontSize: 12, border: "none", cursor: "pointer",
                      background: activeDetailTab === "meta" ? "#fff" : "transparent",
                      boxShadow: activeDetailTab === "meta" ? "0 1px 2px rgba(0,0,0,0.1)" : "none"
                    }}
                  >配置元数据</button>
                  <button 
                    onClick={() => {
                      setActiveDetailTab("results");
                      loadInferenceSignals(selected.strategy_id);
                    }}
                    style={{
                      padding: "4px 12px", borderRadius: 4, fontSize: 12, border: "none", cursor: "pointer",
                      background: activeDetailTab === "results" ? "#fff" : "transparent",
                      boxShadow: activeDetailTab === "results" ? "0 1px 2px rgba(0,0,0,0.1)" : "none"
                    }}
                  >选股结果预览</button>
                </div>
              </div>
              <div style={{ display: "flex", gap: 8 }}>
                <button
                  onClick={() => handleRunInference(selected)}
                  disabled={inferenceLoading}
                  style={{
                    padding: "6px 16px",
                    borderRadius: 6,
                    background: "#10b981",
                    color: "#fff",
                    border: "none",
                    cursor: inferenceLoading ? "not-allowed" : "pointer",
                    fontSize: 13,
                    fontWeight: 600,
                    opacity: inferenceLoading ? 0.7 : 1
                  }}
                >
                  {inferenceLoading ? "推理中..." : "⚡ 执行实时推理"}
                </button>
                <button
                  onClick={() => setSelected(null)}
                  style={{
                    padding: "6px 12px",
                    borderRadius: 6,
                    border: "1px solid #e5e7eb",
                    background: "#fff",
                    cursor: "pointer",
                    fontSize: 13,
                  }}
                >
                  关闭
                </button>
              </div>
            </div>

            {activeDetailTab === "meta" ? (
              <>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
                  <div>
                    <div style={{ fontWeight: 600, marginBottom: 8, fontSize: 13 }}>基本元数据</div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 8, fontSize: 12, color: "#4b5563" }}>
                      <div>场景: {selected.scenario || "-"}</div>
                      <div>步骤: {selected.step_name || "-"}</div>
                      <div>动作: {selected.action || "-"}</div>
                      <div style={{ marginTop: 8 }}>
                        示例 Task Run: {selected.workspace_example?.task_run_id || "-"}
                      </div>
                      <div>示例 Loop: {selected.workspace_example?.loop_id ?? "-"}</div>
                      <div>示例 Workspace: {selected.workspace_example?.workspace_id || "-"}</div>
                      <div style={{ wordBreak: "break-all" }}>路径: {selected.workspace_example?.workspace_path || "-"}</div>
                      <div style={{ marginTop: 8, borderTop: "1px solid #f3f4f6", paddingTop: 8 }}>
                        <div>市场: {selected.market || "-"}</div>
                        <div>频率: {selected.freq || "-"}</div>
                      </div>
                      {selected.python_implementation && (
                        <div style={{ marginTop: 12, padding: 12, background: "#f0fdf4", borderRadius: 8, border: "1px solid #bbf7d0" }}>
                          <div style={{ fontSize: 12, fontWeight: 600, color: "#166534", marginBottom: 8 }}>Phase 3 Python 实现 (Python Implementation)</div>
                          <div style={{ fontSize: 11, color: "#374151" }}>模块: <span style={{ fontFamily: "monospace" }}>{selected.python_implementation.module}</span></div>
                          <div style={{ fontSize: 11, color: "#374151", marginTop: 4 }}>函数: <span style={{ fontWeight: 600, color: "#15803d", fontFamily: "monospace" }}>{selected.python_implementation.func}</span></div>
                        </div>
                      )}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontWeight: 600, marginBottom: 8, fontSize: 13 }}>配置概览 (Configs)</div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                      <div>
                        <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 4 }}>Template Files</div>
                        <pre style={{ background: "#f9fafb", padding: 8, borderRadius: 6, fontSize: 11, maxHeight: 100, overflow: "auto" }}>
                          {JSON.stringify(selected.template_files || {}, null, 2)}
                        </pre>
                      </div>
                      <div>
                        <button 
                          onClick={() => openLoopPage(selected)}
                          style={{
                            width: "100%",
                            padding: "8px",
                            borderRadius: 6,
                            background: "#6366f1",
                            color: "#fff",
                            border: "none",
                            cursor: "pointer",
                            fontSize: 13,
                            fontWeight: 500
                          }}
                        >
                          跳转至关联 Loop 列表
                        </button>
                      </div>
                    </div>
                  </div>
                </div>

                <div style={{ marginTop: 24, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
                  <div>
                    <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>Data Config</div>
                    <pre style={{ background: "#f8fafc", padding: 10, borderRadius: 8, fontSize: 11, border: "1px solid #e2e8f0", maxHeight: 200, overflow: "auto" }}>
                      {JSON.stringify(selected.data_config || {}, null, 2)}
                    </pre>
                  </div>
                  <div>
                    <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>Dataset Config</div>
                    <pre style={{ background: "#f8fafc", padding: 10, borderRadius: 8, fontSize: 11, border: "1px solid #e2e8f0", maxHeight: 200, overflow: "auto" }}>
                      {JSON.stringify(selected.dataset_config || {}, null, 2)}
                    </pre>
                  </div>
                  <div>
                    <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>Model Config</div>
                    <pre style={{ background: "#f8fafc", padding: 10, borderRadius: 8, fontSize: 11, border: "1px solid #e2e8f0", maxHeight: 200, overflow: "auto" }}>
                      {JSON.stringify(selected.model_config || {}, null, 2)}
                    </pre>
                  </div>
                  <div>
                    <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>Backtest Config</div>
                    <pre style={{ background: "#f8fafc", padding: 10, borderRadius: 8, fontSize: 11, border: "1px solid #e2e8f0", maxHeight: 200, overflow: "auto" }}>
                      {JSON.stringify(selected.backtest_config || {}, null, 2)}
                    </pre>
                  </div>
                  <div>
                    <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>Feature List</div>
                    <pre style={{ background: "#f8fafc", padding: 10, borderRadius: 8, fontSize: 11, border: "1px solid #e2e8f0", maxHeight: 200, overflow: "auto" }}>
                      {JSON.stringify(selected.feature_list || {}, null, 2)}
                    </pre>
                  </div>
                  <div>
                    <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>Instruments Pool</div>
                    <pre style={{ background: "#f8fafc", padding: 10, borderRadius: 8, fontSize: 11, border: "1px solid #e2e8f0", maxHeight: 200, overflow: "auto" }}>
                      {JSON.stringify(selected.instruments || {}, null, 2)}
                    </pre>
                  </div>
                </div>
              </>
            ) : (
              <div style={{ minHeight: 400 }}>
                {inferenceResults.length === 0 ? (
                  <div style={{ padding: 60, textAlign: "center", color: "#6b7280" }}>
                    暂无推理结果。请点击“⚡ 执行实时推理”生成最新选股分。
                  </div>
                ) : (
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse" }}>
                      <thead>
                        <tr style={{ background: "#f9fafb" }}>
                          <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>排名</th>
                          <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>股票代码</th>
                          <th style={{ padding: 12, textAlign: "right", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>选股分数</th>
                          <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>操作建议</th>
                        </tr>
                      </thead>
                      <tbody>
                        {inferenceResults.map((r, idx) => (
                          <tr key={idx} style={{ borderBottom: "1px solid #f3f4f6" }}>
                            <td style={{ padding: 12, fontSize: 13, fontWeight: 600, color: idx < 3 ? "#ef4444" : "#374151" }}>
                              #{r.rank}
                            </td>
                            <td style={{ padding: 12, fontSize: 13, color: "#111827" }}>{r.symbol}</td>
                            <td style={{ padding: 12, fontSize: 13, textAlign: "right", color: "#059669", fontFamily: "monospace" }}>
                              {r.score?.toFixed(4) || "-"}
                            </td>
                            <td style={{ padding: 12, fontSize: 12 }}>
                              <span style={{ 
                                padding: "2px 8px", borderRadius: 4, 
                                background: idx < 5 ? "#ecfdf5" : "#f3f4f6",
                                color: idx < 5 ? "#059669" : "#6b7280"
                              }}>
                                {idx < 5 ? "建议买入" : "观望"}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </main>
  );
}
