"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface LoopItem {
  display_name?: string | null;
  task_run_id: string;
  loop_id: number;
  strategy_id: string | null;
  status: string | null;
  log_dir?: string | null;
  workspace_role?: string | null;
  metrics: Record<string, any> | null;
  decision: string | null;
  summary_texts?: {
    execution: string | null;
    value_feedback: string | null;
    shape_feedback: string | null;
  } | null;
  code_critic?: string[] | null;
  limitations?: string[] | null;
  materialization_status?: "pending" | "running" | "done" | "failed" | null;
  materialization_error?: string | null;
  materialization_updated_at_utc?: string | null;
  annualized_return?: number | null;
  max_drawdown?: number | null;
  sharpe?: number | null;
  ic?: number | null;
  ic_ir?: number | null;
  win_rate?: number | null;
  factor_names?: string[] | null;
  asset_bundle_id?: string | null;
  is_solidified?: boolean | null;
  sync_status?: string | null;
  paths?: {
    factor_meta: string | null;
    factor_perf: string | null;
    feedback: string | null;
    ret_curve: string | null;
    dd_curve: string | null;
    mlruns?: string | null;
    model_files?: string | null;
  };
}

interface LoopResponse {
  total: number;
  items: LoopItem[];
}

export default function RDagentLoopsPage() {
  const [items, setItems] = useState<LoopItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [artifactsLoading, setArtifactsLoading] = useState(false);
  const [artifactsError, setArtifactsError] = useState<string | null>(null);
  const [artifactsData, setArtifactsData] = useState<any>(null);

  const [strategyId, setStrategyId] = useState<string>("");
  const [status, setStatus] = useState<string>("success");
  const [stepName, setStepName] = useState<string>("");
  const [action, setAction] = useState<string>("");
  const [selectedLoop, setSelectedLoop] = useState<LoopItem | null>(null);

  const [opsRunning, setOpsRunning] = useState(false);
  const [opsMessage, setOpsMessage] = useState<string | null>(null);

  function addLoopToSelection(lp: LoopItem) {
    try {
      const key = "aistock_inference_loops";
      const stored = localStorage.getItem(key);
      let arr: any[] = [];
      if (stored) {
        try {
          arr = JSON.parse(stored) || [];
        } catch {
          arr = [];
        }
      }
      const exists = arr.some(
        (x) => x && x.task_run_id === lp.task_run_id && x.loop_id === lp.loop_id,
      );
      if (!exists) {
        arr.push({
          task_run_id: lp.task_run_id,
          loop_id: lp.loop_id,
          strategy_id: lp.strategy_id || null,
          display_name: lp.display_name || null,
        });
        localStorage.setItem(key, JSON.stringify(arr));
        setOpsMessage(
          `已加入选股中心：${lp.display_name || `${lp.task_run_id}/${lp.loop_id}`}（请到“多策略选股中心”执行选股）`,
        );
      } else {
        setOpsMessage(`该 loop 已在选股中心：${lp.display_name || `${lp.task_run_id}/${lp.loop_id}`}`);
      }
    } catch (e: any) {
      alert(`加入选股中心失败: ${e?.message || String(e)}`);
    }
  }

  useEffect(() => {
    const sp = new URLSearchParams(window.location.search);
    const sid = sp.get("strategy_id");
    const autoOpen = sp.get("auto_open") === "true";
    if (sid) setStrategyId(sid);
    loadData(sid || undefined, autoOpen);
  }, []);

  useEffect(() => {
    async function loadArtifacts() {
      if (!selectedLoop) return;
      setArtifactsLoading(true);
      setArtifactsError(null);
      setArtifactsData(null);
      try {
        const res = await fetch(
          `${API_BASE}/rdagent/catalogs/loops/${encodeURIComponent(
            selectedLoop.task_run_id
          )}/${selectedLoop.loop_id}/artifacts`
        );
        if (!res.ok) throw new Error(`加载 artifacts 失败: ${res.status}`);
        const data = await res.json();
        setArtifactsData(data);
      } catch (e: any) {
        setArtifactsError(e?.message || "加载 artifacts 失败");
      } finally {
        setArtifactsLoading(false);
      }
    }
    loadArtifacts();
  }, [selectedLoop]);

  async function loadData(strategyIdOverride?: string, autoOpenBest?: boolean) {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("limit", "200");
      params.set("offset", "0");
      const sid = strategyIdOverride ?? strategyId;
      if (sid) params.set("strategy_id", sid);
      if (status) params.set("status", status);
      if (stepName) params.set("step_name", stepName);
      if (action) params.set("action", action);
      const res = await fetch(`${API_BASE}/rdagent/catalogs/loops?${params.toString()}`);
      if (!res.ok) throw new Error(`加载 loop 目录失败: ${res.status}`);
      const data: LoopResponse = await res.json();
      const loops = data.items || [];
      setItems(loops);
      setTotal(data.total || 0);

      if (autoOpenBest && loops.length > 0) {
        setSelectedLoop(loops[0]);
      }
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }

  async function handleTriggerMaterialize() {
    if (!confirm("确定要触发 RD-Agent 侧的物化采集与全量同步吗？这可能需要几分钟时间。")) return;
    setOpsRunning(true);
    setOpsMessage(null);
    try {
      const res = await fetch(`${API_BASE}/rdagent/sync/materialize`, { method: "POST" });
      const data = await res.json();
      if (res.ok) {
        setOpsMessage("物化与同步任务已在后台启动，请稍后刷新列表查看状态。");
        setTimeout(() => loadData(), 3000);
      } else {
        throw new Error(data.error || "触发失败");
      }
    } catch (e: any) {
      alert(`操作失败: ${e.message}`);
    } finally {
      setOpsRunning(false);
    }
  }

  function shortId(id: string | null) {
    if (!id) return "-";
    return id.length > 8 ? `${id.slice(0, 8)}...` : id;
  }

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #f97316 0%, #22c55e 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>RD-Agent 实验 / loop 目录</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          浏览从 RD-Agent 导出的实验 / loop catalog（aistock_loop_catalog）
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
              value={strategyId}
              onChange={(e) => setStrategyId(e.target.value)}
              aria-label="按策略 ID 过滤"
              title="按策略 ID 过滤"
              placeholder="按策略 ID 过滤"
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 200 }}
            />
          </label>
          <label style={{ fontSize: 12 }}>
            status:
            <input
              value={status}
              onChange={(e) => setStatus(e.target.value)}
              aria-label="按状态过滤"
              title="按状态过滤"
              placeholder="如 success/failed"
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 120 }}
            />
          </label>
          <label style={{ fontSize: 12 }}>
            step_name:
            <input
              value={stepName}
              onChange={(e) => setStepName(e.target.value)}
              aria-label="按 step_name 过滤"
              title="按 step_name 过滤"
              placeholder="如 running/feedback"
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 140 }}
            />
          </label>
          <label style={{ fontSize: 12 }}>
            action:
            <input
              value={action}
              onChange={(e) => setAction(e.target.value)}
              aria-label="按 action 过滤"
              title="按 action 过滤"
              placeholder="如 factor/model"
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 140 }}
            />
          </label>
          <button
            type="button"
            onClick={() => loadData()}
            style={{
              padding: "6px 10px",
              borderRadius: 6,
              border: "1px solid #e5e7eb",
              background: "#f9fafb",
              fontSize: 12,
              cursor: loading ? "not-allowed" : "pointer",
            }}
            disabled={loading}
          >
            {loading ? "加载中..." : "重新加载"}
          </button>
          <button
            type="button"
            onClick={handleTriggerMaterialize}
            style={{
              padding: "6px 10px",
              borderRadius: 6,
              border: "1px solid #f97316",
              background: "#fff7ed",
              color: "#ea580c",
              fontSize: 12,
              cursor: opsRunning ? "not-allowed" : "pointer",
              fontWeight: 600,
            }}
            disabled={opsRunning}
          >
            {opsRunning ? "正在触发..." : "触发物化补齐"}
          </button>
          <span style={{ fontSize: 12, color: "#6b7280" }}>总计 {total} 条 loop 记录</span>
        </div>
        {opsMessage && (
          <div style={{ marginTop: 12, padding: "8px 12px", background: "#f0fdf4", color: "#166534", borderRadius: 6, fontSize: 12, border: "1px solid #bbf7d0" }}>
            {opsMessage}
          </div>
        )}
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
            暂无 loop 数据，请先在后台执行 catalog 导入任务。
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ background: "#f9fafb" }}>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    task_run / loop
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    资产包状态
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    物化状态
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    strategy_id / status
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    metrics
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    决策 & 总结（截断显示）
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    操作
                  </th>
                </tr>
              </thead>
              <tbody>
                {items.map((lp) => (
                  <tr
                    key={`${lp.task_run_id}_${lp.loop_id}_${lp.strategy_id || "_"}`}
                    style={{ borderBottom: "1px solid #e5e7eb", cursor: "pointer" }}
                    onClick={() => setSelectedLoop(lp)}
                  >
                    <td style={{ padding: 8, fontSize: 11 }}>
                      <div style={{ fontWeight: 700, color: "#111827" }}>
                        {lp.display_name || `${shortId(lp.task_run_id)}-loop${lp.loop_id}`}
                      </div>
                      <div style={{ marginTop: 2, color: "#6b7280" }}>
                        {lp.task_run_id}/{lp.loop_id}
                      </div>
                    </td>
                    <td style={{ padding: 8, fontSize: 11 }}>
                      {lp.asset_bundle_id ? (
                        <span style={{ 
                          padding: "2px 8px", 
                          borderRadius: 4, 
                          background: "#ecfdf5", 
                          color: "#059669", 
                          fontSize: 11, 
                          fontWeight: 500 
                        }}>
                          已关联
                        </span>
                      ) : (
                        <span style={{ 
                          padding: "2px 8px", 
                          borderRadius: 4, 
                          background: "#fef3c7", 
                          color: "#d97706", 
                          fontSize: 11 
                        }}>
                          未关联
                        </span>
                      )}
                    </td>
                    <td style={{ padding: 8, fontSize: 11 }}>
                      {lp.materialization_status === "done" ? (
                        <span style={{ color: "#059669", background: "#ecfdf5", padding: "2px 6px", borderRadius: 4, fontWeight: 600 }}>已完成</span>
                      ) : lp.materialization_status === "running" ? (
                        <span style={{ color: "#2563eb", background: "#eff6ff", padding: "2px 6px", borderRadius: 4, fontWeight: 600 }}>物化中...</span>
                      ) : lp.materialization_status === "failed" ? (
                        <span style={{ color: "#dc2626", background: "#fef2f2", padding: "2px 6px", borderRadius: 4, fontWeight: 600 }} title={lp.materialization_error || ""}>失败</span>
                      ) : (
                        <span style={{ color: "#6b7280", background: "#f3f4f6", padding: "2px 6px", borderRadius: 4 }}>待处理</span>
                      )}
                    </td>
                    <td style={{ padding: 8, fontSize: 11 }}>
                      <div>strategy: {shortId(lp.strategy_id)}</div>
                      <div style={{ marginTop: 2, color: "#4b5563" }}>{lp.status || "-"}</div>
                      {lp.factor_names && lp.factor_names.length > 0 && (
                        <div style={{ marginTop: 4, color: "#059669", fontSize: 10 }}>
                          因子数: {lp.factor_names.length}
                        </div>
                      )}
                    </td>
                    <td style={{ padding: 8, fontSize: 11, color: "#6b7280" }}>
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2px 8px" }}>
                        <div>AnnRet: <span style={{ color: (lp.annualized_return ?? 0) > 0 ? "#dc2626" : "#2563eb" }}>{(lp.annualized_return ?? 0).toFixed(4)}</span></div>
                        <div>MDD: <span style={{ color: "#059669" }}>{(lp.max_drawdown ?? 0).toFixed(4)}</span></div>
                        <div>Sharpe: <span>{(lp.sharpe ?? 0).toFixed(2)}</span></div>
                        <div>IC: <span>{(lp.ic ?? 0).toFixed(4)}</span></div>
                      </div>
                    </td>
                    <td style={{ padding: 8, fontSize: 11, color: "#4b5563", maxWidth: 520 }}>
                      {lp.decision !== null && (
                        <div style={{ marginBottom: 4 }}>决策: {String(lp.decision)}</div>
                      )}
                      {lp.summary_texts?.execution && (
                        <div style={{ marginBottom: 4 }}>
                          执行: {lp.summary_texts.execution.slice(0, 120)}
                          {lp.summary_texts.execution.length > 120 ? "..." : ""}
                        </div>
                      )}
                      {lp.summary_texts?.value_feedback && (
                        <div style={{ marginBottom: 4 }}>
                          收益: {lp.summary_texts.value_feedback.slice(0, 120)}
                          {lp.summary_texts.value_feedback.length > 120 ? "..." : ""}
                        </div>
                      )}
                      {lp.summary_texts?.shape_feedback && (
                        <div>
                          形态: {lp.summary_texts.shape_feedback.slice(0, 120)}
                          {lp.summary_texts.shape_feedback.length > 120 ? "..." : ""}
                        </div>
                      )}
                    </td>
                    <td style={{ padding: 8, fontSize: 12 }} onClick={(e) => e.stopPropagation()}>
                      <div style={{ display: "flex", gap: 8 }}>
                        <button
                          type="button"
                          onClick={() => addLoopToSelection(lp)}
                          style={{
                            padding: "4px 10px",
                            borderRadius: 6,
                            border: "none",
                            background: "#10b981",
                            color: "#fff",
                            fontSize: 12,
                            cursor: "pointer",
                            whiteSpace: "nowrap",
                          }}
                        >
                          加入选股中心
                        </button>
                        <button
                          type="button"
                          onClick={() => window.open("/rdagent/multi-selection", "_blank")}
                          style={{
                            padding: "4px 10px",
                            borderRadius: 6,
                            border: "1px solid #e5e7eb",
                            background: "#f9fafb",
                            color: "#111827",
                            fontSize: 12,
                            cursor: "pointer",
                            whiteSpace: "nowrap",
                          }}
                        >
                          打开选股中心
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
      {selectedLoop && (
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
              padding: 20,
              width: "96%",
              maxWidth: 1200,
              maxHeight: "90vh",
              overflow: "auto",
              boxShadow: "0 10px 40px rgba(0,0,0,0.3)",
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                marginBottom: 12,
              }}
            >
              <div>
                <div style={{ fontSize: 14, fontWeight: 600 }}>loop 详情</div>
                <div style={{ fontSize: 11, color: "#6b7280", marginTop: 4 }}>
                  task_run: {selectedLoop.task_run_id} | loop: {selectedLoop.loop_id} |
                  strategy_id: {selectedLoop.strategy_id || "-"}
                </div>
              </div>
              <button
                type="button"
                onClick={() => setSelectedLoop(null)}
                style={{
                  padding: "6px 12px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                  background: "#f9fafb",
                  cursor: "pointer",
                  fontSize: 13,
                }}
              >
                关闭
              </button>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 12, fontSize: 12 }}>
              <div>
                <div style={{ fontWeight: 600, marginBottom: 4 }}>核心指标 (KPIs)</div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12, background: "#f8fafc", padding: 12, borderRadius: 8, border: "1px solid #e2e8f0" }}>
                  <div>
                    <div style={{ color: "#64748b", fontSize: 11 }}>年化收益</div>
                    <div style={{ fontSize: 14, fontWeight: 600, color: (selectedLoop.annualized_return ?? 0) > 0 ? "#dc2626" : "#2563eb" }}>{(selectedLoop.annualized_return ?? 0).toFixed(4)}</div>
                  </div>
                  <div>
                    <div style={{ color: "#64748b", fontSize: 11 }}>最大回撤</div>
                    <div style={{ fontSize: 14, fontWeight: 600, color: "#059669" }}>{(selectedLoop.max_drawdown ?? 0).toFixed(4)}</div>
                  </div>
                  <div>
                    <div style={{ color: "#64748b", fontSize: 11 }}>夏普比率</div>
                    <div style={{ fontSize: 14, fontWeight: 600 }}>{(selectedLoop.sharpe ?? 0).toFixed(2)}</div>
                  </div>
                  <div>
                    <div style={{ color: "#64748b", fontSize: 11 }}>IC</div>
                    <div style={{ fontSize: 14, fontWeight: 600 }}>{(selectedLoop.ic ?? 0).toFixed(4)}</div>
                  </div>
                  <div>
                    <div style={{ color: "#64748b", fontSize: 11 }}>IC_IR</div>
                    <div style={{ fontSize: 14, fontWeight: 600 }}>{(selectedLoop.ic_ir ?? 0).toFixed(4)}</div>
                  </div>
                  <div>
                    <div style={{ color: "#64748b", fontSize: 11 }}>胜率</div>
                    <div style={{ fontSize: 14, fontWeight: 600 }}>{(selectedLoop.win_rate ?? 0).toFixed(4)}</div>
                  </div>
                </div>
              </div>
              {selectedLoop.factor_names && selectedLoop.factor_names.length > 0 && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>使用因子 ({selectedLoop.factor_names.length})</div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                    {selectedLoop.factor_names.map(fn => (
                      <span key={fn} style={{ padding: "2px 6px", background: "#f1f5f9", borderRadius: 4, fontSize: 10, color: "#475569", border: "1px solid #e2e8f0" }}>{fn}</span>
                    ))}
                  </div>
                </div>
              )}
              <div>
                <div style={{ fontWeight: 600, marginBottom: 4 }}>基本信息</div>
                <div style={{ color: "#4b5563" }}>
                  状态: {selectedLoop.status || "-"}
                </div>
                <div style={{ color: "#4b5563", marginTop: 4 }}>
                  物化状态: {selectedLoop.materialization_status === "done" ? (
                    <span style={{ color: "#059669", fontWeight: 600 }}>已完成</span>
                  ) : selectedLoop.materialization_status === "running" ? (
                    <span style={{ color: "#2563eb", fontWeight: 600 }}>进行中</span>
                  ) : selectedLoop.materialization_status === "failed" ? (
                    <span style={{ color: "#dc2626", fontWeight: 600 }}>失败 ({selectedLoop.materialization_error})</span>
                  ) : (
                    <span>未开始</span>
                  )}
                </div>
                {selectedLoop.materialization_status === "done" && selectedLoop.paths?.ret_curve && (
                  <div style={{ marginTop: 12 }}>
                    <div style={{ fontWeight: 600, marginBottom: 8 }}>回测收益曲线 (ret_curve.png)</div>
                    <div style={{ background: "#f8fafc", padding: 12, borderRadius: 8, border: "1px solid #e2e8f0", textAlign: "center" }}>
                      <img 
                        src={`${API_BASE}/rdagent/catalogs/loops/${encodeURIComponent(selectedLoop.task_run_id)}/${selectedLoop.loop_id}/files/${selectedLoop.paths.ret_curve}`} 
                        alt="Equity Curve" 
                        style={{ maxWidth: "100%", height: "auto", borderRadius: 4 }}
                        onError={(e) => {
                          (e.target as HTMLImageElement).style.display = 'none';
                        }}
                      />
                    </div>
                  </div>
                )}
                {selectedLoop.materialization_status === "done" && selectedLoop.paths?.dd_curve && (
                  <div style={{ marginTop: 12 }}>
                    <div style={{ fontWeight: 600, marginBottom: 8 }}>最大回撤曲线 (dd_curve.png)</div>
                    <div style={{ background: "#f8fafc", padding: 12, borderRadius: 8, border: "1px solid #e2e8f0", textAlign: "center" }}>
                      <img 
                        src={`${API_BASE}/rdagent/catalogs/loops/${encodeURIComponent(selectedLoop.task_run_id)}/${selectedLoop.loop_id}/files/${selectedLoop.paths.dd_curve}`} 
                        alt="Drawdown Curve" 
                        style={{ maxWidth: "100%", height: "auto", borderRadius: 4 }}
                        onError={(e) => {
                          (e.target as HTMLImageElement).style.display = 'none';
                        }}
                      />
                    </div>
                  </div>
                )}
                {selectedLoop.materialization_updated_at_utc && (
                  <div style={{ color: "#6b7280", marginTop: 4, fontSize: 11 }}>
                    物化更新时间: {new Date(selectedLoop.materialization_updated_at_utc).toLocaleString()}
                  </div>
                )}
                {selectedLoop.workspace_role && (
                  <div style={{ color: "#4b5563", marginTop: 4 }}>
                    workspace_role: {selectedLoop.workspace_role}
                  </div>
                )}
                {selectedLoop.log_dir && (
                  <div style={{ color: "#4b5563", marginTop: 4, wordBreak: "break-all" }}>
                    log_dir: {selectedLoop.log_dir}
                  </div>
                )}
              </div>
              <div>
                <div style={{ fontWeight: 600, marginBottom: 4 }}>metrics（原始 JSON）</div>
                <pre
                  style={{
                    background: "#f9fafb",
                    borderRadius: 8,
                    padding: 10,
                    border: "1px solid #e5e7eb",
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-all",
                  }}
                >
                  {JSON.stringify(selectedLoop.metrics ?? {}, null, 2)}
                </pre>
              </div>
              <div>
                <div style={{ fontWeight: 600, marginBottom: 4 }}>artifacts（来自 results-api registry 视图）</div>
                {artifactsLoading ? (
                  <div style={{ color: "#6b7280" }}>加载中...</div>
                ) : artifactsError ? (
                  <div style={{ color: "#b91c1c" }}>{artifactsError}</div>
                ) : (
                  <pre
                    style={{
                      background: "#f9fafb",
                      borderRadius: 8,
                      padding: 10,
                      border: "1px solid #e5e7eb",
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-all",
                    }}
                  >
                    {JSON.stringify(artifactsData ?? {}, null, 2)}
                  </pre>
                )}
              </div>
              {selectedLoop.decision !== null && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>决策</div>
                  <div style={{ whiteSpace: "pre-wrap", color: "#374151" }}>
                    {String(selectedLoop.decision)}
                  </div>
                </div>
              )}
              {selectedLoop.summary_texts?.execution && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>执行总结</div>
                  <div style={{ whiteSpace: "pre-wrap", color: "#374151" }}>
                    {selectedLoop.summary_texts.execution}
                  </div>
                </div>
              )}
              {selectedLoop.summary_texts?.value_feedback && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>收益/价值反馈</div>
                  <div style={{ whiteSpace: "pre-wrap", color: "#374151" }}>
                    {selectedLoop.summary_texts.value_feedback}
                  </div>
                </div>
              )}
              {selectedLoop.summary_texts?.shape_feedback && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>曲线形态/风险反馈</div>
                  <div style={{ whiteSpace: "pre-wrap", color: "#374151" }}>
                    {selectedLoop.summary_texts.shape_feedback}
                  </div>
                </div>
              )}
              {selectedLoop.code_critic && selectedLoop.code_critic.length > 0 && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>代码审阅反馈 (Code Critic)</div>
                  <ul style={{ paddingLeft: 20, margin: 0, color: "#374151" }}>
                    {selectedLoop.code_critic.map((item, i) => (
                      <li key={i} style={{ marginBottom: 4 }}>{item}</li>
                    ))}
                  </ul>
                </div>
              )}
              {selectedLoop.limitations && selectedLoop.limitations.length > 0 && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>局限性说明 (Limitations)</div>
                  <ul style={{ paddingLeft: 20, margin: 0, color: "#374151" }}>
                    {selectedLoop.limitations.map((item, i) => (
                      <li key={i} style={{ marginBottom: 4 }}>{item}</li>
                    ))}
                  </ul>
                </div>
              )}
              {selectedLoop.paths && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>关联路径 (Paths)</div>
                  <div style={{ background: "#f8fafc", borderRadius: 8, padding: 10, border: "1px solid #e2e8f0" }}>
                    <div style={{ display: "grid", gridTemplateColumns: "120px 1fr", gap: "8px 12px" }}>
                      <div style={{ color: "#64748b" }}>因子元数据:</div>
                      <div style={{ wordBreak: "break-all" }}>{selectedLoop.paths.factor_meta || "-"}</div>
                      
                      <div style={{ color: "#64748b" }}>因子绩效:</div>
                      <div style={{ wordBreak: "break-all" }}>{selectedLoop.paths.factor_perf || "-"}</div>
                      
                      <div style={{ color: "#64748b" }}>反馈文件:</div>
                      <div style={{ wordBreak: "break-all" }}>{selectedLoop.paths.feedback || "-"}</div>
                      
                      <div style={{ color: "#64748b" }}>收益曲线:</div>
                      <div style={{ wordBreak: "break-all" }}>{selectedLoop.paths.ret_curve || "-"}</div>
                      
                      <div style={{ color: "#64748b" }}>回撤曲线:</div>
                      <div style={{ wordBreak: "break-all" }}>{selectedLoop.paths.dd_curve || "-"}</div>

                      <div style={{ color: "#64748b" }}>mlruns:</div>
                      <div style={{ wordBreak: "break-all" }}>{selectedLoop.paths.mlruns || "-"}</div>

                      <div style={{ color: "#64748b" }}>model_files:</div>
                      <div style={{ wordBreak: "break-all" }}>{selectedLoop.paths.model_files || "-"}</div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
