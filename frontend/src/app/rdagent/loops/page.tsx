"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface LoopItem {
  task_run_id: string;
  loop_id: number;
  strategy_id: string | null;
  status: string | null;
  metrics: Record<string, any> | null;
  decision: string | null;
  summary_execution: string | null;
  summary_value_feedback: string | null;
  summary_shape_feedback: string | null;
  paths?: {
    factor_meta: string | null;
    factor_perf: string | null;
    feedback: string | null;
    ret_curve: string | null;
    dd_curve: string | null;
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

  const [strategyId, setStrategyId] = useState<string>("");
  const [status, setStatus] = useState<string>("success");
  const [stepName, setStepName] = useState<string>("");
  const [action, setAction] = useState<string>("");
  const [selectedLoop, setSelectedLoop] = useState<LoopItem | null>(null);

  useEffect(() => {
    // 若 URL 上有 ?strategy_id=xxx, 优先用作默认过滤
    const sp = new URLSearchParams(window.location.search);
    const sid = sp.get("strategy_id");
    const autoOpen = sp.get("auto_open") === "true";
    if (sid) setStrategyId(sid);
    loadData(sid || undefined, autoOpen);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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

      // 如果带了 auto_open，尝试自动打开第一个记录（通常是聚合出来的最佳或最新记录）
      if (autoOpenBest && loops.length > 0) {
        setSelectedLoop(loops[0]);
      }
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
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
              placeholder="按策略 ID 过滤"
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 200 }}
            />
          </label>
          <label style={{ fontSize: 12 }}>
            status:
            <input
              value={status}
              onChange={(e) => setStatus(e.target.value)}
              placeholder="如 success/failed"
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 120 }}
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
            onClick={() => loadData()}
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
          <span style={{ fontSize: 12, color: "#6b7280" }}>总计 {total} 条 loop 记录</span>
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
                    strategy_id / status
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    metrics
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    决策 & 总结（截断显示）
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
                      <div>task_run: {shortId(lp.task_run_id)}</div>
                      <div>loop: {lp.loop_id}</div>
                    </td>
                    <td style={{ padding: 8, fontSize: 11 }}>
                      <div>strategy: {shortId(lp.strategy_id)}</div>
                      <div style={{ marginTop: 2, color: "#4b5563" }}>{lp.status || "-"}</div>
                    </td>
                    <td style={{ padding: 8, fontSize: 11, color: "#6b7280" }}>
                      {(() => {
                        const m = lp.metrics || {};
                        const keys = Object.keys(m);
                        if (!keys.length) return "-";
                        return keys
                          .slice(0, 4)
                          .map((k) => `${k}: ${String(m[k]).slice(0, 10)}`)
                          .join("; ");
                      })()}
                    </td>
                    <td style={{ padding: 8, fontSize: 11, color: "#4b5563", maxWidth: 520 }}>
                      {lp.decision !== null && (
                        <div style={{ marginBottom: 4 }}>决策: {String(lp.decision)}</div>
                      )}
                      {lp.summary_execution && (
                        <div style={{ marginBottom: 4 }}>
                          执行: {lp.summary_execution.slice(0, 120)}
                          {lp.summary_execution.length > 120 ? "..." : ""}
                        </div>
                      )}
                      {lp.summary_value_feedback && (
                        <div style={{ marginBottom: 4 }}>
                          收益: {lp.summary_value_feedback.slice(0, 120)}
                          {lp.summary_value_feedback.length > 120 ? "..." : ""}
                        </div>
                      )}
                      {lp.summary_shape_feedback && (
                        <div>
                          形态: {lp.summary_shape_feedback.slice(0, 120)}
                          {lp.summary_shape_feedback.length > 120 ? "..." : ""}
                        </div>
                      )}
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
                <div style={{ fontWeight: 600, marginBottom: 4 }}>基本信息</div>
                <div style={{ color: "#4b5563" }}>
                  状态: {selectedLoop.status || "-"}
                </div>
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
              {selectedLoop.decision !== null && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>决策</div>
                  <div style={{ whiteSpace: "pre-wrap", color: "#374151" }}>
                    {String(selectedLoop.decision)}
                  </div>
                </div>
              )}
              {selectedLoop.summary_execution && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>执行总结</div>
                  <div style={{ whiteSpace: "pre-wrap", color: "#374151" }}>
                    {selectedLoop.summary_execution}
                  </div>
                </div>
              )}
              {selectedLoop.summary_value_feedback && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>收益/价值反馈</div>
                  <div style={{ whiteSpace: "pre-wrap", color: "#374151" }}>
                    {selectedLoop.summary_value_feedback}
                  </div>
                </div>
              )}
              {selectedLoop.summary_shape_feedback && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>曲线形态/风险反馈</div>
                  <div style={{ whiteSpace: "pre-wrap", color: "#374151" }}>
                    {selectedLoop.summary_shape_feedback}
                  </div>
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
