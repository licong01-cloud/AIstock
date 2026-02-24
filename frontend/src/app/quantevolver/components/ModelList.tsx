"use client";

import { useEffect, useState } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export type Model = {
  model_id: string;
  model_name: string;
  model_type?: string;
  display_name?: string;
  ic?: number;
  annualized_return?: number;
  max_drawdown?: number;
  information_ratio?: number;
  is_sota?: boolean;
  task_run_id?: string;
  loop_id?: number;
  hypothesis_text?: string;
  model_architecture?: string;
  description?: string;
  generated_at_utc?: string;
};

export default function ModelList({
  mode = "display",
  selectedModel = null,
  onSelectModel,
}: {
  mode?: "display" | "selection";
  selectedModel?: string | null;
  onSelectModel?: (modelId: string) => void;
}) {
  const [models, setModels] = useState<Model[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [sotaOnly, setSotaOnly] = useState(false);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [batchLoading, setBatchLoading] = useState(false);
  const [batchResult, setBatchResult] = useState<{ total?: number; analyzed?: number; errors?: string[] } | null>(null);

  async function loadModels() {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({ limit: "100", offset: "0" });
      if (search) params.set("search", search);
      if (sotaOnly) params.set("sota_only", "true");
      const res = await fetch(${API}/quantevolver/models?);
      const data = await res.json();
      setModels(data.items || []);
      setTotal(data.total || 0);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    }
    setLoading(false);
  }

  useEffect(() => { loadModels(); }, [search, sotaOnly]);

  const isSelectionMode = mode === "selection";

  return (
    <div style={!isSelectionMode ? { padding: 24 } : {}}>
      {!isSelectionMode && (
        <section
          style={{
            background: "linear-gradient(135deg, #06b6d4 0%, #2563eb 100%)",
            borderRadius: 16, padding: 20, color: "#fff", marginBottom: 16,
          }}
        >
          <h1 style={{ margin: 0, fontSize: 24 }}>模型库</h1>
          <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
            浏览RDAgent训练的模型，查看性能指标、假设和架构信息
          </p>
        </section>
      )}

      {/* 筛选栏 */}
      <section style={{ background: "#fff", borderRadius: 12, padding: 16, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <input
            placeholder="搜索模型名称/类型..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            onKeyDown={e => e.key === "Enter" && loadModels()}
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", width: 200 }}
          />
          <label style={{ display: "flex", gap: 4, alignItems: "center", fontSize: 12 }}>
            <input type="checkbox" checked={sotaOnly} onChange={e => setSotaOnly(e.target.checked)} />
            仅SOTA
          </label>
          <button onClick={loadModels} disabled={loading} style={{ padding: "6px 12px", fontSize: 12, cursor: "pointer", borderRadius: 6, border: "1px solid #d1d5db", background: "#fff" }}>
            {loading ? "加载中..." : "刷新"}
          </button>
          
          {!isSelectionMode && (
            <>
              <button
                onClick={async () => {
                  if (!confirm("确定要批量分析所有模型吗？")) return;
                  setBatchLoading(true); setBatchResult(null);
                  try {
                    const res = await fetch(\\/quantevolver/model-analyst/batch-analyze\, {
                      method: "POST", headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({ use_llm: false }),
                    });
                    const data = await res.json();
                    setBatchResult(data);
                    if (data.analyzed > 0) loadModels();
                  } catch (e: any) { alert("批量分析失败: " + (e?.message || "")); }
                  setBatchLoading(false);
                }}
                disabled={batchLoading}
                style={{
                  padding: "6px 14px", fontSize: 12, cursor: batchLoading ? "not-allowed" : "pointer",
                  borderRadius: 6, border: "none", background: "#7c3aed", color: "#fff", fontWeight: 600,
                  opacity: batchLoading ? 0.6 : 1,
                }}
              >
                {batchLoading ? "分析中..." : "批量分析模型"}
              </button>
              <span style={{ fontSize: 12, color: "#9ca3af" }}>共 {total} 条</span>
            </>
          )}
        </div>
        {error && <div style={{ marginTop: 8, padding: 8, background: "#fee2e2", borderRadius: 6, fontSize: 12 }}>{error}</div>}
        {batchResult && (
          <div style={{
            marginTop: 8, padding: 10, borderRadius: 6, fontSize: 12,
            background: (batchResult.errors?.length || 0) > 0 ? "#fef3c7" : "#d1fae5",
            color: (batchResult.errors?.length || 0) > 0 ? "#92400e" : "#065f46",
          }}>
            <strong>批量分析完成：</strong>
            共 {batchResult.total} 个模型，成功分析 {batchResult.analyzed} 个
            {(batchResult.errors?.length || 0) > 0 && (
              <span>，{batchResult.errors!.length} 个失败</span>
            )}
          </div>
        )}
      </section>

      {/* 模型卡片列表 */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(360px, 1fr))", gap: 16 }}>
        {models.map(m => {
          const isSelected = selectedModel === m.model_id;
          return (
            <div
              key={m.model_id}
              style={{
                background: isSelected && isSelectionMode ? "#eff6ff" : "#fff", 
                borderRadius: 12, padding: 16,
                boxShadow: isSelected && isSelectionMode ? "0 0 0 2px #3b82f6" : "0 1px 3px rgba(0,0,0,0.08)",
                borderLeft: m.is_sota ? "4px solid #f59e0b" : "4px solid #e5e7eb",
                cursor: "pointer",
                transition: "all 0.2s ease"
              }}
              onClick={() => {
                if (isSelectionMode && onSelectModel) {
                  onSelectModel(m.model_id);
                } else {
                  setExpandedId(expandedId === m.model_id ? null : m.model_id);
                }
              }}
            >
              <div style={{ display: "flex", justify-content: "space-between", alignItems: "flex-start" }}>
                <div style={{ display: "flex", gap: "10px", alignItems: "center" }}>
                  {isSelectionMode && (
                    <input 
                      type="radio" 
                      checked={isSelected} 
                      onChange={() => onSelectModel && onSelectModel(m.model_id)}
                      onClick={(e) => e.stopPropagation()}
                      style={{ transform: "scale(1.2)", cursor: "pointer", accentColor: "#3b82f6" }}
                    />
                  )}
                  <div>
                    <div style={{ fontWeight: 700, fontSize: 14, color: isSelected && isSelectionMode ? "#1e40af" : "#111827" }}>
                      {m.display_name || m.model_name || m.model_id}
                      {m.is_sota && <span style={{ marginLeft: 8, fontSize: 11, background: "#fef3c7", color: "#92400e", padding: "1px 6px", borderRadius: 4 }}> SOTA</span>}
                    </div>
                    <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2 }}>
                      {m.model_type || "未知类型"}  Loop #{m.loop_id ?? "-"}
                    </div>
                  </div>
                </div>
                {isSelectionMode && (
                  <span
                    onClick={(e) => {
                      e.stopPropagation();
                      setExpandedId(expandedId === m.model_id ? null : m.model_id);
                    }}
                    style={{
                      color: "#3b82f6", cursor: "pointer", fontSize: 12, borderBottom: "1px dashed #3b82f6", userSelect: "none"
                    }}
                  >
                    {expandedId === m.model_id ? "收起" : "详情"}
                  </span>
                )}
              </div>

              {/* 模型描述 */}
              {m.description && (
                <div style={{ marginTop: 8, fontSize: 11, color: "#6b7280", lineHeight: 1.5, background: "#f9fafb", padding: "6px 10px", borderRadius: 6 }}>
                  {m.description}
                </div>
              )}

              {/* 指标 */}
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8, marginTop: 12 }}>
                <MetricBox label="IC" value={m.ic} fmt={v => v.toFixed(4)} color="#2563eb" />
                <MetricBox label="年化收益" value={m.annualized_return} fmt={v => (v * 100).toFixed(2) + "%"} color="#10b981" />
                <MetricBox label="最大回撤" value={m.max_drawdown} fmt={v => (v * 100).toFixed(2) + "%"} color="#ef4444" />
                <MetricBox label="IR" value={m.information_ratio} fmt={v => v.toFixed(3)} color="#8b5cf6" />
              </div>

              {/* 展开详情 */}
              {(expandedId === m.model_id || (!isSelectionMode && expandedId === m.model_id)) && (
                <div style={{ marginTop: 12, paddingTop: 12, borderTop: "1px solid #f3f4f6", fontSize: 12 }}>
                  {m.hypothesis_text && (
                    <div style={{ marginBottom: 8 }}>
                      <strong>假设：</strong>
                      <div style={{ color: "#6b7280", marginTop: 2, whiteSpace: "pre-wrap" }}>{m.hypothesis_text}</div>
                    </div>
                  )}
                  {m.model_architecture && (
                    <div style={{ marginBottom: 8 }}>
                      <strong>架构：</strong>
                      <div style={{ color: "#6b7280", marginTop: 2 }}>{m.model_architecture}</div>
                    </div>
                  )}
                  <div style={{ color: "#9ca3af", fontSize: 11 }}>
                    Model ID: {m.model_id}  Task: {m.task_run_id || "-"}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {!loading && models.length === 0 && (
        <div style={{ textAlign: "center", padding: 60, color: "#9ca3af", background: "#fff", borderRadius: 12 }}>
          暂无模型数据。请先同步RDAgent任务或等待任务完成。
        </div>
      )}
    </div>
  );
}

function MetricBox({ label, value, fmt, color }: {
  label: string; value?: number | null; fmt: (v: number) => string; color: string;
}) {
  return (
    <div style={{ textAlign: "center" }}>
      <div style={{ fontSize: 15, fontWeight: 700, color: value != null ? color : "#d1d5db" }}>
        {value != null ? fmt(value) : "-"}
      </div>
      <div style={{ fontSize: 10, color: "#9ca3af" }}>{label}</div>
    </div>
  );
}
