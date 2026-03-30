"use client";

import { useEffect, useState, useCallback, Fragment } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface TaskItem {
  task_id: string;
  sync_status: string | null;
  is_enabled_for_selection: boolean | null;
  sota_factors_count: number | null;
  sota_models_count: number | null;
  loops_count: number | null;
  created_at_utc: string | null;
  // aggregated SOTA metrics
  sota_factors: number;
  sota_models: number;
  best_ic: number | null;
  best_sharpe: number | null;
  best_ann_return: number | null;
  best_max_drawdown: number | null;
  best_model_ic: number | null;
  best_model_sharpe: number | null;
  best_model_ann_return: number | null;
  best_model_max_drawdown: number | null;
}

interface SotaFactor {
  factor_name: string;
  source: string | null;
  ic: number | null;
  sharpe: number | null;
  annualized_return: number | null;
  max_drawdown: number | null;
  icir: number | null;
  factor_type: string | null;
  data_source: string | null;
  description_cn: string | null;
  source_loop_tag: string | null;
  code_text: string | null;
  expression: string | null;
}

interface SotaModel {
  model_name: string | null;
  model_type: string | null;
  ic: number | null;
  sharpe: number | null;
  annualized_return: number | null;
  max_drawdown: number | null;
  information_ratio: number | null;
  loop_id: number | null;
  model_grade: string | null;
  grade_reason: string | null;
  best_epoch: number | null;
  total_epochs: number | null;
  convergence_ratio: number | null;
  overfit_ratio: number | null;
  training_failed: boolean | null;
  train_loss_final: number | null;
  val_loss_final: number | null;
  hypothesis_text: string | null;
  feedback_decision: string | null;
  feedback_reason: string | null;
  code_text: string | null;
}

interface TaskDetails {
  sota_factors: SotaFactor[];
  sota_models: SotaModel[];
  task_info: any;
}

export default function RDAgentTasksPage() {
  const [items, setItems] = useState<TaskItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Sort
  type SortState = null | "asc" | "desc";
  const [sortBy, setSortBy] = useState<string>("best_ic");
  const [sortOrder, setSortOrder] = useState<SortState>("desc");

  // Expanded row detail
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [expandedTaskId, setExpandedTaskId] = useState<string | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [taskDetails, setTaskDetails] = useState<TaskDetails | null>(null);

  // Detail tab
  const [detailTab, setDetailTab] = useState<"factors" | "models" | "info">("factors");

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("limit", "200");
      params.set("offset", "0");
      if (sortBy && sortOrder) {
        params.set("sort_by", sortBy);
        params.set("sort_order", sortOrder);
      }
      const res = await fetch(`${API_BASE}/rdagent/tasks/local-with-metrics?${params.toString()}`);
      if (!res.ok) throw new Error(`加载 task 列表失败: ${res.status}`);
      const data = await res.json();
      setItems(data.items || []);
      setTotal(data.total || 0);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }, [sortBy, sortOrder]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  function handleSort(field: string) {
    if (sortBy !== field) {
      setSortBy(field);
      setSortOrder("desc");
    } else if (sortOrder === "desc") {
      setSortOrder("asc");
    } else if (sortOrder === "asc") {
      setSortBy("");
      setSortOrder(null);
    } else {
      setSortOrder("desc");
    }
  }

  function getSortIndicator(field: string): string {
    if (sortBy !== field) return "";
    if (sortOrder === "desc") return " ▼";
    if (sortOrder === "asc") return " ▲";
    return "";
  }

  async function toggleEnable(taskId: string, nextValue: boolean) {
    try {
      const url = nextValue
        ? `${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/enable_for_selection`
        : `${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/disable_for_selection`;
      const res = await fetch(url, { method: "POST" });
      const payload = await res.json().catch(() => null);
      if (!res.ok) throw new Error(payload?.detail || payload?.error || "操作失败");
      await loadData();
    } catch (e: any) {
      alert(e?.message || "操作失败");
    }
  }

  async function handleRowClick(taskId: string) {
    if (expandedTaskId === taskId) {
      setExpandedTaskId(null);
      setTaskDetails(null);
      return;
    }
    setExpandedTaskId(taskId);
    setDetailLoading(true);
    setDetailTab("factors");
    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/local/${encodeURIComponent(taskId)}/sota-details`);
      if (!res.ok) throw new Error(`加载详情失败: ${res.status}`);
      const data = await res.json();
      setTaskDetails(data);
    } catch (e: any) {
      setTaskDetails(null);
      alert(e?.message || "加载详情失败");
    } finally {
      setDetailLoading(false);
    }
  }

  const fmt = (v: number | null | undefined, digits = 4) =>
    v != null && isFinite(v) ? v.toFixed(digits) : "-";
  const fmtPct = (v: number | null | undefined) =>
    v != null && isFinite(v) ? (v * 100).toFixed(2) + "%" : "-";

  return (
    <main style={{ padding: 24 }}>
      {/* Header */}
      <section
        style={{
          background: "linear-gradient(135deg, #8b5cf6 0%, #06b6d4 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>RD-Agent Task 列表</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          来自 aistock_task_catalog · 展示每个 Task 的 SOTA 因子与模型回测聚合指标 · 点击展开查看详情
        </p>
      </section>

      {/* Toolbar */}
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
          <button
            type="button"
            onClick={loadData}
            disabled={loading}
            style={{
              padding: "6px 10px",
              borderRadius: 6,
              border: "1px solid #e5e7eb",
              background: "#f9fafb",
              fontSize: 12,
              cursor: loading ? "wait" : "pointer",
            }}
          >
            {loading ? "加载中..." : "刷新"}
          </button>
          <button
            onClick={() => window.open("/rdagent/tasks-sync", "_blank")}
            style={{ padding: "6px 10px", borderRadius: 6, border: "1px solid #e5e7eb", background: "#f9fafb", fontSize: 12, cursor: "pointer" }}
          >
            打开同步页
          </button>
          <button
            disabled={!selectedTaskId}
            onClick={() => {
              if (selectedTaskId) {
                window.location.href = `/quantevolver/evolution?source_task_id=${encodeURIComponent(selectedTaskId)}`;
              }
            }}
            style={{
              padding: "6px 14px", borderRadius: 6, border: "none",
              background: selectedTaskId ? "#2563eb" : "#94a3b8",
              color: "#fff", fontSize: 12, fontWeight: 600,
              cursor: selectedTaskId ? "pointer" : "not-allowed",
              opacity: selectedTaskId ? 1 : 0.6,
            }}
          >
            开始演进
          </button>
          <span style={{ fontSize: 12, color: "#6b7280" }}>
            总计 {total} 个 Task
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

      {/* Table */}
      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 20,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        {loading && items.length === 0 ? (
          <div>加载中...</div>
        ) : items.length === 0 ? (
          <div style={{ padding: 40, textAlign: "center", color: "#6b7280" }}>
            暂无 task 数据，请先执行 HTTP sync 导入 task 目录。
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ background: "#f9fafb" }}>
                  <th style={{ padding: 8, textAlign: "center", borderBottom: "2px solid #e5e7eb", fontSize: 12, width: 40 }}>
                    选择
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    Task ID
                  </th>
                  <th style={{ padding: 8, textAlign: "center", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    SOTA 因子
                  </th>
                  <th style={{ padding: 8, textAlign: "center", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    SOTA 模型
                  </th>
                  <th
                    onClick={() => handleSort("best_ic")}
                    style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12, cursor: "pointer", userSelect: "none" }}
                  >
                    Best IC{getSortIndicator("best_ic")}
                  </th>
                  <th
                    onClick={() => handleSort("best_sharpe")}
                    style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12, cursor: "pointer", userSelect: "none" }}
                  >
                    Best Sharpe{getSortIndicator("best_sharpe")}
                  </th>
                  <th
                    onClick={() => handleSort("best_ann_return")}
                    style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12, cursor: "pointer", userSelect: "none" }}
                  >
                    年化收益{getSortIndicator("best_ann_return")}
                  </th>
                  <th
                    onClick={() => handleSort("best_max_drawdown")}
                    style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12, cursor: "pointer", userSelect: "none" }}
                  >
                    最大回撤{getSortIndicator("best_max_drawdown")}
                  </th>
                  <th style={{ padding: 8, textAlign: "center", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    Task选股
                  </th>
                  <th style={{ padding: 8, textAlign: "center", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    状态
                  </th>
                </tr>
              </thead>
              <tbody>
                {items.map((t) => {
                  const isExpanded = expandedTaskId === t.task_id;
                  return (
                    <Fragment key={t.task_id}>
                      <tr
                        style={{
                          borderBottom: isExpanded ? "none" : "1px solid #e5e7eb",
                          cursor: "pointer",
                          background: isExpanded ? "#f0f9ff" : "#fff",
                        }}
                        onClick={() => handleRowClick(t.task_id)}
                        onMouseEnter={(e) => {
                          if (!isExpanded)
                            (e.currentTarget as HTMLElement).style.background = "#fafafa";
                        }}
                        onMouseLeave={(e) => {
                          if (!isExpanded)
                            (e.currentTarget as HTMLElement).style.background = "#fff";
                        }}
                      >
                        <td style={{ padding: 8, textAlign: "center" }} onClick={(e) => e.stopPropagation()}>
                          <input
                            type="radio"
                            name="task-radio"
                            checked={selectedTaskId === t.task_id}
                            onChange={() => setSelectedTaskId(t.task_id)}
                            style={{ cursor: "pointer" }}
                          />
                        </td>
                        <td style={{ padding: 8, fontSize: 12, fontWeight: 600, fontFamily: "monospace" }}>
                          <span style={{ marginRight: 6, fontSize: 10, color: "#94a3b8" }}>
                            {isExpanded ? "▼" : "▶"}
                          </span>
                          {t.task_id}
                        </td>
                        <td style={{ padding: 8, textAlign: "center", fontSize: 12 }}>
                          {t.sota_factors > 0 ? (
                            <span style={{ background: "#dbeafe", color: "#2563eb", padding: "2px 8px", borderRadius: 12, fontSize: 11, fontWeight: 600 }}>
                              {t.sota_factors}
                            </span>
                          ) : (
                            <span style={{ color: "#9ca3af", fontSize: 11 }}>0</span>
                          )}
                        </td>
                        <td style={{ padding: 8, textAlign: "center", fontSize: 12 }}>
                          {t.sota_models > 0 ? (
                            <span style={{ background: "#d1fae5", color: "#059669", padding: "2px 8px", borderRadius: 12, fontSize: 11, fontWeight: 600 }}>
                              {t.sota_models}
                            </span>
                          ) : (
                            <span style={{ color: "#9ca3af", fontSize: 11 }}>0</span>
                          )}
                        </td>
                        <td style={{ padding: 8, fontSize: 12, fontWeight: 600, fontFamily: "monospace", color: "#111827" }}>
                          {fmt(t.best_ic)}
                        </td>
                        <td style={{ padding: 8, fontSize: 12, fontFamily: "monospace", color: "#374151" }}>
                          {fmt(t.best_sharpe, 2)}
                        </td>
                        <td style={{ padding: 8, fontSize: 12, fontFamily: "monospace", color: (t.best_ann_return ?? 0) > 0 ? "#dc2626" : "#2563eb" }}>
                          {fmtPct(t.best_ann_return)}
                        </td>
                        <td style={{ padding: 8, fontSize: 12, fontFamily: "monospace", color: "#059669" }}>
                          {fmtPct(t.best_max_drawdown)}
                        </td>
                        <td style={{ padding: 8, textAlign: "center" }}>
                          <input
                            type="checkbox"
                            checked={!!t.is_enabled_for_selection}
                            onChange={(e) => {
                              e.stopPropagation();
                              toggleEnable(t.task_id, e.target.checked);
                            }}
                            onClick={(e) => e.stopPropagation()}
                            style={{ cursor: "pointer" }}
                          />
                        </td>
                        <td style={{ padding: 8, textAlign: "center" }}>
                          <span
                            style={{
                              fontSize: 10,
                              fontWeight: 600,
                              padding: "2px 8px",
                              borderRadius: 12,
                              background: t.sync_status === "success" ? "#dcfce7" : t.sync_status === "error" ? "#fee2e2" : "#f3f4f6",
                              color: t.sync_status === "success" ? "#166534" : t.sync_status === "error" ? "#991b1b" : "#6b7280",
                            }}
                          >
                            {t.sync_status || "-"}
                          </span>
                        </td>
                      </tr>

                      {/* Expanded detail row */}
                      {isExpanded && (
                        <tr>
                          <td colSpan={10} style={{ padding: 0, borderBottom: "1px solid #e5e7eb" }}>
                            <div style={{ padding: "16px 24px", background: "#f8fafc", borderTop: "1px solid #e2e8f0" }}>
                              {detailLoading ? (
                                <div style={{ padding: 20, textAlign: "center", color: "#6b7280", fontSize: 13 }}>加载详情中...</div>
                              ) : taskDetails ? (
                                <>
                                  {/* Tab bar */}
                                  <div style={{ display: "flex", gap: 4, marginBottom: 16 }}>
                                    {([
                                      { key: "factors" as const, label: `SOTA 因子 (${taskDetails.sota_factors.length})` },
                                      { key: "models" as const, label: `SOTA 模型 (${taskDetails.sota_models.length})` },
                                      { key: "info" as const, label: "Task 信息" },
                                    ]).map((tab) => (
                                      <button
                                        key={tab.key}
                                        onClick={(e) => { e.stopPropagation(); setDetailTab(tab.key); }}
                                        style={{
                                          padding: "6px 14px",
                                          border: "1px solid",
                                          borderRadius: 6,
                                          fontSize: 12,
                                          fontWeight: 600,
                                          cursor: "pointer",
                                          background: detailTab === tab.key ? "#2563eb" : "#fff",
                                          color: detailTab === tab.key ? "#fff" : "#374151",
                                          borderColor: detailTab === tab.key ? "#2563eb" : "#e5e7eb",
                                        }}
                                      >
                                        {tab.label}
                                      </button>
                                    ))}
                                    <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
                                      <button
                                        onClick={(e) => { e.stopPropagation(); window.open(`/rdagent/task-selection?task_id=${encodeURIComponent(t.task_id)}`, "_blank"); }}
                                        style={{ padding: "6px 10px", borderRadius: 6, border: "1px solid #2563eb", background: "#eff6ff", color: "#2563eb", fontSize: 11, fontWeight: 600, cursor: "pointer" }}
                                      >
                                        一键选股
                                      </button>
                                    </div>
                                  </div>

                                  {/* SOTA Factors table */}
                                  {detailTab === "factors" && (
                                    taskDetails.sota_factors.length === 0 ? (
                                      <div style={{ padding: 20, textAlign: "center", color: "#9ca3af", fontSize: 13 }}>无 SOTA 因子</div>
                                    ) : (
                                      <div style={{ overflowX: "auto" }}>
                                        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                                          <thead>
                                            <tr style={{ background: "#f0f9ff" }}>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #bae6fd", fontSize: 11 }}>因子名</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #bae6fd", fontSize: 11 }}>IC</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #bae6fd", fontSize: 11 }}>ICIR</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #bae6fd", fontSize: 11 }}>Sharpe</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #bae6fd", fontSize: 11 }}>年化收益</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #bae6fd", fontSize: 11 }}>最大回撤</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #bae6fd", fontSize: 11 }}>类型</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #bae6fd", fontSize: 11 }}>数据源</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #bae6fd", fontSize: 11 }}>Loop</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {taskDetails.sota_factors.map((f, i) => (
                                              <tr key={i} style={{ borderBottom: "1px solid #f1f5f9" }}>
                                                <td style={{ padding: 6, fontWeight: 600, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                                                  title={f.factor_name}
                                                >{f.factor_name}</td>
                                                <td style={{ padding: 6, fontFamily: "monospace", fontWeight: 600, color: "#111827" }}>{fmt(f.ic)}</td>
                                                <td style={{ padding: 6, fontFamily: "monospace", color: "#374151" }}>{fmt(f.icir)}</td>
                                                <td style={{ padding: 6, fontFamily: "monospace", color: "#374151" }}>{fmt(f.sharpe, 2)}</td>
                                                <td style={{ padding: 6, fontFamily: "monospace", color: (f.annualized_return ?? 0) > 0 ? "#dc2626" : "#2563eb" }}>{fmtPct(f.annualized_return)}</td>
                                                <td style={{ padding: 6, fontFamily: "monospace", color: "#059669" }}>{fmtPct(f.max_drawdown)}</td>
                                                <td style={{ padding: 6 }}>
                                                  <span style={{
                                                    background: f.factor_type === "CrossSection" ? "#dbeafe" : f.factor_type === "TimeSeries" ? "#fef3c7" : "#f3f4f6",
                                                    padding: "1px 6px", borderRadius: 4, fontSize: 10,
                                                  }}>
                                                    {f.factor_type || "-"}
                                                  </span>
                                                </td>
                                                <td style={{ padding: 6 }}>
                                                  <span style={{
                                                    background: f.data_source ? "#e0f2fe" : "#f3f4f6",
                                                    padding: "1px 6px", borderRadius: 4, fontSize: 10,
                                                  }}>
                                                    {f.data_source || "-"}
                                                  </span>
                                                </td>
                                                <td style={{ padding: 6, fontSize: 10, color: "#6b7280", fontFamily: "monospace" }}>{f.source_loop_tag || "-"}</td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    )
                                  )}

                                  {/* SOTA Models table */}
                                  {detailTab === "models" && (
                                    taskDetails.sota_models.length === 0 ? (
                                      <div style={{ padding: 20, textAlign: "center", color: "#9ca3af", fontSize: 13 }}>无 SOTA 模型</div>
                                    ) : (
                                      <div style={{ overflowX: "auto" }}>
                                        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                                          <thead>
                                            <tr style={{ background: "#ecfdf5" }}>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #a7f3d0", fontSize: 11 }}>模型名</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #a7f3d0", fontSize: 11 }}>类型</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #a7f3d0", fontSize: 11 }}>IC</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #a7f3d0", fontSize: 11 }}>Sharpe</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #a7f3d0", fontSize: 11 }}>年化收益</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #a7f3d0", fontSize: 11 }}>最大回撤</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #a7f3d0", fontSize: 11 }}>评级</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #a7f3d0", fontSize: 11 }}>训练</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #a7f3d0", fontSize: 11 }}>Loop</th>
                                              <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #a7f3d0", fontSize: 11 }}>反馈</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {taskDetails.sota_models.map((m, i) => (
                                              <tr key={i} style={{ borderBottom: "1px solid #f1f5f9" }}>
                                                <td style={{ padding: 6, fontWeight: 600, maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                                                  title={m.model_name || "-"}
                                                >{m.model_name || "-"}</td>
                                                <td style={{ padding: 6 }}>
                                                  <span style={{ background: "#ede9fe", padding: "1px 6px", borderRadius: 4, fontSize: 10, color: "#7c3aed" }}>
                                                    {m.model_type || "-"}
                                                  </span>
                                                </td>
                                                <td style={{ padding: 6, fontFamily: "monospace", fontWeight: 600, color: "#111827" }}>{fmt(m.ic)}</td>
                                                <td style={{ padding: 6, fontFamily: "monospace", color: "#374151" }}>{fmt(m.sharpe, 2)}</td>
                                                <td style={{ padding: 6, fontFamily: "monospace", color: (m.annualized_return ?? 0) > 0 ? "#dc2626" : "#2563eb" }}>{fmtPct(m.annualized_return)}</td>
                                                <td style={{ padding: 6, fontFamily: "monospace", color: "#059669" }}>{fmtPct(m.max_drawdown)}</td>
                                                <td style={{ padding: 6 }}>
                                                  {m.model_grade ? (
                                                    <span title={m.grade_reason || ""} style={{
                                                      fontSize: 10, fontWeight: 600, padding: "1px 6px", borderRadius: 4,
                                                      background: m.model_grade === "A" ? "#dcfce7" : m.model_grade === "B" ? "#fef3c7" : "#fee2e2",
                                                      color: m.model_grade === "A" ? "#166534" : m.model_grade === "B" ? "#92400e" : "#991b1b",
                                                    }}>
                                                      {m.model_grade}
                                                    </span>
                                                  ) : <span style={{ color: "#9ca3af", fontSize: 10 }}>-</span>}
                                                </td>
                                                <td style={{ padding: 6, fontSize: 10, color: "#6b7280" }}>
                                                  {m.training_failed ? (
                                                    <span style={{ color: "#dc2626" }}>失败</span>
                                                  ) : m.best_epoch != null ? (
                                                    <span>{m.best_epoch}/{m.total_epochs || "?"}</span>
                                                  ) : "-"}
                                                  {m.convergence_ratio != null && (
                                                    <div style={{ fontSize: 9, color: "#94a3b8" }}>conv: {(m.convergence_ratio * 100).toFixed(0)}%</div>
                                                  )}
                                                </td>
                                                <td style={{ padding: 6, fontSize: 10, color: "#6b7280", fontFamily: "monospace" }}>{m.loop_id ?? "-"}</td>
                                                <td style={{ padding: 6, fontSize: 10 }}>
                                                  {m.feedback_decision ? (
                                                    <span style={{
                                                      padding: "1px 6px", borderRadius: 4, fontSize: 10, fontWeight: 600,
                                                      background: m.feedback_decision === "keep" ? "#dcfce7" : "#fee2e2",
                                                      color: m.feedback_decision === "keep" ? "#166534" : "#991b1b",
                                                    }}>
                                                      {m.feedback_decision}
                                                    </span>
                                                  ) : <span style={{ color: "#9ca3af" }}>-</span>}
                                                </td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    )
                                  )}

                                  {/* Task Info */}
                                  {detailTab === "info" && taskDetails.task_info && (
                                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
                                      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                                        {([
                                          { label: "Task ID", value: taskDetails.task_info.task_id },
                                          { label: "同步状态", value: taskDetails.task_info.sync_status },
                                          { label: "Loops 数", value: taskDetails.task_info.loops_count ?? "-" },
                                          { label: "SOTA 因子数 (catalog)", value: taskDetails.task_info.sota_factors_count ?? "-" },
                                          { label: "SOTA 模型数 (catalog)", value: taskDetails.task_info.sota_models_count ?? "-" },
                                          { label: "创建时间", value: taskDetails.task_info.created_at_utc || "-" },
                                          { label: "Task 目录", value: taskDetails.task_info.task_dir || "-" },
                                        ]).map((item) => (
                                          <div key={item.label}>
                                            <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 2 }}>{item.label}</div>
                                            <div style={{ fontSize: 13, color: "#374151", fontFamily: "monospace", wordBreak: "break-all" }}>
                                              {String(item.value)}
                                            </div>
                                          </div>
                                        ))}
                                      </div>
                                      <div>
                                        <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 4 }}>SOTA 聚合指标</div>
                                        <div style={{ background: "#f0f9ff", padding: 12, borderRadius: 8, border: "1px solid #bae6fd" }}>
                                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, fontSize: 12 }}>
                                            <div><span style={{ color: "#64748b" }}>Best IC:</span> <span style={{ fontWeight: 600 }}>{fmt(t.best_ic)}</span></div>
                                            <div><span style={{ color: "#64748b" }}>Best Sharpe:</span> <span style={{ fontWeight: 600 }}>{fmt(t.best_sharpe, 2)}</span></div>
                                            <div><span style={{ color: "#64748b" }}>年化收益:</span> <span style={{ fontWeight: 600, color: (t.best_ann_return ?? 0) > 0 ? "#dc2626" : "#2563eb" }}>{fmtPct(t.best_ann_return)}</span></div>
                                            <div><span style={{ color: "#64748b" }}>最大回撤:</span> <span style={{ fontWeight: 600, color: "#059669" }}>{fmtPct(t.best_max_drawdown)}</span></div>
                                          </div>
                                          {t.best_model_ic != null && (
                                            <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px solid #bae6fd", fontSize: 11 }}>
                                              <div style={{ color: "#0369a1", fontWeight: 600, marginBottom: 4 }}>模型侧指标</div>
                                              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 4 }}>
                                                <div><span style={{ color: "#64748b" }}>IC:</span> {fmt(t.best_model_ic)}</div>
                                                <div><span style={{ color: "#64748b" }}>Sharpe:</span> {fmt(t.best_model_sharpe, 2)}</div>
                                                <div><span style={{ color: "#64748b" }}>年化:</span> {fmtPct(t.best_model_ann_return)}</div>
                                                <div><span style={{ color: "#64748b" }}>回撤:</span> {fmtPct(t.best_model_max_drawdown)}</div>
                                              </div>
                                            </div>
                                          )}
                                        </div>
                                      </div>
                                    </div>
                                  )}
                                </>
                              ) : (
                                <div style={{ padding: 20, textAlign: "center", color: "#9ca3af", fontSize: 13 }}>无数据</div>
                              )}
                            </div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}
