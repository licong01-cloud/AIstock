"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface StrategyCatalogItem {
  strategy_id: string;
  scenario: string | null;
  step_name: string | null;
  action: string | null;
  example_task_run_id: string | null;
  example_loop_id: number | null;
  example_workspace_id: string | null;
  example_workspace_path: string | null;
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

  const [stepName, setStepName] = useState<string>("");
  const [action, setAction] = useState<string>("");
  const [strategyIdFilter, setStrategyIdFilter] = useState<string>("");

  useEffect(() => {
    const sp = new URLSearchParams(window.location.search);
    const sid = sp.get("strategy_id");
    if (sid) {
      setStrategyIdFilter(sid);
    }
    loadData();
  }, []);

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

  function openLoopPage(item: StrategyCatalogItem) {
    if (!item.example_task_run_id || item.example_loop_id == null) return;
    const url = `/rdagent/loops?strategy_id=${encodeURIComponent(
      item.strategy_id,
    )}`;
    window.open(url, "_blank");
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
                      backgroundColor: strategyIdFilter &&
                        s.strategy_id.toLowerCase().includes(strategyIdFilter.toLowerCase())
                        ? "#ecfdf5"
                        : undefined,
                    }}
                  >
                    <td style={{ padding: 8, fontSize: 11 }}>{s.strategy_id}</td>
                    <td style={{ padding: 8, fontSize: 12 }}>
                      <div>{s.scenario || "-"}</div>
                      <div style={{ marginTop: 2, color: "#4b5563" }}>
                        {s.step_name || "-"} / {s.action || "-"}
                      </div>
                    </td>
                    <td style={{ padding: 8, fontSize: 11, color: "#6b7280" }}>
                      <div>task_run: {s.example_task_run_id || "-"}</div>
                      <div>loop: {s.example_loop_id ?? "-"}</div>
                      <div style={{ marginTop: 2 }}>{s.example_workspace_id || "-"}</div>
                    </td>
                    <td style={{ padding: 8, fontSize: 12 }}>
                      <button
                        type="button"
                        onClick={() => openLoopPage(s)}
                        disabled={!s.example_task_run_id || s.example_loop_id == null}
                        style={{
                          padding: "4px 8px",
                          borderRadius: 6,
                          border: "1px solid #e5e7eb",
                          background: "#f9fafb",
                          fontSize: 12,
                          cursor: !s.example_task_run_id || s.example_loop_id == null ? "not-allowed" : "pointer",
                        }}
                      >
                        查看相关 loop
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}
