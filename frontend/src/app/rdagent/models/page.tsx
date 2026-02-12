"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface ModelItem {
  task_run_id: string;
  loop_id: number;
  workspace_id: string;
  workspace_path: string | null;
  log_dir?: string | null;
  model_id?: string | null;
  model_type?: string | null;
  model_config: Record<string, any> | null;
  dataset_config: Record<string, any> | null;
  feature_schema: Record<string, any> | null;
  flattened_feature_list?: string[] | null;
  model_artifacts: Record<string, any> | null;
}

interface ModelResponse {
  total: number;
  items: ModelItem[];
}

export default function RDagentModelsPage() {
  const [items, setItems] = useState<ModelItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [taskRunId, setTaskRunId] = useState<string>("");
  const [workspaceId, setWorkspaceId] = useState<string>("");
  const [selected, setSelected] = useState<ModelItem | null>(null);

  useEffect(() => {
    loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function loadData() {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("limit", "200");
      params.set("offset", "0");
      if (taskRunId) params.set("task_run_id", taskRunId);
      if (workspaceId) params.set("workspace_id", workspaceId);

      const res = await fetch(`${API_BASE}/rdagent/catalogs/models?${params.toString()}`);
      if (!res.ok) throw new Error(`加载 model 目录失败: ${res.status}`);
      const data: ModelResponse = await res.json();
      setItems(data.items || []);
      setTotal(data.total || 0);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #8b5cf6 0%, #06b6d4 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>RD-Agent Model 目录</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          浏览从 RD-Agent 导出的 model catalog（aistock_model_catalog）
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
            task_run_id:
            <input
              value={taskRunId}
              onChange={(e) => setTaskRunId(e.target.value)}
              placeholder="如 71a7..."
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 260 }}
            />
          </label>
          <label style={{ fontSize: 12 }}>
            workspace_id:
            <input
              value={workspaceId}
              onChange={(e) => setWorkspaceId(e.target.value)}
              placeholder="如 xxx"
              style={{ marginLeft: 4, padding: 4, fontSize: 12, minWidth: 240 }}
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
          <span style={{ fontSize: 12, color: "#6b7280" }}>总计 {total} 个模型条目</span>
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
            暂无 model 数据，请先执行 HTTP sync 或导入模型目录。
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ background: "#f9fafb" }}>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    task_run_id
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    loop_id
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    workspace_id
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    workspace_path
                  </th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #e5e7eb", fontSize: 12 }}>
                    model_type
                  </th>
                </tr>
              </thead>
              <tbody>
                {items.map((m) => (
                  <tr
                    key={`${m.task_run_id}-${m.workspace_id}-${m.loop_id}`}
                    style={{ borderBottom: "1px solid #e5e7eb", cursor: "pointer" }}
                    onClick={() => setSelected(m)}
                  >
                    <td style={{ padding: 8, fontSize: 11, color: "#111827", fontWeight: 600, wordBreak: "break-all" }}>
                      {m.task_run_id}
                    </td>
                    <td style={{ padding: 8, fontSize: 12, color: "#374151" }}>{m.loop_id}</td>
                    <td style={{ padding: 8, fontSize: 11, color: "#374151", wordBreak: "break-all" }}>{m.workspace_id}</td>
                    <td style={{ padding: 8, fontSize: 11, color: "#6b7280", wordBreak: "break-all" }}>
                      {m.workspace_path || "-"}
                    </td>
                    <td style={{ padding: 8, fontSize: 11, color: "#4b5563" }}>
                      {m.model_type || "-"}
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
                <div style={{ fontSize: 14, fontWeight: 600 }}>模型详情</div>
                <div style={{ fontSize: 11, color: "#6b7280", marginTop: 4 }}>
                  task_run: {selected.task_run_id} | loop: {selected.loop_id} | workspace_id: {selected.workspace_id}
                </div>
              </div>
              <button
                type="button"
                onClick={() => setSelected(null)}
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
                <div style={{ color: "#4b5563" }}>model_id: {selected.model_id || "-"}</div>
                <div style={{ color: "#4b5563", marginTop: 4 }}>model_type: {selected.model_type || "-"}</div>
                {selected.workspace_path && (
                  <div style={{ color: "#4b5563", marginTop: 4, wordBreak: "break-all" }}>
                    workspace_path: {selected.workspace_path}
                  </div>
                )}
                {selected.log_dir && (
                  <div style={{ color: "#4b5563", marginTop: 4, wordBreak: "break-all" }}>
                    log_dir: {selected.log_dir}
                  </div>
                )}
              </div>
              {selected.flattened_feature_list && selected.flattened_feature_list.length > 0 && (
                <div>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>特征列表 (Features - {selected.flattened_feature_list.length})</div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 4, maxHeight: 150, overflow: "auto", padding: 8, background: "#f8fafc", borderRadius: 8, border: "1px solid #e2e8f0" }}>
                    {selected.flattened_feature_list.map((f, i) => (
                      <span key={i} style={{ padding: "2px 6px", background: "#f1f5f9", borderRadius: 4, fontSize: 10, color: "#475569", border: "1px solid #e2e8f0" }}>{f}</span>
                    ))}
                  </div>
                </div>
              )}
              <div>
                <div style={{ fontWeight: 600, marginBottom: 4 }}>model_config</div>
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
                  {JSON.stringify(selected.model_config ?? {}, null, 2)}
                </pre>
              </div>
              <div>
                <div style={{ fontWeight: 600, marginBottom: 4 }}>dataset_config</div>
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
                  {JSON.stringify(selected.dataset_config ?? {}, null, 2)}
                </pre>
              </div>
              <div>
                <div style={{ fontWeight: 600, marginBottom: 4 }}>feature_schema</div>
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
                  {JSON.stringify(selected.feature_schema ?? {}, null, 2)}
                </pre>
              </div>
              <div>
                <div style={{ fontWeight: 600, marginBottom: 4 }}>model_artifacts</div>
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
                  {JSON.stringify(selected.model_artifacts ?? {}, null, 2)}
                </pre>
              </div>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
