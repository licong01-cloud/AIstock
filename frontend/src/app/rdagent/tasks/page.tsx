"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type LocalTaskItem = {
  task_id: string;
  log_dir?: string | null;
  task_run_id?: string | null;
  created_at_utc?: string | null;
  updated_at_utc?: string | null;
  task_dir?: string | null;
  manifest_path?: string | null;
  manifest_sha1?: string | null;
  sync_status?: string | null;
  sync_error?: string | null;
  is_enabled_for_selection?: boolean | null;
};

type LocalTasksResp = {
  ok: boolean;
  items: LocalTaskItem[];
  limit: number;
  offset: number;
};

export default function RDAgentTasksPage() {
  const [items, setItems] = useState<LocalTaskItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadData() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/local?limit=200&offset=0`);
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `加载本地 task 列表失败: ${res.status}`);
      }
      const data = (await res.json()) as LocalTasksResp;
      setItems(data.items || []);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
  }, []);

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

  function openDetail(taskId: string) {
    window.open(`/rdagent/tasks/${encodeURIComponent(taskId)}`, "_blank");
  }

  function openSelection(taskId: string) {
    window.open(`/rdagent/task-selection?task_id=${encodeURIComponent(taskId)}`, "_blank");
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
        <h1 style={{ margin: 0, fontSize: 24 }}>Task 列表（本地）</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          来自 aistock_task_catalog：同步后可在此查看 task 资产状态，并加入 Task 选股
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
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <button
            onClick={loadData}
            disabled={loading}
            style={{ padding: "6px 10px", fontSize: 12, cursor: "pointer" }}
          >
            {loading ? "加载中..." : "刷新"}
          </button>

          <button
            onClick={() => window.open("/rdagent/tasks-sync", "_blank")}
            style={{ padding: "6px 10px", fontSize: 12, cursor: "pointer" }}
          >
            打开同步页
          </button>
        </div>

        {error && (
          <div style={{ marginTop: 10, padding: 10, background: "#fee2e2", borderRadius: 8, fontSize: 12 }}>
            {error}
          </div>
        )}
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <div style={{ fontSize: 12, opacity: 0.8, marginBottom: 8 }}>共 {items.length} 条</div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>
                <th style={{ padding: 8 }}>task_id</th>
                <th style={{ padding: 8 }}>sync_status</th>
                <th style={{ padding: 8 }}>task_run_id</th>
                <th style={{ padding: 8 }}>加入 Task 选股</th>
                <th style={{ padding: 8, width: 180 }}>操作</th>
              </tr>
            </thead>
            <tbody>
              {items.map((it) => {
                const tid = String(it.task_id || "");
                const enabled = !!it.is_enabled_for_selection;
                return (
                  <tr key={tid} style={{ borderBottom: "1px solid #f3f4f6" }}>
                    <td style={{ padding: 8, fontFamily: "monospace" }}>{tid}</td>
                    <td style={{ padding: 8 }}>{it.sync_status || "-"}</td>
                    <td style={{ padding: 8, fontFamily: "monospace" }}>{it.task_run_id || "-"}</td>
                    <td style={{ padding: 8 }}>
                      <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
                        <input
                          type="checkbox"
                          checked={enabled}
                          onChange={(e) => toggleEnable(tid, e.target.checked)}
                        />
                        <span>{enabled ? "已加入" : "未加入"}</span>
                      </label>
                    </td>
                    <td style={{ padding: 8, display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <button
                        onClick={() => openDetail(tid)}
                        style={{ padding: "4px 8px", fontSize: 12, cursor: "pointer" }}
                      >
                        详情
                      </button>
                      <button
                        onClick={() => openSelection(tid)}
                        style={{ padding: "4px 8px", fontSize: 12, cursor: "pointer" }}
                      >
                        选股
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
