"use client";

import { useEffect, useMemo, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type LocalTaskDetailResp = {
  ok: boolean;
  task?: any;
  error?: string;
};

type LocalManifestResp = {
  ok: boolean;
  task_id?: string;
  manifest_path?: string;
  content?: string;
  error?: string;
};

export default function RDAgentTaskDetailPage({ params }: { params: { taskId: string } }) {
  const taskId = decodeURIComponent(String(params?.taskId || "")).trim();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [task, setTask] = useState<any>(null);
  const [manifest, setManifest] = useState<LocalManifestResp | null>(null);

  const manifestObj = useMemo(() => {
    try {
      if (!manifest?.content) return null;
      return JSON.parse(manifest.content);
    } catch {
      return null;
    }
  }, [manifest]);

  async function loadAll() {
    setLoading(true);
    setError(null);
    try {
      const [tRes, mRes] = await Promise.all([
        fetch(`${API_BASE}/rdagent/tasks/local/${encodeURIComponent(taskId)}`),
        fetch(`${API_BASE}/rdagent/tasks/local/${encodeURIComponent(taskId)}/manifest`),
      ]);

      const tPayload = (await tRes.json().catch(() => null)) as LocalTaskDetailResp | null;
      const mPayload = (await mRes.json().catch(() => null)) as LocalManifestResp | null;

      if (!tRes.ok) throw new Error((tPayload as any)?.detail || tPayload?.error || `加载 task 失败: ${tRes.status}`);
      if (!mRes.ok) throw new Error((mPayload as any)?.detail || mPayload?.error || `加载 manifest 失败: ${mRes.status}`);

      setTask((tPayload as any)?.task || null);
      setManifest(mPayload || null);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskId]);

  async function toggleEnable(nextValue: boolean) {
    try {
      const url = nextValue
        ? `${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/enable_for_selection`
        : `${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/disable_for_selection`;
      const res = await fetch(url, { method: "POST" });
      const payload = await res.json().catch(() => null);
      if (!res.ok) throw new Error(payload?.detail || payload?.error || "操作失败");
      await loadAll();
    } catch (e: any) {
      alert(e?.message || "操作失败");
    }
  }

  function openSelection() {
    window.open(`/rdagent/task-selection?task_id=${encodeURIComponent(taskId)}`, "_blank");
  }

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #111827 0%, #4f46e5 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>Task 详情</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13, fontFamily: "monospace" }}>{taskId}</p>
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
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
          <button
            onClick={loadAll}
            disabled={loading}
            style={{ padding: "6px 10px", fontSize: 12, cursor: "pointer" }}
          >
            {loading ? "刷新中..." : "刷新"}
          </button>

          <button
            onClick={openSelection}
            style={{ padding: "6px 10px", fontSize: 12, cursor: "pointer" }}
          >
            打开 Task 选股
          </button>

          <label style={{ display: "flex", gap: 6, alignItems: "center", fontSize: 12 }}>
            <input
              type="checkbox"
              checked={!!task?.is_enabled_for_selection}
              onChange={(e) => toggleEnable(e.target.checked)}
            />
            <span>{task?.is_enabled_for_selection ? "已加入 Task 选股" : "未加入 Task 选股"}</span>
          </label>
        </div>

        {error && (
          <div style={{ marginTop: 10, padding: 10, background: "#fee2e2", borderRadius: 8, fontSize: 12 }}>
            {error}
          </div>
        )}
      </section>

      <section
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 16,
          marginBottom: 16,
        }}
      >
        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 8 }}>本地记录（aistock_task_catalog）</div>
          <pre style={{ margin: 0, fontSize: 12, whiteSpace: "pre-wrap" }}>{JSON.stringify(task, null, 2)}</pre>
        </div>

        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 8 }}>manifest.json（AIstock 同步生成）</div>
          <div style={{ fontSize: 12, opacity: 0.8, marginBottom: 8, fontFamily: "monospace" }}>
            {manifest?.manifest_path || "-"}
          </div>
          <pre style={{ margin: 0, fontSize: 12, whiteSpace: "pre-wrap" }}>{manifest?.content || ""}</pre>
        </div>
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <div style={{ fontWeight: 600, marginBottom: 8 }}>manifest 推导信息（辅助排查）</div>
        <pre style={{ margin: 0, fontSize: 12, whiteSpace: "pre-wrap" }}>{JSON.stringify(manifestObj, null, 2)}</pre>
      </section>
    </main>
  );
}
