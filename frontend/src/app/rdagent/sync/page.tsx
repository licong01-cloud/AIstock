"use client";

import { useCallback, useEffect, useRef, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type SyncState = "idle" | "running" | "success" | "failed";

interface ImportSummaryItem {
  inserted?: number;
  updated?: number;
  skipped?: number;
  errors?: number;
  [key: string]: any;
}

interface SyncStatusPayload {
  state: SyncState;
  phase?: string;
  progress?: number;
  started_at_ts: number | null;
  finished_at_ts: number | null;
  last_error: string | null;
  last_result: Record<string, ImportSummaryItem> | null;
}

function formatTs(ts: number | null): string {
  if (!ts) return "-";
  const d = new Date(ts * 1000);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleString();
}

function formatDuration(start: number | null, end: number | null): string {
  if (!start || !end) return "-";
  const ms = (end - start) * 1000;
  if (!Number.isFinite(ms) || ms <= 0) return "-";
  const sec = Math.round(ms / 100) / 10;
  return `${sec}s`;
}

const SECTION_LABELS: Record<string, string> = {
  factor: "因子目录 (factors)",
  strategy: "策略目录 (strategies)",
  loop: "实验 / loop 目录 (loops)",
  model: "模型目录 (models)",
  alpha158: "Alpha158 因子库",
  alpha360: "Alpha360 因子库",
};

export default function RDAgentSyncPage() {
  const [status, setStatus] = useState<SyncStatusPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [polling, setPolling] = useState(false);
  const [clean, setClean] = useState(false);
  const [syncMode, setSyncMode] = useState<"upsert" | "full_refresh" | "reconcile" | "materialize_and_sync" | "incremental">("upsert");
  const [syncMetadataOnly, setSyncMetadataOnly] = useState(false);
  const [syncAssetsOnly, setSyncAssetsOnly] = useState(false);

  const pollTimerRef = useRef<NodeJS.Timeout | null>(null);

  const stopPolling = useCallback(() => {
    if (pollTimerRef.current) {
      clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
    }
    setPolling(false);
  }, []);

  const fetchStatus = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/rdagent/sync/status`);
      if (!res.ok) throw new Error(`获取同步状态失败: ${res.status}`);
      const data = (await res.json()) as SyncStatusPayload;
      setStatus(data);
      if (data.state === "success" || data.state === "failed" || data.state === "idle") {
        stopPolling();
      }
    } catch (e: any) {
      setError(e?.message || "获取同步状态失败");
      stopPolling();
    }
  }, [stopPolling]);

  useEffect(() => {
    fetchStatus();
    return () => {
      if (pollTimerRef.current) {
        clearInterval(pollTimerRef.current);
      }
    };
  }, [fetchStatus]);

  async function triggerSync(modeOverride?: "upsert" | "full_refresh" | "reconcile" | "materialize_and_sync" | "incremental") {
    setLoading(true);
    setError(null);
    try {
      const finalMode = modeOverride || syncMode;
      const body = { 
        mode: finalMode, 
        force: false, 
        clean: modeOverride ? false : clean,
        sync_metadata_only: modeOverride ? false : syncMetadataOnly,
        sync_assets_only: modeOverride ? false : syncAssetsOnly
      };
      const res = await fetch(`${API_BASE}/rdagent/sync/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `触发同步失败: ${res.status}`);
      }
      const data = (await res.json()) as SyncStatusPayload;
      setStatus(data);
      setPolling(true);
      pollTimerRef.current = setInterval(fetchStatus, 1500);
    } catch (e: any) {
      console.error("触发同步失败", e);
      setError(e?.message || JSON.stringify(e) || "触发同步失败");
    } finally {
      setLoading(false);
    }
  }

  const state = status?.state ?? "idle";
  const phase = status?.phase ?? "-";
  const progress = typeof status?.progress === "number" ? status!.progress : null;
  const lastResult = status?.last_result ?? null;

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>RD-Agent Catalog 同步管理</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          从此页面可以手动触发 RD-Agent Catalog 的 HTTP 同步（因子 / 策略 / loop /
          模型）。同步完成后将在下方展示本次各目录的插入 / 更新数量汇总。
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
        <h3 style={{ marginTop: 0, marginBottom: 12, fontSize: 16 }}>同步选项</h3>
        
        <div
          style={{
            display: "flex",
            gap: 12,
            flexWrap: "wrap",
            alignItems: "center",
            marginBottom: 12,
            padding: "8px 12px",
            background: "#f0f9ff",
            borderRadius: 8,
            border: "1px solid #bae6fd",
          }}
        >
          <label style={{ fontSize: 13, color: "#0369a1", fontWeight: 500 }}>同步模式：</label>
          <select
            value={syncMode}
            onChange={(e) => setSyncMode(e.target.value as any)}
            disabled={loading || state === "running"}
            aria-label="选择同步模式"
            title="选择同步模式"
            style={{
              padding: "6px 10px",
              borderRadius: 6,
              border: "1px solid #bae6fd",
              background: "#fff",
              fontSize: 13,
              cursor: loading || state === "running" ? "not-allowed" : "pointer",
            }}
          >
            <option value="upsert">增量更新 (upsert)</option>
            <option value="full_refresh">全量刷新 (full_refresh)</option>
            <option value="reconcile">对齐清理 (reconcile)</option>
            <option value="materialize_and_sync">物化并同步 (materialize_and_sync)</option>
            <option value="incremental">增量同步 (incremental)</option>
          </select>
        </div>

        <div
          style={{
            display: "flex",
            gap: 12,
            flexWrap: "wrap",
            alignItems: "center",
            marginBottom: 12,
            padding: "8px 12px",
            background: "#fff7ed",
            borderRadius: 8,
            border: "1px solid #ffedd5",
          }}
        >
          <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer", fontSize: 13, color: "#9a3412" }}>
            <input
              type="checkbox"
              checked={clean}
              onChange={(e) => setClean(e.target.checked)}
              disabled={loading || state === "running"}
              style={{ width: 16, height: 16 }}
            />
            同步前清空现有数据 (全量重装)
          </label>
          <span style={{ fontSize: 11, color: "#c2410c" }}>
            * 勾选后将执行 TRUNCATE 操作，删除本地所有 Catalog 记录后重新从 RD-Agent 导入。
          </span>
        </div>

        <div
          style={{
            display: "flex",
            gap: 12,
            flexWrap: "wrap",
            alignItems: "center",
            marginBottom: 12,
            padding: "8px 12px",
            background: "#f0fdf4",
            borderRadius: 8,
            border: "1px solid #bbf7d0",
          }}
        >
          <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer", fontSize: 13, color: "#166534" }}>
            <input
              type="checkbox"
              checked={syncMetadataOnly}
              onChange={(e) => {
                setSyncMetadataOnly(e.target.checked);
                if (e.target.checked) setSyncAssetsOnly(false);
              }}
              disabled={loading || state === "running"}
              style={{ width: 16, height: 16 }}
            />
            仅同步结构化数据（跳过资产包下载）
          </label>
          <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer", fontSize: 13, color: "#166534" }}>
            <input
              type="checkbox"
              checked={syncAssetsOnly}
              onChange={(e) => {
                setSyncAssetsOnly(e.target.checked);
                if (e.target.checked) setSyncMetadataOnly(false);
              }}
              disabled={loading || state === "running"}
              style={{ width: 16, height: 16 }}
            />
            仅下载资产包（不更新数据库）
          </label>
          <span style={{ fontSize: 11, color: "#15803d" }}>
            * 两个选项互斥，用于独立同步结构化数据或文件资产。
          </span>
        </div>

        <div
          style={{
            display: "flex",
            gap: 8,
            flexWrap: "wrap",
            alignItems: "center",
          }}
        >
          <button
            type="button"
            onClick={() => triggerSync()}
            disabled={loading || state === "running"}
            style={{
              padding: "8px 14px",
              borderRadius: 8,
              border: "none",
              background: "#2563eb",
              color: "#fff",
              fontSize: 13,
              cursor: loading || state === "running" ? "not-allowed" : "pointer",
            }}
          >
            {state === "running" ? "正在同步 RD-Agent Catalog..." : "立即触发同步"}
          </button>
          <button
            type="button"
            onClick={() => triggerSync("incremental")}
            disabled={loading || state === "running"}
            style={{
              padding: "8px 14px",
              borderRadius: 8,
              border: "none",
              background: "#059669",
              color: "#fff",
              fontSize: 13,
              cursor: loading || state === "running" ? "not-allowed" : "pointer",
            }}
            title="强制走 mode=incremental（增量 catalog + zip 下载解压），并忽略 clean/仅元数据/仅资产选项"
          >
            一键增量同步（zip）
          </button>
          <button
            type="button"
            onClick={fetchStatus}
            disabled={loading}
            style={{
              padding: "6px 10px",
              borderRadius: 6,
              border: "1px solid #e5e7eb",
              background: "#f9fafb",
              fontSize: 12,
              cursor: loading ? "not-allowed" : "pointer",
            }}
          >
            手动刷新状态
          </button>
          <span style={{ fontSize: 12, color: "#374151" }}>
            当前状态：<strong>{state}</strong>
            {polling ? "（轮询中）" : ""}
          </span>
        </div>

        {error && (
          <div
            style={{
              marginTop: 12,
              padding: 10,
              background: "#fee2e2",
              borderRadius: 8,
              border: "1px solid #fecaca",
              color: "#b91c1c",
              fontSize: 13,
            }}
          >
            错误：{error}
          </div>
        )}

        <div
          style={{
            marginTop: 12,
            padding: 12,
            borderRadius: 10,
            background: "#f9fafb",
            border: "1px solid #e5e7eb",
            fontSize: 12,
          }}
        >
          <div>
            <span style={{ fontWeight: 500 }}>当前阶段：</span>
            {phase}
          </div>
          <div>
            <span style={{ fontWeight: 500 }}>进度：</span>
            {progress == null ? "-" : `${Math.round(progress * 1000) / 10}%`}
          </div>
          {progress != null && (
            <div style={{ marginTop: 6 }}>
              <div
                style={{
                  height: 8,
                  background: "#e5e7eb",
                  borderRadius: 999,
                  overflow: "hidden",
                }}
              >
                <div
                  style={{
                    height: 8,
                    width: `${Math.min(100, Math.max(0, progress * 100))}%`,
                    background: state === "failed" ? "#ef4444" : "#3b82f6",
                  }}
                />
              </div>
            </div>
          )}
          <div>
            <span style={{ fontWeight: 500 }}>最近一次同步开始时间：</span>
            {formatTs(status?.started_at_ts ?? null)}
          </div>
          <div>
            <span style={{ fontWeight: 500 }}>最近一次同步结束时间：</span>
            {formatTs(status?.finished_at_ts ?? null)}
          </div>
          <div>
            <span style={{ fontWeight: 500 }}>本次耗时：</span>
            {formatDuration(status?.started_at_ts ?? null, status?.finished_at_ts ?? null)}
          </div>
          {status?.last_error && (
            <div style={{ marginTop: 4, color: "#b91c1c", wordBreak: "break-all" }}>
              <span style={{ fontWeight: 500 }}>最后一次错误：</span>
              {status.last_error}
            </div>
          )}
        </div>
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 20,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <h2 style={{ marginTop: 0, marginBottom: 12, fontSize: 18 }}>最近一次同步结果汇总</h2>
        {!lastResult ? (
          <div style={{ fontSize: 13, color: "#6b7280" }}>暂无同步结果。</div>
        ) : (
          <div
            style={{
              display: "grid",
              gap: 12,
              gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
            }}
          >
            {Object.entries(lastResult).map(([key, v]) => {
              const label = SECTION_LABELS[key] ?? key;
              const inserted = v.inserted ?? 0;
              const updated = v.updated ?? 0;
              const skipped = v.skipped ?? 0;
              const errors = v.errors ?? 0;
              return (
                <div
                  key={key}
                  style={{
                    border: "1px solid #e5e7eb",
                    borderRadius: 10,
                    padding: 12,
                    background: "#f9fafb",
                    fontSize: 12,
                  }}
                >
                  <div style={{ fontWeight: 500, marginBottom: 6 }}>{label}</div>
                  <div style={{ lineHeight: 1.6 }}>
                    <div>插入：{inserted}</div>
                    <div>更新：{updated}</div>
                    <div>跳过：{skipped}</div>
                    <div>错误：{errors}</div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </section>

      <section style={{ marginTop: 16, fontSize: 12, color: "#6b7280", maxWidth: 720 }}>
        说明：
        增量同步（mode=incremental）将调用 RD-Agent 增量 Catalog 接口并对新增/变更的资产包执行 zip 下载与解压；
        勾选“清空”选项则用于全量重装（full_refresh）。
      </section>
    </main>
  );
}
