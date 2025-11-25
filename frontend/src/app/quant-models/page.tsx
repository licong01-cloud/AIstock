"use client";

import { useEffect, useMemo, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface ModelSchedule {
  id: number;
  model_name: string;
  schedule_name: string;
  task_type: "train" | "inference" | string;
  frequency: string;
  enabled: boolean;
  config_json: any;
  last_run_at?: string | null;
  next_run_at?: string | null;
  last_status?: string | null;
  last_error?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

interface TrainRun {
  id: number;
  model_name: string;
  config_snapshot: any;
  status: string;
  start_time?: string | null;
  end_time?: string | null;
  duration_seconds?: number | null;
  symbols_covered_count?: number | null;
  time_range_start?: string | null;
  time_range_end?: string | null;
  data_granularity?: string | null;
  metrics_json?: any;
  log_path?: string | null;
}

interface InferenceRun {
  id: number;
  model_name: string;
  schedule_name?: string | null;
  config_snapshot: any;
  status: string;
  start_time?: string | null;
  end_time?: string | null;
  duration_seconds?: number | null;
  symbols_covered?: number | null;
  time_of_data?: string | null;
  metrics_json?: any;
}

interface ModelStatusSummary {
  model_name: string;
  last_train: {
    id: number;
    status: string;
    start_time?: string | null;
    end_time?: string | null;
    duration_seconds?: number | null;
    metrics_json?: any;
  } | null;
  last_inference: {
    id: number;
    schedule_name?: string | null;
    status: string;
    start_time?: string | null;
    end_time?: string | null;
    duration_seconds?: number | null;
    symbols_covered?: number | null;
    time_of_data?: string | null;
    metrics_json?: any;
  } | null;
}

type TabKey = "schedules" | "runs" | "status";

type RunTab = "train" | "inference";

function formatDateTime(value?: string | null): string {
  if (!value) return "-";
  let s = String(value).trim();
  if (!s) return "-";
  s = s.replace("T", " ");
  const dotIndex = s.indexOf(".");
  if (dotIndex >= 0) {
    s = s.slice(0, dotIndex);
  }
  s = s.replace(/Z$/, "");
  s = s.replace(/[+-]\d{2}:?\d{2}$/, "");
  s = s.trim();
  if (s.length >= 19) return s.slice(0, 19);
  if (s.length >= 10) return s.slice(0, 10);
  return s;
}

function shortStatusColor(status?: string | null): string {
  const s = (status || "").toUpperCase();
  if (s === "SUCCESS") return "#16a34a";
  if (s === "FAILED") return "#dc2626";
  if (s === "RUNNING") return "#0ea5e9";
  if (s === "PENDING") return "#6b7280";
  return "#6b7280";
}

async function apiRequest<T = any>(
  method: string,
  path: string,
  body?: any,
): Promise<T> {
  const url = `${API_BASE.replace(/\/$/, "")}${path}`;
  const res = await fetch(url, {
    method,
    headers: {
      "Content-Type": "application/json",
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    let text = "";
    try {
      text = await res.text();
    } catch {
      text = "";
    }
    throw new Error(
      `请求失败: HTTP ${res.status} ${res.statusText}${text ? ` | ${text}` : ""}`,
    );
  }
  if (res.status === 204) return {} as T;
  const txt = await res.text();
  if (!txt) return {} as T;
  return JSON.parse(txt) as T;
}

export default function QuantModelsPage() {
  const [activeTab, setActiveTab] = useState<TabKey>("schedules");

  return (
    <main style={{ padding: 24 }}>
      <section style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>🧠 模型调度与运行管理</h1>
        <p style={{ marginTop: 4, fontSize: 13, color: "#666" }}>
          管理 LSTM / DeepAR 等量化模型的训练与推理调度计划，并查看最近运行状态。
        </p>
      </section>

      <section style={{ marginBottom: 12 }}>
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 6,
            borderBottom: "1px solid #e5e7eb",
            paddingBottom: 4,
            marginBottom: 8,
          }}
        >
          <button
            type="button"
            onClick={() => setActiveTab("schedules")}
            style={{
              padding: "6px 10px",
              borderRadius: 999,
              border: "none",
              background: activeTab === "schedules" ? "#0f766e" : "transparent",
              color: activeTab === "schedules" ? "#fff" : "#374151",
              fontSize: 13,
              cursor: "pointer",
            }}
          >
            调度计划
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("runs")}
            style={{
              padding: "6px 10px",
              borderRadius: 999,
              border: "none",
              background: activeTab === "runs" ? "#0f766e" : "transparent",
              color: activeTab === "runs" ? "#fff" : "#374151",
              fontSize: 13,
              cursor: "pointer",
            }}
          >
            运行记录
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("status")}
            style={{
              padding: "6px 10px",
              borderRadius: 999,
              border: "none",
              background: activeTab === "status" ? "#0f766e" : "transparent",
              color: activeTab === "status" ? "#fff" : "#374151",
              fontSize: 13,
              cursor: "pointer",
            }}
          >
            模型概览
          </button>
        </div>
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 14,
          boxShadow: "0 2px 10px rgba(0,0,0,0.04)",
          fontSize: 13,
        }}
      >
        {activeTab === "schedules" && <SchedulesTab />}
        {activeTab === "runs" && <RunsTab />}
        {activeTab === "status" && <StatusTab />}        
      </section>
    </main>
  );
}

function SchedulesTab() {
  const [schedules, setSchedules] = useState<ModelSchedule[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [selectedId, setSelectedId] = useState<number | null>(null);

  const [modelName, setModelName] = useState("LSTM_SHARED");
  const [scheduleName, setScheduleName] = useState("");
  const [taskType, setTaskType] = useState<"train" | "inference">("train");
  const [frequency, setFrequency] = useState("7 days");
  const [enabled, setEnabled] = useState(true);
  const [configText, setConfigText] = useState(
    '{\n  "kind": "lstm_shared_train",\n  "params": {\n    "universe-name": "ALL_EQ_CLEAN",\n    "start": "2020-01-01T09:30:00",\n    "end": "2024-01-01T15:00:00",\n    "seq-len": 60\n  }\n}',
  );

  const [runDryRun, setRunDryRun] = useState(false);

  const apiBaseDisplay = useMemo(
    () => API_BASE.replace(/\/$/, ""),
    [],
  );

  async function loadSchedules() {
    setLoading(true);
    setError(null);
    try {
      const data: ModelSchedule[] = await apiRequest("GET", "/models/schedules");
      setSchedules(data || []);
    } catch (e: any) {
      setError(e?.message || "加载调度计划失败");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadSchedules();
  }, []);

  function handleSelect(s: ModelSchedule) {
    setSelectedId(s.id);
    setModelName(s.model_name);
    setScheduleName(s.schedule_name);
    setTaskType((s.task_type as any) === "inference" ? "inference" : "train");
    setFrequency(s.frequency || "");
    setEnabled(Boolean(s.enabled));
    try {
      setConfigText(JSON.stringify(s.config_json ?? {}, null, 2));
    } catch {
      setConfigText("{}");
    }
  }

  async function handleUpsert() {
    setError(null);
    let cfg: any;
    try {
      cfg = configText.trim() ? JSON.parse(configText) : {};
    } catch (e: any) {
      setError(e?.message || "config_json 不是合法 JSON");
      return;
    }

    try {
      await apiRequest("POST", "/models/schedules", {
        model_name: modelName.trim(),
        schedule_name: scheduleName.trim(),
        task_type: taskType,
        frequency: frequency.trim(),
        enabled,
        config_json: cfg,
      });
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "保存调度计划失败");
    }
  }

  async function handleDelete(id: number) {
    if (typeof window !== "undefined") {
      const ok = window.confirm("确认删除该调度计划？该操作不可恢复。");
      if (!ok) return;
    }
    setError(null);
    try {
      await apiRequest("DELETE", `/models/schedules/${id}`);
      if (selectedId === id) {
        setSelectedId(null);
      }
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "删除失败");
    }
  }

  async function handleToggleEnabled(id: number, current: boolean) {
    setError(null);
    try {
      await apiRequest("PATCH", `/models/schedules/${id}`, {
        enabled: !current,
      });
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "更新启用状态失败");
    }
  }

  async function handleRunOnce(id: number) {
    setError(null);
    try {
      await apiRequest("POST", `/models/schedules/${id}/run-once`, {
        dry_run: runDryRun,
      });
    } catch (e: any) {
      setError(e?.message || "触发运行失败");
    }
  }

  return (
    <div>
      <div
        style={{
          marginBottom: 12,
          padding: 10,
          borderRadius: 10,
          background: "#f9fafb",
          border: "1px solid #e5e7eb",
        }}
      >
        <div style={{ fontSize: 13, color: "#374151" }}>
          当前 API 地址：
          <code
            style={{
              padding: "2px 6px",
              borderRadius: 4,
              background: "#e5e7eb",
              fontSize: 12,
            }}
          >
            {apiBaseDisplay}
          </code>
        </div>
        <div style={{ marginTop: 4, fontSize: 12, color: "#6b7280" }}>
          该页面通过 /models/* 系列接口管理调度（不依赖旧版 tdx_scheduler 进程）。
        </div>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "minmax(0, 2.1fr) minmax(0, 1.5fr)",
          gap: 16,
        }}
      >
        <div>
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: 8,
            }}
          >
            <h2
              style={{
                margin: 0,
                fontSize: 15,
                color: "#111827",
              }}
            >
              调度计划列表
            </h2>
            <button
              type="button"
              onClick={() => void loadSchedules()}
              disabled={loading}
              style={{
                padding: "4px 10px",
                borderRadius: 999,
                border: "1px solid #d1d5db",
                background: "#fff",
                cursor: "pointer",
                fontSize: 12,
              }}
            >
              {loading ? "刷新中..." : "刷新"}
            </button>
          </div>

          <div
            style={{
              border: "1px solid #e5e7eb",
              borderRadius: 8,
              maxHeight: 420,
              overflow: "auto",
              fontSize: 12,
            }}
          >
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
              }}
            >
              <thead>
                <tr style={{ background: "#f3f4f6" }}>
                  <th style={{ padding: "6px 8px", textAlign: "left" }}>选择</th>
                  <th style={{ padding: "6px 8px", textAlign: "left" }}>模型</th>
                  <th style={{ padding: "6px 8px", textAlign: "left" }}>名称</th>
                  <th style={{ padding: "6px 8px", textAlign: "left" }}>类型</th>
                  <th style={{ padding: "6px 8px", textAlign: "left" }}>freq</th>
                  <th style={{ padding: "6px 8px", textAlign: "left" }}>kind</th>
                  <th style={{ padding: "6px 8px", textAlign: "left" }}>启用</th>
                  <th style={{ padding: "6px 8px", textAlign: "left" }}>最近状态</th>
                  <th style={{ padding: "6px 8px", textAlign: "left" }}>操作</th>
                </tr>
              </thead>
              <tbody>
                {schedules.length === 0 && (
                  <tr>
                    <td
                      colSpan={9}
                      style={{ padding: 8, color: "#6b7280", fontSize: 12 }}
                    >
                      暂无调度计划，请在右侧填写信息后点击“保存/更新”。
                    </td>
                  </tr>
                )}
                {schedules.map((s) => {
                  const kind = s.config_json?.kind || "";
                  const status = s.last_status || "";
                  return (
                    <tr
                      key={s.id}
                      style={{
                        background:
                          selectedId === s.id ? "#ecfeff" : "transparent",
                      }}
                    >
                      <td style={{ padding: "4px 6px" }}>
                        <input
                          type="radio"
                          checked={selectedId === s.id}
                          onChange={() => handleSelect(s)}
                        />
                      </td>
                      <td style={{ padding: "4px 6px", whiteSpace: "nowrap" }}>
                        {s.model_name}
                      </td>
                      <td style={{ padding: "4px 6px", whiteSpace: "nowrap" }}>
                        {s.schedule_name}
                      </td>
                      <td style={{ padding: "4px 6px" }}>{s.task_type}</td>
                      <td style={{ padding: "4px 6px" }}>{s.frequency}</td>
                      <td style={{ padding: "4px 6px" }}>{kind}</td>
                      <td style={{ padding: "4px 6px" }}>
                        <label style={{ fontSize: 11 }}>
                          <input
                            type="checkbox"
                            checked={Boolean(s.enabled)}
                            onChange={() =>
                              void handleToggleEnabled(s.id, Boolean(s.enabled))
                            }
                            style={{ marginRight: 4 }}
                          />
                          {s.enabled ? "启用" : "停用"}
                        </label>
                      </td>
                      <td style={{ padding: "4px 6px" }}>
                        <span
                          style={{
                            color: shortStatusColor(status),
                            fontWeight: 500,
                          }}
                        >
                          {status || "-"}
                        </span>
                        <div style={{ fontSize: 11, color: "#6b7280" }}>
                          {s.last_run_at
                            ? formatDateTime(s.last_run_at)
                            : "无记录"}
                        </div>
                      </td>
                      <td style={{ padding: "4px 6px" }}>
                        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                          <button
                            type="button"
                            onClick={() => handleSelect(s)}
                            style={{
                              padding: "2px 6px",
                              borderRadius: 6,
                              border: "1px solid #d1d5db",
                              background: "#fff",
                              cursor: "pointer",
                              fontSize: 11,
                            }}
                          >
                            编辑
                          </button>
                          <button
                            type="button"
                            onClick={() => void handleRunOnce(s.id)}
                            style={{
                              padding: "2px 6px",
                              borderRadius: 6,
                              border: "1px solid #0f766e",
                              background: "#0f766e",
                              color: "#fff",
                              cursor: "pointer",
                              fontSize: 11,
                            }}
                          >
                            运行一次
                          </button>
                          <button
                            type="button"
                            onClick={() => void handleDelete(s.id)}
                            style={{
                              padding: "2px 6px",
                              borderRadius: 6,
                              border: "1px solid #fecaca",
                              background: "#fee2e2",
                              color: "#b91c1c",
                              cursor: "pointer",
                              fontSize: 11,
                            }}
                          >
                            删除
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        <div>
          <h2
            style={{
              margin: 0,
              marginBottom: 8,
              fontSize: 15,
              color: "#111827",
            }}
          >
            新建 / 编辑调度计划
          </h2>

          <div
            style={{
              display: "flex",
              flexDirection: "column",
              gap: 8,
            }}
          >
            <div
              style={{ display: "flex", gap: 8, flexWrap: "wrap", fontSize: 12 }}
            >
              <label style={{ flex: 1, minWidth: 180 }}>
                <span style={{ display: "block", marginBottom: 2 }}>模型名称</span>
                <input
                  value={modelName}
                  onChange={(e) => setModelName(e.target.value)}
                  placeholder="如 LSTM_SHARED / LSTM_PER_STOCK / DEEPAR_DAILY"
                  style={{
                    width: "100%",
                    padding: "4px 6px",
                    borderRadius: 6,
                    border: "1px solid #d1d5db",
                    fontSize: 12,
                  }}
                />
              </label>
              <label style={{ flex: 1, minWidth: 180 }}>
                <span style={{ display: "block", marginBottom: 2 }}>调度名称</span>
                <input
                  value={scheduleName}
                  onChange={(e) => setScheduleName(e.target.value)}
                  placeholder="如 weekly_shared_train / daily_deepar_infer"
                  style={{
                    width: "100%",
                    padding: "4px 6px",
                    borderRadius: 6,
                    border: "1px solid #d1d5db",
                    fontSize: 12,
                  }}
                />
              </label>
            </div>

            <div
              style={{ display: "flex", gap: 8, flexWrap: "wrap", fontSize: 12 }}
            >
              <label>
                <span style={{ display: "block", marginBottom: 2 }}>任务类型</span>
                <select
                  value={taskType}
                  onChange={(e) =>
                    setTaskType(e.target.value === "inference" ? "inference" : "train")
                  }
                  style={{
                    padding: "4px 6px",
                    borderRadius: 6,
                    border: "1px solid #d1d5db",
                    fontSize: 12,
                  }}
                >
                  <option value="train">train · 训练</option>
                  <option value="inference">inference · 推理</option>
                </select>
              </label>
              <label style={{ minWidth: 160 }}>
                <span style={{ display: "block", marginBottom: 2 }}>frequency</span>
                <input
                  value={frequency}
                  onChange={(e) => setFrequency(e.target.value)}
                  placeholder="如 '7 days' / '1 day' / '15 minutes'"
                  style={{
                    width: "100%",
                    padding: "4px 6px",
                    borderRadius: 6,
                    border: "1px solid #d1d5db",
                    fontSize: 12,
                  }}
                />
              </label>
              <label
                style={{
                  display: "flex",
                  alignItems: "flex-end",
                  gap: 4,
                  fontSize: 12,
                }}
              >
                <input
                  type="checkbox"
                  checked={enabled}
                  onChange={(e) => setEnabled(e.target.checked)}
                  style={{ marginRight: 4 }}
                />
                启用
              </label>
            </div>

            <label style={{ fontSize: 12 }}>
              <span style={{ display: "block", marginBottom: 2 }}>config_json</span>
              <textarea
                value={configText}
                onChange={(e) => setConfigText(e.target.value)}
                rows={14}
                style={{
                  width: "100%",
                  padding: 8,
                  borderRadius: 8,
                  border: "1px solid #d1d5db",
                  fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
                  fontSize: 12,
                  resize: "vertical",
                  whiteSpace: "pre",
                }}
              />
            </label>

            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: 8,
                marginTop: 4,
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <button
                  type="button"
                  onClick={() => void handleUpsert()}
                  style={{
                    padding: "6px 12px",
                    borderRadius: 999,
                    border: "none",
                    background: "#0f766e",
                    color: "#fff",
                    cursor: "pointer",
                    fontSize: 13,
                  }}
                >
                  保存 / 更新
                </button>
                <label style={{ fontSize: 12 }}>
                  <input
                    type="checkbox"
                    checked={runDryRun}
                    onChange={(e) => setRunDryRun(e.target.checked)}
                    style={{ marginRight: 4 }}
                  />
                  运行一次时使用 dry-run（仅打印命令）
                </label>
              </div>
              {selectedId && (
                <div style={{ fontSize: 11, color: "#6b7280" }}>
                  正在编辑 ID = {selectedId} 的调度记录
                </div>
              )}
            </div>

            {error && (
              <div
                style={{
                  marginTop: 4,
                  padding: "6px 8px",
                  borderRadius: 6,
                  background: "#fef2f2",
                  color: "#b91c1c",
                  fontSize: 12,
                }}
              >
                {error}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function RunsTab() {
  const [activeRunTab, setActiveRunTab] = useState<RunTab>("train");
  const [modelFilter, setModelFilter] = useState("LSTM_SHARED");
  const [statusFilter, setStatusFilter] = useState("");
  const [runs, setRuns] = useState<Array<TrainRun | InferenceRun>>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadRuns() {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (modelFilter.trim()) params.set("model_name", modelFilter.trim());
      if (statusFilter.trim()) params.set("status", statusFilter.trim());
      params.set("limit", "50");
      const basePath =
        activeRunTab === "train" ? "/models/train-runs" : "/models/inference-runs";
      const path = `${basePath}?${params.toString()}`;
      const data: any[] = await apiRequest("GET", path);
      setRuns(data || []);
    } catch (e: any) {
      setError(e?.message || "加载运行记录失败");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadRuns();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeRunTab]);

  return (
    <div>
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          justifyContent: "space-between",
          alignItems: "center",
          gap: 8,
          marginBottom: 8,
        }}
      >
        <div style={{ display: "flex", gap: 6 }}>
          <button
            type="button"
            onClick={() => setActiveRunTab("train")}
            style={{
              padding: "4px 10px",
              borderRadius: 999,
              border: "none",
              background: activeRunTab === "train" ? "#0f766e" : "transparent",
              color: activeRunTab === "train" ? "#fff" : "#374151",
              fontSize: 12,
              cursor: "pointer",
            }}
          >
            训练任务
          </button>
          <button
            type="button"
            onClick={() => setActiveRunTab("inference")}
            style={{
              padding: "4px 10px",
              borderRadius: 999,
              border: "none",
              background:
                activeRunTab === "inference" ? "#0f766e" : "transparent",
              color: activeRunTab === "inference" ? "#fff" : "#374151",
              fontSize: 12,
              cursor: "pointer",
            }}
          >
            推理任务
          </button>
        </div>

        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 8,
            alignItems: "center",
            fontSize: 12,
          }}
        >
          <label>
            <span style={{ marginRight: 4 }}>模型：</span>
            <input
              value={modelFilter}
              onChange={(e) => setModelFilter(e.target.value)}
              placeholder="如 LSTM_SHARED / LSTM_PER_STOCK / DEEPAR_DAILY"
              style={{
                padding: "3px 6px",
                borderRadius: 6,
                border: "1px solid #d1d5db",
                fontSize: 12,
              }}
            />
          </label>
          <label>
            <span style={{ marginRight: 4 }}>状态：</span>
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              style={{
                padding: "3px 6px",
                borderRadius: 6,
                border: "1px solid #d1d5db",
                fontSize: 12,
              }}
            >
              <option value="">全部</option>
              <option value="SUCCESS">SUCCESS</option>
              <option value="FAILED">FAILED</option>
              <option value="RUNNING">RUNNING</option>
              <option value="PENDING">PENDING</option>
            </select>
          </label>
          <button
            type="button"
            onClick={() => void loadRuns()}
            disabled={loading}
            style={{
              padding: "4px 10px",
              borderRadius: 999,
              border: "1px solid #d1d5db",
              background: "#fff",
              cursor: "pointer",
              fontSize: 12,
            }}
          >
            {loading ? "刷新中..." : "刷新"}
          </button>
        </div>
      </div>

      <div
        style={{
          border: "1px solid #e5e7eb",
          borderRadius: 8,
          maxHeight: 440,
          overflow: "auto",
          fontSize: 12,
        }}
      >
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
          }}
        >
          <thead>
            <tr style={{ background: "#f3f4f6" }}>
              <th style={{ padding: "6px 8px", textAlign: "left" }}>ID</th>
              <th style={{ padding: "6px 8px", textAlign: "left" }}>模型</th>
              {activeRunTab === "inference" && (
                <th style={{ padding: "6px 8px", textAlign: "left" }}>调度</th>
              )}
              <th style={{ padding: "6px 8px", textAlign: "left" }}>状态</th>
              <th style={{ padding: "6px 8px", textAlign: "left" }}>开始</th>
              <th style={{ padding: "6px 8px", textAlign: "left" }}>结束</th>
              <th style={{ padding: "6px 8px", textAlign: "left" }}>耗时(s)</th>
              <th style={{ padding: "6px 8px", textAlign: "left" }}>覆盖股票/样本</th>
              <th style={{ padding: "6px 8px", textAlign: "left" }}>备注</th>
            </tr>
          </thead>
          <tbody>
            {runs.length === 0 && (
              <tr>
                <td
                  colSpan={activeRunTab === "train" ? 8 : 9}
                  style={{ padding: 8, color: "#6b7280" }}
                >
                  暂无运行记录。
                </td>
              </tr>
            )}
            {runs.map((r: any) => {
              const status = r.status || "";
              const metrics = r.metrics_json || {};
              const brief =
                metrics.best_val_loss !== undefined
                  ? `best_val_loss=${metrics.best_val_loss}`
                  : metrics.error
                  ? String(metrics.error)
                  : "";
              const count =
                r.symbols_covered_count ?? r.symbols_covered ?? metrics.train_samples;
              return (
                <tr key={r.id}>
                  <td style={{ padding: "4px 6px" }}>{r.id}</td>
                  <td style={{ padding: "4px 6px" }}>{r.model_name}</td>
                  {activeRunTab === "inference" && (
                    <td style={{ padding: "4px 6px" }}>{r.schedule_name || "-"}</td>
                  )}
                  <td style={{ padding: "4px 6px" }}>
                    <span
                      style={{
                        color: shortStatusColor(status),
                        fontWeight: 500,
                      }}
                    >
                      {status}
                    </span>
                  </td>
                  <td style={{ padding: "4px 6px" }}>
                    {formatDateTime(r.start_time)}
                  </td>
                  <td style={{ padding: "4px 6px" }}>
                    {formatDateTime(r.end_time)}
                  </td>
                  <td style={{ padding: "4px 6px" }}>
                    {r.duration_seconds != null
                      ? Number(r.duration_seconds).toFixed(1)
                      : "-"}
                  </td>
                  <td style={{ padding: "4px 6px" }}>{count ?? "-"}</td>
                  <td style={{ padding: "4px 6px" }}>{brief}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {error && (
        <div
          style={{
            marginTop: 4,
            padding: "6px 8px",
            borderRadius: 6,
            background: "#fef2f2",
            color: "#b91c1c",
            fontSize: 12,
          }}
        >
          {error}
        </div>
      )}
    </div>
  );
}

function StatusTab() {
  const [modelName, setModelName] = useState("LSTM_SHARED");
  const [status, setStatus] = useState<ModelStatusSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadStatus() {
    if (!modelName.trim()) return;
    setLoading(true);
    setError(null);
    setStatus(null);
    try {
      const params = new URLSearchParams();
      params.set("model_name", modelName.trim());
      const data: ModelStatusSummary = await apiRequest(
        "GET",
        `/models/status?${params.toString()}`,
      );
      setStatus(data);
    } catch (e: any) {
      setError(e?.message || "加载模型状态失败");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 8,
          alignItems: "center",
          marginBottom: 8,
          fontSize: 12,
        }}
      >
        <label>
          <span style={{ marginRight: 4 }}>模型名称：</span>
          <input
            value={modelName}
            onChange={(e) => setModelName(e.target.value)}
            placeholder="如 LSTM_SHARED / LSTM_PER_STOCK / LSTM_REFINEMENT / DEEPAR_DAILY"
            style={{
              padding: "4px 6px",
              borderRadius: 6,
              border: "1px solid #d1d5db",
              fontSize: 12,
              minWidth: 260,
            }}
          />
        </label>
        <button
          type="button"
          onClick={() => void loadStatus()}
          disabled={loading}
          style={{
            padding: "6px 12px",
            borderRadius: 999,
            border: "1px solid #d1d5db",
            background: "#fff",
            cursor: "pointer",
            fontSize: 12,
          }}
        >
          {loading ? "查询中..." : "查询"}
        </button>
      </div>

      {status && (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr)",
            gap: 12,
            marginTop: 4,
            fontSize: 12,
          }}
        >
          <div
            style={{
              borderRadius: 8,
              border: "1px solid #e5e7eb",
              padding: 10,
            }}
          >
            <h3
              style={{
                margin: 0,
                marginBottom: 6,
                fontSize: 14,
                color: "#111827",
              }}
            >
              最近一次训练
            </h3>
            {status.last_train ? (
              <div>
                <p style={{ margin: 0 }}>
                  ID: {status.last_train.id} · 状态：
                  <span
                    style={{
                      color: shortStatusColor(status.last_train.status),
                      fontWeight: 500,
                    }}
                  >
                    {" "}
                    {status.last_train.status}
                  </span>
                </p>
                <p style={{ margin: "4px 0 0", color: "#4b5563" }}>
                  开始：{formatDateTime(status.last_train.start_time)}
                  <br />
                  结束：{formatDateTime(status.last_train.end_time)}
                  <br />
                  耗时：
                  {status.last_train.duration_seconds != null
                    ? Number(status.last_train.duration_seconds).toFixed(1)
                    : "-"}
                  s
                </p>
              </div>
            ) : (
              <p style={{ margin: 0, color: "#6b7280" }}>暂无训练记录。</p>
            )}
          </div>

          <div
            style={{
              borderRadius: 8,
              border: "1px solid #e5e7eb",
              padding: 10,
            }}
          >
            <h3
              style={{
                margin: 0,
                marginBottom: 6,
                fontSize: 14,
                color: "#111827",
              }}
            >
              最近一次推理
            </h3>
            {status.last_inference ? (
              <div>
                <p style={{ margin: 0 }}>
                  ID: {status.last_inference.id} · 状态：
                  <span
                    style={{
                      color: shortStatusColor(status.last_inference.status),
                      fontWeight: 500,
                    }}
                  >
                    {" "}
                    {status.last_inference.status}
                  </span>
                </p>
                <p style={{ margin: "4px 0 0", color: "#4b5563" }}>
                  调度：{status.last_inference.schedule_name || "-"}
                  <br />
                  数据时间：{formatDateTime(status.last_inference.time_of_data)}
                  <br />
                  开始：{formatDateTime(status.last_inference.start_time)}
                  <br />
                  结束：{formatDateTime(status.last_inference.end_time)}
                  <br />
                  耗时：
                  {status.last_inference.duration_seconds != null
                    ? Number(status.last_inference.duration_seconds).toFixed(1)
                    : "-"}
                  s · 覆盖股票数：
                  {status.last_inference.symbols_covered ?? "-"}
                </p>
              </div>
            ) : (
              <p style={{ margin: 0, color: "#6b7280" }}>暂无推理记录。</p>
            )}
          </div>
        </div>
      )}

      {error && (
        <div
          style={{
            marginTop: 8,
            padding: "6px 8px",
            borderRadius: 6,
            background: "#fef2f2",
            color: "#b91c1c",
            fontSize: 12,
          }}
        >
          {error}
        </div>
      )}
    </div>
  );
}
