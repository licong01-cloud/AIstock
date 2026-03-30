"use client";

import React, { useEffect, useState, useCallback, useRef } from "react";

export interface ProgressInfo {
  status: "idle" | "computing" | "success" | "failed";
  phase: string;
  phase_label: string;
  total: number;
  done: number;
  percent: number;
  elapsed_sec: number;
  mode: string;
  error: string | null;
}

export interface ComputeStatus {
  status: "idle" | "computing";
  progress: ProgressInfo;
  db_correlation_count: number;
  uncorrelated_factor_count: number;
  latest_computation: {
    as_of_date: string;
    num_factors: number;
    num_high_corr_pairs: number;
    avg_correlation: number;
    computation_time_sec: number;
    hdf5_path: string;
    created_at: string;
  } | null;
  in_memory_result: boolean;
}

export type ComputeScope = "full" | "cache" | "incremental" | "smart_incremental";

interface CacheStatus {
  cached_count: number;
  total_computable: number;
  uncached_count: number;
  total_size_mb: number;
  as_of_date: string | null;
  generated_at: string | null;
  date_range: string | null;
}

interface ScheduleItem {
  schedule_id: string;
  dataset: string;
  frequency: string;
  enabled: boolean;
  last_run_at: string | null;
  next_run_at: string | null;
  last_status: string | null;
  options: Record<string, any>;
}

interface LogEntry {
  index: number;
  ts: string;
  level: string;
  msg: string;
}

interface Props {
  status: ComputeStatus | null;
  computing: boolean;
  scope: ComputeScope;
  asOfDate: string;
  onScopeChange: (s: ComputeScope) => void;
  onAsOfDateChange: (d: string) => void;
  onCompute: () => void;
}

const SCOPE_OPTIONS: { value: ComputeScope; label: string; desc: string }[] = [
  { value: "smart_incremental", label: "智能增量", desc: "自动检测新因子，仅对新因子计算相关性（推荐）" },
  { value: "full", label: "全量计算", desc: "执行因子代码 + 计算矩阵（约1-2小时）" },
  { value: "cache", label: "仅用缓存", desc: "用已有 parquet 缓存计算矩阵（几分钟）" },
  { value: "incremental", label: "增量计算", desc: "选择特定因子计算" },
];

const DATASET_OPTIONS = [
  { value: "correlation_full", label: "全量计算" },
  { value: "correlation_cache_only", label: "仅用缓存" },
  { value: "correlation_smart_incremental", label: "智能增量" },
];

const FREQ_OPTIONS = [
  { value: "weekly", label: "每周" },
  { value: "daily", label: "每日" },
  { value: "manual", label: "手动" },
];

const DAY_OPTIONS = [
  { value: "monday", label: "周一" },
  { value: "tuesday", label: "周二" },
  { value: "wednesday", label: "周三" },
  { value: "thursday", label: "周四" },
  { value: "friday", label: "周五" },
  { value: "saturday", label: "周六" },
  { value: "sunday", label: "周日" },
];

const selectStyle: React.CSSProperties = {
  padding: "8px 12px",
  fontSize: 12,
  fontWeight: 500,
  borderRadius: 8,
  border: "none",
  background: "rgba(255,255,255,0.2)",
  color: "#fff",
  outline: "none",
};

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const BASE = `${API}/quantevolver/evolution`;

function formatElapsed(sec: number): string {
  if (sec < 60) return `${Math.round(sec)}s`;
  const m = Math.floor(sec / 60);
  const s = Math.round(sec % 60);
  return `${m}m${s}s`;
}

export default function ComputePanel({
  status,
  computing,
  scope,
  asOfDate,
  onScopeChange,
  onAsOfDateChange,
  onCompute,
}: Props) {
  const lc = status?.latest_computation;
  const progress = status?.progress;
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [schedules, setSchedules] = useState<ScheduleItem[]>([]);
  const [showSchedulePanel, setShowSchedulePanel] = useState(false);
  const [newDataset, setNewDataset] = useState("correlation_full");
  const [newFreq, setNewFreq] = useState("weekly");
  const [newDay, setNewDay] = useState("saturday");
  const [newAt, setNewAt] = useState("02:00");
  const [showLogs, setShowLogs] = useState(false);
  const [logEntries, setLogEntries] = useState<LogEntry[]>([]);
  const logCursorRef = useRef(-1);
  const logPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const logEndRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    fetch(`${BASE}/correlations/cache-status`)
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => d && setCacheStatus(d))
      .catch(() => {});
  }, [computing]);

  const loadSchedules = useCallback(() => {
    fetch(`${BASE}/correlations/schedules`)
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => d && setSchedules(d.items || []))
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (showSchedulePanel) loadSchedules();
  }, [showSchedulePanel, loadSchedules]);

  // ── 日志轮询 ──
  const logFetchingRef = useRef(false);

  const fetchLogs = useCallback(async (fullReload = false) => {
    if (logFetchingRef.current) return;
    logFetchingRef.current = true;
    try {
      const cursor = fullReload ? -1 : logCursorRef.current;
      const res = await fetch(`${BASE}/correlations/logs?after_index=${cursor}`);
      if (!res.ok) return;
      const data = await res.json();
      const entries: LogEntry[] = data.entries || [];
      if (entries.length > 0) {
        logCursorRef.current = entries[entries.length - 1].index;
        if (fullReload) {
          // 全量替换，不追加
          setLogEntries(entries);
        } else {
          // 增量追加，按 index 去重
          setLogEntries((prev) => {
            const existing = new Set(prev.map((e) => e.index));
            const fresh = entries.filter((e) => !existing.has(e.index));
            return fresh.length > 0 ? [...prev, ...fresh] : prev;
          });
        }
      }
    } catch {
      // ignore
    } finally {
      logFetchingRef.current = false;
    }
  }, []);

  useEffect(() => {
    if (showLogs) {
      // 首次展开：全量加载累积日志
      fetchLogs(true);
      // 开始增量轮询
      logPollRef.current = setInterval(() => fetchLogs(false), 2000);
    }
    return () => {
      if (logPollRef.current) {
        clearInterval(logPollRef.current);
        logPollRef.current = null;
      }
    };
  }, [showLogs, fetchLogs]);

  // 自动滚动到底部
  useEffect(() => {
    if (showLogs && logEndRef.current) {
      logEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logEntries, showLogs]);

  const handleCreateSchedule = async () => {
    const opts: Record<string, string> = { at: newAt };
    if (newFreq === "weekly") opts.day_of_week = newDay;
    const res = await fetch(`${BASE}/correlations/schedules`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dataset: newDataset,
        frequency: newFreq,
        enabled: true,
        options: opts,
      }),
    });
    if (res.ok) loadSchedules();
  };

  const handleToggle = async (id: string, enabled: boolean) => {
    await fetch(`${BASE}/correlations/schedules/${id}/toggle?enabled=${!enabled}`, { method: "POST" });
    loadSchedules();
  };

  const handleRunNow = async (id: string) => {
    await fetch(`${BASE}/correlations/schedules/${id}/run`, { method: "POST" });
  };

  const handleDelete = async (id: string) => {
    await fetch(`${BASE}/correlations/schedules/${id}`, { method: "DELETE" });
    loadSchedules();
  };

  const isComputing = computing || progress?.status === "computing";

  return (
    <div
      style={{
        background:
          "linear-gradient(135deg, #7c3aed 0%, #2563eb 50%, #06b6d4 100%)",
        borderRadius: 16,
        padding: "24px 28px",
        color: "#fff",
        marginBottom: 24,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          flexWrap: "wrap",
          gap: 16,
        }}
      >
        <div>
          <h1 style={{ margin: 0, fontSize: 24, fontWeight: 700 }}>
            因子相关性分析
          </h1>
          <p style={{ margin: "4px 0 0", fontSize: 13, opacity: 0.85 }}>
            Spearman + EWMA 截面相关性引擎 · 252 天滚动窗口 · 半衰期 125 天
          </p>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
          <select
            value={scope}
            onChange={(e) => onScopeChange(e.target.value as ComputeScope)}
            disabled={isComputing}
            title="计算范围"
            style={{ ...selectStyle, cursor: isComputing ? "not-allowed" : "pointer" }}
          >
            {SCOPE_OPTIONS.map((o) => (
              <option key={o.value} value={o.value} style={{ color: "#374151" }}>
                {o.label} — {o.desc}
              </option>
            ))}
          </select>
          <input
            type="date"
            value={asOfDate}
            onChange={(e) => onAsOfDateChange(e.target.value)}
            disabled={isComputing}
            title="截止日期（留空使用数据最新日期）"
            style={{
              ...selectStyle,
              cursor: isComputing ? "not-allowed" : "pointer",
              width: 140,
              colorScheme: "dark",
            }}
          />
          <button
            onClick={onCompute}
            disabled={isComputing}
            style={{
              padding: "10px 24px",
              fontSize: 13,
              fontWeight: 600,
              background: isComputing ? "rgba(255,255,255,0.3)" : "#fff",
              color: isComputing ? "#fff" : "#7c3aed",
              border: "none",
              borderRadius: 8,
              cursor: isComputing ? "not-allowed" : "pointer",
              whiteSpace: "nowrap",
            }}
          >
            {isComputing
              ? "计算中..."
              : scope === "full"
              ? "全量计算"
              : scope === "cache"
              ? "缓存计算"
              : scope === "smart_incremental"
              ? "智能增量"
              : "增量计算"}
          </button>
          {isComputing && (
            <button
              onClick={async () => {
                await fetch(`${BASE}/correlations/cancel`, { method: "POST" });
              }}
              style={{
                padding: "10px 20px",
                fontSize: 13,
                fontWeight: 600,
                background: "rgba(239,68,68,0.7)",
                color: "#fff",
                border: "none",
                borderRadius: 8,
                cursor: "pointer",
                whiteSpace: "nowrap",
              }}
            >
              取消计算
            </button>
          )}
          <button
            onClick={() => setShowSchedulePanel(!showSchedulePanel)}
            style={{
              padding: "10px 16px",
              fontSize: 13,
              fontWeight: 500,
              background: showSchedulePanel ? "rgba(255,255,255,0.3)" : "rgba(255,255,255,0.15)",
              color: "#fff",
              border: "1px solid rgba(255,255,255,0.3)",
              borderRadius: 8,
              cursor: "pointer",
              whiteSpace: "nowrap",
            }}
          >
            {showSchedulePanel ? "收起调度" : "定时调度"}
          </button>
          <button
            onClick={() => setShowLogs(!showLogs)}
            style={{
              padding: "10px 16px",
              fontSize: 13,
              fontWeight: 500,
              background: showLogs ? "rgba(255,255,255,0.3)" : "rgba(255,255,255,0.15)",
              color: "#fff",
              border: "1px solid rgba(255,255,255,0.3)",
              borderRadius: 8,
              cursor: "pointer",
              whiteSpace: "nowrap",
            }}
          >
            {showLogs ? "收起日志" : "计算日志"}
            {logEntries.length > 0 && !showLogs && (
              <span style={{
                marginLeft: 6,
                padding: "1px 6px",
                fontSize: 10,
                background: "rgba(255,255,255,0.3)",
                borderRadius: 10,
              }}>
                {logEntries.length}
              </span>
            )}
          </button>
        </div>
      </div>

      {/* 进度条 */}
      {isComputing && (
        <div style={{ marginTop: 16 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, marginBottom: 4, opacity: 0.95 }}>
            <span>
              {progress?.status === "computing"
                ? (progress.phase_label || "准备中") + (progress.total > 0 ? ` ${progress.done}/${progress.total}` : "")
                : "正在启动计算任务..."}
            </span>
            <span>
              {progress?.status === "computing"
                ? `${progress.percent}% · ${formatElapsed(progress.elapsed_sec)}`
                : "0%"}
            </span>
          </div>
          <div style={{ background: "rgba(255,255,255,0.2)", borderRadius: 999, height: 8, overflow: "hidden" }}>
            <div
              style={{
                width: `${progress?.status === "computing" ? progress.percent : 0}%`,
                height: 8,
                background: "#fff",
                borderRadius: 999,
                transition: "width 0.5s ease",
              }}
            />
          </div>
          {progress?.error && (
            <div style={{ marginTop: 6, fontSize: 12, color: "#fca5a5" }}>
              {progress.error}
            </div>
          )}
        </div>
      )}

      {/* 统计指标 */}
      <div
        style={{
          display: "flex",
          gap: 32,
          marginTop: 20,
          flexWrap: "wrap",
        }}
      >
        {cacheStatus && (
          <StatItem
            label="因子缓存"
            value={`${cacheStatus.cached_count}/${cacheStatus.total_computable}`}
          />
        )}
        {cacheStatus && cacheStatus.total_size_mb > 0 && (
          <StatItem
            label="缓存大小"
            value={`${cacheStatus.total_size_mb.toFixed(0)} MB`}
          />
        )}
        {cacheStatus && cacheStatus.date_range && (
          <StatItem
            label="缓存时间段"
            value={cacheStatus.date_range}
          />
        )}
        {status && (
          <StatItem
            label="DB 记录对数"
            value={String(status.db_correlation_count)}
          />
        )}
        {status && status.uncorrelated_factor_count > 0 && (
          <StatItem
            label="待计算因子"
            value={String(status.uncorrelated_factor_count)}
          />
        )}
        {lc && (
          <>
            <StatItem label="数据截止" value={lc.as_of_date} />
            <StatItem label="矩阵因子数" value={String(lc.num_factors)} />
            <StatItem
              label="高相关对 (|r|>0.7)"
              value={String(lc.num_high_corr_pairs)}
            />
            <StatItem
              label="计算耗时"
              value={`${lc.computation_time_sec.toFixed(1)}s`}
            />
            <StatItem
              label="最后计算"
              value={
                lc.created_at
                  ? new Date(lc.created_at).toLocaleDateString("zh-CN")
                  : "-"
              }
            />
          </>
        )}
      </div>

      {!lc && !cacheStatus && (
        <p style={{ marginTop: 16, fontSize: 13, opacity: 0.8 }}>
          暂无相关性计算数据，请选择范围后点击计算
        </p>
      )}

      {/* 工作流提示 */}
      <div
        style={{
          marginTop: 16,
          padding: "10px 14px",
          background: "rgba(255,255,255,0.12)",
          borderRadius: 8,
          fontSize: 12,
          lineHeight: 1.8,
          opacity: 0.9,
        }}
      >
        <strong>计算模式说明：</strong>
        <strong>智能增量</strong>自动检测未计算相关性的新因子，执行代码生成缓存后计算完整相关性矩阵（向量化 GEMM，约1-2分钟）；
        <strong>全量计算</strong>执行全部因子实盘代码生成缓存并计算相关性矩阵（缓存生成约1-2小时，矩阵计算约1-2分钟）；
        <strong>缓存计算</strong>使用已有的单因子 parquet 缓存直接计算矩阵（约1-2分钟）；
        <strong>增量计算</strong>仅对新入库因子计算与现有因子的相关性。
      </div>

      {/* 调度管理面板 */}
      {showSchedulePanel && (
        <div
          style={{
            marginTop: 16,
            padding: "16px 18px",
            background: "rgba(255,255,255,0.12)",
            borderRadius: 12,
          }}
        >
          <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 12 }}>
            定时调度管理
          </div>

          {/* 已有调度列表 */}
          {schedules.length > 0 && (
            <table style={{ width: "100%", fontSize: 12, marginBottom: 12, borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ opacity: 0.7, textAlign: "left" }}>
                  <th style={{ padding: "4px 8px" }}>模式</th>
                  <th style={{ padding: "4px 8px" }}>频率</th>
                  <th style={{ padding: "4px 8px" }}>状态</th>
                  <th style={{ padding: "4px 8px" }}>上次运行</th>
                  <th style={{ padding: "4px 8px" }}>下次运行</th>
                  <th style={{ padding: "4px 8px" }}>操作</th>
                </tr>
              </thead>
              <tbody>
                {schedules.map((s) => (
                  <tr key={s.schedule_id} style={{ borderTop: "1px solid rgba(255,255,255,0.1)" }}>
                    <td style={{ padding: "6px 8px" }}>
                      {DATASET_OPTIONS.find((d) => d.value === s.dataset)?.label || s.dataset}
                    </td>
                    <td style={{ padding: "6px 8px" }}>{s.frequency}</td>
                    <td style={{ padding: "6px 8px" }}>
                      <span
                        style={{
                          padding: "2px 8px",
                          borderRadius: 12,
                          fontSize: 11,
                          background: s.enabled ? "rgba(16,185,129,0.3)" : "rgba(239,68,68,0.3)",
                        }}
                      >
                        {s.enabled ? "启用" : "禁用"}
                      </span>
                    </td>
                    <td style={{ padding: "6px 8px", opacity: 0.8 }}>
                      {s.last_run_at ? new Date(s.last_run_at).toLocaleString("zh-CN") : "-"}
                    </td>
                    <td style={{ padding: "6px 8px", opacity: 0.8 }}>
                      {s.next_run_at ? new Date(s.next_run_at).toLocaleString("zh-CN") : "-"}
                    </td>
                    <td style={{ padding: "6px 8px", display: "flex", gap: 4 }}>
                      <SmallBtn label="立即执行" onClick={() => handleRunNow(s.schedule_id)} />
                      <SmallBtn
                        label={s.enabled ? "禁用" : "启用"}
                        onClick={() => handleToggle(s.schedule_id, s.enabled)}
                      />
                      <SmallBtn label="删除" onClick={() => handleDelete(s.schedule_id)} danger />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}

          {schedules.length === 0 && (
            <div style={{ fontSize: 12, opacity: 0.7, marginBottom: 12 }}>
              暂无定时调度，请创建新调度
            </div>
          )}

          {/* 新建调度表单 */}
          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
            <select
              value={newDataset}
              onChange={(e) => setNewDataset(e.target.value)}
              style={{ ...selectStyle, fontSize: 11 }}
            >
              {DATASET_OPTIONS.map((o) => (
                <option key={o.value} value={o.value} style={{ color: "#374151" }}>
                  {o.label}
                </option>
              ))}
            </select>
            <select
              value={newFreq}
              onChange={(e) => setNewFreq(e.target.value)}
              style={{ ...selectStyle, fontSize: 11 }}
            >
              {FREQ_OPTIONS.map((o) => (
                <option key={o.value} value={o.value} style={{ color: "#374151" }}>
                  {o.label}
                </option>
              ))}
            </select>
            {newFreq === "weekly" && (
              <select
                value={newDay}
                onChange={(e) => setNewDay(e.target.value)}
                style={{ ...selectStyle, fontSize: 11 }}
              >
                {DAY_OPTIONS.map((o) => (
                  <option key={o.value} value={o.value} style={{ color: "#374151" }}>
                    {o.label}
                  </option>
                ))}
              </select>
            )}
            <input
              type="time"
              value={newAt}
              onChange={(e) => setNewAt(e.target.value)}
              style={{ ...selectStyle, fontSize: 11, width: 100, colorScheme: "dark" }}
              title="执行时间"
            />
            <button
              onClick={handleCreateSchedule}
              style={{
                padding: "6px 16px",
                fontSize: 12,
                fontWeight: 600,
                background: "#fff",
                color: "#7c3aed",
                border: "none",
                borderRadius: 6,
                cursor: "pointer",
              }}
            >
              创建调度
            </button>
          </div>
        </div>
      )}

      {/* 实时计算日志面板 */}
      {showLogs && (
        <div
          style={{
            marginTop: 16,
            background: "rgba(0,0,0,0.35)",
            borderRadius: 12,
            overflow: "hidden",
          }}
        >
          <div
            style={{
              padding: "10px 16px",
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              borderBottom: "1px solid rgba(255,255,255,0.1)",
            }}
          >
            <span style={{ fontWeight: 600, fontSize: 13 }}>
              计算日志
              {logEntries.length > 0 && (
                <span style={{ fontWeight: 400, opacity: 0.7, marginLeft: 8, fontSize: 11 }}>
                  {logEntries.length} 条
                </span>
              )}
            </span>
            {isComputing && (
              <span style={{ fontSize: 11, opacity: 0.7 }}>
                实时更新中...
              </span>
            )}
          </div>
          <div
            style={{
              maxHeight: 320,
              overflowY: "auto",
              padding: "8px 0",
              fontFamily: "'Cascadia Code', 'Fira Code', 'Consolas', monospace",
              fontSize: 11,
              lineHeight: 1.7,
            }}
          >
            {logEntries.length === 0 ? (
              <div style={{ padding: "16px", opacity: 0.5, textAlign: "center" }}>
                暂无日志，启动计算后将在此显示实时日志
              </div>
            ) : (
              logEntries.map((entry) => (
                <div
                  key={entry.index}
                  style={{
                    padding: "1px 16px",
                    color: entry.level === "ERROR"
                      ? "#fca5a5"
                      : entry.level === "WARN"
                      ? "#fcd34d"
                      : "rgba(255,255,255,0.85)",
                  }}
                >
                  <span style={{ opacity: 0.5 }}>{entry.ts}</span>{" "}
                  {entry.msg}
                </div>
              ))
            )}
            <div ref={logEndRef} />
          </div>
        </div>
      )}
    </div>
  );
}

function StatItem({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div style={{ fontSize: 22, fontWeight: 700 }}>{value}</div>
      <div style={{ fontSize: 12, opacity: 0.8, marginTop: 2 }}>{label}</div>
    </div>
  );
}

function SmallBtn({
  label,
  onClick,
  danger,
}: {
  label: string;
  onClick: () => void;
  danger?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: "3px 10px",
        fontSize: 11,
        border: "1px solid rgba(255,255,255,0.3)",
        borderRadius: 4,
        background: danger ? "rgba(239,68,68,0.3)" : "rgba(255,255,255,0.15)",
        color: "#fff",
        cursor: "pointer",
      }}
    >
      {label}
    </button>
  );
}
