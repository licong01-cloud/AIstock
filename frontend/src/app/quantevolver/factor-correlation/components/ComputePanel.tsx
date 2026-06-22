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
  // 实时计数 (反映当前 is_available 筛选状态 + qe_factor_correlations 剩余 pair)
  // 与 latest_computation.num_high_corr_pairs (compute 时的冻结快照) 区分
  live_high_corr_count_07?: number;
  live_high_corr_count_05?: number;
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
  official_cache?: OfficialCacheStatus;
}

export type ComputeScope = "cache";

export interface OfficialCacheStatus {
  cached_count: number;
  total_size_mb: number;
  date_range: string | null;
  as_of_date: string | null;
  cache_root?: string | null;
  cache_source?: string | null;
  data_source_mode?: string | null;
  window_train_start?: string | null;
  window_backtest_end?: string | null;
  disk_factor_count?: number | null;
  meta_factor_count?: number | null;
  orphan_parquet_count?: number | null;
  integrity_ok?: boolean | null;
}

export interface OfficialCacheWindow {
  start?: string | null;
  end?: string | null;
  date_range?: string | null;
  cache_root?: string | null;
  cache_source?: string | null;
}

export interface FactorStat {
  total: number;
  evaluated: number;
  correlation_cached: number;
}

export interface FactorStats {
  all: FactorStat;
  enabled: FactorStat;
  disabled: FactorStat;
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
  includeDisabled: boolean;
  factorStats: FactorStats | null;
  singleCache: OfficialCacheStatus | null;
  officialCacheWindow: OfficialCacheWindow | null;
  onScopeChange: (s: ComputeScope) => void;
  onIncludeDisabledChange: (v: boolean) => void;
  onCompute: () => void;
}

const SCOPE_OPTIONS: { value: ComputeScope; label: string; desc: string }[] = [
  { value: "cache", label: "全量重算相关性", desc: "使用 rdagent_assets/factor_values/single 官方因子值缓存重算相关性矩阵" },
];

const DATASET_OPTIONS = [
  { value: "correlation_full", label: "官方缓存相关性" },
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
const OFFICIAL_CACHE_DEFAULT_START = "2018-08-01";
const OFFICIAL_CACHE_DEFAULT_END = "2026-04-30";

function isIsoDate(value?: string | null): value is string {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function splitDateRangeEnd(value?: string | null): string | null {
  if (!value) return null;
  const end = value.split("~").pop()?.trim();
  return isIsoDate(end) ? end : null;
}

function resolveOfficialDisplayWindow(
  officialCacheWindow: OfficialCacheWindow | null,
  singleCache: OfficialCacheStatus | null,
): { start: string; end: string } {
  const startCandidates = [
    OFFICIAL_CACHE_DEFAULT_START,
    officialCacheWindow?.start,
    singleCache?.window_train_start,
  ].filter(isIsoDate);
  const endCandidates = [
    OFFICIAL_CACHE_DEFAULT_END,
    officialCacheWindow?.end,
    singleCache?.window_backtest_end,
    singleCache?.as_of_date,
    splitDateRangeEnd(singleCache?.date_range),
  ].filter(isIsoDate);

  const sortedEndCandidates = endCandidates.sort();

  return {
    start: startCandidates.sort()[0] || OFFICIAL_CACHE_DEFAULT_START,
    end: sortedEndCandidates[sortedEndCandidates.length - 1] || OFFICIAL_CACHE_DEFAULT_END,
  };
}

function formatElapsed(sec: number): string {
  if (sec < 60) return `${Math.round(sec)}s`;
  const m = Math.floor(sec / 60);
  const s = Math.round(sec % 60);
  return `${m}m${s}s`;
}


function formatSize(mb: number): string {
  if (mb >= 1024) return `${(mb / 1024).toFixed(1)} GB`;
  return `${mb.toFixed(0)} MB`;
}

export default function ComputePanel({
  status,
  computing,
  scope,
  includeDisabled,
  factorStats,
  singleCache,
  officialCacheWindow,
  onScopeChange,
  onIncludeDisabledChange,
  onCompute,
}: Props) {
  const lc = status?.latest_computation;
  const progress = status?.progress;
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
          setLogEntries(entries);
        } else {
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
      fetchLogs(true);
      logPollRef.current = setInterval(() => fetchLogs(false), 2000);
    }
    return () => {
      if (logPollRef.current) {
        clearInterval(logPollRef.current);
        logPollRef.current = null;
      }
    };
  }, [showLogs, fetchLogs]);

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
  const officialDisplayWindow = resolveOfficialDisplayWindow(officialCacheWindow, singleCache);


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
          <div
            title="官方相关性只读取 rdagent_assets/factor_values，不选择非官方缓存"
            style={{
              padding: "8px 12px",
              fontSize: 12,
              fontWeight: 600,
              borderRadius: 8,
              background: "rgba(255,255,255,0.16)",
              border: "1px solid rgba(255,255,255,0.22)",
              color: "#fff",
              whiteSpace: "nowrap",
            }}
          >
            官方窗口 {officialDisplayWindow.start} ~ {officialDisplayWindow.end}
          </div>
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
          <label
            title="包含已禁用因子参与计算和展示"
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              fontSize: 12,
              cursor: isComputing ? "not-allowed" : "pointer",
              color: "#fff",
              opacity: isComputing ? 0.5 : 1,
              userSelect: "none",
              whiteSpace: "nowrap",
            }}
          >
            <input
              type="checkbox"
              checked={includeDisabled}
              onChange={(e) => onIncludeDisabledChange(e.target.checked)}
              disabled={isComputing}
              style={{ width: 14, height: 14, accentColor: "#7c3aed" }}
            />
            含禁用因子
          </label>
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
              : "计算相关性"}
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

      {/* ── 因子统计卡片组 ── */}
      {factorStats && (
        <div style={{ marginTop: 20 }}>
          <div style={{ fontSize: 12, opacity: 0.75, marginBottom: 8 }}>
            因子统计（官方缓存口径）
            <span style={{ marginLeft: 8, opacity: 0.7 }}>
              独立指标 / 相关性 / QE 回测共用 {officialDisplayWindow.start} ~ {officialDisplayWindow.end}
            </span>
          </div>
          <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
            {includeDisabled ? (
              <>
                {/* 全部因子 */}
                <StatCard
                  label="全部因子"
                  stats={factorStats.all}
                  color="rgba(255,255,255,0.15)"
                />
                {/* 启用因子 */}
                <StatCard
                  label="启用因子"
                  stats={factorStats.enabled}
                  color="rgba(16,185,129,0.15)"
                />
                {/* 禁用因子 */}
                <StatCard
                  label="禁用因子"
                  stats={factorStats.disabled}
                  color="rgba(239,68,68,0.15)"
                />
              </>
            ) : (
              /* 不含禁用 → 只显示启用因子 */
              <StatCard
                label="启用因子"
                stats={factorStats.enabled}
                color="rgba(16,185,129,0.15)"
              />
            )}
          </div>
        </div>
      )}

      {/* 相关性计算元数据 + 缓存信息 */}
      <div
        style={{
          display: "flex",
          gap: 32,
          marginTop: factorStats ? 16 : 20,
          flexWrap: "wrap",
        }}
      >
        {singleCache && (
          <>
            <StatItem label="缓存来源" value="官方缓存" />
            <StatItem label="共用时间段" value={`${officialDisplayWindow.start} ~ ${officialDisplayWindow.end}`} />
            <StatItem label="因子缓存文件" value={`${singleCache.cached_count} 个`} />
            {singleCache.disk_factor_count != null && (
              <StatItem label="磁盘/Meta" value={`${singleCache.disk_factor_count} / ${singleCache.meta_factor_count ?? "-"}`} />
            )}
            {(singleCache.orphan_parquet_count || 0) > 0 && (
              <StatItem label="待补元数据" value={`${singleCache.orphan_parquet_count} 个`} />
            )}
            {singleCache.total_size_mb > 0 && (
              <StatItem label="缓存大小" value={formatSize(singleCache.total_size_mb)} />
            )}
            {singleCache.date_range && (
              <StatItem label="缓存文件日期" value={singleCache.date_range} />
            )}
            {singleCache.cache_root && (
              <StatItem label="缓存目录" value={singleCache.cache_root} />
            )}
          </>
        )}
        {status && (
          <StatItem label="DB 记录对数" value={String(status.db_correlation_count)} />
        )}
        {status && status.uncorrelated_factor_count > 0 && (
          <StatItem label="待计算因子" value={String(status.uncorrelated_factor_count)} />
        )}
        {lc && (
          <>
            <StatItem label="数据截止" value={lc.as_of_date} />
            <StatItem label="矩阵因子数" value={String(lc.num_factors)} />
            <StatItem
              label="高相关对 (|r|>0.7)"
              value={String(
                status?.live_high_corr_count_07 ?? lc.num_high_corr_pairs
              )}
            />
            <StatItem label="计算耗时" value={`${lc.computation_time_sec.toFixed(1)}s`} />
            <StatItem
              label="最后计算"
              value={lc.created_at ? new Date(lc.created_at).toLocaleDateString("zh-CN") : "-"}
            />
          </>
        )}
      </div>

      {!lc && !factorStats && (
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
        相关性计算只使用 <strong>rdagent_assets/factor_values/single</strong> official factor-value parquet cache，
        与官方独立指标和 QE 回测共用同一份因子值缓存；默认展示全量数据集
        <strong>2018-08-01 ~ 2026-04-30</strong>，缺缓存的因子不会回退到非官方缓存或旧快照。
        如需补齐因子值，请在因子库提交官方全量因子计算后再重新计算相关性。
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

/** 统计卡片：显示 total / evaluated / correlation_cached 三行 */
function StatCard({
  label,
  stats,
  color,
}: {
  label: string;
  stats: FactorStat;
  color: string;
}) {
  const pct = (n: number, d: number) => (d > 0 ? ((n / d) * 100).toFixed(1) : "0.0");
  return (
    <div
      style={{
        flex: "1 1 200px",
        background: color,
        borderRadius: 10,
        padding: "12px 16px",
        minWidth: 200,
      }}
    >
      <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 8, opacity: 0.95 }}>
        {label} ({stats.total})
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        <StatRow
          label="独立指标"
          current={stats.evaluated}
          total={stats.total}
          pct={pct(stats.evaluated, stats.total)}
        />
        <StatRow
          label="相关性缓存"
          current={stats.correlation_cached}
          total={stats.total}
          pct={pct(stats.correlation_cached, stats.total)}
        />
      </div>
    </div>
  );
}

function StatRow({ label, current, total, pct }: { label: string; current: number; total: number; pct: string }) {
  const barPct = total > 0 ? (current / total) * 100 : 0;
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, opacity: 0.85 }}>
        <span>{label}</span>
        <span>{current}/{total} ({pct}%)</span>
      </div>
      <div style={{ background: "rgba(0,0,0,0.2)", borderRadius: 999, height: 4, marginTop: 3, overflow: "hidden" }}>
        <div
          style={{
            width: `${barPct}%`,
            height: 4,
            background: "#fff",
            borderRadius: 999,
            transition: "width 0.3s ease",
          }}
        />
      </div>
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
