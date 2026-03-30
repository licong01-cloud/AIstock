"use client";

import React from "react";

export type LoopDetail = {
  loop_id: number;
  hypothesis: string | null;
  reason: string | null;
  exp_type: string | null;
  tested_count?: number | null;
  total_count?: number | null;
  valid_score: number | null;
  test_score: number | null;
  mle_score: any;
  feedback: boolean | "true" | "false" | null;
  is_sota: boolean;
  annualized_return: number | null;
  max_drawdown: number | null;
  information_ratio: number | null;
};

export type SyncCandidateItem = {
  task_id: string;
  node_id?: string;
  node_display_name?: string;
  latest?: any;
  summary?: any;
  local?: any;
  discovery?: any;
  is_synced?: boolean;
  is_enabled_for_selection?: boolean;
};

export interface TaskRowCallbacks {
  onToggleLoopDetails: (taskId: string) => void;
  onToggleEnable: (taskId: string, nextValue: boolean) => void;
  onRefreshTask: (taskId: string) => void;
  onLoadWorkspace: (taskId: string) => void;
  onOpenTaskDetail: (taskId: string) => void;
}

export interface TaskRowProps {
  item: SyncCandidateItem;
  isSelected: boolean;
  isExpanded: boolean;
  isTaskRefreshing: boolean;
  isWorkspaceLoading: boolean;
  onSelectChange: (taskId: string, checked: boolean) => void;
  callbacks: TaskRowCallbacks;
}

/**
 * TaskRow — 只渲染主行（不含展开面板）。
 *
 * Props 全部引用稳定：
 *   - item: 来自 pagedItems，父不重渲染则引用不变
 *   - isSelected: boolean primitive
 *   - isExpanded: boolean primitive（只有 1-2 行变化）
 *   - isTaskRefreshing / isWorkspaceLoading: boolean primitive
 *   - callbacks: useMemo 打包，引用永不变
 *   - onSelectChange: 来自父组件 props，引用稳定
 *
 * 展开的 Loop 面板由 TaskTableSection 直接渲染 ExpandedLoopPanel，
 * 不经过 TaskRow，这样 Loop 相关状态变化完全不触发 TaskRow memo 检查。
 */
export default React.memo(function TaskRow({
  item,
  isSelected,
  isExpanded,
  isTaskRefreshing,
  isWorkspaceLoading,
  onSelectChange,
  callbacks,
}: TaskRowProps) {
  const tid = String(item.task_id || "");
  const local = item.local || null;

  const {
    onToggleLoopDetails,
    onToggleEnable,
    onRefreshTask,
    onLoadWorkspace,
    onOpenTaskDetail,
  } = callbacks;

  // 同步状态
  let syncStatus = "未同步";
  if (local?.sync_status) {
    if (local.sync_status === "success") syncStatus = "同步成功";
    else if (local.sync_status === "failed") syncStatus = "同步失败";
    else syncStatus = local.sync_status;
  } else if (item.is_synced) {
    syncStatus = "同步成功";
  }
  const isSynced = syncStatus === "同步成功";

  const enabled = !!(local?.is_enabled_for_selection ?? item.is_enabled_for_selection);
  const discovery = item.discovery || null;
  const hasSota = discovery?.has_sota;
  const sotaCount = discovery?.sota_factors_count;
  const v2 = discovery?.v2_alignment || null;
  const alphaCount = v2?.alpha_factors_count;
  const modelFeatureCount = v2?.model_feature_count;
  const isAligned = v2?.is_aligned;
  const v2HistLen = v2?.hist_len;

  const remoteSummary = (item as { remote?: unknown }).remote;
  const summaryData = item.summary ?? item.latest ?? remoteSummary ?? {};
  const summaryErr = (summaryData as any)?.error;

  // 构建Task概要信息
  let taskSummaryLine = "";
  if (summaryErr) {
    taskSummaryLine = `ERROR: ${String(summaryErr).slice(0, 80)}`;
  } else if (v2) {
    const parts: string[] = [];
    if (v2HistLen) parts.push(`LOOP: ${v2HistLen}个`);
    if (typeof sotaCount === "number" && sotaCount > 0) parts.push(`SOTA因子: ${sotaCount}个`);
    if (typeof alphaCount === "number") parts.push(`Alpha: ${alphaCount}个`);
    if (typeof modelFeatureCount === "number") parts.push(`特征: ${modelFeatureCount}`);
    taskSummaryLine = parts.length > 0 ? parts.join(" | ") : "V2已检查";
  } else {
    const loopCountFromSummary = (summaryData as any)?.loop_count;
    taskSummaryLine = loopCountFromSummary ? `LOOP: ${loopCountFromSummary}个` : "暂无数据";
  }

  // SOTA Loop 摘要指标
  const sotaLoopSummary = discovery?.sota_loop_summary || null;

  return (
    <tr style={{ borderBottom: isExpanded ? "none" : "1px solid #f3f4f6" }}>
      <td style={{ padding: 8 }}>
        <input
          type="checkbox"
          aria-label={`选择 task ${tid}`}
          checked={isSelected}
          disabled={isSynced}
          onChange={(e) => onSelectChange(tid, e.target.checked)}
        />
      </td>
      <td style={{ padding: 8, fontFamily: "monospace" }}>{tid}</td>
      <td style={{ padding: 8 }}>
        {item.node_id ? (
          <span style={{
            padding: "1px 6px", borderRadius: 3, fontSize: 10, fontWeight: 600,
            background: item.node_id.includes("wsl") ? "#dbeafe" : "#fce7f3",
            color: item.node_id.includes("wsl") ? "#1d4ed8" : "#be185d",
            whiteSpace: "nowrap",
          }}>
            {item.node_display_name || item.node_id}
          </span>
        ) : (
          <span style={{ color: "#9ca3af", fontSize: 10 }}>-</span>
        )}
      </td>
      <td style={{ padding: 8 }}>
        {hasSota === null || hasSota === undefined ? (
          <span style={{ color: "#9ca3af" }}>未检查</span>
        ) : hasSota ? (
          <div>
            <div style={{ color: "#10b981", fontWeight: 600 }}>SOTA: {sotaCount ?? 0}</div>
            {typeof alphaCount === "number" && <div style={{ fontSize: 10, color: "#6b7280" }}>Alpha: {alphaCount}</div>}
            {typeof modelFeatureCount === "number" && <div style={{ fontSize: 10, color: "#6b7280" }}>特征: {modelFeatureCount}</div>}
          </div>
        ) : (
          <span style={{ color: "#9ca3af" }}>无SOTA</span>
        )}
      </td>
      <td style={{ padding: 8 }}>
        {isAligned === true ? (
          <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, background: "#d1fae5", color: "#065f46", fontWeight: 600 }}>ALIGNED</span>
        ) : isAligned === false ? (
          <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, background: "#fef3c7", color: "#92400e", fontWeight: 600 }}>MISMATCH</span>
        ) : (
          <span style={{ color: "#9ca3af", fontSize: 11 }}>-</span>
        )}
      </td>
      <td style={{ padding: 8 }}>
        <span style={{
          padding: "2px 8px", borderRadius: 4, fontSize: 11,
          background: syncStatus === "同步成功" ? "#d1fae5" : syncStatus === "同步失败" ? "#fee2e2" : "#f3f4f6",
          color: syncStatus === "同步成功" ? "#065f46" : syncStatus === "同步失败" ? "#991b1b" : "#374151"
        }}>
          {syncStatus}
        </span>
      </td>
      <td style={{ padding: 8 }}>
        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <input
            type="checkbox"
            checked={enabled}
            onChange={(e) => onToggleEnable(tid, e.target.checked)}
          />
          <span>{enabled ? "已加入" : "未加入"}</span>
        </label>
      </td>
      <td style={{ padding: 8, fontSize: 11, overflow: "hidden" }}>
        <div>{taskSummaryLine}</div>
        {sotaLoopSummary && (
          <div style={{ marginTop: 3, display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center" }}>
            <span style={{ padding: "1px 5px", borderRadius: 3, fontSize: 10, background: "#dbeafe", color: "#1e40af", fontWeight: 600 }}>
              SOTA Loop: {sotaLoopSummary.sota_loops_count ?? 0}
            </span>
            {sotaLoopSummary.final_sota_ic != null && (
              <span style={{
                padding: "1px 5px", borderRadius: 3, fontSize: 10,
                background: sotaLoopSummary.final_sota_ic > 0.04 ? "#d1fae5" : "#fef3c7",
                color: sotaLoopSummary.final_sota_ic > 0.04 ? "#065f46" : "#92400e",
              }}>
                IC: {(sotaLoopSummary.final_sota_ic as number).toFixed(4)}
              </span>
            )}
            {sotaLoopSummary.final_sota_annualized_return != null && (
              <span style={{
                padding: "1px 5px", borderRadius: 3, fontSize: 10,
                background: sotaLoopSummary.final_sota_annualized_return > 0 ? "#d1fae5" : "#fee2e2",
                color: sotaLoopSummary.final_sota_annualized_return > 0 ? "#065f46" : "#991b1b",
              }}>
                年化: {((sotaLoopSummary.final_sota_annualized_return as number) * 100).toFixed(1)}%
              </span>
            )}
            {sotaLoopSummary.final_sota_max_drawdown != null && (
              <span style={{ padding: "1px 5px", borderRadius: 3, fontSize: 10, background: "#fef3c7", color: "#92400e" }}>
                回撤: {((sotaLoopSummary.final_sota_max_drawdown as number) * 100).toFixed(1)}%
              </span>
            )}
          </div>
        )}
      </td>
      <td style={{ padding: 8 }}>
        <div style={{ display: "flex", gap: 4, flexWrap: "nowrap" }}>
          <button
            onClick={() => onRefreshTask(tid)}
            disabled={isTaskRefreshing}
            style={{
              padding: "4px 10px", fontSize: 11,
              cursor: isTaskRefreshing ? "wait" : "pointer",
              background: "#10b981", color: "#fff", border: "none", borderRadius: 4, whiteSpace: "nowrap"
            }}
            title="清除数据库缓存并重新获取"
          >
            {isTaskRefreshing ? "刷新中..." : "刷新Task"}
          </button>
          <button
            onClick={() => onToggleLoopDetails(tid)}
            style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", whiteSpace: "nowrap" }}
          >
            {isExpanded ? "收起" : "LOOP详情"}
          </button>
          <button
            onClick={() => onLoadWorkspace(tid)}
            disabled={isWorkspaceLoading}
            style={{ padding: "4px 10px", fontSize: 11, cursor: isWorkspaceLoading ? "wait" : "pointer", whiteSpace: "nowrap" }}
          >
            {isWorkspaceLoading ? "加载中..." : "Workspace"}
          </button>
          <button
            onClick={() => onOpenTaskDetail(tid)}
            style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", whiteSpace: "nowrap" }}
          >
            Task详情
          </button>
        </div>
      </td>
    </tr>
  );
});
