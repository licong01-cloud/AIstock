"use client";

import React from "react";
import type { LoopDetail } from "./TaskRow";

/**
 * 展开的 Loop 面板 — 独立渲染，不经过 TaskRow。
 *
 * 当 loopsData / loopsLoading / loopSyncing 等状态变化时，
 * 只有这一个组件重渲染，100 个 TaskRow 完全不受影响。
 */

interface ExpandedLoopPanelProps {
  tid: string;
  loops: LoopDetail[] | undefined;
  isLoadingLoops: boolean;
  isLoopRefreshing: boolean;
  loopSyncing: Record<string, boolean>;
  syncedLoopIdSet: Set<number> | undefined;
  onRefreshLoopData: (taskId: string) => void;
  onSyncLoopFactors: (taskId: string, loopId: number) => void;
}

const emptyLoops: LoopDetail[] = [];

export default React.memo(function ExpandedLoopPanel({
  tid,
  loops: rawLoops,
  isLoadingLoops,
  isLoopRefreshing,
  loopSyncing,
  syncedLoopIdSet,
  onRefreshLoopData,
  onSyncLoopFactors,
}: ExpandedLoopPanelProps) {
  const loops = rawLoops || emptyLoops;

  return (
    <tr style={{ borderBottom: "1px solid #f3f4f6" }}>
      <td colSpan={8} style={{ padding: 16, background: "#f9fafb", contain: "content" }}>
        {isLoadingLoops ? (
          <div style={{ textAlign: "center", color: "#6b7280" }}>加载LOOP详情中...</div>
        ) : loops.length === 0 ? (
          <div style={{ textAlign: "center", color: "#6b7280" }}>暂无LOOP数据</div>
        ) : (
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
              <div style={{ fontWeight: 600, fontSize: 13 }}>
                LOOP 详情 (共 {loops.length} 个)
                {loops.filter(l => l.exp_type === 'unknown').length > 0 && (
                  <span style={{ marginLeft: 8, fontSize: 11, color: "#dc2626" }}>
                    含 {loops.filter(l => l.exp_type === 'unknown').length} 个未完成LOOP
                  </span>
                )}
              </div>
              <button
                onClick={() => onRefreshLoopData(tid)}
                disabled={isLoopRefreshing}
                style={{
                  padding: "4px 12px", fontSize: 11,
                  cursor: isLoopRefreshing ? "wait" : "pointer",
                  background: "#3b82f6", color: "#fff", border: "none", borderRadius: 4, whiteSpace: "nowrap"
                }}
                title="仅用于正在执行的Task产生新LOOP时，强制从RD-Agent API获取最新LOOP数据"
              >
                {isLoopRefreshing ? "刷新中..." : "刷新LOOP数据"}
              </button>
            </div>
            <LoopTable
              tid={tid}
              loops={loops}
              loopSyncing={loopSyncing}
              syncedLoopIdSet={syncedLoopIdSet}
              onSyncLoopFactors={onSyncLoopFactors}
            />
          </div>
        )}
      </td>
    </tr>
  );
});

// ── Loop 子表格 ──

interface LoopTableProps {
  tid: string;
  loops: LoopDetail[];
  loopSyncing: Record<string, boolean>;
  syncedLoopIdSet: Set<number> | undefined;
  onSyncLoopFactors: (taskId: string, loopId: number) => void;
}

const LoopTable = React.memo(function LoopTable({
  tid, loops, loopSyncing, syncedLoopIdSet, onSyncLoopFactors,
}: LoopTableProps) {
  return (
    <div style={{ overflowX: "auto", maxHeight: 600, overflowY: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, background: "#fff", tableLayout: "fixed" }}>
        <colgroup>
          <col style={{ width: 50 }} />
          <col style={{ width: 60 }} />
          <col style={{ width: 200 }} />
          <col style={{ width: 150 }} />
          <col style={{ width: 70 }} />
          <col style={{ width: 80 }} />
          <col style={{ width: 80 }} />
          <col style={{ width: 70 }} />
          <col style={{ width: 70 }} />
          <col style={{ width: 80 }} />
        </colgroup>
        <thead>
          <tr style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb", background: "#f3f4f6", position: "sticky", top: 0, zIndex: 1 }}>
            <th style={{ padding: 6 }}>ID</th>
            <th style={{ padding: 6 }}>类型</th>
            <th style={{ padding: 6 }}>假设描述</th>
            <th style={{ padding: 6 }} title="通过回测的因子（final_decision=True），括号中显示 通过数/总数">回测因子</th>
            <th style={{ padding: 6 }}>IC值</th>
            <th style={{ padding: 6 }}>年化收益</th>
            <th style={{ padding: 6 }}>最大回撤</th>
            <th style={{ padding: 6 }}>信息比率</th>
            <th style={{ padding: 6 }}>Decision</th>
            <th style={{ padding: 6 }}>操作</th>
          </tr>
        </thead>
        <tbody>
          {loops.map((loop) => (
            <LoopRow
              key={loop.loop_id}
              tid={tid}
              loop={loop}
              isSyncing={!!loopSyncing[`${tid}_${loop.loop_id}`]}
              isSynced={!!syncedLoopIdSet?.has(loop.loop_id)}
              onSyncLoopFactors={onSyncLoopFactors}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
});

// ── 单行 Loop ──

const loopRowTr: React.CSSProperties = { borderBottom: "1px solid #f3f4f6" };
const cellPad: React.CSSProperties = { padding: 6 };
const cellType: React.CSSProperties = { padding: 6, fontSize: 10 };
const cellHypo: React.CSSProperties = { padding: 6, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontSize: 11 };
const cellFactor: React.CSSProperties = { padding: 6, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontFamily: "monospace", fontSize: 10 };
const cellDrawdown: React.CSSProperties = { padding: 6, color: "#ef4444" };
const cellDecisionTrue: React.CSSProperties = { padding: 6, fontWeight: 600, color: "#10b981" };
const cellDecisionFalse: React.CSSProperties = { padding: 6, fontWeight: 600, color: "#6b7280" };
const syncedBadge: React.CSSProperties = { padding: "2px 8px", fontSize: 10, background: "#10b981", color: "#fff", borderRadius: 4 };

interface LoopRowProps {
  tid: string;
  loop: LoopDetail;
  isSyncing: boolean;
  isSynced: boolean;
  onSyncLoopFactors: (taskId: string, loopId: number) => void;
}

const LoopRow = React.memo(function LoopRow({ tid, loop, isSyncing, isSynced, onSyncLoopFactors }: LoopRowProps) {
  let expTypeShort = "-";
  if (loop.exp_type) {
    if (loop.exp_type.includes("Factor")) expTypeShort = "factor";
    else if (loop.exp_type.includes("Model")) expTypeShort = "model";
    else if (loop.exp_type === "unknown" || loop.exp_type === "error") expTypeShort = loop.exp_type;
  }

  let factorNames = "-";
  let hypothesisSummary = "-";

  if (expTypeShort === "factor") {
    const testedCount = loop.tested_count ?? (loop.hypothesis ? loop.hypothesis.split('; ').filter(Boolean).length : 0);
    const totalCount = loop.total_count ?? testedCount;
    const countLabel = totalCount > testedCount ? ` (${testedCount}/${totalCount})` : testedCount > 0 ? ` (${testedCount})` : "";
    factorNames = (loop.hypothesis || "-") + countLabel;
    if (loop.reason) {
      let cleaned = loop.reason.replace(/^\[.*?\]\s*/, '');
      hypothesisSummary = cleaned.length > 80 ? cleaned.substring(0, 80) + '...' : cleaned;
    }
  } else if (expTypeShort === "model") {
    const modelName = loop.hypothesis || "-";
    factorNames = modelName;
    hypothesisSummary = modelName !== "-" ? `模型: ${modelName}` : "-";
  }

  return (
    <tr style={loopRowTr}>
      <td style={cellPad}>{loop.loop_id}</td>
      <td style={cellType}>{expTypeShort}</td>
      <td style={cellHypo} title={loop.reason || ""}>
        {hypothesisSummary}
      </td>
      <td style={cellFactor} title={factorNames}>
        {factorNames}
      </td>
      <td style={{ padding: 6, color: loop.valid_score && loop.valid_score > 0 ? "#10b981" : "#ef4444" }}>
        {loop.valid_score !== null && loop.valid_score !== undefined ? loop.valid_score.toFixed(5) : "-"}
      </td>
      <td style={{ padding: 6, color: loop.annualized_return && loop.annualized_return > 0 ? "#10b981" : "#ef4444" }}>
        {loop.annualized_return !== null && loop.annualized_return !== undefined ? (loop.annualized_return * 100).toFixed(2) + "%" : "-"}
      </td>
      <td style={cellDrawdown}>
        {loop.max_drawdown !== null && loop.max_drawdown !== undefined ? (loop.max_drawdown * 100).toFixed(2) + "%" : "-"}
      </td>
      <td style={{ padding: 6, color: loop.information_ratio && loop.information_ratio > 0 ? "#10b981" : "#6b7280" }}>
        {loop.information_ratio !== null && loop.information_ratio !== undefined ? loop.information_ratio.toFixed(3) : "-"}
      </td>
      <td style={(loop.feedback === true || loop.feedback === "true") ? cellDecisionTrue : cellDecisionFalse}>
        {(loop.feedback === true || loop.feedback === "true") ? "True" : (loop.feedback === false || loop.feedback === "false") ? "False" : (loop.feedback === null || loop.feedback === undefined) ? "-" : String(loop.feedback)}
      </td>
      <td style={cellPad}>
        {expTypeShort === "factor" && !(loop.feedback === true || loop.feedback === "true") && (
          isSynced ? (
            <span style={syncedBadge}>
              已同步
            </span>
          ) : (
            <button
              onClick={() => onSyncLoopFactors(tid, loop.loop_id)}
              disabled={isSyncing}
              style={{
                padding: "2px 8px", fontSize: 10, background: "#3b82f6", color: "#fff",
                border: "none", borderRadius: 4, cursor: isSyncing ? "wait" : "pointer",
                opacity: isSyncing ? 0.6 : 1,
              }}
              title="将此Loop的因子同步到因子库"
            >
              {isSyncing ? "同步中..." : "同步因子"}
            </button>
          )
        )}
      </td>
    </tr>
  );
});
