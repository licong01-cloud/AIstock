"use client";

import { useState } from "react";
import type { JsonObject } from "@/lib/paper-v2/types";

const ERROR_CODE_LABELS: Record<string, string> = {
  READINESS_FAILED: "就绪检查未通过",
  MANIFEST_MISMATCH: "Manifest 不匹配",
  EXECUTION_POLICY_MISMATCH: "执行策略不匹配",
  RUNTIME_PROFILE_MISMATCH: "运行配置不匹配",
  HMM_COEFFICIENTS_MISSING: "HMM 系数缺失",
  STALE_INITIAL_BACKTEST_MODEL: "初始回测模型已过期",
  LIVE_INFERENCE_PREFLIGHT_FAILED: "实时推理前置检查失败",
  INVALID_STATE_TRANSITION: "状态切换被拒绝",
  PAPER_DATA_BLOCKED: "模拟盘数据阻断",
  TRADE_CALENDAR_MISSING: "交易日历缺失",
  MINUTE_BAR_MISSING: "分钟线缺失",
  LIMIT_PRICE_MISSING: "涨跌停价格缺失",
  SUSPEND_FLAG_MISSING: "停牌标记缺失",
  SELECTION_ARTIFACT_MISSING: "选股工件缺失",
  POINT_IN_TIME_VIOLATION: "时点口径违规",
  CASH_INSUFFICIENT: "现金不足",
  ORDER_REJECTED: "订单被拒绝",
  RISK_BLOCKED: "风控阻断",
};

function codeLabel(code: string): string {
  return ERROR_CODE_LABELS[code] || code;
}

function formatTime(value: unknown): string {
  if (!value) return "-";
  return String(value).slice(0, 19).replace("T", " ");
}

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function contextSummary(context: unknown): string {
  if (!context || typeof context !== "object") return "-";
  const entries = Object.entries(context as Record<string, unknown>)
    .filter(([, value]) => value !== null && value !== undefined && value !== "")
    .slice(0, 6);
  if (!entries.length) return "-";
  return entries.map(([key, value]) => `${key}=${typeof value === "object" ? safeJson(value) : String(value)}`).join("；");
}

function diagnosticText(row: JsonObject): string {
  return [
    "Paper v2 持久化错误诊断",
    `错误码: ${String(row.error_code || row.code || "ERROR")}`,
    `阶段: ${String(row.stage || row.error_stage || "-")}`,
    `时间: ${formatTime(row.created_at || row.timestamp)}`,
    `说明: ${String(row.message || row.error_message || "-")}`,
    "",
    safeJson(row),
  ].join("\n");
}

function ErrorRow({ row }: { row: JsonObject }) {
  const [copied, setCopied] = useState(false);
  const code = String(row.error_code || row.code || "ERROR");
  const message = String(row.message || row.error_message || "");
  const stage = String(row.stage || row.error_stage || "");
  const ctx = row.context || row.error_context;

  async function copy() {
    await navigator.clipboard.writeText(diagnosticText(row));
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1600);
  }

  return (
    <div className="pv2-error-card-row">
      <div className="pv2-error-card-head">
        <span className="pv2-badge pv2-badge-danger" title={code}>{codeLabel(code)}</span>
        {stage ? <span className="pv2-chip">阶段 {stage}</span> : null}
        <span className="pv2-muted">{formatTime(row.created_at || row.timestamp)}</span>
      </div>
      {message ? <div className="pv2-error-card-message">{message}</div> : null}
      <div className="pv2-muted">诊断摘要：{contextSummary(ctx)}</div>
      <button className="pv2-link-button" onClick={copy} type="button">
        {copied ? "已复制" : "复制诊断信息给 Codex"}
      </button>
    </div>
  );
}

export default function ErrorListCard({
  rows,
  empty = "暂无持久化错误。",
}: {
  rows: JsonObject[];
  empty?: string;
}) {
  if (!rows.length) return <div className="pv2-muted">{empty}</div>;
  return (
    <div className="pv2-error-card-list">
      {rows.map((row, index) => (
        <ErrorRow row={row} key={String(row.error_id || row.id || `${row.error_code || "err"}-${index}`)} />
      ))}
    </div>
  );
}
