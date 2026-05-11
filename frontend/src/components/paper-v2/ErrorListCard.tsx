"use client";

import { useState } from "react";
import type { JsonObject } from "@/lib/paper-v2/types";
import JsonPanel from "./JsonPanel";

const ERROR_CODE_LABELS: Record<string, string> = {
  READINESS_FAILED: "就绪检查未通过",
  MANIFEST_MISMATCH: "Manifest 与运行时不匹配",
  EXECUTION_POLICY_MISMATCH: "执行策略与策略包不匹配",
  RUNTIME_PROFILE_MISMATCH: "运行配置与策略包不匹配",
  HMM_COEFFICIENTS_MISSING: "HMM 系数缺失",
  STALE_INITIAL_BACKTEST_MODEL: "初始回测模型已过期",
  LIVE_INFERENCE_PREFLIGHT_FAILED: "实时推理前置检查失败",
  INVALID_STATE_TRANSITION: "状态切换被禁止",
  PAPER_DATA_BLOCKED: "模拟盘数据阻断",
  TRADE_CALENDAR_MISSING: "交易日历缺失",
  MINUTE_BAR_MISSING: "分钟线缺失",
  LIMIT_PRICE_MISSING: "涨跌停价格缺失",
  SUSPEND_FLAG_MISSING: "停牌标记缺失",
  SELECTION_ARTIFACT_MISSING: "选股工件缺失",
  POINT_IN_TIME_VIOLATION: "时点口径违规",
  CASH_INSUFFICIENT: "现金不足",
  POSITION_NOT_FOUND: "未找到对应持仓",
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

function ContextPreview({ context }: { context: unknown }) {
  if (!context || typeof context !== "object") return null;
  const obj = context as Record<string, unknown>;
  const entries = Object.entries(obj).filter(([, value]) => value !== null && value !== undefined && value !== "");
  if (!entries.length) return null;
  return (
    <ul className="pv2-error-card-context">
      {entries.slice(0, 6).map(([key, value]) => (
        <li key={key}>
          <span className="pv2-error-card-context-key">{key}</span>
          <span className="pv2-error-card-context-value">
            {typeof value === "object" ? JSON.stringify(value) : String(value)}
          </span>
        </li>
      ))}
      {entries.length > 6 ? <li className="pv2-muted">… 共 {entries.length} 个字段，详情见下方折叠区</li> : null}
    </ul>
  );
}

function ErrorRow({ row }: { row: JsonObject }) {
  const [open, setOpen] = useState(false);
  const code = String(row.error_code || row.code || "ERROR");
  const message = String(row.message || row.error_message || "");
  const stage = String(row.stage || row.error_stage || "");
  const createdAt = formatTime(row.created_at || row.timestamp);
  const ctx = (row.context || row.error_context) as unknown;

  return (
    <div className="pv2-error-card-row">
      <div className="pv2-error-card-head">
        <span className="pv2-badge pv2-badge-danger" title={code}>{codeLabel(code)}</span>
        {stage ? <span className="pv2-chip">阶段 {stage}</span> : null}
        <span className="pv2-muted">{createdAt}</span>
      </div>
      {message ? <div className="pv2-error-card-message">{message}</div> : null}
      <ContextPreview context={ctx} />
      {ctx ? (
        <button
          className="pv2-link-button"
          onClick={() => setOpen((value) => !value)}
          type="button"
        >
          {open ? "隐藏原始 JSON（开发者）" : "显示原始 JSON（开发者）"}
        </button>
      ) : null}
      {open && ctx ? <JsonPanel value={ctx} /> : null}
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
