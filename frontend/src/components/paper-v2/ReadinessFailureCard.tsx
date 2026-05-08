"use client";

import { useState } from "react";
import { statusLabel, statusTone } from "@/lib/paper-v2/format";
import type { ReadinessCheck, ReadinessResult } from "@/lib/paper-v2/types";
import JsonPanel from "./JsonPanel";

const CHECK_NAME_LABELS: Record<string, string> = {
  package_status: "策略包状态",
  execution_policy: "执行策略",
  manifest_pinning: "Manifest 冻结",
  data_source: "数据源",
  selection_artifact: "选股工件",
  hmm_coefficients: "HMM 系数",
  trade_calendar: "交易日历",
  point_in_time: "时点口径",
  industry_blacklist: "行业黑名单",
  tradability: "可交易性",
  cash_buffer: "现金保留",
  top_k: "TopK",
};

function checkLabel(name: string): string {
  return CHECK_NAME_LABELS[name] || name;
}

function ContextEntries({ context }: { context: Record<string, unknown> }) {
  const entries = Object.entries(context).filter(([, value]) => value !== null && value !== undefined && value !== "");
  if (!entries.length) return null;
  return (
    <ul className="pv2-readiness-context">
      {entries.map(([key, value]) => (
        <li key={key}>
          <span className="pv2-readiness-context-key">{key}</span>
          <span className="pv2-readiness-context-value">
            {typeof value === "object" ? JSON.stringify(value) : String(value)}
          </span>
        </li>
      ))}
    </ul>
  );
}

function CheckRow({ check }: { check: ReadinessCheck }) {
  const tone = statusTone(check.status);
  const icon = tone === "success" ? "✓" : tone === "danger" ? "✗" : tone === "warning" ? "!" : "·";
  return (
    <div className={`pv2-readiness-row pv2-readiness-row-${tone}`}>
      <div className="pv2-readiness-row-head">
        <span className={`pv2-readiness-icon pv2-readiness-icon-${tone}`} aria-hidden="true">{icon}</span>
        <span className="pv2-readiness-name">{checkLabel(check.check_name)}</span>
        <span className={`pv2-badge pv2-badge-${tone}`} title={String(check.status || "")}>{statusLabel(check.status)}</span>
      </div>
      {check.context ? <ContextEntries context={check.context} /> : null}
    </div>
  );
}

export default function ReadinessFailureCard({
  result,
  title = "就绪检查结果",
}: {
  result: ReadinessResult;
  title?: string;
}) {
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const checks = result.checks || [];
  const failed = checks.filter((item) => statusTone(item.status) === "danger");
  const warned = checks.filter((item) => statusTone(item.status) === "warning");
  const passed = checks.length - failed.length - warned.length;

  return (
    <div className="pv2-readiness-card">
      <div className="pv2-readiness-head">
        <strong>{title}</strong>
        <span className="pv2-muted">交易日 {result.trade_date} / 数据源 {result.data_source}</span>
      </div>
      <div className="pv2-readiness-summary">
        <span className="pv2-readiness-tally pv2-readiness-tally-success">通过 {passed}</span>
        <span className="pv2-readiness-tally pv2-readiness-tally-warning">告警 {warned.length}</span>
        <span className="pv2-readiness-tally pv2-readiness-tally-danger">阻断 {failed.length}</span>
        <span className="pv2-muted">候选 {result.raw_candidate_count} → 可交易 {result.tradable_candidate_count}（剔除 {result.excluded_candidate_count}）→ 目标 {result.target_count} / 订单意图 {result.order_intent_count}</span>
      </div>
      <div className="pv2-readiness-list">
        {checks.length === 0 ? <div className="pv2-muted">无检查项。</div> : checks.map((check, index) => <CheckRow check={check} key={`${check.check_name}-${index}`} />)}
      </div>
      <button
        className="pv2-link-button"
        type="button"
        onClick={() => setAdvancedOpen((value) => !value)}
        style={{ marginTop: 8 }}
      >
        {advancedOpen ? "隐藏原始 JSON（开发者）" : "显示原始 JSON（开发者）"}
      </button>
      {advancedOpen ? <JsonPanel value={result as unknown} /> : null}
    </div>
  );
}
