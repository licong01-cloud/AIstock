"use client";

import { useEffect, useRef, useState } from "react";
import { advisoryApi, type AdvisoryEconomicEntryProjection, type AdvisoryEconomicEntryResearch, type AdvisoryEconomicEntryStatus } from "@/lib/api/advisory";

const stateLabels = {
  ACCEPTABLE_PRICE_SET: "可接受价格集合（不保证成交或盈利）",
  NO_ACCEPTABLE_PRICE: "已知条件下无合适价格",
  PARTIAL_UNKNOWN: "已知价格均不合适，仍有未知条件",
  UNAVAILABLE: "估值未知，不能视为不推荐",
  QUERY_DOMAIN_UNAVAILABLE: "查询价格域不可用",
  RISK_CONTRACT_UNCONFIGURED: "未配置风险合同",
};

const number = (value: number | null | undefined, suffix = "") =>
  typeof value === "number" && Number.isFinite(value) ? `${value.toFixed(2)}${suffix}` : "未知";

const roleStateLabels = {
  NOT_CONFIGURED: "尚无经过经济确认的买价模型，当前不输出实盘价格建议。",
  DISABLED: "独立价格角色已停用，不输出当前建议。",
  NOT_EFFECTIVE: "独立价格角色尚未生效。",
  NOT_CAPTURED: "已有合格角色，但该目标日尚无原生冻结产物；读取不会补算。",
  PUBLISHED: "已启用的日频价格条件；不是下单指令，也不保证收益或成交。",
  NO_CANDIDATES: "该日原候选名单为空，没有价格建议。",
  STALE: "历史产物已过 T 开盘有效期，不能作为当前买入建议。",
};
const digest = (value: unknown) => typeof value === "string" && /^[0-9a-f]{64}$/.test(value);

function validEconomicRow(row: AdvisoryEconomicEntryProjection, programId: string, target: string, formal = false) {
  return row?.schema_version === "economic_entry_daily_projection_v1" && row.role === "ENTRY_VALUE"
    && row.objective_contract === "RISK_MANAGED_ADVISORY" && row.deployable === formal
    && row.decision_use === (formal ? "ADVISORY_ONLY" : "NAVIGATION_ONLY")
    && row.evidence_state === (formal ? "CONFIRMED_ENTRY_VALUE" : "RESEARCH_NAVIGATION")
    && (formal ? digest(row.role_binding_sha256) : row.role_binding_sha256 == null)
    && row.prediction_input?.program_id === programId && row.prediction_input.target_date === target
    && typeof row.prediction_input.binding_version_id === "string" && !!row.prediction_input.binding_version_id.trim()
    && /^\d{6}\.(SH|SZ|BJ)$/.test(row.prediction_input.instrument)
    && Number.isInteger(row.prediction_input.selection_rank) && row.prediction_input.selection_rank >= 1 && row.prediction_input.selection_rank <= 20
    && ["RECOVERED_LIMITED", "NATIVE_COMPLETE"].includes(row.prediction_input.source_evidence)
    && (!formal || (row.prediction_input.source_evidence === "NATIVE_COMPLETE" && row.prediction_input.evidence_level === "PROSPECTIVE_INPUT"
      && !!row.prediction_input.run_id && !!row.prediction_input.list_id && row.prediction_input.restored_cohort_sha256 == null
      && row.risk_budget?.reference_use === "EXPLICIT_BUSINESS_CONFIGURATION" && digest(row.risk_budget.configuration_sha256)))
    && Object.prototype.hasOwnProperty.call(stateLabels, row.recommendation_status)
    && ["COMPLETE", "PARTIAL", "UNAVAILABLE"].includes(row.availability)
    && [row.model_bundle_sha256, row.original_advice_sha256, row.projection_sha256].every((value) => typeof value === "string" && /^[0-9a-f]{64}$/.test(value))
    && Array.isArray(row.acceptable_price_intervals) && row.acceptable_price_intervals.every((value) =>
      [value.minimum_cny, value.maximum_cny, value.grid_step_cny, value.expected_net_return_min_bps,
        value.expected_net_return_max_bps, value.entry_net_max_loss_q90_max_bps].every((value) => typeof value === "number" && Number.isFinite(value))
      && value.minimum_cny > 0 && value.minimum_cny <= value.maximum_cny && value.grid_step_cny > 0
      && Number.isInteger(value.node_count) && value.node_count > 0)
    && Number.isInteger(row.query_node_count) && row.query_node_count >= 0 && row.query_node_count <= 5000
    && row.node_status_counts != null && Object.values(row.node_status_counts).every((count) => Number.isInteger(count) && count >= 0)
    && Object.values(row.node_status_counts).reduce((sum, count) => sum + count, 0) === row.query_node_count;
}

function validRoleStatus(value: AdvisoryEconomicEntryStatus, programId: string, target?: string) {
  if (value.schema_version !== "economic_entry_consumer_status_v1" || value.program_id !== programId
      || value.target_date !== (target || null) || value.role !== "ENTRY_VALUE" || value.objective_contract !== "RISK_MANAGED_ADVISORY"
      || !Object.prototype.hasOwnProperty.call(roleStateLabels, value.status) || !Array.isArray(value.qualification_gaps)
      || !Array.isArray(value.advice) || value.advice.length > 20
      || new Set(value.advice.map((row) => row?.prediction_input?.instrument)).size !== value.advice.length
      || new Set(value.advice.map((row) => row?.prediction_input?.selection_rank)).size !== value.advice.length) return false;
  if (["NOT_CONFIGURED", "DISABLED"].includes(value.status)) {
    return value.evidence_state === "UNCONFIRMED" && value.deployable === false && value.advice.length === 0 && !value.automatic_capture_enabled;
  }
  const published = ["PUBLISHED", "NO_CANDIDATES", "STALE"].includes(value.status);
  if (value.evidence_state !== "CONFIRMED_ENTRY_VALUE" || !value.automatic_capture_enabled || value.qualification_gaps.length !== 0
      || !/^advecserve_[0-9a-f]{24}$/.test(value.bundle_id || "") || !digest(value.role_binding_sha256) || !digest(value.pointer_sha256)
      || !value.binding_version_id || value.deployable !== ["PUBLISHED", "NO_CANDIDATES"].includes(value.status)) return false;
  if (!published) return value.advice.length === 0;
  return value.decision_use === "ADVISORY_ONLY" && digest(value.original_batch_sha256) && !!value.resolved_target_date
    && value.advice.every((row) => validEconomicRow(row, programId, value.resolved_target_date!, true)
      && row.role_binding_sha256 === value.role_binding_sha256 && row.prediction_input.binding_version_id === value.binding_version_id)
    && (value.status !== "NO_CANDIDATES" || value.advice.length === 0);
}

function EconomicRows({ rows }: { rows: AdvisoryEconomicEntryProjection[] }) {
  return <>{rows.map((row) => <div className="mt-2 border-t pt-2" key={row.prediction_input.instrument}>
    <strong>{row.prediction_input.instrument} · {stateLabels[row.recommendation_status]}</strong>
    <p className="text-sm">D {row.prediction_input.decision_date} → T {row.prediction_input.target_date}；{row.prediction_input.source_evidence}；{row.availability}；{row.reason_code || "无额外错误"}</p>
    {row.acceptable_price_intervals.map((interval, index) => <p key={index}>
      {number(interval.minimum_cny)}～{number(interval.maximum_cny)} CNY（{number(interval.grid_step_cny)} 步长 / {interval.node_count}节点）；
      条件净收益估值 {number(interval.expected_net_return_min_bps)}～{number(interval.expected_net_return_max_bps, " bps")}；入场锚定损失 q90 ≤ {number(interval.entry_net_max_loss_q90_max_bps, " bps")}
    </p>)}
    <p className="text-sm">风险预算：{row.risk_budget ? `${row.risk_budget.reference_use} / ${number(row.risk_budget.maximum_loss_bps, " bps")}` : "未配置"}；节点状态：{Object.entries(row.node_status_counts).map(([key, count]) => `${key} ${count}`).join(" / ") || "无可估值节点"}</p>
    <details><summary>身份与限制</summary><pre className="overflow-auto text-xs">{JSON.stringify({ input: row.prediction_input, model: row.model_bundle_sha256, role: row.role_binding_sha256, advice: row.original_advice_sha256, projection: row.projection_sha256 }, null, 2)}</pre></details>
  </div>)}</>;
}

export function EconomicEntryValueCard({ programId, targetTradeDate }: { programId?: string; targetTradeDate?: string }) {
  const [status, setStatus] = useState<AdvisoryEconomicEntryStatus | null>(null);
  const [statusError, setStatusError] = useState("");
  const [loading, setLoading] = useState(false);
  const [bundleId, setBundleId] = useState("");
  const [researchTarget, setResearchTarget] = useState("");
  const [research, setResearch] = useState<AdvisoryEconomicEntryResearch | null>(null);
  const [researchError, setResearchError] = useState("");
  const [researchLoading, setResearchLoading] = useState(false);
  const sequence = useRef(0);

  useEffect(() => {
    let cancelled = false;
    sequence.current += 1;
    setStatus(null);
    setStatusError("");
    setResearch(null);
    setResearchError("");
    setResearchLoading(false);
    setResearchTarget(targetTradeDate || "");
    if (!programId) { setLoading(false); return; }
    setLoading(true);
    advisoryApi.economicEntryStatus(programId, targetTradeDate).then((value) => {
      if (!validRoleStatus(value, programId, targetTradeDate)) {
        throw new Error("经济价格状态身份或证据不一致");
      }
      if (!cancelled) setStatus(value);
    }).catch((error: unknown) => {
      if (!cancelled) setStatusError(error instanceof Error ? error.message : "经济价格状态读取失败");
    }).finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [programId, targetTradeDate]);

  async function readResearch() {
    if (!programId) return;
    const request = ++sequence.current;
    setResearch(null);
    setResearchError("");
    setResearchLoading(true);
    try {
      const value = await advisoryApi.economicEntryResearch(programId, bundleId, researchTarget);
      if (value.program_id !== programId || value.bundle_id !== bundleId || value.target_date !== researchTarget
          || value.deployable !== false || !["RESEARCH_NAVIGATION", "NO_CANDIDATES", "NOT_CAPTURED"].includes(value.status)
          || !Array.isArray(value.advice) || value.advice.length > 20
          || new Set(value.advice.map((row) => row?.prediction_input?.instrument)).size !== value.advice.length
          || new Set(value.advice.map((row) => row?.prediction_input?.selection_rank)).size !== value.advice.length
          || (value.status !== "NOT_CAPTURED" && (value.evidence_state !== "RESEARCH_NAVIGATION" || value.decision_use !== "NAVIGATION_ONLY"))
          || value.advice.some((row) => !validEconomicRow(row, programId, researchTarget))) {
        throw new Error("研究价格产物身份或证据不一致");
      }
      if (sequence.current === request) setResearch(value);
    } catch (error: unknown) {
      if (sequence.current === request) setResearchError(error instanceof Error ? error.message : "研究价格读取失败");
    } finally {
      if (sequence.current === request) setResearchLoading(false);
    }
  }

  return <section className="mt-3 rounded-lg border bg-card p-4 text-card-foreground" data-testid="advisory-economic-entry-value">
    <h3 className="font-semibold">经济买入价格条件</h3>
    <p className="text-sm text-muted-foreground">RISK_MANAGED_ADVISORY · 日频 D 冻结、T 价格条件 · CNY。不同于开盘价分布；不预测最佳分钟买卖点。</p>
    {!programId ? <p>未选择荐股任务</p> : loading ? <p>正在读取经济价格状态…</p> : statusError ?
      <p role="alert" data-testid="economic-entry-status-error">{statusError}；不回退规则价格或旧模型。</p> : status ? <div data-testid="economic-entry-role-status">
        <p>{status.status}：{roleStateLabels[status.status]}</p>
        <p className="text-sm">目标日：{status.resolved_target_date || status.target_date || "未指定"}；{status.reason_code}</p>
        {status.status === "STALE" ? <p className="text-sm">下列区间仅展示已过期的原始估值，不进行新预测或替代当前建议。</p> : null}
        <EconomicRows rows={status.advice} />
        <details><summary>剩余资格缺口</summary><ul>{status.qualification_gaps.map((gap) => <li key={gap}>{gap}</li>)}</ul></details>
      </div> : null}
    <details className="mt-3" data-testid="economic-entry-research-panel">
      <summary>只读查看已存在的探索产物（不用于实盘）</summary>
      <p className="text-sm">只读取指定 bundle / 日期，不执行捕获、训练或结算。均值不是盈利概率，风险 q90 不是收益覆盖率。</p>
      <div className="flex flex-wrap gap-2">
        <label>研究 bundle<input aria-label="经济价格研究bundle" className="pv2-input" value={bundleId} onChange={(event) => { sequence.current += 1; setBundleId(event.target.value); setResearch(null); setResearchLoading(false); }} /></label>
        <label>目标交易日<input aria-label="经济价格研究目标日" className="pv2-input" type="date" value={researchTarget} onChange={(event) => { sequence.current += 1; setResearchTarget(event.target.value); setResearch(null); setResearchLoading(false); }} /></label>
        <button className="pv2-button" type="button" disabled={!programId || !/^adveserve_[0-9a-f]{24}$/.test(bundleId) || !researchTarget || researchLoading} onClick={() => void readResearch()}>读取探索产物</button>
      </div>
      {researchError ? <p role="alert">{researchError}</p> : null}
      {researchLoading ? <p>正在只读加载…</p> : null}
      {research ? <div data-testid="economic-entry-research-result">
        <p>未确认探索估值，不用于实盘。{research.status === "NOT_CAPTURED" ? "该日无已捕获产物" : research.status === "NO_CANDIDATES" ? "该日原候选名单为空" : "RESEARCH_NAVIGATION"}</p>
        <EconomicRows rows={research.advice} />
      </div> : null}
    </details>
  </section>;
}
