"use client";

import { useEffect, useState } from "react";
import { advisoryApi, type AdvisorySectorEntryDaily, type AdvisorySectorPriceCandidate } from "@/lib/api/advisory";

const labels = {
  ACCEPTABLE_PRICE_SET: "有条件买入价格集合",
  NO_ACCEPTABLE_PRICE: "已知支持内无合适价格（未知节点不能判为不适合）",
  UNKNOWN_INPUT_OR_SUPPORT: "输入或支持未知，不能判为不推荐",
  QUERY_DOMAIN_UNAVAILABLE: "原D价格属性未知",
  QUERY_DOMAIN_OVER_BUDGET: "完整价格格点超出计算预算，未估值",
};
const finite = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const digest = (value: unknown) => typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
const money = (value: number) => value.toFixed(2);

function validCandidate(row: AdvisorySectorPriceCandidate) {
  if (!row || !/^\d{6}\.(SH|SZ|BJ)$/.test(row.instrument) || !Number.isInteger(row.selection_effective_rank)
    || row.selection_effective_rank < 1 || row.selection_effective_rank > 20 || !Object.hasOwn(labels, row.status)
    || !Number.isInteger(row.legal_node_count) || row.legal_node_count < 0 || row.legal_node_count > 5000
    || !Number.isInteger(row.unknown_node_count) || row.unknown_node_count < 0 || row.unknown_node_count > row.legal_node_count
    || !Array.isArray(row.intervals)) return false;
  let accepted = 0;
  for (const [index, band] of row.intervals.entries()) {
    if (![band.low_cny, band.high_cny, band.tick_cny, band.expected_net_bps_min, band.expected_net_bps_max,
      band.downside_q90_bps_max].every(finite) || band.low_cny <= 0 || band.high_cny < band.low_cny || band.tick_cny <= 0
      || !Number.isInteger(band.node_count) || band.node_count <= 0 || band.expected_net_bps_min > band.expected_net_bps_max
      || Math.abs((band.high_cny - band.low_cny) / band.tick_cny + 1 - band.node_count) > 1e-6
      || (index > 0 && row.intervals[index - 1].high_cny >= band.low_cny)) return false;
    accepted += band.node_count;
  }
  return accepted + row.unknown_node_count <= row.legal_node_count
    && ((row.status === "ACCEPTABLE_PRICE_SET") === (accepted > 0))
    && (row.status !== "NO_ACCEPTABLE_PRICE" || row.legal_node_count > row.unknown_node_count)
    && (row.status !== "UNKNOWN_INPUT_OR_SUPPORT" || row.unknown_node_count === row.legal_node_count)
    && (!row.status.startsWith("QUERY_DOMAIN_") || row.legal_node_count === 0);
}

function validDaily(value: AdvisorySectorEntryDaily, programId: string, target?: string, listId?: string) {
  if (value?.schema_version !== "economic_sector_daily_service_v1" || value.model_family !== "M1_SECTOR_PRICE_VALUE_V1"
    || value.program_id !== programId || value.requested_target_date !== (target || null)
    || value.requested_list_version_id !== (listId || null) || value.decision_use !== "NAVIGATION_ONLY" || value.deployable !== false
    || value.economic_effectiveness !== "NOT_CONFIRMED" || value.database_written !== false || value.outcomes_read !== false
    || value.package_qualification_rechecked !== false || value.fit_count !== 0
    || !Array.isArray(value.candidates) || !Array.isArray(value.unmodeled_items)) return false;
  if (value.status === "NOT_CONFIGURED") return value.candidates.length === 0 && value.unmodeled_items.length === 0;
  const receipt = value.candidate_receipt;
  return ["COMPUTED", "NO_CANDIDATES"].includes(value.status) && !!receipt && receipt.program_id === programId
    && receipt.candidate_scope === "ORIGINAL_PUBLISHED_TOP20" && receipt.native_receipt_created === false
    && [receipt.list_version_id, receipt.review_run_id, receipt.selection_run_id, receipt.binding_version_id]
      .every((id) => typeof id === "string" && id.trim().length > 0)
    && (!listId || receipt.list_version_id === listId)
    && receipt.target_date === value.target_date && (!target || value.target_date === target)
    && receipt.decision_date === value.decision_date && !!value.decision_date && !!value.target_date && value.decision_date < value.target_date
    && receipt.source_review_policy_sha256 === value.source_review_policy_sha256
    && (receipt.source_review_policy_sha256 == null || digest(receipt.source_review_policy_sha256))
    && [value.model_sha256, value.bundle_sha256, value.config_sha256, value.projection_sha256, receipt.candidate_roster_sha256,
      value.model_parent_policy_identity, value.model_value_policy_identity].every(digest)
    && receipt.candidate_count === value.candidates.length && value.candidates.length <= 20
    && receipt.original_list_item_count === value.candidates.length + value.unmodeled_items.length
    && (value.status === "NO_CANDIDATES" ? value.candidates.length === 0 : value.candidates.length > 0)
    && new Set(value.candidates.map((row) => row.instrument)).size === value.candidates.length
    && value.candidates.every((row, index) => row.selection_effective_rank === index + 1 && validCandidate(row));
}

export function SectorEntryPriceCard({ programId, targetTradeDate, listVersionId }: {
  programId?: string; targetTradeDate?: string; listVersionId?: string;
}) {
  const [result, setResult] = useState<AdvisorySectorEntryDaily | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  useEffect(() => {
    let cancelled = false;
    setResult(null); setError(""); setLoading(!!programId);
    if (!programId) return;
    advisoryApi.sectorEntryPrice(programId, targetTradeDate, listVersionId).then((value) => {
      if (!validDaily(value, programId, targetTradeDate, listVersionId)) throw new Error("M1原名单、价格字段或角色不一致");
      if (!cancelled) setResult(value);
    }).catch((failure: unknown) => {
      if (!cancelled) setError(failure instanceof Error ? failure.message : "M1价格计算失败");
    }).finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [programId, targetTradeDate, listVersionId]);
  return <section className="mt-3 rounded-lg border bg-card p-4" data-testid="advisory-sector-entry-price">
    <h3 className="font-semibold">模型买入价格条件（M1 板块）</h3>
    <p className="text-sm">日频数据库原名单 · CNY · 当前增量收益尚未独立确认；只提供建议，不下单或配置资金仓位。</p>
    <p className="text-sm">条件净价值不是胜率；路径损失 q90 不是置信区间或卖出目标价。不预测开盘价或最佳分钟，不保证成交或盈利。</p>
    {!programId ? <p>未选择荐股任务</p> : loading ? <p>正在读取原名单并计算…</p> : error ?
      <p role="alert">{error}；原名单保持不变，不回退规则价格。</p> : result?.status === "NOT_CONFIGURED" ?
        <p>NOT_CONFIGURED：M1消费者未配置，不代表没有可盈利股票；原荐股名单不受影响。</p> : result ? <div>
          <p>{result.status} · D {result.decision_date} → T {result.target_date}</p>
          <p>原名单 {result.candidate_receipt?.original_list_item_count} 项；原Top20模型候选 {result.candidates.length} 只；
            有价格集合 {result.candidates.filter((row) => row.status === "ACCEPTABLE_PRICE_SET").length} 只；
            模型范围外/非买入候选 {result.unmodeled_items.length} 项，原动作保留。</p>
          {result.candidates.map((row) => <div className="mt-2 border-t pt-2" key={row.instrument}>
            <strong>{row.selection_effective_rank}. {row.instrument} · {labels[row.status]}</strong>
            {row.intervals.map((band, index) => <p key={index}>{money(band.low_cny)}～{money(band.high_cny)} CNY；
              条件净价值 {money(band.expected_net_bps_min)}～{money(band.expected_net_bps_max)} bps；
              入场锚定路径损失 q90 ≤ {money(band.downside_q90_bps_max)} bps</p>)}
            <p className="text-sm">完整法律格点 {row.legal_node_count}；未知节点 {row.unknown_node_count}。
              价格不在集合只是条件未成立，不证明公司出现问题。</p>
          </div>)}
          <details><summary>原名单与模型政策（分别披露，不作额外资格门）</summary>
            <pre className="overflow-auto text-xs">{JSON.stringify({ source: result.candidate_receipt, model: result.model_sha256,
              model_parent_policy: result.model_parent_policy_identity, model_value_policy: result.model_value_policy_identity,
              unmodeled: result.unmodeled_items }, null, 2)}</pre>
          </details>
        </div> : null}
  </section>;
}
