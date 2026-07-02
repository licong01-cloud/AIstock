"use client";

import { Fragment, Suspense, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useSearchParams } from "next/navigation";
import {
  advisoryApi,
  type AdvisoryBindingPayload,
  type AdvisoryEpisode,
  type AdvisoryListVersionDetail,
  type AdvisoryLeaderboardRow,
  type AdvisoryPackageMode,
  type AdvisoryProgram,
  type AdvisoryQualityReport,
  type AdvisoryRecommendationListItem,
  type AdvisoryRecommendationListVersion,
  type AdvisoryReviewDecision,
  type AdvisoryReviewResult,
  type AdvisoryStrategyBindingVersion,
  type AdvisoryTradingDayDefaults,
} from "@/lib/api/advisory";
import { selectionCenterApi } from "@/lib/paper-v2/api";
import { packageDisplayLabel, shortHash } from "@/lib/paper-v2/format";
import type { SelectablePackage } from "@/lib/paper-v2/types";
import type { JsonObject } from "@/lib/api/selectionCenter";

type SortDirection = "asc" | "desc";
type ActivePoolSortKey =
  | "symbol"
  | "status"
  | "signal_date"
  | "effective_entry_date"
  | "entry_price"
  | "entry_rank"
  | "current_rank"
  | "holding_trading_days"
  | "return_bps"
  | "max_drawdown_bps"
  | "price_quality_status"
  | "exit_reason";

type ActivePoolSort = { key: ActivePoolSortKey; direction: SortDirection } | null;
type ListDetailSource = "latest" | "review_result" | "timeline";
type PackageWeightRow = { rowId: string; packageId: string; weight: string };
type ProgramStrategyDraft = {
  packageMode: AdvisoryPackageMode;
  targetCount: string;
  rows: PackageWeightRow[];
  activationReason: string;
  activeBindingVersionId?: string | null;
  replayResult: JsonObject | null;
  applyResult: AdvisoryStrategyBindingVersion | null;
};
type ReviewDateOption = {
  value: string;
  label: string;
  selectionAsOfDate?: string | null;
  description: string;
  disabled?: boolean;
};
type ReviewProgress = {
  firstPublishedTargetDate: string | null;
  latestPublishedTargetDate: string | null;
  reviewedThroughDate: string | null;
  missingTargetDates: string[];
  selectedRemainingMissingDates: string[];
};
type QualityInputRow = {
  rowId: string;
  code: string;
  tradeDate: string;
  currentPrice: string;
  maxBuyPrice: string;
  action: string;
  reasonCode: string;
  dayLow: string;
  dayHigh: string;
  scoreBucket: string;
  regime: string;
};

type ActivePoolColumn = {
  key: ActivePoolSortKey;
  label: string;
  value: (row: AdvisoryEpisode) => string | number | boolean | null | undefined;
  render: (row: AdvisoryEpisode) => ReactNode;
};

function stockLabel(row: { symbol: string; stock_name?: string | null; symbol_name?: string | null }): ReactNode {
  const name = row.stock_name || row.symbol_name;
  return (
    <span>
      <strong>{name || row.symbol}</strong>
      {name ? <><br /><span className="pv2-muted pv2-mono">{row.symbol}</span></> : null}
    </span>
  );
}

const REVIEW_PAGE_SIZE_OPTIONS = [20, 50, 100] as const;
const PACKAGE_MODE_OPTIONS: AdvisoryPackageMode[] = ["single_package", "weighted_rank_fusion", "fusion_pool", "union", "intersection"];

function fmtPct(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(1)}%` : "-";
}

function fmtBps(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? `${(value / 100).toFixed(2)}%` : "-";
}

function metricStatusText(row: AdvisoryLeaderboardRow): string {
  if (row.metric_status === "READY") {
    return row.metric_mark_trade_date ? `盯市 ${row.metric_mark_trade_date}` : "盯市已更新";
  }
  if (row.metric_status === "WAITING_MARKET_DATA") {
    const missing = row.missing_open_mark_count ?? row.active_count ?? 0;
    return missing > 0 ? `缺行情 ${missing}` : "等待行情";
  }
  if (row.metric_status === "LOADING") return "统计加载中";
  return row.metric_status || "";
}

function fmtNumber(value: number | null | undefined, digits = 0): string {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "-";
}

function fmtPrice(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(2) : "-";
}

function short(value: unknown, len = 10): string {
  const text = String(value ?? "-");
  return text.length > len ? `${text.slice(0, len)}...` : text;
}

function packageIdsFromText(text: string): string[] {
  return text.split(/[,\n]/).map((item) => item.trim()).filter(Boolean);
}

function packageRowsFromText(text: string): PackageWeightRow[] {
  const ids = packageIdsFromText(text);
  if (!ids.length) return [{ rowId: "pkg-1", packageId: "", weight: "1" }];
  return ids.map((packageId, index) => ({ rowId: `pkg-${index + 1}`, packageId, weight: "1" }));
}

function newPackageRow(index: number): PackageWeightRow {
  return { rowId: `pkg-new-${Date.now()}-${index}`, packageId: "", weight: "1" };
}

function packageRowsFromIds(packageIds: string[] = [], packageWeights: Record<string, number> = {}): PackageWeightRow[] {
  if (!packageIds.length) return [{ rowId: "pkg-1", packageId: "", weight: "1" }];
  return packageIds.map((packageId, index) => ({
    rowId: `pkg-${index + 1}`,
    packageId,
    weight: String(packageWeights[packageId] ?? 1),
  }));
}

function packageOptionLabel(pkg: SelectablePackage): string {
  const parts = [packageDisplayLabel(pkg), pkg.package_status];
  if (pkg.alpha_mode) parts.push(String(pkg.alpha_mode));
  if (pkg.latest_selection_run?.trade_date) parts.push(`最近选股 ${pkg.latest_selection_run.trade_date}`);
  return parts.filter(Boolean).join(" / ");
}

function statusText(defaults: { trading_day_status?: JsonObject } | null | undefined, key: string): string | null {
  const value = defaults?.trading_day_status?.[key];
  return typeof value === "string" && value ? value : null;
}

function isTradingDay(defaults: { trading_day_status?: JsonObject } | null | undefined): boolean {
  return defaults?.trading_day_status?.is_trading_day === true;
}

function dateContextFromListVersion(version: AdvisoryRecommendationListVersion | null | undefined): { targetTradeDate: string | null; selectionAsOfDate: string | null } {
  const summary = (version?.summary_json || {}) as JsonObject;
  const context = (summary.advisory_date_context || {}) as JsonObject;
  const target = version?.target_trade_date || context.target_trade_date || summary.target_trade_date || version?.trade_date || null;
  const selectionAsOf = version?.selection_as_of_trade_date || context.selection_as_of_trade_date || summary.selection_as_of_trade_date || null;
  return {
    targetTradeDate: typeof target === "string" ? target : null,
    selectionAsOfDate: typeof selectionAsOf === "string" ? selectionAsOf : null,
  };
}

function publishedTargetDates(program: Pick<AdvisoryProgram, "latest_recommendation_target_trade_date" | "published_recommendation_target_trade_dates"> | null | undefined, versions: AdvisoryRecommendationListVersion[] = []): Set<string> {
  const dates = new Set<string>();
  (program?.published_recommendation_target_trade_dates || []).forEach((value) => { if (value) dates.add(value); });
  if (program?.latest_recommendation_target_trade_date) dates.add(program.latest_recommendation_target_trade_date);
  versions
    .filter((version) => version.version_status === "PUBLISHED")
    .forEach((version) => {
      const context = dateContextFromListVersion(version);
      if (context.targetTradeDate) dates.add(context.targetTradeDate);
    });
  return dates;
}

function hasPublishedTarget(program: AdvisoryProgram | null | undefined, targetDate: string, versions: AdvisoryRecommendationListVersion[] = []): boolean {
  return Boolean(targetDate && publishedTargetDates(program, versions).has(targetDate));
}

function sortedDates(values: Iterable<string | null | undefined>): string[] {
  return [...new Set([...values].filter((value): value is string => typeof value === "string" && value.length > 0))].sort();
}

function latestRecommendationMeta(program: AdvisoryProgram | null | undefined, versions: AdvisoryRecommendationListVersion[] = []) {
  const latestVersion = versions.find((version) => version.version_status === "PUBLISHED");
  if (latestVersion) {
    const context = dateContextFromListVersion(latestVersion);
    return {
      listVersionId: latestVersion.list_version_id,
      targetTradeDate: context.targetTradeDate || latestVersion.trade_date,
      selectionAsOfDate: context.selectionAsOfDate,
      generatedAt: latestVersion.created_at || null,
      versionStatus: latestVersion.version_status,
    };
  }
  return {
    listVersionId: program?.latest_recommendation_list_version_id || null,
    targetTradeDate: program?.latest_recommendation_target_trade_date || program?.latest_recommendation_trade_date || program?.latest_review_trade_date || null,
    selectionAsOfDate: program?.latest_recommendation_selection_as_of_trade_date || null,
    generatedAt: program?.latest_recommendation_generated_at || null,
    versionStatus: program?.latest_recommendation_version_status || null,
  };
}

function selectionAsOfForTarget(targetDate: string, defaults: AdvisoryTradingDayDefaults | null | undefined): string | null {
  const latestDataDate = defaults?.data_ready_latest_date || defaults?.latest_trading_day || null;
  const previous = statusText(defaults, "previous_trading_day") || null;
  const tradingDays = Array.isArray(defaults?.trading_days) ? defaults.trading_days : [];
  const candidates = sortedDates([
    ...tradingDays,
    defaults?.latest_trading_day,
    previous,
  ]).filter((date) => date < targetDate && (!latestDataDate || date <= latestDataDate));
  return candidates.at(-1) || null;
}

function reviewTargetCandidates(defaults: AdvisoryTradingDayDefaults | null | undefined): string[] {
  const tradingDays = Array.isArray(defaults?.trading_days) ? defaults?.trading_days : [];
  const today = defaults?.as_of_date || "";
  return sortedDates([
    ...(tradingDays || []),
    isTradingDay(defaults) ? today : null,
    defaults?.latest_trading_day,
    defaults?.next_trading_day,
  ]);
}

function catchUpCutoffDate(defaults: AdvisoryTradingDayDefaults | null | undefined): string {
  return defaults?.as_of_date || defaults?.latest_trading_day || "";
}

function missingTargetDates(defaults: AdvisoryTradingDayDefaults | null | undefined, published: Set<string>): string[] {
  if (!published.size) return [];
  const firstPublished = sortedDates(published)[0];
  const cutoff = catchUpCutoffDate(defaults);
  return reviewTargetCandidates(defaults)
    .filter((date) => date > firstPublished && (!cutoff || date <= cutoff) && !published.has(date));
}

function earliestMissingTargetDate(defaults: AdvisoryTradingDayDefaults | null | undefined, published: Set<string>): string | null {
  return missingTargetDates(defaults, published)[0] || null;
}

function reviewProgress(
  defaults: AdvisoryTradingDayDefaults | null | undefined,
  program?: AdvisoryProgram | null,
  versions: AdvisoryRecommendationListVersion[] = [],
  selectedTargetDate = "",
): ReviewProgress {
  const published = publishedTargetDates(program, versions);
  const publishedDates = sortedDates(published);
  const missingDates = missingTargetDates(defaults, published);
  const allKnownDates = sortedDates([...reviewTargetCandidates(defaults), ...publishedDates]);
  const firstMissing = missingDates[0] || null;
  const latestPublished = publishedDates.at(-1) || null;
  const reviewedThrough = firstMissing
    ? allKnownDates.filter((date) => date < firstMissing && published.has(date)).at(-1) || publishedDates[0] || null
    : latestPublished;
  return {
    firstPublishedTargetDate: publishedDates[0] || null,
    latestPublishedTargetDate: latestPublished,
    reviewedThroughDate: reviewedThrough,
    missingTargetDates: missingDates,
    selectedRemainingMissingDates: selectedTargetDate
      ? missingDates.filter((date) => date > selectedTargetDate)
      : missingDates.slice(1),
  };
}

function blockingMissingDate(progress: ReviewProgress, targetDate: string, targetPublished: boolean): string | null {
  const firstMissing = progress.missingTargetDates[0];
  if (!firstMissing || !targetDate || targetPublished) return null;
  return targetDate !== firstMissing && targetDate > firstMissing ? firstMissing : null;
}

function compactDateList(dates: string[], maxVisible = 4): string {
  if (!dates.length) return "无";
  const visible = dates.slice(0, maxVisible).join("、");
  return dates.length > maxVisible ? `${visible} 等 ${dates.length} 天` : visible;
}

function defaultReviewTargetDate(
  defaults: AdvisoryTradingDayDefaults | null | undefined,
  program?: AdvisoryProgram | null,
  versions: AdvisoryRecommendationListVersion[] = [],
): string {
  const missing = earliestMissingTargetDate(defaults, publishedTargetDates(program, versions));
  return missing || defaults?.next_trading_day || defaults?.latest_trading_day || defaults?.as_of_date || "";
}

function reviewDateOptions(
  defaults: AdvisoryTradingDayDefaults | null | undefined,
  program?: AdvisoryProgram | null,
  versions: AdvisoryRecommendationListVersion[] = [],
): ReviewDateOption[] {
  const seen = new Set<string>();
  const options: ReviewDateOption[] = [];
  const add = (option: ReviewDateOption | null) => {
    if (!option?.value || seen.has(option.value)) return;
    seen.add(option.value);
    options.push(option);
  };
  const latest = defaults?.latest_trading_day || "";
  const previous = statusText(defaults, "previous_trading_day") || (latest && latest !== defaults?.as_of_date ? latest : null);
  const today = defaults?.as_of_date || latest;
  const progress = reviewProgress(defaults, program, versions);
  const published = publishedTargetDates(program, versions);
  const firstMissing = progress.missingTargetDates[0] || null;
  const nextSelectionAsOf = defaults?.next_trading_day ? selectionAsOfForTarget(defaults.next_trading_day, defaults) : null;
  const todaySelectionAsOf = today ? selectionAsOfForTarget(today, defaults) : null;
  const latestSelectionAsOf = latest ? selectionAsOfForTarget(latest, defaults) : null;
  progress.missingTargetDates.forEach((missing, index) => {
    const missingSelectionAsOf = selectionAsOfForTarget(missing, defaults);
    add({
      value: missing,
      label: index === 0 ? `待补跑目标日 ${missing}` : `后续缺失目标日 ${missing}（先补 ${firstMissing}）`,
      selectionAsOfDate: missingSelectionAsOf,
      description: index === 0
        ? `按顺序补齐缺失荐股：用 ${missingSelectionAsOf || "系统解析"} 数据生成 ${missing} 荐股名单；主按钮只执行这一个目标日。`
        : `该交易日仍缺失，但必须先补齐 ${firstMissing}，避免跳过中间交易日。`,
      disabled: index > 0,
    });
  });
  add(defaults?.next_trading_day ? {
    value: defaults.next_trading_day,
    label: firstMissing && defaults.next_trading_day > firstMissing ? `下一交易日 ${defaults.next_trading_day}（先补 ${firstMissing}）` : `下一交易日 ${defaults.next_trading_day}`,
    selectionAsOfDate: nextSelectionAsOf,
    description: firstMissing && defaults.next_trading_day > firstMissing
      ? `当前仍有缺失目标日 ${firstMissing}，请先补齐；该按钮不会跳过中间交易日。`
      : nextSelectionAsOf ? `默认盘后复评：用 ${nextSelectionAsOf} 已就绪数据生成 ${defaults.next_trading_day} 荐股名单。` : "默认盘后复评：等待最新数据就绪后生成下一交易日荐股名单。",
    disabled: Boolean(firstMissing && defaults.next_trading_day > firstMissing && !published.has(defaults.next_trading_day)),
  } : null);
  add(today && isTradingDay(defaults) ? {
    value: today,
    label: firstMissing && today > firstMissing ? `当天交易日 ${today}（先补 ${firstMissing}）` : `当天交易日 ${today}`,
    selectionAsOfDate: todaySelectionAsOf || previous,
    description: firstMissing && today > firstMissing
      ? `当前仍有缺失目标日 ${firstMissing}，请先补齐；不能直接跳到 ${today}。`
      : todaySelectionAsOf ? `盘中补跑：用 ${todaySelectionAsOf} 数据生成 ${today} 荐股名单。` : `盘中补跑：生成 ${today} 荐股名单。`,
    disabled: Boolean(firstMissing && today > firstMissing && !published.has(today)),
  } : null);
  add(latest ? {
    value: latest,
    label: firstMissing && latest > firstMissing ? `最新已就绪交易日 ${latest}（先补 ${firstMissing}）` : `最新已就绪交易日 ${latest}`,
    selectionAsOfDate: latestSelectionAsOf || (latest === today ? previous : null),
    description: firstMissing && latest > firstMissing
      ? `当前仍有缺失目标日 ${firstMissing}，请先补齐；不能直接跳到 ${latest}。`
      : latest === today && previous ? `用 ${previous} 数据生成 ${latest} 荐股名单。` : `使用 ${latest} 作为目标荐股交易日。`,
    disabled: Boolean(firstMissing && latest > firstMissing && !published.has(latest)),
  } : null);
  return options;
}

function packageIdsFromRows(rows: PackageWeightRow[], mode: AdvisoryPackageMode): string[] {
  const ids = rows.map((row) => row.packageId.trim()).filter(Boolean);
  return mode === "single_package" ? ids.slice(0, 1) : ids;
}

function packageWeightsFromRows(rows: PackageWeightRow[], packageIds: string[]): Record<string, number> {
  return Object.fromEntries(packageIds.map((packageId) => {
    const row = rows.find((item) => item.packageId.trim() === packageId);
    const parsed = Number(row?.weight ?? "1");
    return [packageId, Number.isFinite(parsed) && parsed > 0 ? parsed : 1];
  }));
}

function newQualityRow(index: number): QualityInputRow {
  return {
    rowId: `quality-${Date.now()}-${index}`,
    code: "",
    tradeDate: "",
    currentPrice: "",
    maxBuyPrice: "",
    action: "HOLD",
    reasonCode: "HOLD",
    dayLow: "",
    dayHigh: "",
    scoreBucket: "",
    regime: "",
  };
}

function optionalNumber(value: string): number | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function requiredNumber(value: string, label: string): number {
  const parsed = optionalNumber(value);
  if (parsed === undefined) throw new Error(`${label} 必须是有效数字`);
  return parsed;
}

function buildQualityRecord(row: QualityInputRow, index: number): JsonObject {
  const code = row.code.trim();
  if (!code) throw new Error(`第 ${index + 1} 行缺少股票代码`);
  if (!row.tradeDate) throw new Error(`第 ${index + 1} 行缺少交易日`);
  const record: JsonObject = {
    code,
    trade_date: row.tradeDate,
    current_price: requiredNumber(row.currentPrice, `第 ${index + 1} 行当前价`),
    entry_band_json: { max_buy_price: requiredNumber(row.maxBuyPrice, `第 ${index + 1} 行最高买入价`) },
    action: row.action,
    reason_code: row.reasonCode.trim() || row.action,
  };
  const dayLow = optionalNumber(row.dayLow);
  const dayHigh = optionalNumber(row.dayHigh);
  if (dayLow !== undefined) record.day_low = dayLow;
  if (dayHigh !== undefined) record.day_high = dayHigh;
  if (row.scoreBucket.trim()) record.score_bucket = row.scoreBucket.trim();
  if (row.regime.trim()) record.regime = row.regime.trim();
  return record;
}

function compareValues(left: string | number | boolean | null | undefined, right: string | number | boolean | null | undefined): number {
  const leftEmpty = left === null || left === undefined || left === "";
  const rightEmpty = right === null || right === undefined || right === "";
  if (leftEmpty && rightEmpty) return 0;
  if (leftEmpty) return 1;
  if (rightEmpty) return -1;
  if (typeof left === "number" && typeof right === "number") return left - right;
  if (typeof left === "boolean" && typeof right === "boolean") return Number(left) - Number(right);
  return String(left).localeCompare(String(right), "zh-Hans-CN", { numeric: true, sensitivity: "base" });
}

function nextSort(current: ActivePoolSort, key: ActivePoolSortKey): ActivePoolSort {
  if (!current || current.key !== key) return { key, direction: "asc" };
  if (current.direction === "asc") return { key, direction: "desc" };
  return null;
}

function SortHeader({ column, sort, onClick }: { column: ActivePoolColumn; sort: ActivePoolSort; onClick: () => void }) {
  const active = sort?.key === column.key;
  const marker = active ? (sort.direction === "asc" ? " ▲" : " ▼") : " ↕";
  return (
    <button
      aria-label={`${column.label} 排序`}
      className={`pv2-sort-button ${active ? "pv2-sort-button-active" : ""}`}
      data-testid={`advisory-active-sort-${column.key}`}
      onClick={onClick}
      type="button"
    >
      {column.label}{marker}
    </button>
  );
}

function reviewRuntimeConfig(program: AdvisoryProgram, dateContext?: { targetTradeDate?: string; selectionAsOfDate?: string | null }): JsonObject {
  const topK = Math.max(1, Math.min(50, Number(program.target_count || 20)));
  const selectionArtifactConfig: JsonObject = {
    auto_generate: true,
    inference_backend: "wsl",
    pit_mode: "PREVIOUS_TRADING_DAY_CLOSE",
  };
  if (dateContext?.selectionAsOfDate) {
    selectionArtifactConfig.cutoff_date = dateContext.selectionAsOfDate;
    selectionArtifactConfig.cutoff_policy = "FIXED_CUTOFF";
    selectionArtifactConfig.fixed_cutoff = true;
  }
  return {
    top_k: topK,
    display_top_n: topK,
    st_pit_authoritative: true,
    advisory_date_context: {
      target_trade_date: dateContext?.targetTradeDate,
      selection_as_of_trade_date: dateContext?.selectionAsOfDate || undefined,
    },
    selection_artifact_config: selectionArtifactConfig,
    runtime_profile: {
      selection: { top_k: topK },
      tradability: { exclude_suspended: !dateContext?.selectionAsOfDate },
      industry_blacklist: [],
      hmm: { enabled: false, model_config_id: null, model_snapshot_id: null, signal_preset: null },
    },
  };
}

function buildReviewPayload(program: AdvisoryProgram, targetDate: string, selectionAsOfDate?: string | null) {
  return {
    trade_date: targetDate,
    target_trade_date: targetDate,
    selection_as_of_trade_date: selectionAsOfDate || undefined,
    runtime_config: reviewRuntimeConfig(program, { targetTradeDate: targetDate, selectionAsOfDate }),
  };
}

function modeNeedsWeights(mode: AdvisoryPackageMode): boolean {
  return mode === "fusion_pool" || mode === "weighted_rank_fusion";
}

function strategyDraftFromProgram(program: AdvisoryProgram, binding?: AdvisoryStrategyBindingVersion | null): ProgramStrategyDraft {
  const source = binding || program;
  return {
    packageMode: source.package_mode,
    targetCount: String(program.target_count || 20),
    rows: packageRowsFromIds(source.package_ids, source.package_weights || {}),
    activationReason: `替换荐股任务「${program.program_name}」策略包配置`,
    activeBindingVersionId: binding?.binding_version_id || null,
    replayResult: null,
    applyResult: null,
  };
}

function bindingPayloadFromDraft(draft: ProgramStrategyDraft): AdvisoryBindingPayload {
  const packageIds = packageIdsFromRows(draft.rows, draft.packageMode);
  if (!packageIds.length) throw new Error("至少需要从下拉菜单选择一个策略包");
  const targetCount = Number(draft.targetCount || 20);
  if (!Number.isFinite(targetCount) || targetCount <= 0) throw new Error("目标数量必须是有效正数");
  return {
    package_mode: draft.packageMode,
    package_ids: packageIds,
    package_weights: draft.packageMode === "single_package" ? { [packageIds[0]]: 1 } : packageWeightsFromRows(draft.rows, packageIds),
    target_count: Math.min(100, Math.max(1, Math.round(targetCount))),
    runtime_config_json: {},
  };
}

function adviceText(item: AdvisoryRecommendationListItem): string {
  return String(item.operation_advice_json?.human_label || item.operation_advice_json?.reason_summary || item.reason_code || "-");
}

function finalRecommendationItems(items: AdvisoryRecommendationListItem[]): AdvisoryRecommendationListItem[] {
  return items
    .filter((item) => item.symbol && item.item_state === "ACTIVE" && item.action !== "EXIT")
    .sort((left, right) => {
      const rankDiff = (left.rank ?? 999999) - (right.rank ?? 999999);
      return rankDiff !== 0 ? rankDiff : left.symbol.localeCompare(right.symbol);
    });
}

function defaultWatchlistCategoryName(program: AdvisoryProgram | null | undefined, version: { trade_date?: string } | null | undefined): string {
  const programName = (program?.program_name || "荐股名单").trim();
  const tradeDate = String(version?.trade_date || "").trim() || "未定日期";
  return `${programName} ${tradeDate}`;
}

function reviewState(program: AdvisoryProgram, targetDate: string, hasPriorList = Boolean(program.latest_review_trade_date), targetPublished = false, blockingMissingTargetDate?: string | null) {
  if (!targetDate) return { canPreview: false, canRun: false, label: "等待交易日", previewLabel: "等待交易日", hint: "等待交易日服务返回最近交易日。", isInitialRun: false };
  if (program.status !== "ENABLED") return { canPreview: false, canRun: false, label: "未启用", previewLabel: "预览", hint: "任务启用后才可执行首次运行或每日复评。", isInitialRun: false };
  if (targetPublished) {
    return { canPreview: true, canRun: false, label: "已复评", previewLabel: "预览", hint: `${targetDate} 已完成复评，等待下一个交易日。`, isInitialRun: false };
  }
  if (blockingMissingTargetDate) {
    return {
      canPreview: false,
      canRun: false,
      label: `先补 ${blockingMissingTargetDate}`,
      previewLabel: `先补 ${blockingMissingTargetDate}`,
      hint: `检测到更早缺失目标日 ${blockingMissingTargetDate}；请按顺序补齐，不能直接跳到 ${targetDate}。`,
      isInitialRun: false,
    };
  }
  if (!hasPriorList) {
    return {
      canPreview: true,
      canRun: true,
      label: "生成初始列表",
      previewLabel: "预览初始列表",
      hint: `目标交易日 ${targetDate}，首次运行会自动生成候选并发布第一版荐股列表，无需填写任何内部 ID。`,
      isInitialRun: true,
    };
  }
  return { canPreview: true, canRun: true, label: "执行复评", previewLabel: "预览", hint: `本次只执行目标交易日 ${targetDate}；不会一次性补全所有漏评日，后续缺失会在完成后继续提示。`, isInitialRun: false };
}

function reviewActionContext(
  defaults: AdvisoryTradingDayDefaults | null | undefined,
  program: AdvisoryProgram,
  versions: AdvisoryRecommendationListVersion[] = [],
  preferredTargetDate = "",
) {
  const targetDate = preferredTargetDate || defaultReviewTargetDate(defaults, program, versions);
  const dateChoices = reviewDateOptions(defaults, program, versions);
  const selectedChoice = dateChoices.find((option) => option.value === targetDate);
  const selectionAsOfDate = selectedChoice?.selectionAsOfDate ?? selectionAsOfForTarget(targetDate, defaults);
  const progress = reviewProgress(defaults, program, versions, targetDate);
  const targetPublished = hasPublishedTarget(program, targetDate, versions);
  const hasPriorList = versions.length > 0 || Boolean(latestRecommendationMeta(program, versions).targetTradeDate);
  const blockingMissingTargetDate = blockingMissingDate(progress, targetDate, targetPublished);
  const state = reviewState(program, targetDate, hasPriorList, targetPublished, blockingMissingTargetDate);
  return {
    targetDate,
    dateChoices,
    selectedChoice,
    selectionAsOfDate,
    progress,
    targetPublished,
    hasPriorList,
    blockingMissingTargetDate,
    state,
  };
}

function loadingReviewState(hint: string) {
  return {
    canPreview: false,
    canRun: false,
    label: "加载日期",
    previewLabel: "加载日期",
    hint,
    isInitialRun: false,
  };
}

function AdvisoryPageContent() {
  const params = useSearchParams();
  const prefillPackages = params.get("package_ids") || "";
  const [programs, setPrograms] = useState<AdvisoryProgram[]>([]);
  const [leaderboard, setLeaderboard] = useState<AdvisoryLeaderboardRow[]>([]);
  const [selectedProgramId, setSelectedProgramId] = useState("");
  const [sortBy, setSortBy] = useState("win_rate");
  const [programName, setProgramName] = useState("每日 Top20 荐股任务");
  const [packageMode, setPackageMode] = useState<AdvisoryPackageMode>(packageIdsFromText(prefillPackages).length > 1 ? "weighted_rank_fusion" : "single_package");
  const [packageRows, setPackageRows] = useState<PackageWeightRow[]>(() => packageRowsFromText(prefillPackages));
  const [selectablePackages, setSelectablePackages] = useState<SelectablePackage[]>([]);
  const [targetCount, setTargetCount] = useState(20);
  const [reviewTradeDate, setReviewTradeDate] = useState("");
  const [reviewDateTouched, setReviewDateTouched] = useState(false);
  const [tradingDefaults, setTradingDefaults] = useState<AdvisoryTradingDayDefaults | null>(null);
  const [reviewingKey, setReviewingKey] = useState("");
  const [activePool, setActivePool] = useState<AdvisoryEpisode[]>([]);
  const [bindings, setBindings] = useState<AdvisoryStrategyBindingVersion[]>([]);
  const [listVersions, setListVersions] = useState<AdvisoryRecommendationListVersion[]>([]);
  const [listVersionDetail, setListVersionDetail] = useState<AdvisoryListVersionDetail | null>(null);
  const [selectedListVersionId, setSelectedListVersionId] = useState("");
  const [listVersionLoadingId, setListVersionLoadingId] = useState("");
  const [listDetailSource, setListDetailSource] = useState<ListDetailSource>("latest");
  const [activeSort, setActiveSort] = useState<ActivePoolSort>(null);
  const [reviews, setReviews] = useState<AdvisoryReviewDecision[]>([]);
  const [reviewTotalCount, setReviewTotalCount] = useState(0);
  const [reviewPage, setReviewPage] = useState(1);
  const [reviewPageSize, setReviewPageSize] = useState<(typeof REVIEW_PAGE_SIZE_OPTIONS)[number]>(20);
  const [returns, setReturns] = useState<AdvisoryEpisode[]>([]);
  const [reviewResult, setReviewResult] = useState<AdvisoryReviewResult | null>(null);
  const [replayStart, setReplayStart] = useState("");
  const [replayEnd, setReplayEnd] = useState("");
  const [replayResult, setReplayResult] = useState<JsonObject | null>(null);
  const [expandedStrategyProgramId, setExpandedStrategyProgramId] = useState("");
  const [programStrategyDrafts, setProgramStrategyDrafts] = useState<Record<string, ProgramStrategyDraft>>({});
  const [strategyActionKey, setStrategyActionKey] = useState("");
  const [qualityRows, setQualityRows] = useState<QualityInputRow[]>(() => [{ ...newQualityRow(1), rowId: "quality-1" }]);
  const [qualityReport, setQualityReport] = useState<AdvisoryQualityReport | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [leaderboardLoading, setLeaderboardLoading] = useState(false);
  const [detailsLoading, setDetailsLoading] = useState(false);
  const [loadedDetailsProgramId, setLoadedDetailsProgramId] = useState("");
  const [addingWatchlist, setAddingWatchlist] = useState(false);
  const [catchUpProgress, setCatchUpProgress] = useState<{ programId: string; index: number; total: number; targetDate: string } | null>(null);
  const [tdxAvailable, setTdxAvailable] = useState(false);
  const [tdxSyncing, setTdxSyncing] = useState(false);
  const [tdxSyncResult, setTdxSyncResult] = useState<{ display_name: string; count: number } | null>(null);
  const listDetailRef = useRef<HTMLDivElement | null>(null);
  const refreshSeqRef = useRef(0);
  const detailsSeqRef = useRef(0);

  const selectedProgram = useMemo(
    () => programs.find((item) => item.program_id === selectedProgramId) || programs[0],
    [programs, selectedProgramId],
  );

  const activeColumns = useMemo<ActivePoolColumn[]>(() => [
    { key: "symbol", label: "股票", value: (row) => row.symbol, render: (row) => stockLabel(row) },
    { key: "status", label: "状态", value: (row) => row.status, render: (row) => row.status },
    { key: "signal_date", label: "信号日", value: (row) => row.signal_date, render: (row) => row.signal_date },
    { key: "effective_entry_date", label: "入选生效日", value: (row) => row.effective_entry_date, render: (row) => row.effective_entry_date },
    { key: "entry_price", label: "入选价", value: (row) => row.entry_price, render: (row) => fmtPrice(row.entry_price) },
    { key: "entry_rank", label: "入选排名", value: (row) => row.entry_rank, render: (row) => fmtNumber(row.entry_rank) },
    { key: "current_rank", label: "当前排名", value: (row) => row.current_rank ?? row.entry_rank, render: (row) => fmtNumber(row.current_rank ?? row.entry_rank) },
    { key: "holding_trading_days", label: "持有天数", value: (row) => row.holding_trading_days, render: (row) => fmtNumber(row.holding_trading_days) },
    { key: "return_bps", label: "涨跌幅", value: (row) => row.return_bps, render: (row) => fmtBps(row.return_bps) },
    { key: "max_drawdown_bps", label: "最大回撤", value: (row) => row.max_drawdown_bps, render: (row) => fmtBps(row.max_drawdown_bps) },
    { key: "price_quality_status", label: "价格状态", value: (row) => row.price_quality_status, render: (row) => row.price_quality_status || "-" },
    { key: "exit_reason", label: "退出原因", value: (row) => row.exit_reason, render: (row) => row.exit_reason || "-" },
  ], []);

  const packageById = useMemo(
    () => new Map(selectablePackages.map((pkg) => [pkg.package_id, pkg])),
    [selectablePackages],
  );

  const packageLabel = (packageId: string): string => {
    const pkg = packageById.get(packageId);
    return pkg ? packageDisplayLabel(pkg) : shortHash(packageId, 7);
  };

  const packageSummary = (packageIds: string[]): string => {
    if (!packageIds.length) return "-";
    return packageIds.map(packageLabel).join(" + ");
  };

  const activeBindingForProgram = (programId: string): AdvisoryStrategyBindingVersion | undefined => {
    if (programId === selectedProgram?.program_id && loadedDetailsProgramId === programId) {
      return bindings.find((item) => item.activation_status === "ACTIVE") || bindings[0];
    }
    return undefined;
  };

  const sortedActivePool = useMemo(() => {
    if (!activeSort) return activePool;
    const column = activeColumns.find((item) => item.key === activeSort.key);
    if (!column) return activePool;
    const originalIndex = new Map(activePool.map((row, index) => [row.episode_id, index]));
    return [...activePool].sort((left, right) => {
      const compared = compareValues(column.value(left), column.value(right));
      if (compared !== 0) return activeSort.direction === "asc" ? compared : -compared;
      return (originalIndex.get(left.episode_id) ?? 0) - (originalIndex.get(right.episode_id) ?? 0);
    });
  }, [activeColumns, activePool, activeSort]);

  const reviewTotalPages = Math.max(1, Math.ceil(reviewTotalCount / reviewPageSize));
  const reviewPageSafe = Math.min(reviewPage, reviewTotalPages);
  const pagedReviews = reviews;

  async function loadReviews(programId: string, page: number, pageSize: (typeof REVIEW_PAGE_SIZE_OPTIONS)[number]) {
    const offset = (Math.max(page, 1) - 1) * pageSize;
    const pageData = await advisoryApi.reviews(programId, pageSize, offset);
    setReviews(pageData.reviews);
    setReviewTotalCount(pageData.total_count);
  }

  function applyPackageOptions(packageOptions: SelectablePackage[]) {
    setSelectablePackages(packageOptions);
    setPackageRows((rows) => {
      if (rows.some((row) => row.packageId.trim()) || !packageOptions[0]) return rows;
      return rows.map((row, index) => index === 0 ? { ...row, packageId: packageOptions[0].package_id } : row);
    });
  }

  async function loadSelectablePackageOptions() {
    try {
      applyPackageOptions(await selectionCenterApi.selectablePackages(100, "summary"));
    } catch {
      applyPackageOptions([]);
    }
  }

  async function loadProgramDetails(programId: string, page = 1, pageSize: (typeof REVIEW_PAGE_SIZE_OPTIONS)[number] = reviewPageSize) {
    const requestSeq = ++detailsSeqRef.current;
    const stillCurrent = () => requestSeq === detailsSeqRef.current;
    const offset = (Math.max(page, 1) - 1) * pageSize;
    const [poolRows, reviewPageData, bindingRows, versionRows] = await Promise.all([
      advisoryApi.activePool(programId),
      advisoryApi.reviews(programId, pageSize, offset),
      advisoryApi.bindings(programId),
      advisoryApi.listVersions(programId, 20, 0),
    ]);
    if (!stillCurrent()) return versionRows;
    setActivePool(poolRows);
    setReviews(reviewPageData.reviews);
    setReviewTotalCount(reviewPageData.total_count);
    setBindings(bindingRows);
    setListVersions(versionRows);
    setLoadedDetailsProgramId(programId);
    if (versionRows[0]) {
      const latestVersion = versionRows[0];
      setListVersionDetail({ list_version: latestVersion, items: [] });
      setSelectedListVersionId(latestVersion.list_version_id);
      setListDetailSource("latest");
      setListVersionLoadingId(latestVersion.list_version_id);
      advisoryApi.listVersionDetail(latestVersion.list_version_id)
        .then((detail) => {
          if (!stillCurrent()) return;
          setListVersionDetail(detail);
          setSelectedListVersionId(detail.list_version.list_version_id);
          setListDetailSource("latest");
        })
        .catch((exc) => {
          if (stillCurrent()) setError(exc instanceof Error ? exc.message : String(exc));
        })
        .finally(() => {
          if (stillCurrent()) setListVersionLoadingId("");
        });
    } else {
      setListVersionDetail(null);
      setSelectedListVersionId("");
      setListDetailSource("latest");
    }
    setReturns([]);
    advisoryApi.returns(programId)
      .then((returnRows) => {
        if (stillCurrent()) setReturns(returnRows.returns || []);
      })
      .catch((exc) => {
        if (stillCurrent()) setError(exc instanceof Error ? exc.message : String(exc));
      });
    return versionRows;
  }

  async function refreshAll(nextProgramId?: string, options: { resetReviewDate?: boolean; preserveReviewDate?: boolean } = {}) {
    const requestSeq = ++refreshSeqRef.current;
    const stillCurrent = () => requestSeq === refreshSeqRef.current;
    setLoading(true);
    setLeaderboardLoading(false);
    setDetailsLoading(false);
    setError(null);
    try {
      const [programRows, nextTradingDefaults] = await Promise.all([
        advisoryApi.programs(false),
        advisoryApi.tradingDayDefaults(10),
      ]);
      if (!stillCurrent()) return;
      const placeholderBoardRows = programRows.map((program) => ({ ...program, metric_status: "LOADING" })) as AdvisoryLeaderboardRow[];
      const hydratedPrograms = programRows;
      setPrograms(hydratedPrograms);
      setLeaderboard(placeholderBoardRows);
      setLeaderboardLoading(true);
      setTradingDefaults(nextTradingDefaults);
      if (options.resetReviewDate) setReviewDateTouched(false);
      const resolvedProgramId = nextProgramId || selectedProgramId || hydratedPrograms[0]?.program_id || "";
      const resolvedProgram = hydratedPrograms.find((item) => item.program_id === resolvedProgramId) || hydratedPrograms[0] || null;
      setSelectedProgramId(resolvedProgramId);
      setReviewPage(1);
      setLoadedDetailsProgramId("");
      setLoading(false);
      void loadSelectablePackageOptions();
      advisoryApi.leaderboard(sortBy)
        .then((boardRows) => {
          if (!stillCurrent()) return;
          const boardById = new Map(boardRows.map((row) => [row.program_id, row]));
          setPrograms((rows) => rows.map((program) => ({ ...(boardById.get(program.program_id) || {}), ...program })));
          setLeaderboard(boardRows);
        })
        .catch((exc) => {
          if (stillCurrent()) setError(exc instanceof Error ? exc.message : String(exc));
        })
        .finally(() => {
          if (stillCurrent()) setLeaderboardLoading(false);
        });
      if (resolvedProgramId) {
        setDetailsLoading(true);
        const versionRows = await loadProgramDetails(resolvedProgramId, 1, reviewPageSize);
        if (!stillCurrent()) return;
        setReviewTradeDate((current) => (
          !options.preserveReviewDate && (options.resetReviewDate || !reviewDateTouched || !current)
            ? defaultReviewTargetDate(nextTradingDefaults, resolvedProgram, versionRows)
            : current
        ));
      } else {
        setActivePool([]);
        setReviews([]);
        setReviewTotalCount(0);
        setReturns([]);
        setBindings([]);
        setListVersions([]);
        setListVersionDetail(null);
        setSelectedListVersionId("");
        setLoadedDetailsProgramId("");
        setListDetailSource("latest");
        setReviewResult(null);
        setReviewTradeDate((current) => (!options.preserveReviewDate && (options.resetReviewDate || !reviewDateTouched || !current) ? defaultReviewTargetDate(nextTradingDefaults) : current));
      }
    } catch (exc) {
      if (stillCurrent()) setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      if (stillCurrent()) {
        setLoading(false);
        setDetailsLoading(false);
      }
    }
  }

  async function selectProgram(programId: string) {
    setSelectedProgramId(programId);
    setReviewPage(1);
    setReviewResult(null);
    setLoadedDetailsProgramId("");
    setDetailsLoading(true);
    setError(null);
    try {
      const versionRows = await loadProgramDetails(programId, 1, reviewPageSize);
      const program = programs.find((item) => item.program_id === programId) || null;
      setReviewTradeDate((current) => (!reviewDateTouched || !current ? defaultReviewTargetDate(tradingDefaults, program, versionRows) : current));
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setDetailsLoading(false);
    }
  }

  useEffect(() => {
    refreshAll().catch((exc) => setError(exc instanceof Error ? exc.message : String(exc)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sortBy]);

  useEffect(() => {
    let alive = true;
    advisoryApi.tdxAvailable().then((available) => {
      if (alive) setTdxAvailable(available);
    }).catch(() => {});
    return () => { alive = false; };
  }, []);

  useEffect(() => {
    if (prefillPackages) {
      const ids = packageIdsFromText(prefillPackages);
      setPackageMode(ids.length > 1 ? "weighted_rank_fusion" : "single_package");
      setPackageRows(packageRowsFromText(prefillPackages));
    }
  }, [prefillPackages]);

  useEffect(() => {
    if (!tradingDefaults) return;
    setReplayStart((current) => current || tradingDefaults.replay_start_date || tradingDefaults.trading_days?.[0] || "");
    setReplayEnd((current) => current || tradingDefaults.replay_end_date || tradingDefaults.latest_trading_day || "");
  }, [tradingDefaults]);

  async function createProgram() {
    setError(null);
    try {
      const packageIds = packageIdsFromRows(packageRows, packageMode);
      if (!packageIds.length) throw new Error("至少需要从下拉菜单选择一个策略包");
      const confirmed = window.confirm(`确认创建并启用荐股任务「${programName}」？启用后会进入每日复评与排行榜统计。`);
      if (!confirmed) return;
      const program = await advisoryApi.createProgram({
        program_name: programName,
        package_mode: packageMode,
        package_ids: packageIds,
        target_count: targetCount,
        package_weights: packageWeightsFromRows(packageRows, packageIds),
        status: "ENABLED",
      });
      await refreshAll(program.program_id);
      if (reviewTradeDate) {
        window.setTimeout(() => listDetailRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 0);
      }
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  async function setStatus(programId: string, action: "enable" | "pause" | "archive") {
    setError(null);
    const program = programs.find((item) => item.program_id === programId);
    if (action === "enable") {
      const confirmed = window.confirm(`确认启用荐股任务「${program?.program_name || programId}」？启用后会进入每日复评与排行榜统计。`);
      if (!confirmed) return;
    }
    try {
      if (action === "enable") await advisoryApi.enable(programId);
      if (action === "pause") await advisoryApi.pause(programId);
      if (action === "archive") await advisoryApi.archive(programId);
      await refreshAll(programId);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  async function cloneProgram(programId: string) {
    setError(null);
    try {
      const program = await advisoryApi.clone(programId);
      await refreshAll(program.program_id);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  async function runReview(program: AdvisoryProgram, preview: boolean, targetDate?: string, selectionAsOfDate?: string | null) {
    const programVersions = program.program_id === selectedProgram?.program_id && loadedDetailsProgramId === program.program_id ? listVersions : [];
    const context = reviewActionContext(tradingDefaults, program, programVersions, targetDate || (program.program_id === selectedProgram?.program_id ? reviewTradeDate : ""));
    if (!context.state.canPreview || (!preview && !context.state.canRun)) return;
    setError(null);
    setReviewResult(null);
    setReviewingKey(`${program.program_id}:${preview ? "preview" : "run"}`);
    try {
      const resolvedSelectionAsOfDate = selectionAsOfDate ?? context.selectionAsOfDate;
      const payload = buildReviewPayload(program, context.targetDate, resolvedSelectionAsOfDate);
      const result = preview
        ? await advisoryApi.previewReview(program.program_id, payload)
        : await advisoryApi.runReview(program.program_id, payload);
      if (!preview) await refreshAll(program.program_id, { resetReviewDate: true });
      setReviewResult(result);
      setSelectedListVersionId(result.list_version_id || "");
      if (result.list_items && result.list_version_id) {
        setListDetailSource("review_result");
        const activeCount = result.list_items.filter((item) => item.item_state === "ACTIVE").length;
        setListVersionDetail({
          list_version: {
            list_version_id: result.list_version_id,
            program_id: program.program_id,
            binding_version_id: result.binding_version_id || activeBinding?.binding_version_id || "",
            review_run_id: result.review_run_id || "",
            trade_date: result.trade_date,
            previous_list_version_id: listVersions[0]?.list_version_id || null,
            version_status: result.preview ? "PREVIEW" : "PUBLISHED",
            target_count: program.target_count,
            active_count: activeCount,
            entered_count: Number(result.change_summary?.entered_count ?? 0),
            held_count: Number(result.change_summary?.held_count ?? 0),
            exited_count: Number(result.change_summary?.exited_count ?? 0),
            waiting_count: Number(result.change_summary?.waiting_count ?? 0),
            changed_count: Number(result.change_summary?.changed_count ?? result.decisions.length),
            turnover_rate: result.change_summary?.turnover_rate as number | null | undefined,
            overlap_rate: result.change_summary?.overlap_rate as number | null | undefined,
          },
          items: result.list_items,
        });
      } else {
        setListDetailSource("latest");
      }
      window.setTimeout(() => listDetailRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 0);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setReviewingKey("");
    }
  }

  async function runCatchUp(program: AdvisoryProgram, targetDates: string[]) {
    const programVersions = program.program_id === selectedProgram?.program_id && loadedDetailsProgramId === program.program_id ? listVersions : [];
    const context = reviewActionContext(tradingDefaults, program, programVersions);
    const orderedTargetDates = sortedDates(targetDates).filter((date) => context.progress.missingTargetDates.includes(date));
    if (!orderedTargetDates.length) return;
    setError(null);
    setReviewResult(null);
    setReviewingKey(`${program.program_id}:catchup`);
    try {
      let lastResult: AdvisoryReviewResult | null = null;
      for (const [index, targetDate] of orderedTargetDates.entries()) {
        setCatchUpProgress({ programId: program.program_id, index: index + 1, total: orderedTargetDates.length, targetDate });
        const selectionAsOfDate = selectionAsOfForTarget(targetDate, tradingDefaults);
        lastResult = await advisoryApi.runReview(program.program_id, buildReviewPayload(program, targetDate, selectionAsOfDate));
      }
      await refreshAll(program.program_id, { preserveReviewDate: false, resetReviewDate: true });
      setReviewResult(lastResult);
      if (lastResult?.list_version_id) setSelectedListVersionId(lastResult.list_version_id);
      window.setTimeout(() => listDetailRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 0);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setCatchUpProgress(null);
      setReviewingKey("");
    }
  }

  async function runReplay() {
    if (!selectedProgram) return;
    setError(null);
    setReplayResult(null);
    try {
      const replay = await advisoryApi.replay(selectedProgram.program_id, {
        start_date: replayStart,
        end_date: replayEnd,
      });
      setReplayResult(replay);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  function setProgramStrategyDraft(programId: string, updater: (draft: ProgramStrategyDraft) => ProgramStrategyDraft) {
    setProgramStrategyDrafts((drafts) => {
      const program = programs.find((item) => item.program_id === programId) || leaderboard.find((item) => item.program_id === programId);
      if (!program) return drafts;
      const current = drafts[programId] || strategyDraftFromProgram(program, activeBindingForProgram(programId));
      return { ...drafts, [programId]: updater(current) };
    });
  }

  async function toggleStrategyManager(program: AdvisoryProgram) {
    if (expandedStrategyProgramId === program.program_id) {
      setExpandedStrategyProgramId("");
      return;
    }
    setExpandedStrategyProgramId(program.program_id);
    if (programStrategyDrafts[program.program_id]) return;
    setStrategyActionKey(`${program.program_id}:load-binding`);
    try {
      let binding = activeBindingForProgram(program.program_id);
      if (!binding) {
        binding = await advisoryApi.activeBinding(program.program_id);
      }
      setProgramStrategyDrafts((drafts) => ({
        ...drafts,
        [program.program_id]: strategyDraftFromProgram(program, binding),
      }));
    } catch {
      setProgramStrategyDrafts((drafts) => ({
        ...drafts,
        [program.program_id]: strategyDraftFromProgram(program),
      }));
    } finally {
      setStrategyActionKey("");
    }
  }

  function updateStrategyRow(programId: string, rowId: string, patch: Partial<PackageWeightRow>) {
    setProgramStrategyDraft(programId, (draft) => ({
      ...draft,
      rows: draft.rows.map((row) => row.rowId === rowId ? { ...row, ...patch } : row),
    }));
  }

  async function runProgramStrategyReplay(program: AdvisoryProgram) {
    const draft = programStrategyDrafts[program.program_id] || strategyDraftFromProgram(program, activeBindingForProgram(program.program_id));
    const startDate = replayStart || tradingDefaults?.replay_start_date || tradingDefaults?.trading_days?.[0] || "";
    const endDate = replayEnd || tradingDefaults?.replay_end_date || tradingDefaults?.latest_trading_day || "";
    if (!startDate || !endDate) {
      setError("请先选择回放验证的开始和结束交易日。");
      return;
    }
    setError(null);
    setStrategyActionKey(`${program.program_id}:replay`);
    try {
      const binding = bindingPayloadFromDraft(draft);
      const replay = await advisoryApi.replay(program.program_id, {
        start_date: startDate,
        end_date: endDate,
        draft_binding: binding,
        compare_to_binding_version_id: draft.activeBindingVersionId || activeBindingForProgram(program.program_id)?.binding_version_id || null,
        include_daily_items: false,
      });
      setProgramStrategyDrafts((drafts) => ({
        ...drafts,
        [program.program_id]: { ...(drafts[program.program_id] || draft), replayResult: replay },
      }));
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setStrategyActionKey("");
    }
  }

  async function applyProgramStrategyBinding(program: AdvisoryProgram) {
    const draft = programStrategyDrafts[program.program_id] || strategyDraftFromProgram(program, activeBindingForProgram(program.program_id));
    const activationReason = draft.activationReason.trim() || `更新荐股任务「${program.program_name}」策略包配置`;
    setError(null);
    setStrategyActionKey(`${program.program_id}:apply`);
    try {
      const binding = bindingPayloadFromDraft(draft);
      const replayRun = draft.replayResult?.replay_run as JsonObject | undefined;
      const result = await advisoryApi.applyBinding(program.program_id, {
        binding,
        activation_reason: activationReason,
        source_replay_run_id: typeof replayRun?.replay_run_id === "string" ? replayRun.replay_run_id : null,
        effective_from_trade_date: tradingDefaults?.next_trading_day || tradingDefaults?.latest_trading_day || null,
        created_by: "advisory_ui",
      });
      setProgramStrategyDrafts((drafts) => ({
        ...drafts,
        [program.program_id]: {
          ...strategyDraftFromProgram(result.program, result.binding),
          activationReason,
          activeBindingVersionId: result.binding.binding_version_id,
          replayResult: draft.replayResult,
          applyResult: result.binding,
        },
      }));
      const keepSelectedProgramId = selectedProgramId || program.program_id;
      await refreshAll(keepSelectedProgramId, { preserveReviewDate: true });
      setExpandedStrategyProgramId(program.program_id);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setStrategyActionKey("");
    }
  }

  async function buildQualityReport() {
    setError(null);
    setQualityReport(null);
    try {
      const records = qualityRows
        .filter((row) => row.code.trim() || row.tradeDate || row.currentPrice.trim() || row.maxBuyPrice.trim())
        .map(buildQualityRecord);
      if (!records.length) throw new Error("请至少填写一条诊断样本");
      setQualityReport(await advisoryApi.qualityReport(records, 1));
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  function updatePackageRow(rowId: string, patch: Partial<PackageWeightRow>) {
    setPackageRows((rows) => rows.map((row) => row.rowId === rowId ? { ...row, ...patch } : row));
  }

  function updateQualityRow(rowId: string, patch: Partial<QualityInputRow>) {
    setQualityRows((rows) => rows.map((row) => row.rowId === rowId ? { ...row, ...patch } : row));
  }

  async function changeReviewPage(nextPage: number) {
    if (!selectedProgram) return;
    setDetailsLoading(true);
    setError(null);
    try {
      await loadReviews(selectedProgram.program_id, nextPage, reviewPageSize);
      setReviewPage(nextPage);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setDetailsLoading(false);
    }
  }

  async function changeReviewPageSize(nextPageSize: (typeof REVIEW_PAGE_SIZE_OPTIONS)[number]) {
    if (!selectedProgram) {
      setReviewPageSize(nextPageSize);
      setReviewPage(1);
      return;
    }
    setDetailsLoading(true);
    setError(null);
    try {
      await loadReviews(selectedProgram.program_id, 1, nextPageSize);
      setReviewPageSize(nextPageSize);
      setReviewPage(1);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setDetailsLoading(false);
    }
  }

  async function viewListVersion(listVersionId: string) {
    setListVersionLoadingId(listVersionId);
    setError(null);
    try {
      const detail = await advisoryApi.listVersionDetail(listVersionId);
      setListVersionDetail(detail);
      setSelectedListVersionId(detail.list_version.list_version_id);
      setListDetailSource("timeline");
      setReviewResult(null);
      window.setTimeout(() => listDetailRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 0);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setListVersionLoadingId("");
    }
  }

  const selectedVersionsForContext = selectedProgram && loadedDetailsProgramId === selectedProgram.program_id ? listVersions : [];
  const selectedDateContextLoading = Boolean(selectedProgram && loadedDetailsProgramId !== selectedProgram.program_id);
  const selectedActionContext = selectedProgram
    ? reviewActionContext(tradingDefaults, selectedProgram, selectedVersionsForContext, reviewTradeDate)
    : null;
  const reviewDateChoices = selectedDateContextLoading ? [] : selectedActionContext?.dateChoices || [];
  const selectedReviewDateChoice = selectedDateContextLoading ? undefined : selectedActionContext?.selectedChoice;
  const reviewSelectionAsOfDate = selectedDateContextLoading ? null : selectedActionContext?.selectionAsOfDate || null;
  const selectedReviewProgress = selectedActionContext?.progress || reviewProgress(tradingDefaults, selectedProgram, selectedVersionsForContext, reviewTradeDate);
  const selectedBlockingMissingDate = selectedDateContextLoading ? null : selectedActionContext?.blockingMissingTargetDate || null;
  const selectedReviewState = selectedDateContextLoading
    ? loadingReviewState("正在加载该任务最新预测目标日与缺失复评日期，加载完成后按钮状态会固定。")
    : selectedActionContext?.state || null;
  const selectedPreviewKey = selectedProgram ? `${selectedProgram.program_id}:preview` : "";
  const selectedRunKey = selectedProgram ? `${selectedProgram.program_id}:run` : "";
  const selectedCatchUpKey = selectedProgram ? `${selectedProgram.program_id}:catchup` : "";
  const activeBinding = bindings.find((item) => item.activation_status === "ACTIVE") || bindings[0];
  const selectedLatestMeta = latestRecommendationMeta(selectedProgram, selectedVersionsForContext);
  const reviewResultDetailActive = listDetailSource === "review_result" && Boolean(reviewResult?.list_version_id);
  const visibleListItems = reviewResultDetailActive ? (reviewResult?.list_items || []) : listVersionDetail?.items || [];
  const visibleListVersion: AdvisoryRecommendationListVersion | undefined = reviewResultDetailActive && reviewResult?.list_version_id
    ? {
        list_version_id: reviewResult.list_version_id,
        program_id: reviewResult.program.program_id,
        binding_version_id: reviewResult.binding_version_id || activeBinding?.binding_version_id || "",
        review_run_id: reviewResult.review_run_id || "",
        trade_date: reviewResult.trade_date,
        previous_list_version_id: listVersions[0]?.list_version_id || null,
        version_status: reviewResult.preview ? "PREVIEW" : "PUBLISHED",
        target_count: reviewResult.program.target_count,
        summary_json: reviewResult.change_summary || {},
        target_trade_date: typeof reviewResult.change_summary?.advisory_date_context === "object" ? String((reviewResult.change_summary.advisory_date_context as JsonObject).target_trade_date || reviewResult.trade_date) : reviewResult.trade_date,
        selection_as_of_trade_date: typeof reviewResult.change_summary?.advisory_date_context === "object" ? String((reviewResult.change_summary.advisory_date_context as JsonObject).selection_as_of_trade_date || "") || null : null,
        active_count: visibleListItems.filter((item) => item.item_state === "ACTIVE").length,
        entered_count: Number(reviewResult.change_summary?.entered_count ?? 0),
        held_count: Number(reviewResult.change_summary?.held_count ?? 0),
        exited_count: Number(reviewResult.change_summary?.exited_count ?? 0),
        waiting_count: Number(reviewResult.change_summary?.waiting_count ?? 0),
        changed_count: Number(reviewResult.change_summary?.changed_count ?? reviewResult.decisions.length),
        turnover_rate: reviewResult.change_summary?.turnover_rate as number | null | undefined,
        overlap_rate: reviewResult.change_summary?.overlap_rate as number | null | undefined,
        created_at: null,
      }
    : listVersionDetail?.list_version || undefined;
  const visibleListSourceLabel = listDetailSource === "review_result" ? "刚刚执行结果" : listDetailSource === "timeline" ? "手动查看版本" : "最新列表版本";
  const finalWatchlistItems = useMemo(() => finalRecommendationItems(visibleListItems), [visibleListItems]);
  const watchlistCategoryName = useMemo(
    () => defaultWatchlistCategoryName(selectedProgram, visibleListVersion),
    [selectedProgram, visibleListVersion],
  );
  const canAddFinalWatchlist = Boolean(
    selectedProgram &&
      visibleListVersion?.version_status === "PUBLISHED" &&
      finalWatchlistItems.length &&
      !addingWatchlist,
  );

  async function ensureWatchlistCategory(name: string) {
    const existing = (await advisoryApi.watchlistCategories()).find((category) => category.name === name);
    if (existing) return existing;
    try {
      return await advisoryApi.createWatchlistCategory(name, `荐股中心自动创建：${name}`);
    } catch (exc) {
      const raced = (await advisoryApi.watchlistCategories()).find((category) => category.name === name);
      if (raced) return raced;
      throw exc;
    }
  }

  async function addFinalListToWatchlist() {
    if (!selectedProgram || !visibleListVersion) return;
    if (visibleListVersion.version_status !== "PUBLISHED") {
      setError("只有正式发布后的最终荐股名单可以加入自选股票池；预览结果请先执行正式复评。");
      return;
    }
    if (!finalWatchlistItems.length) {
      setError("当前列表没有 ACTIVE 状态的最终荐股，无法加入自选股票池。");
      return;
    }
    setAddingWatchlist(true);
    setError(null);
    try {
      const category = await ensureWatchlistCategory(watchlistCategoryName);
      const codes = finalWatchlistItems.map((item) => item.symbol);
      const result = await advisoryApi.addWatchlistItems(codes, category.id);
      const added = result.added ?? result.inserted ?? 0;
      const skipped = result.skipped ?? Math.max(0, codes.length - added);
      window.alert(`已将 ${codes.length} 只最终荐股加入自选股票池分类「${category.name}」。新增 ${added} 只，已存在 ${skipped} 只。`);
    } catch (exc) {
      const message = exc instanceof Error ? exc.message : String(exc);
      setError(message);
    } finally {
      setAddingWatchlist(false);
    }
  }

  async function syncFinalListToTdx() {
    if (!canAddFinalWatchlist) return;
    setTdxSyncing(true);
    setTdxSyncResult(null);
    setError(null);
    try {
      const result = await advisoryApi.tdxSyncFromCategory(watchlistCategoryName);
      setTdxSyncResult({ display_name: result.display_name, count: result.count });
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setTdxSyncing(false);
    }
  }

  function renderStrategyManager(program: AdvisoryProgram): ReactNode {
    const draft = programStrategyDrafts[program.program_id] || strategyDraftFromProgram(program, activeBindingForProgram(program.program_id));
    const loadingBinding = strategyActionKey === `${program.program_id}:load-binding`;
    const replayRunning = strategyActionKey === `${program.program_id}:replay`;
    const applyRunning = strategyActionKey === `${program.program_id}:apply`;
    const replayRun = draft.replayResult?.replay_run as JsonObject | undefined;
    const replaySummary = draft.replayResult?.summary as JsonObject | undefined;
    return (
      <div className="pv2-readable-panel" data-testid={`advisory-strategy-manager-${program.program_id}`}>
        <div className="pv2-card-head">
          <div>
            <div className="pv2-kicker">独立策略包配置</div>
            <h3 style={{ margin: 0 }}>{program.program_name}</h3>
            <p className="pv2-muted" data-testid={`advisory-strategy-manager-hint-${program.program_id}`}>
              当前面板只作用于本荐股任务；回放与应用都会按 program_id 调用接口，不会修改其他任务的荐股列表或 active binding。
            </p>
          </div>
          <button
            className="pv2-button-ghost"
            onClick={() => setExpandedStrategyProgramId("")}
            type="button"
          >
            收起
          </button>
        </div>
        <div className="pv2-form-grid" style={{ marginTop: 8 }}>
          <label className="pv2-field">
            策略模式
            <select
              className="pv2-select"
              data-testid={`advisory-strategy-mode-${program.program_id}`}
              disabled={loadingBinding}
              value={draft.packageMode}
              onChange={(event) => setProgramStrategyDraft(program.program_id, (current) => ({
                ...current,
                packageMode: event.target.value as AdvisoryPackageMode,
                replayResult: null,
                applyResult: null,
              }))}
            >
              {PACKAGE_MODE_OPTIONS.map((mode) => <option key={mode} value={mode}>{mode}</option>)}
            </select>
          </label>
          <label className="pv2-field">
            目标数量
            <input
              className="pv2-input"
              data-testid={`advisory-strategy-target-count-${program.program_id}`}
              min={1}
              max={100}
              type="number"
              value={draft.targetCount}
              onChange={(event) => setProgramStrategyDraft(program.program_id, (current) => ({ ...current, targetCount: event.target.value, replayResult: null, applyResult: null }))}
            />
          </label>
          <label className="pv2-field">
            应用原因
            <input
              className="pv2-input"
              data-testid={`advisory-strategy-activation-reason-${program.program_id}`}
              value={draft.activationReason}
              onChange={(event) => setProgramStrategyDraft(program.program_id, (current) => ({ ...current, activationReason: event.target.value }))}
            />
          </label>
        </div>
        <div className="pv2-table-wrap" style={{ marginTop: 10 }}>
          <table className="pv2-table">
            <thead><tr><th>策略包</th><th>权重</th><th>说明</th><th>操作</th></tr></thead>
            <tbody>
              {draft.rows.map((row, index) => (
                <tr key={row.rowId}>
                  <td>
                    <select
                      className="pv2-select"
                      data-testid={`advisory-strategy-package-${program.program_id}-${row.rowId}`}
                      value={row.packageId}
                      onChange={(event) => updateStrategyRow(program.program_id, row.rowId, { packageId: event.target.value })}
                    >
                      <option value="">选择策略包</option>
                      {selectablePackages.map((pkg) => (
                        <option key={pkg.package_id} value={pkg.package_id}>{packageOptionLabel(pkg)}</option>
                      ))}
                      {row.packageId && !packageById.has(row.packageId) ? (
                        <option value={row.packageId}>当前绑定策略包 {shortHash(row.packageId, 7)}</option>
                      ) : null}
                    </select>
                    <div className="pv2-muted" style={{ marginTop: 4 }}>
                      {row.packageId ? `${packageLabel(row.packageId)} / ${packageById.get(row.packageId)?.package_status || "当前绑定"}` : "从下拉菜单选择策略包，不需要填写或记忆内部 ID。"}
                    </div>
                  </td>
                  <td>
                    <input
                      className="pv2-input"
                      min="0.01"
                      step="0.01"
                      type="number"
                      value={row.weight}
                      onChange={(event) => updateStrategyRow(program.program_id, row.rowId, { weight: event.target.value })}
                    />
                  </td>
                  <td className="pv2-muted">
                    {draft.packageMode === "single_package" && index > 0 ? "单策略包模式仅使用第一行" : modeNeedsWeights(draft.packageMode) ? "参与加权融合；权重不作为硬门禁" : "参与荐股集合运算"}
                  </td>
                  <td>
                    <button
                      className="pv2-button-ghost"
                      disabled={draft.rows.length <= 1}
                      onClick={() => setProgramStrategyDraft(program.program_id, (current) => ({ ...current, rows: current.rows.filter((item) => item.rowId !== row.rowId), replayResult: null, applyResult: null }))}
                      type="button"
                    >
                      移除
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 10 }}>
          <button
            className="pv2-button"
            onClick={() => setProgramStrategyDraft(program.program_id, (current) => ({
              ...current,
              rows: [...current.rows, newPackageRow(current.rows.length + 1)],
              replayResult: null,
              applyResult: null,
            }))}
            type="button"
          >
            添加策略包
          </button>
          <span className="pv2-muted">回放区间：{replayStart || "未选择"} 至 {replayEnd || "未选择"}；可在页面底部回放区间中调整。</span>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 10 }}>
          <button
            className="pv2-button"
            data-testid={`advisory-strategy-replay-${program.program_id}`}
            disabled={loadingBinding || replayRunning || applyRunning || !replayStart || !replayEnd}
            onClick={() => void runProgramStrategyReplay(program)}
            type="button"
          >
            {replayRunning ? "回放验证中..." : "回放验证"}
          </button>
          <button
            className="pv2-button-primary"
            data-testid={`advisory-strategy-apply-${program.program_id}`}
            disabled={loadingBinding || replayRunning || applyRunning}
            onClick={() => void applyProgramStrategyBinding(program)}
            type="button"
          >
            {applyRunning ? "应用中..." : "应用新策略绑定"}
          </button>
          <span className="pv2-muted">回放是人工验证入口，不设置程序硬门禁；应用后仅替换本任务策略包配置，后续复评继续从本任务最新荐股列表迭代。</span>
        </div>
        {draft.replayResult ? (
          <div className="pv2-readable-panel" style={{ marginTop: 10 }} data-testid={`advisory-strategy-replay-result-${program.program_id}`}>
            回放状态：{String(replayRun?.status || "-")}；胜率 {fmtPct(replaySummary?.win_rate as number | null | undefined)}；平均涨幅 {fmtBps(replaySummary?.avg_return_bps as number | null | undefined)}
          </div>
        ) : null}
        {draft.applyResult ? (
          <div className="pv2-readable-panel" style={{ marginTop: 10 }} data-testid={`advisory-strategy-apply-result-${program.program_id}`}>
            已应用新策略绑定：{draft.applyResult.package_mode} / {packageSummary(draft.applyResult.package_ids)}
          </div>
        ) : null}
      </div>
    );
  }

  return (
    <main className="pv2-main">
      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-kicker">荐股中心</div>
            <h2>运行中的荐股任务排行榜</h2>
            <p className="pv2-muted">默认按胜率排序；自动保留的质量/状态字段仅为 last_review_status。</p>
          </div>
          <div className="pv2-row-actions">
            <select className="pv2-select" value={sortBy} onChange={(event) => setSortBy(event.target.value)}>
              <option value="win_rate">胜率</option>
              <option value="avg_return_bps">平均涨幅</option>
              <option value="median_return_bps">中位涨幅</option>
              <option value="entered_episode_count">累计荐股数</option>
              <option value="enabled_since">启用时间</option>
              <option value="max_drawdown_bps">最大回撤</option>
            </select>
            <button className="pv2-button" onClick={() => refreshAll()} disabled={loading} type="button">刷新</button>
          </div>
        </div>
        {error ? <div className="pv2-error-panel">{error}</div> : null}
        {loading || leaderboardLoading ? (
          <div className="pv2-readable-panel" data-testid="advisory-fast-loading-status" style={{ marginBottom: 12 }}>
            {loading
              ? "正在加载荐股任务基础信息；任务列表会先显示，排行榜统计和明细随后异步补齐。"
              : "荐股任务已显示，排行榜统计和所选任务明细仍在异步补齐。"}
          </div>
        ) : null}
        <div className="pv2-table-wrap">
          <table className="pv2-table">
            <thead>
              <tr>
                <th>荐股任务</th>
                <th>状态</th>
                <th>启用时间</th>
                <th>累计</th>
                <th>当前持有</th>
                <th>止盈</th>
                <th>止损</th>
                <th>胜率</th>
                <th>平均涨幅</th>
                <th>最近复评</th>
                <th>每日复评</th>
              </tr>
            </thead>
            <tbody>
              {leaderboard.map((row) => {
                const rowVersions = row.program_id === selectedProgram?.program_id && loadedDetailsProgramId === row.program_id ? listVersions : [];
                const rowLatestMeta = latestRecommendationMeta(row, rowVersions);
                const rowActionContext = reviewActionContext(tradingDefaults, row, rowVersions);
                const rowProgress = rowActionContext.progress;
                const rowContextLoading = (leaderboardLoading && row.metric_status === "LOADING")
                  || (row.program_id === selectedProgram?.program_id && loadedDetailsProgramId !== row.program_id);
                const state = rowContextLoading ? loadingReviewState("正在加载该任务复评日期上下文，加载完成后按钮状态会固定。") : rowActionContext.state;
                const previewKey = `${row.program_id}:preview`;
                const runKey = `${row.program_id}:run`;
                const catchUpKey = `${row.program_id}:catchup`;
                const catchUpRunning = catchUpProgress?.programId === row.program_id;
                return (
                  <Fragment key={row.program_id}>
                    <tr onClick={() => void selectProgram(row.program_id)}>
                      <td><strong>{row.program_name}</strong><br /><span className="pv2-muted">{packageSummary(row.package_ids)}</span></td>
                      <td>{row.status}</td>
                      <td>{short(row.enabled_since, 16)}</td>
                      <td>{row.entered_episode_count ?? 0}</td>
                      <td>{row.active_count ?? 0}</td>
                      <td>{row.take_profit_count ?? 0}</td>
                      <td>{row.stop_loss_count ?? 0}</td>
                      <td>{fmtPct(row.win_rate)}<br /><span className="pv2-muted">{metricStatusText(row)}</span></td>
                      <td>{fmtBps(row.avg_return_bps)}<br /><span className="pv2-muted">样本 {row.metric_evaluable_count ?? 0}/{row.active_count ?? 0}</span></td>
                      <td data-testid={`advisory-row-latest-context-${row.program_id}`}>
                        {row.last_review_status || "STALE"}
                        <br /><span className="pv2-muted">预测目标：{rowLatestMeta.targetTradeDate || "尚未生成"}</span>
                        <br /><span className="pv2-muted">数据截止：{rowLatestMeta.selectionAsOfDate || "未记录"}</span>
                        <br /><span className="pv2-muted">生成时间：{short(rowLatestMeta.generatedAt, 16)}</span>
                      </td>
                      <td>
                        <div className="pv2-muted" data-testid={`advisory-row-review-progress-${row.program_id}`} style={{ marginBottom: 6 }}>
                          已复评到：{rowProgress.reviewedThroughDate || "尚未开始"}；缺失：{compactDateList(rowProgress.missingTargetDates)}
                          <br />本行待执行目标：{rowActionContext.targetDate || "-"}；数据截止：{rowActionContext.selectionAsOfDate || "系统解析"}
                          {catchUpRunning ? <><br />补跑 {catchUpProgress.index}/{catchUpProgress.total}：{catchUpProgress.targetDate}</> : null}
                          {rowContextLoading ? <><br />正在加载该任务完整版本时间线...</> : null}
                        </div>
                        <div className="pv2-row-actions">
                          <button
                            className="pv2-button"
                            data-testid={`advisory-preview-${row.program_id}`}
                            disabled={!state.canPreview || Boolean(reviewingKey)}
                            onClick={(event) => { event.stopPropagation(); void runReview(row, true, rowActionContext.targetDate, rowActionContext.selectionAsOfDate); }}
                            type="button"
                          >
                            {reviewingKey === previewKey ? "预览中..." : state.previewLabel}
                          </button>
                          <button
                            className="pv2-button-primary"
                            data-testid={`advisory-run-${row.program_id}`}
                            disabled={!state.canRun || Boolean(reviewingKey)}
                            onClick={(event) => { event.stopPropagation(); void runReview(row, false, rowActionContext.targetDate, rowActionContext.selectionAsOfDate); }}
                            type="button"
                          >
                            {reviewingKey === runKey ? "执行中..." : state.label}
                          </button>
                          <button
                            className="pv2-button"
                            data-testid={`advisory-catchup-${row.program_id}`}
                            disabled={rowContextLoading || !rowProgress.missingTargetDates.length || Boolean(reviewingKey)}
                            onClick={(event) => { event.stopPropagation(); void runCatchUp(row, rowProgress.missingTargetDates); }}
                            type="button"
                          >
                            {reviewingKey === catchUpKey && catchUpProgress ? `补跑 ${catchUpProgress.index}/${catchUpProgress.total}` : `补齐全部缺失复评（${rowProgress.missingTargetDates.length} 天）`}
                          </button>
                          <button
                            className="pv2-button"
                            data-testid={`advisory-manage-strategy-${row.program_id}`}
                            onClick={(event) => { event.stopPropagation(); void toggleStrategyManager(row); }}
                            type="button"
                          >
                            {expandedStrategyProgramId === row.program_id ? "收起策略包" : "策略包配置"}
                          </button>
                        </div>
                        <span className="pv2-muted">{state.hint}</span>
                      </td>
                    </tr>
                    {expandedStrategyProgramId === row.program_id ? (
                      <tr>
                        <td colSpan={11}>{renderStrategyManager(row)}</td>
                      </tr>
                    ) : null}
                  </Fragment>
                );
              })}
              {!leaderboard.length ? (
                <tr><td colSpan={11}>暂无启用中的荐股任务；请在下方创建，页面不会展示 mock 行。</td></tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-kicker">任务设置</div>
            <h2>创建或管理独立荐股任务</h2>
            <p className="pv2-muted">每个任务按 program_id 隔离，支持单策略包或加权融合策略包组合。</p>
          </div>
          <button className="pv2-button-primary" onClick={createProgram} type="button">创建并启用</button>
        </div>
        <div className="pv2-form-grid">
          <label className="pv2-field">任务名称<input className="pv2-input" value={programName} onChange={(event) => setProgramName(event.target.value)} /></label>
          <label className="pv2-field">策略模式<select className="pv2-select" value={packageMode} onChange={(event) => setPackageMode(event.target.value as AdvisoryPackageMode)}>{PACKAGE_MODE_OPTIONS.map((mode) => <option key={mode} value={mode}>{mode}</option>)}</select></label>
          <label className="pv2-field">目标数量<input className="pv2-input" type="number" min={1} max={100} value={targetCount} onChange={(event) => setTargetCount(Number(event.target.value))} /></label>
        </div>
        <div className="pv2-table-wrap" style={{ marginTop: 12 }}>
          <table className="pv2-table">
            <thead><tr><th>策略包</th><th>权重</th><th>说明</th><th>操作</th></tr></thead>
            <tbody>
              {packageRows.map((row, index) => (
                <tr key={row.rowId}>
                  <td>
                    <select
                      className="pv2-select"
                      data-testid={`advisory-package-select-${row.rowId}`}
                      value={row.packageId}
                      onChange={(event) => updatePackageRow(row.rowId, { packageId: event.target.value })}
                    >
                      <option value="">选择策略包</option>
                      {selectablePackages.map((pkg) => (
                        <option key={pkg.package_id} value={pkg.package_id}>{packageOptionLabel(pkg)}</option>
                      ))}
                      {row.packageId && !packageById.has(row.packageId) ? (
                        <option value={row.packageId}>已预填策略包 {shortHash(row.packageId, 7)}</option>
                      ) : null}
                    </select>
                    <div className="pv2-muted" style={{ marginTop: 4 }}>
                      {row.packageId ? `${packageLabel(row.packageId)} / ${packageById.get(row.packageId)?.package_status || "已预填"}` : "请从下拉菜单选择，无需记忆内部编号。"}
                    </div>
                  </td>
                  <td><input className="pv2-input" min="0.01" step="0.01" type="number" value={row.weight} onChange={(event) => updatePackageRow(row.rowId, { weight: event.target.value })} /></td>
                  <td className="pv2-muted">{packageMode === "single_package" && index > 0 ? "单策略包模式仅使用第一行" : modeNeedsWeights(packageMode) ? "参与加权融合；权重仅作配置，不写入策略包" : "参与荐股集合运算"}</td>
                  <td><button className="pv2-button-ghost" disabled={packageRows.length <= 1} onClick={() => setPackageRows((rows) => rows.filter((item) => item.rowId !== row.rowId))} type="button">移除</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 12 }}>
          <button className="pv2-button" onClick={() => setPackageRows((rows) => [...rows, newPackageRow(rows.length + 1)])} type="button">添加策略包</button>
          <span className="pv2-muted">加权融合模式使用每行权重；union/intersection 不把收益或换手指标作为硬门禁。</span>
        </div>
        <div className="pv2-table-wrap" style={{ marginTop: 16 }}>
          <table className="pv2-table">
            <thead><tr><th>名称</th><th>策略模式</th><th>状态</th><th>版本</th><th>操作</th></tr></thead>
            <tbody>
              {programs.map((program) => (
                <tr key={program.program_id}>
                  <td><button className="pv2-link-button" onClick={() => void selectProgram(program.program_id)} type="button">{program.program_name}</button></td>
                  <td>{program.package_mode}</td>
                  <td>{program.status}</td>
                  <td>{program.version}</td>
                  <td className="pv2-row-actions">
                    <button className="pv2-button" data-testid={`advisory-enable-${program.program_id}`} disabled={program.status === "ENABLED"} onClick={() => setStatus(program.program_id, "enable")} type="button">{program.status === "ENABLED" ? "已启用" : "启用"}</button>
                    <button className="pv2-button-ghost" onClick={() => setStatus(program.program_id, "pause")} type="button">暂停</button>
                    <button className="pv2-button-ghost" onClick={() => cloneProgram(program.program_id)} type="button">克隆</button>
                    <button className="pv2-button-danger" onClick={() => setStatus(program.program_id, "archive")} type="button">归档</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-kicker">{selectedReviewState?.isInitialRun ? "首次运行" : "每日复评"}</div>
            <h2>{selectedProgram ? selectedProgram.program_name : "未选择荐股任务"}</h2>
            <p className="pv2-muted">
              {detailsLoading ? "正在加载所选任务明细，排行榜和复评入口已可先查看。" : "上方表格是该荐股任务的最新/指定列表版本；下方“当前推荐股票”是仍处于 ACTIVE 的持有池，列表版本还会保留 ENTER/HOLD/EXIT/WAITING 的每日调整方案。"}
            </p>
          </div>
          <div className="pv2-row-actions">
            <button
              className="pv2-button"
              data-testid="advisory-selected-preview"
              onClick={() => selectedProgram && void runReview(selectedProgram, true)}
              disabled={!selectedProgram || !selectedReviewState?.canPreview || Boolean(reviewingKey)}
              type="button"
            >
              {reviewingKey === selectedPreviewKey ? "预览中..." : selectedReviewState?.previewLabel || "预览"}
            </button>
            <button
              className="pv2-button-primary"
              data-testid="advisory-selected-run"
              onClick={() => selectedProgram && void runReview(selectedProgram, false)}
              disabled={!selectedProgram || !selectedReviewState?.canRun || Boolean(reviewingKey)}
              type="button"
            >
              {reviewingKey === selectedRunKey ? "执行中..." : selectedReviewState?.label || "执行复评"}
            </button>
            <button
              className="pv2-button"
              data-testid="advisory-selected-catchup"
              onClick={() => selectedProgram && void runCatchUp(selectedProgram, selectedReviewProgress.missingTargetDates)}
              disabled={selectedDateContextLoading || !selectedProgram || !selectedReviewProgress.missingTargetDates.length || Boolean(reviewingKey)}
              type="button"
            >
              {reviewingKey === selectedCatchUpKey && catchUpProgress ? `补跑 ${catchUpProgress.index}/${catchUpProgress.total}` : `补齐全部缺失复评（${selectedReviewProgress.missingTargetDates.length} 天）`}
            </button>
          </div>
        </div>
        <div className="pv2-readable-panel" style={{ marginTop: 12 }}>
          <label className="pv2-field" style={{ marginBottom: 8 }}>
            目标荐股交易日
            <select
              className="pv2-select"
              data-testid="advisory-review-target-select"
              value={reviewTradeDate}
              onChange={(event) => { setReviewDateTouched(true); setReviewTradeDate(event.target.value); }}
              disabled={selectedDateContextLoading}
            >
              {!reviewDateChoices.length ? <option value={reviewTradeDate || ""}>{selectedDateContextLoading ? "正在加载复评日期上下文" : "等待交易日"}</option> : null}
              {reviewDateChoices.map((option) => (
                <option key={option.value} value={option.value} disabled={option.disabled}>{option.label}</option>
              ))}
              {reviewDateChoices.length && reviewTradeDate && !reviewDateChoices.some((option) => option.value === reviewTradeDate) ? <option value={reviewTradeDate}>{reviewTradeDate}</option> : null}
            </select>
          </label>
          <div data-testid="advisory-selected-latest-context">
            <strong>最新荐股结果：</strong>
            <span className="pv2-muted"> 预测目标 {selectedLatestMeta.targetTradeDate || "尚未生成"}；数据截止 {selectedLatestMeta.selectionAsOfDate || "未记录"}；生成时间 {short(selectedLatestMeta.generatedAt, 16)}；列表 {short(selectedLatestMeta.listVersionId, 18)}</span>
          </div>
          <strong>目标复评交易日： <span data-testid="advisory-review-target-date">{selectedDateContextLoading ? "加载中" : reviewTradeDate || "-"}</span></strong>
          <span className="pv2-muted"> {selectedReviewState?.hint || "选择启用中的荐股任务后即可复评。"}</span>
          <div className="pv2-muted" style={{ marginTop: 6 }} data-testid="advisory-review-data-cutoff">
            {selectedDateContextLoading
              ? "正在加载该任务最新预测目标日、数据截止日与缺失复评日期。"
              : `选股数据截止日：${reviewSelectionAsOfDate || "系统自动按目标交易日解析"}；${selectedReviewDateChoice?.description || "请选择目标荐股交易日。"}`}
          </div>
          <div className="pv2-readable-panel" style={{ marginTop: 10 }} data-testid="advisory-review-progress-summary">
            <strong>复评补跑进度：</strong>
            <span className="pv2-muted">
              {" "}首个列表目标日 {selectedReviewProgress.firstPublishedTargetDate || "尚未生成"}；
              最新已发布目标日 {selectedReviewProgress.latestPublishedTargetDate || "尚未生成"}；
              当前连续复评到 {selectedReviewProgress.reviewedThroughDate || "尚未开始"}。
            </span>
            <div className="pv2-muted" style={{ marginTop: 6 }} data-testid="advisory-review-missing-dates">
              后续缺失目标日：{compactDateList(selectedReviewProgress.missingTargetDates)}。
            </div>
            <div className="pv2-muted" style={{ marginTop: 6 }} data-testid="advisory-review-execution-scope">
              本次按钮只执行目标交易日 {selectedDateContextLoading ? "加载中" : reviewTradeDate || "-"}，不是一次性补全所有漏评日；
              {selectedReviewProgress.selectedRemainingMissingDates.length
                ? `执行成功后仍需继续补跑：${compactDateList(selectedReviewProgress.selectedRemainingMissingDates)}。`
                : "执行成功后若没有新的缺失日期，按钮会变为已复评或指向下一目标日。"}
              {selectedBlockingMissingDate ? ` 当前选择被限制：必须先补 ${selectedBlockingMissingDate}。` : ""}
              {catchUpProgress && catchUpProgress.programId === selectedProgram?.program_id ? ` 正在补齐全部缺失复评：${catchUpProgress.index}/${catchUpProgress.total}，目标 ${catchUpProgress.targetDate}。` : ""}
            </div>
          </div>
          <div className="pv2-muted" style={{ marginTop: 6 }}>生效日按目标荐股交易日与选股数据截止日计算；盘中运行若只用昨日收盘数据，入选生效日应落到今天这个交易日，而不是简单自然日加一天。</div>
          {activeBinding ? (
            <div className="pv2-muted" style={{ marginTop: 6 }}>
              当前策略绑定：{activeBinding.package_mode} / {packageSummary(activeBinding.package_ids)} / {short(activeBinding.binding_version_id, 18)}
            </div>
          ) : null}
        </div>
        {reviewResult ? (
          <div className="pv2-readable-panel" style={{ marginTop: 12 }}>
            <strong>复评状态： {reviewResult.review_status}</strong>
            <span className="pv2-muted"> 决策数： {reviewResult.decisions.length}; 活跃快照数： {reviewResult.active_pool.length}; 列表版本： {short(reviewResult.list_version_id, 20)}</span>
          </div>
        ) : null}
        {visibleListVersion ? (
          <div className="pv2-readable-panel" ref={listDetailRef} style={{ marginTop: 12 }} data-testid="advisory-list-version-summary" tabIndex={-1}>
            <strong>当前查看：{visibleListSourceLabel} / {short(visibleListVersion.list_version_id, 22)} / {visibleListVersion.trade_date} / {visibleListVersion.version_status}</strong>
            <span className="pv2-muted">
              {" "}ACTIVE {visibleListVersion.active_count}，ENTER {visibleListVersion.entered_count}，HOLD {visibleListVersion.held_count}，EXIT {visibleListVersion.exited_count}，WAITING {visibleListVersion.waiting_count}，换手 {fmtPct(visibleListVersion.turnover_rate)}，重合 {fmtPct(visibleListVersion.overlap_rate)}
            </span>
            <div className="pv2-muted" style={{ marginTop: 6 }} data-testid="advisory-visible-list-date-context">
              预测目标交易日：{dateContextFromListVersion(visibleListVersion).targetTradeDate || visibleListVersion.trade_date}；选股数据截止日：{dateContextFromListVersion(visibleListVersion).selectionAsOfDate || "未记录"}；生成时间：{short(visibleListVersion.created_at, 16)}
            </div>
            {visibleListVersion.waiting_count > 0 ? (
              <div className="pv2-muted" style={{ marginTop: 6 }} data-testid="advisory-list-waiting-hint">
                WAITING 表示该版本已经生成，但对应股票仍在等待行情、停复牌或可交易性确认；这不是未刷新旧表。
              </div>
            ) : null}
            <div className="pv2-row-actions" style={{ marginTop: 10 }}>
              <button
                className="pv2-button-primary"
                data-testid="advisory-add-final-watchlist"
                disabled={!canAddFinalWatchlist}
                onClick={() => void addFinalListToWatchlist()}
                type="button"
              >
                {addingWatchlist ? "加入中..." : "一键加入自选股票池"}
              </button>
              {tdxAvailable && (
                <button
                  className="pv2-button"
                  data-testid="advisory-tdx-sync"
                  disabled={!canAddFinalWatchlist || tdxSyncing}
                  onClick={() => void syncFinalListToTdx()}
                  type="button"
                >
                  📡 加入通达信自选
                </button>
              )}
              <span className="pv2-muted" data-testid="advisory-watchlist-category-hint">
                默认分类：{watchlistCategoryName}；仅加入当前 PUBLISHED 列表中 ACTIVE 的最终荐股，共 {finalWatchlistItems.length} 只。
              </span>
              {tdxSyncResult && (
                <div className="pv2-readable-panel" style={{ marginTop: 8 }} data-testid="advisory-tdx-sync-result">
                  ✅ 板块「{tdxSyncResult.display_name}」，已同步 {tdxSyncResult.count} 只股票到通达信客户端。
                </div>
              )}
            </div>
          </div>
        ) : (
          <div className="pv2-readable-panel" ref={listDetailRef} style={{ marginTop: 12 }} data-testid="advisory-list-version-summary" tabIndex={-1}>
            <strong>尚未生成初始列表</strong>
            <span className="pv2-muted"> 点击“预览初始列表”可先检查候选，点击“生成初始列表”会发布第一版推荐列表；全程自动生成候选，无需填写内部编号。</span>
          </div>
        )}
        <div className="pv2-table-wrap" style={{ marginTop: 12 }}>
          <table className="pv2-table" data-testid="advisory-list-items-table">
            <thead><tr><th>股票</th><th>状态</th><th>动作</th><th>排名/评分</th><th>变化</th><th>操作建议</th><th>生效日</th><th>原因</th></tr></thead>
            <tbody>
              {visibleListItems.map((item) => (
                <tr key={item.list_item_id}>
                  <td>{stockLabel(item)}</td>
                  <td>{item.item_state}</td>
                  <td>{item.action}</td>
                  <td>{item.rank ?? "-"} / {fmtNumber(item.score, 3)}</td>
                  <td>{item.previous_action ? `${item.previous_action} -> ${item.action}` : item.action}</td>
                  <td>{adviceText(item)}<br /><span className="pv2-muted">{item.price_basis || "-"} @ {fmtPrice(item.action === "EXIT" ? item.exit_price : item.entry_price)}</span></td>
                  <td>{item.effective_trade_date || "-"}</td>
                  <td>{item.reason_code}</td>
                </tr>
              ))}
              {!visibleListItems.length ? <tr><td colSpan={8}>暂无列表版本明细；执行预览或复评后会显示 ENTER/HOLD/EXIT/WAITING 调整方案。</td></tr> : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-kicker">列表版本时间线</div>
            <h2>初始列表与每日复评后的推荐列表</h2>
            <p className="pv2-muted">这里是版本时间线，用于回看初始列表和每个交易日复评后的完整列表；与上方当前查看区域联动，不是另一个策略包输出表。</p>
          </div>
        </div>
        <div className="pv2-table-wrap">
          <table className="pv2-table" data-testid="advisory-list-versions-table">
            <thead><tr><th>交易日</th><th>状态</th><th>ACTIVE</th><th>ENTER</th><th>HOLD</th><th>EXIT</th><th>WAITING</th><th>换手</th><th>重合</th><th>操作</th></tr></thead>
            <tbody>
              {listVersions.map((version) => {
                const selected = selectedListVersionId === version.list_version_id;
                const loadingDetail = listVersionLoadingId === version.list_version_id;
                return (
                  <tr key={version.list_version_id} data-testid={`advisory-list-version-row-${version.list_version_id}`} aria-selected={selected} style={selected ? { outline: "2px solid #2563eb", outlineOffset: -2 } : undefined}>
                    <td>{version.trade_date}<br /><span className="pv2-muted pv2-mono">{short(version.list_version_id, 18)}</span></td>
                    <td>{version.version_status}</td>
                    <td>{version.active_count}</td>
                    <td>{version.entered_count}</td>
                    <td>{version.held_count}</td>
                    <td>{version.exited_count}</td>
                    <td>{version.waiting_count}</td>
                    <td>{fmtPct(version.turnover_rate)}</td>
                    <td>{fmtPct(version.overlap_rate)}</td>
                    <td>
                      <button
                        className={selected ? "pv2-button-primary" : "pv2-button"}
                        data-testid={`advisory-view-list-version-${version.list_version_id}`}
                        disabled={loadingDetail}
                        onClick={() => void viewListVersion(version.list_version_id)}
                        type="button"
                      >
                        {loadingDetail ? "加载中..." : selected ? "当前明细" : "查看明细"}
                      </button>
                    </td>
                  </tr>
                );
              })}
              {!listVersions.length ? <tr><td colSpan={10}>暂无列表版本；请在上方点击“预览初始列表”或“生成初始列表”。</td></tr> : null}
            </tbody>
          </table>
        </div>
      </section>

      <div className="pv2-grid pv2-grid-2">
        <section className="pv2-card">
          <div className="pv2-card-head"><div><div className="pv2-kicker">当前荐股池</div><h2>当前推荐股票</h2><p className="pv2-muted">这里只显示最新列表中仍然 ACTIVE 的股票；退出/等待/新进入的完整调整请看上方列表版本明细。点击任意列名切换排序。</p></div></div>
          <div className="pv2-table-wrap">
            <table className="pv2-table" data-testid="advisory-active-table">
              <thead>
                <tr>
                  {activeColumns.map((column) => (
                    <th key={column.key}><SortHeader column={column} sort={activeSort} onClick={() => setActiveSort((current) => nextSort(current, column.key))} /></th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedActivePool.map((row) => (
                  <tr data-testid="advisory-active-row" key={row.episode_id}>
                    {activeColumns.map((column) => <td data-testid={`advisory-active-cell-${column.key}`} key={column.key}>{column.render(row)}</td>)}
                  </tr>
                ))}
                {!activePool.length ? <tr><td colSpan={activeColumns.length}>当前任务暂无活跃荐股。</td></tr> : null}
              </tbody>
            </table>
          </div>
        </section>
        <section className="pv2-card">
          <div className="pv2-card-head">
            <div><div className="pv2-kicker">复评记录</div><h2>只追加决策记录</h2></div>
            <div className="pv2-row-actions">
              <label className="pv2-field" style={{ minWidth: 110 }}>每页
                <select className="pv2-select" data-testid="advisory-review-page-size" value={reviewPageSize} onChange={(event) => void changeReviewPageSize(Number(event.target.value) as (typeof REVIEW_PAGE_SIZE_OPTIONS)[number])}>
                  {REVIEW_PAGE_SIZE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
                </select>
              </label>
            </div>
          </div>
          <div className="pv2-table-wrap">
            <table className="pv2-table" data-testid="advisory-review-table">
              <thead><tr><th>日期</th><th>股票</th><th>动作</th><th>原因</th><th>状态</th><th>排名</th></tr></thead>
              <tbody>
                {pagedReviews.map((row, index) => (
                  <tr data-testid="advisory-review-row" key={`${row.trade_date}-${row.symbol}-${row.action}-${row.episode_id || index}`}><td>{row.trade_date}</td><td>{stockLabel(row)}</td><td>{row.action}</td><td>{row.reason_code}</td><td>{row.review_status}</td><td>{row.rank ?? "-"}</td></tr>
                ))}
                {!reviews.length ? <tr><td colSpan={6}>暂无复评记录。</td></tr> : null}
              </tbody>
            </table>
          </div>
          <div className="pv2-pagination">
            <button className="pv2-button-ghost" disabled={reviewPageSafe <= 1} onClick={() => void changeReviewPage(Math.max(1, reviewPageSafe - 1))} type="button">上一页</button>
            <span className="pv2-muted">第 {reviewPageSafe} / {reviewTotalPages} 页，共 {reviewTotalCount} 条</span>
            <button className="pv2-button-ghost" disabled={reviewPageSafe >= reviewTotalPages} onClick={() => void changeReviewPage(Math.min(reviewTotalPages, reviewPageSafe + 1))} type="button">下一页</button>
          </div>
        </section>
      </div>

      <section className="pv2-card">
        <div className="pv2-card-head"><div><div className="pv2-kicker">荐股收益</div><h2>Episode 收益与胜率证据</h2><p className="pv2-muted">默认入选价格口径为 next_open_executable；signal_close 与 next_close 仅作敏感性口径。</p></div></div>
        <div className="pv2-table-wrap">
          <table className="pv2-table">
            <thead><tr><th>股票</th><th>入选口径</th><th>入选生效日</th><th>退出原因</th><th>涨跌幅</th><th>胜负</th><th>回撤</th></tr></thead>
            <tbody>
              {returns.map((row) => (
                <tr key={row.episode_id}><td>{stockLabel(row)}</td><td>{row.entry_price_basis}</td><td>{row.effective_entry_date}</td><td>{row.exit_reason || row.status}</td><td>{fmtBps(row.return_bps)}</td><td>{row.is_win === true ? "Y" : row.is_win === false ? "N" : "-"}</td><td>{fmtBps(row.max_drawdown_bps)}</td></tr>
              ))}
              {!returns.length ? <tr><td colSpan={7}>暂无 episode 收益。</td></tr> : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="pv2-card">
        <div className="pv2-card-head">
          <div><div className="pv2-kicker">生命周期回放</div><h2>历史荐股生命周期回放</h2><p className="pv2-muted">回放仅为投顾事后诊断；按全局交易日服务与任务绑定策略包运行，不模拟账户，也不产生交易副作用。</p></div>
          <button className="pv2-button-primary" onClick={runReplay} disabled={!selectedProgram || !replayStart || !replayEnd} type="button">执行回放</button>
        </div>
        <div className="pv2-form-grid">
          <label className="pv2-field">开始<input className="pv2-input" type="date" value={replayStart} onChange={(event) => setReplayStart(event.target.value)} /></label>
          <label className="pv2-field">结束<input className="pv2-input" type="date" value={replayEnd} onChange={(event) => setReplayEnd(event.target.value)} /></label>
        </div>
        {replayResult ? <div className="pv2-readable-panel">回放状态： {String((replayResult.replay_run as JsonObject | undefined)?.status || "-")} / 胜率 {fmtPct((replayResult.summary as JsonObject | undefined)?.win_rate as number | null | undefined)}</div> : null}
      </section>

      <section className="pv2-card">
        <div className="pv2-card-head">
          <div><div className="pv2-kicker">质量报告</div><h2>事后诊断</h2><p className="pv2-muted">用结构化字段录入诊断样本；decision input 中禁止未来结果字段，报告不是 validated PnL。</p></div>
          <div className="pv2-row-actions">
            <button className="pv2-button" onClick={() => setQualityRows((rows) => [...rows, newQualityRow(rows.length + 1)])} type="button">添加样本</button>
            <button className="pv2-button-primary" onClick={buildQualityReport} type="button">生成报告</button>
          </div>
        </div>
        <div className="pv2-table-wrap">
          <table className="pv2-table">
            <thead><tr><th>股票</th><th>交易日</th><th>当前价</th><th>最高买入价</th><th>动作</th><th>原因</th><th>日低</th><th>日高</th><th>分层标签</th><th>操作</th></tr></thead>
            <tbody>
              {qualityRows.map((row) => (
                <tr key={row.rowId}>
                  <td><input className="pv2-input" value={row.code} onChange={(event) => updateQualityRow(row.rowId, { code: event.target.value })} placeholder="000001.SZ" /></td>
                  <td><input className="pv2-input" type="date" value={row.tradeDate} onChange={(event) => updateQualityRow(row.rowId, { tradeDate: event.target.value })} /></td>
                  <td><input className="pv2-input" type="number" value={row.currentPrice} onChange={(event) => updateQualityRow(row.rowId, { currentPrice: event.target.value })} /></td>
                  <td><input className="pv2-input" type="number" value={row.maxBuyPrice} onChange={(event) => updateQualityRow(row.rowId, { maxBuyPrice: event.target.value })} /></td>
                  <td><select className="pv2-select" value={row.action} onChange={(event) => updateQualityRow(row.rowId, { action: event.target.value })}><option value="HOLD">HOLD</option><option value="ENTER">ENTER</option><option value="SKIP">SKIP</option><option value="WAITING">WAITING</option><option value="EXIT">EXIT</option></select></td>
                  <td><input className="pv2-input" value={row.reasonCode} onChange={(event) => updateQualityRow(row.rowId, { reasonCode: event.target.value })} /></td>
                  <td><input className="pv2-input" type="number" value={row.dayLow} onChange={(event) => updateQualityRow(row.rowId, { dayLow: event.target.value })} /></td>
                  <td><input className="pv2-input" type="number" value={row.dayHigh} onChange={(event) => updateQualityRow(row.rowId, { dayHigh: event.target.value })} /></td>
                  <td>
                    <input className="pv2-input" value={row.scoreBucket} onChange={(event) => updateQualityRow(row.rowId, { scoreBucket: event.target.value })} placeholder="score" />
                    <input className="pv2-input" style={{ marginTop: 6 }} value={row.regime} onChange={(event) => updateQualityRow(row.rowId, { regime: event.target.value })} placeholder="regime" />
                  </td>
                  <td><button className="pv2-button-ghost" disabled={qualityRows.length <= 1} onClick={() => setQualityRows((rows) => rows.filter((item) => item.rowId !== row.rowId))} type="button">移除</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {qualityReport ? (
          <div className="pv2-readable-panel" style={{ marginTop: 12 }}>
            样本数 {qualityReport.sample_count}; 买入区间命中 {fmtPct(qualityReport.metrics.entry_zone_hit_rate as number | null | undefined)}; 可成交 {fmtPct(qualityReport.metrics.entry_zone_fillable_rate as number | null | undefined)}
          </div>
        ) : null}
      </section>
    </main>
  );
}

export default function PaperV2AdvisoryPage() {
  return (
    <Suspense fallback={<main className="pv2-main"><section className="pv2-card">荐股中心加载中...</section></main>}>
      <AdvisoryPageContent />
    </Suspense>
  );
}


