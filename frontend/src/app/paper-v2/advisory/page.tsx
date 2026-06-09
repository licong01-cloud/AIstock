"use client";

import { Suspense, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useSearchParams } from "next/navigation";
import {
  advisoryApi,
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

const REVIEW_PAGE_SIZE_OPTIONS = [20, 50, 100] as const;
const PACKAGE_MODE_OPTIONS: AdvisoryPackageMode[] = ["single_package", "weighted_rank_fusion", "fusion_pool", "union", "intersection"];

function fmtPct(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(1)}%` : "-";
}

function fmtBps(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? `${(value / 100).toFixed(2)}%` : "-";
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

function packageOptionLabel(pkg: SelectablePackage): string {
  const parts = [packageDisplayLabel(pkg), pkg.package_status];
  if (pkg.alpha_mode) parts.push(String(pkg.alpha_mode));
  if (pkg.latest_selection_run?.trade_date) parts.push(`最近选股 ${pkg.latest_selection_run.trade_date}`);
  return parts.filter(Boolean).join(" / ");
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

function reviewRuntimeConfig(program: AdvisoryProgram): JsonObject {
  const topK = Math.max(1, Math.min(50, Number(program.target_count || 20)));
  return {
    top_k: topK,
    display_top_n: topK,
    st_pit_authoritative: true,
    selection_artifact_config: {
      auto_generate: true,
      inference_backend: "wsl",
      pit_mode: "PREVIOUS_TRADING_DAY_CLOSE",
    },
    runtime_profile: {
      selection: { top_k: topK },
      tradability: { exclude_suspended: true },
      industry_blacklist: [],
      hmm: { enabled: false, model_config_id: null, model_snapshot_id: null, signal_preset: null },
    },
  };
}

function modeNeedsWeights(mode: AdvisoryPackageMode): boolean {
  return mode === "fusion_pool" || mode === "weighted_rank_fusion";
}

function adviceText(item: AdvisoryRecommendationListItem): string {
  return String(item.operation_advice_json?.human_label || item.operation_advice_json?.reason_summary || item.reason_code || "-");
}

function reviewState(program: AdvisoryProgram, targetDate: string, hasPriorList = Boolean(program.latest_review_trade_date)) {
  if (!targetDate) return { canPreview: false, canRun: false, label: "等待交易日", previewLabel: "等待交易日", hint: "等待交易日服务返回最近交易日。", isInitialRun: false };
  if (program.status !== "ENABLED") return { canPreview: false, canRun: false, label: "未启用", previewLabel: "预览", hint: "任务启用后才可执行首次运行或每日复评。", isInitialRun: false };
  if (program.latest_review_trade_date && program.latest_review_trade_date >= targetDate) {
    return { canPreview: true, canRun: false, label: "已复评", previewLabel: "预览", hint: `${targetDate} 已完成复评，等待下一个交易日。`, isInitialRun: false };
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
  return { canPreview: true, canRun: true, label: "执行复评", previewLabel: "预览", hint: `目标交易日 ${targetDate}，由系统自动生成选股候选。`, isInitialRun: false };
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
  const [qualityRows, setQualityRows] = useState<QualityInputRow[]>(() => [{ ...newQualityRow(1), rowId: "quality-1" }]);
  const [qualityReport, setQualityReport] = useState<AdvisoryQualityReport | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const listDetailRef = useRef<HTMLDivElement | null>(null);

  const selectedProgram = useMemo(
    () => programs.find((item) => item.program_id === selectedProgramId) || programs[0],
    [programs, selectedProgramId],
  );

  const activeColumns = useMemo<ActivePoolColumn[]>(() => [
    { key: "symbol", label: "股票", value: (row) => row.symbol, render: (row) => <strong>{row.symbol}</strong> },
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

  async function loadProgramDetails(programId: string, page = 1, pageSize: (typeof REVIEW_PAGE_SIZE_OPTIONS)[number] = reviewPageSize) {
    const offset = (Math.max(page, 1) - 1) * pageSize;
    const [poolRows, reviewPageData, returnRows, bindingRows, versionRows] = await Promise.all([
      advisoryApi.activePool(programId),
      advisoryApi.reviews(programId, pageSize, offset),
      advisoryApi.returns(programId),
      advisoryApi.bindings(programId),
      advisoryApi.listVersions(programId, 20, 0),
    ]);
    setActivePool(poolRows);
    setReviews(reviewPageData.reviews);
    setReviewTotalCount(reviewPageData.total_count);
    setReturns(returnRows.returns || []);
    setBindings(bindingRows);
    setListVersions(versionRows);
    if (versionRows[0]) {
      const detail = await advisoryApi.listVersionDetail(versionRows[0].list_version_id);
      setListVersionDetail(detail);
      setSelectedListVersionId(detail.list_version.list_version_id);
      setListDetailSource("latest");
    } else {
      setListVersionDetail(null);
      setSelectedListVersionId("");
      setListDetailSource("latest");
    }
  }

  async function refreshAll(nextProgramId?: string) {
    setLoading(true);
    setError(null);
    try {
      const [programRows, boardRows, tradingDefaults, packageOptions] = await Promise.all([
        advisoryApi.programs(false),
        advisoryApi.leaderboard(sortBy),
        advisoryApi.tradingDayDefaults(10),
        selectionCenterApi.selectablePackages(300).catch(() => []),
      ]);
      setPrograms(programRows);
      setLeaderboard(boardRows);
      setSelectablePackages(packageOptions);
      setPackageRows((rows) => {
        if (rows.some((row) => row.packageId.trim()) || !packageOptions[0]) return rows;
        return rows.map((row, index) => index === 0 ? { ...row, packageId: packageOptions[0].package_id } : row);
      });
      setReviewTradeDate(tradingDefaults.latest_trading_day);
      const resolvedProgramId = nextProgramId || selectedProgramId || programRows[0]?.program_id || "";
      setSelectedProgramId(resolvedProgramId);
      setReviewPage(1);
      if (resolvedProgramId) {
        await loadProgramDetails(resolvedProgramId, 1, reviewPageSize);
      } else {
        setActivePool([]);
        setReviews([]);
        setReviewTotalCount(0);
        setReturns([]);
        setBindings([]);
        setListVersions([]);
        setListVersionDetail(null);
        setSelectedListVersionId("");
        setListDetailSource("latest");
        setReviewResult(null);
      }
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }

  async function selectProgram(programId: string) {
    setSelectedProgramId(programId);
    setReviewPage(1);
    setReviewResult(null);
    setLoading(true);
    setError(null);
    try {
      await loadProgramDetails(programId, 1, reviewPageSize);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refreshAll().catch((exc) => setError(exc instanceof Error ? exc.message : String(exc)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sortBy]);

  useEffect(() => {
    if (prefillPackages) {
      const ids = packageIdsFromText(prefillPackages);
      setPackageMode(ids.length > 1 ? "weighted_rank_fusion" : "single_package");
      setPackageRows(packageRowsFromText(prefillPackages));
    }
  }, [prefillPackages]);

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

  async function runReview(program: AdvisoryProgram, preview: boolean) {
    const hasPriorList = program.program_id === selectedProgram?.program_id ? listVersions.length > 0 : Boolean(program.latest_review_trade_date);
    const state = reviewState(program, reviewTradeDate, hasPriorList);
    if (!state.canPreview || (!preview && !state.canRun)) return;
    setError(null);
    setReviewResult(null);
    setReviewingKey(`${program.program_id}:${preview ? "preview" : "run"}`);
    try {
      const payload = {
        trade_date: reviewTradeDate,
        runtime_config: reviewRuntimeConfig(program),
      };
      const result = preview
        ? await advisoryApi.previewReview(program.program_id, payload)
        : await advisoryApi.runReview(program.program_id, payload);
      await refreshAll(program.program_id);
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
    setLoading(true);
    setError(null);
    try {
      await loadReviews(selectedProgram.program_id, nextPage, reviewPageSize);
      setReviewPage(nextPage);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }

  async function changeReviewPageSize(nextPageSize: (typeof REVIEW_PAGE_SIZE_OPTIONS)[number]) {
    if (!selectedProgram) {
      setReviewPageSize(nextPageSize);
      setReviewPage(1);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      await loadReviews(selectedProgram.program_id, 1, nextPageSize);
      setReviewPageSize(nextPageSize);
      setReviewPage(1);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
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

  const selectedReviewState = selectedProgram ? reviewState(selectedProgram, reviewTradeDate, listVersions.length > 0) : null;
  const selectedPreviewKey = selectedProgram ? `${selectedProgram.program_id}:preview` : "";
  const selectedRunKey = selectedProgram ? `${selectedProgram.program_id}:run` : "";
  const activeBinding = bindings.find((item) => item.activation_status === "ACTIVE") || bindings[0];
  const reviewResultDetailActive = listDetailSource === "review_result" && Boolean(reviewResult?.list_version_id);
  const visibleListItems = reviewResultDetailActive ? (reviewResult?.list_items || []) : listVersionDetail?.items || [];
  const visibleListVersion = reviewResultDetailActive && reviewResult?.list_version_id
    ? {
        list_version_id: reviewResult.list_version_id,
        trade_date: reviewResult.trade_date,
        version_status: reviewResult.preview ? "PREVIEW" : "PUBLISHED",
        active_count: visibleListItems.filter((item) => item.item_state === "ACTIVE").length,
        entered_count: Number(reviewResult.change_summary?.entered_count ?? 0),
        held_count: Number(reviewResult.change_summary?.held_count ?? 0),
        exited_count: Number(reviewResult.change_summary?.exited_count ?? 0),
        waiting_count: Number(reviewResult.change_summary?.waiting_count ?? 0),
        turnover_rate: reviewResult.change_summary?.turnover_rate as number | null | undefined,
        overlap_rate: reviewResult.change_summary?.overlap_rate as number | null | undefined,
      }
    : listVersionDetail?.list_version;
  const visibleListSourceLabel = listDetailSource === "review_result" ? "刚刚执行结果" : listDetailSource === "timeline" ? "手动查看版本" : "最新列表版本";

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
                const hasPriorList = row.program_id === selectedProgram?.program_id ? listVersions.length > 0 : Boolean(row.latest_review_trade_date);
                const state = reviewState(row, reviewTradeDate, hasPriorList);
                const previewKey = `${row.program_id}:preview`;
                const runKey = `${row.program_id}:run`;
                return (
                  <tr key={row.program_id} onClick={() => void selectProgram(row.program_id)}>
                    <td><strong>{row.program_name}</strong><br /><span className="pv2-muted">{packageSummary(row.package_ids)}</span></td>
                    <td>{row.status}</td>
                    <td>{short(row.enabled_since, 16)}</td>
                    <td>{row.entered_episode_count ?? 0}</td>
                    <td>{row.active_count ?? 0}</td>
                    <td>{row.take_profit_count ?? 0}</td>
                    <td>{row.stop_loss_count ?? 0}</td>
                    <td>{fmtPct(row.win_rate)}</td>
                    <td>{fmtBps(row.avg_return_bps)}</td>
                    <td>{row.last_review_status || "STALE"}<br /><span className="pv2-muted">{row.latest_review_trade_date || "尚未复评"}</span></td>
                    <td>
                      <div className="pv2-row-actions">
                        <button
                          className="pv2-button"
                          data-testid={`advisory-preview-${row.program_id}`}
                          disabled={!state.canPreview || reviewingKey === previewKey}
                          onClick={(event) => { event.stopPropagation(); void runReview(row, true); }}
                          type="button"
                        >
                          {reviewingKey === previewKey ? "预览中..." : state.previewLabel}
                        </button>
                        <button
                          className="pv2-button-primary"
                          data-testid={`advisory-run-${row.program_id}`}
                          disabled={!state.canRun || reviewingKey === runKey}
                          onClick={(event) => { event.stopPropagation(); void runReview(row, false); }}
                          type="button"
                        >
                          {reviewingKey === runKey ? "执行中..." : state.label}
                        </button>
                      </div>
                      <span className="pv2-muted">{state.hint}</span>
                    </td>
                  </tr>
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
            <p className="pv2-muted">首次运行与每日复评都使用 active_pool ∪ TopK；候选与行情由 Selection Center 自动生成，页面不暴露内部选股运行编号。</p>
          </div>
          <div className="pv2-row-actions">
            <button
              className="pv2-button"
              data-testid="advisory-selected-preview"
              onClick={() => selectedProgram && void runReview(selectedProgram, true)}
              disabled={!selectedProgram || !selectedReviewState?.canPreview || reviewingKey === selectedPreviewKey}
              type="button"
            >
              {reviewingKey === selectedPreviewKey ? "预览中..." : selectedReviewState?.previewLabel || "预览"}
            </button>
            <button
              className="pv2-button-primary"
              data-testid="advisory-selected-run"
              onClick={() => selectedProgram && void runReview(selectedProgram, false)}
              disabled={!selectedProgram || !selectedReviewState?.canRun || reviewingKey === selectedRunKey}
              type="button"
            >
              {reviewingKey === selectedRunKey ? "执行中..." : selectedReviewState?.label || "执行复评"}
            </button>
          </div>
        </div>
        <div className="pv2-readable-panel" style={{ marginTop: 12 }}>
          <strong>目标复评交易日： <span data-testid="advisory-review-target-date">{reviewTradeDate || "-"}</span></strong>
          <span className="pv2-muted"> {selectedReviewState?.hint || "选择启用中的荐股任务后即可复评。"}</span>
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
            {visibleListVersion.waiting_count > 0 ? (
              <div className="pv2-muted" style={{ marginTop: 6 }} data-testid="advisory-list-waiting-hint">
                WAITING 表示该版本已经生成，但对应股票仍在等待行情、停复牌或可交易性确认；这不是未刷新旧表。
              </div>
            ) : null}
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
                  <td><strong>{item.symbol}</strong></td>
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
            <p className="pv2-muted">每次正式复评生成一个 PUBLISHED 列表版本；退出股票不会消失，会以 EXIT 动作和操作建议保留在版本明细中。</p>
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
          <div className="pv2-card-head"><div><div className="pv2-kicker">当前荐股池</div><h2>当前推荐股票</h2><p className="pv2-muted">点击任意列名切换正序、倒序、取消排序。</p></div></div>
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
                  <tr data-testid="advisory-review-row" key={`${row.trade_date}-${row.symbol}-${row.action}-${row.episode_id || index}`}><td>{row.trade_date}</td><td>{row.symbol}</td><td>{row.action}</td><td>{row.reason_code}</td><td>{row.review_status}</td><td>{row.rank ?? "-"}</td></tr>
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
                <tr key={row.episode_id}><td>{row.symbol}</td><td>{row.entry_price_basis}</td><td>{row.effective_entry_date}</td><td>{row.exit_reason || row.status}</td><td>{fmtBps(row.return_bps)}</td><td>{row.is_win === true ? "Y" : row.is_win === false ? "N" : "-"}</td><td>{fmtBps(row.max_drawdown_bps)}</td></tr>
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


