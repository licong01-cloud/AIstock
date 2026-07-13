"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperIndustryBlacklistSelector, { selectedIndustryCodes, selectedIndustryTrace, type Sw2Entry } from "@/components/paper-v2/PaperIndustryBlacklistSelector";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import WorkflowStepper from "@/components/paper-v2/WorkflowStepper";
import { hmmTrainingApi, paperV2Api, selectionCenterApi } from "@/lib/paper-v2/api";
import { advisoryApi } from "@/lib/api/advisory";
import { asText, dataSourceLabel, formatNumber, formatPercent, paperV2WorkflowSteps, selectionRunLabel, shortHash, statusLabel, todayIso } from "@/lib/paper-v2/format";
import type { HmmConfig, JsonObject, SelectablePackage, SelectionCandidate, SelectionDataSource, SelectionMode, SelectionRun, SelectionWatchlistImportResult } from "@/lib/paper-v2/types";

function runLabel(run: SelectionRun): string {
  return `${run.trade_date} / ${statusLabel(run.mode)} / ${run.package_ids.map((item) => shortHash(item, 5)).join(", ")}`;
}

function packageHealth(packageRow: SelectablePackage): JsonObject {
  return (packageRow.selection_health || {}) as JsonObject;
}

function packageHealthStatus(packageRow: SelectablePackage): string {
  return String(packageHealth(packageRow).status || "UNKNOWN");
}

function packageHealthHint(packageRow: SelectablePackage): string {
  const health = packageHealth(packageRow);
  const checks = Array.isArray(health.checks) ? health.checks as JsonObject[] : [];
  const first = checks.find((item) => ["BLOCKED", "WARN"].includes(String(item.status))) || checks[0];
  return String(first?.message || health.status || "UNKNOWN");
}

function priceText(value: unknown): string {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(2) : "-";
}

function guidanceBand(row: SelectionCandidate): JsonObject | null {
  return (row.suggested_entry_price_band || null) as JsonObject | null;
}

function stopZone(row: SelectionCandidate): JsonObject | null {
  return (row.suggested_stop_loss_zone || null) as JsonObject | null;
}

function renderEntryBand(row: SelectionCandidate): string {
  const band = guidanceBand(row);
  if (!band) return "未提供";
  const green = (band.green || {}) as JsonObject;
  const yellow = (band.yellow || {}) as JsonObject;
  return `绿 ${priceText(green.min_price)}-${priceText(green.max_price)} / 黄 ${priceText(yellow.min_price)}-${priceText(yellow.max_price)}`;
}

function renderStopZone(row: SelectionCandidate): string {
  const zone = stopZone(row);
  if (!zone) return "未提供";
  return `软 ${priceText(zone.soft_stop_price)} / 硬 ${priceText(zone.hard_stop_price)}`;
}

function renderLimitBoundary(row: SelectionCandidate): string {
  const band = guidanceBand(row);
  if (!band) return "-";
  return `跌停 ${priceText(band.limit_down)} / 涨停 ${priceText(band.limit_up)}`;
}

export default function PaperV2SelectionPage() {
  const [packages, setPackages] = useState<SelectablePackage[]>([]);
  const [runs, setRuns] = useState<SelectionRun[]>([]);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [selectedRuns, setSelectedRuns] = useState<Record<string, boolean>>({});
  const [runPage, setRunPage] = useState(1);
  const [runPagination, setRunPagination] = useState<JsonObject>({ page: 1, page_size: 20, total: 0, total_pages: 1 });
  const mode: SelectionMode = "single_package";
  const [tradeDate, setTradeDate] = useState(todayIso());
  const [pitMode, setPitMode] = useState("PREVIOUS_TRADING_DAY_CLOSE");
  const [pitContext, setPitContext] = useState<JsonObject | null>(null);
  const [dataSource, setDataSource] = useState<SelectionDataSource>("DB_HISTORICAL");
  const [topK, setTopK] = useState(20);
  const [industryBlacklist, setIndustryBlacklist] = useState<Sw2Entry[]>([]);
  const [excludeSuspended, setExcludeSuspended] = useState(true);
  const [hmmEnabled, setHmmEnabled] = useState(false);
  const [hmmConfigs, setHmmConfigs] = useState<HmmConfig[]>([]);
  const [hmmConfigId, setHmmConfigId] = useState("");
  const [hmmPreset, setHmmPreset] = useState("preset_A");
  const [run, setRun] = useState<SelectionRun | null>(null);
  const [excluded, setExcluded] = useState<Record<string, unknown[]> | null>(null);
  const [watchlistCategoryName, setWatchlistCategoryName] = useState("");
  const [watchlistResult, setWatchlistResult] = useState<SelectionWatchlistImportResult | null>(null);
  const [tdxAvailable, setTdxAvailable] = useState(false);
  const [tdxSyncing, setTdxSyncing] = useState(false);
  const [tdxSyncResult, setTdxSyncResult] = useState<{ display_name: string; count: number } | null>(null);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const selectedPackages = useMemo(() => packages.filter((item) => selected[item.package_id]), [packages, selected]);
  const selectedHistoryRunIds = useMemo(
    () => Object.entries(selectedRuns).filter(([, checked]) => checked).map(([runId]) => runId),
    [selectedRuns],
  );
  const selectedPackageName = selectedPackages[0]?.package_name || "策略包";
  const loadPackages = useCallback(async () => {
    setLoading(true);
    setError(null);
    const tasks = [
      selectionCenterApi.selectablePackages(100, "summary").then((rows) => {
        setPackages(rows);
        const firstPackageId = rows[0]?.package_id;
        setSelected((prev) => {
          const keptPackageId = rows.find((item) => prev[item.package_id])?.package_id || firstPackageId;
          return Object.fromEntries(rows.map((item) => [item.package_id, item.package_id === keptPackageId]));
        });
      }),
      selectionCenterApi.listRunsPage({ page: runPage, pageSize: 20 }).then((runRows) => {
        setRuns(runRows.runs);
        setRunPagination(runRows.pagination);
      }),
      hmmTrainingApi.configs().then((configRows) => {
        setHmmConfigs(configRows);
        setHmmConfigId((current) => current || configRows[0]?.config_id || "");
      }),
    ];
    const results = await Promise.allSettled(tasks);
    const firstFailure = results.find((item): item is PromiseRejectedResult => item.status === "rejected");
    if (firstFailure) setError(firstFailure.reason);
    setLoading(false);
  }, [runPage]);

  useEffect(() => { loadPackages(); }, [loadPackages]);

  useEffect(() => {
    let alive = true;
    advisoryApi.tdxAvailable().then((available) => {
      if (alive) setTdxAvailable(available);
    }).catch(() => {});
    return () => { alive = false; };
  }, []);

  useEffect(() => {
    let alive = true;
    paperV2Api.tradingDayDefaults(10).then((defaults) => {
      if (!alive) return;
      setTradeDate((current) => (current === todayIso() ? defaults.latest_trading_day : current));
    }).catch((exc) => {
      if (alive) setError(exc);
    });
    return () => { alive = false; };
  }, []);

  useEffect(() => {
    let alive = true;
    setPitContext(null);
    if (pitMode === "NONE" || !tradeDate) return () => { alive = false; };
    selectionCenterApi.resolvePitCutoff({ trade_date: tradeDate, pit_mode: pitMode }).then((context) => {
      if (alive) setPitContext(context);
    }).catch((exc) => {
      if (alive) setError(exc);
    });
    return () => { alive = false; };
  }, [pitMode, tradeDate]);

  function updatePackageSelection(packageId: string) {
    setSelected(Object.fromEntries(packages.map((item) => [item.package_id, item.package_id === packageId])));
  }

  function runtimeConfig(): JsonObject {
    const blacklist = selectedIndustryCodes(industryBlacklist);
    const runtimeProfile: JsonObject = {
      industry_blacklist: blacklist,
      tradability: { exclude_suspended: excludeSuspended },
      selection: { top_k: topK },
      hmm: {
        enabled: hmmEnabled,
        model_config_id: hmmEnabled ? hmmConfigId : null,
        model_snapshot_id: null,
        signal_preset: hmmEnabled ? hmmPreset : null,
      },
    };
    const artifactConfig: JsonObject = { auto_generate: true, inference_backend: "wsl" };
    if (pitMode !== "NONE") {
      artifactConfig.pit_mode = pitMode;
      if (pitContext?.cutoff_date) artifactConfig.cutoff_date = pitContext.cutoff_date;
    }
    const config: JsonObject = {
      top_k: topK,
      display_top_n: topK,
      st_pit_authoritative: true,
      selection_artifact_config: artifactConfig,
      runtime_profile: runtimeProfile,
      industry_blacklist_trace: selectedIndustryTrace(industryBlacklist),
    };
    return config;
  }

  async function hydrateRun(next: SelectionRun) {
    setRun(next);
    const excludedRows = await selectionCenterApi.excludedResults(next.run_id);
    setExcluded(excludedRows);
    setWatchlistCategoryName(`选股中心-${next.trade_date}-${selectedPackageName}`);
  }

  async function runSelection() {
    setRunning(true);
    setError(null);
    setRun(null);
    setExcluded(null);
    setWatchlistResult(null);
    try {
      const packageIds = selectedPackages.map((item) => item.package_id);
      if (topK < 1 || topK > 50) throw new Error("TopK 必须在 1 到 50 之间。");
      if (hmmEnabled && !hmmConfigId) throw new Error("HMM enabled: select a model config; daily coefficients are computed and cached automatically.");
      if (pitMode !== "NONE") {
        const pitMessage = pitContext?.cutoff_date
          ? `本次选股将按 ${tradeDate} 的上一交易日 ${pitContext.cutoff_date} 作为 PIT 截止日，入池参考价使用 ${pitContext.reference_price_trade_date || pitContext.cutoff_date} 的收盘价。是否继续？`
          : `本次选股将由后端官方交易日服务自动解析 ${tradeDate} 的上一交易日 PIT 截止日；如果交易日历缺失，本次 run 会失败并给出诊断，但不会禁用策略包。是否继续？`;
        if (!window.confirm(pitMessage)) return;
      }
      if (packageIds.length !== 1) throw new Error("Selection Center 手工入口只允许选择一个 StrategyPackage；组合 alpha 请先用 from-multi-alpha-combine-run 生成已验证的多Alpha策略包。");
      const next = await selectionCenterApi.runSelection({ package_ids: packageIds, trade_date: tradeDate, data_source: dataSource, mode, runtime_config: runtimeConfig() });
      await hydrateRun(next);
      setRunPage(1);
      const pageRows = await selectionCenterApi.listRunsPage({ page: 1, pageSize: 20 });
      setRuns(pageRows.runs);
      setRunPagination(pageRows.pagination);
    } catch (exc) {
      setError(exc);
    } finally {
      setRunning(false);
    }
  }

  async function showHistoryRun(runId: string) {
    setError(null);
    setWatchlistResult(null);
    try {
      const next = await selectionCenterApi.getRun(runId);
      await hydrateRun(next);
    } catch (exc) {
      setError(exc);
    }
  }

  async function addToWatchlist() {
    if (!run) return;
    setError(null);
    setWatchlistResult(null);
    try {
      const result = await selectionCenterApi.addToWatchlist(run.run_id, {
        category_name: watchlistCategoryName || undefined,
        top_k: topK,
        on_conflict: "ignore",
      });
      setWatchlistResult(result);
    } catch (exc) {
      setError(exc);
    }
  }

  async function syncToTdx() {
    if (!watchlistResult) return;
    setTdxSyncing(true);
    setTdxSyncResult(null);
    setError(null);
    try {
      const catName = watchlistCategoryName || String((watchlistResult as JsonObject).category_name || "");
      if (!catName) throw new Error("缺少自选股票池分类名称，无法同步通达信自选。");
      const result = await advisoryApi.tdxSyncFromCategory(catName);
      setTdxSyncResult({ display_name: result.display_name, count: result.count });
    } catch (exc) {
      setError(exc);
    } finally {
      setTdxSyncing(false);
    }
  }

  async function deleteSelectedRuns() {
    const ids = selectedHistoryRunIds;
    if (!ids.length) return;
    if (!window.confirm(`确认删除 ${ids.length} 条历史选股记录？已加入自选股票池的股票不受影响。`)) return;
    setRunning(true);
    setError(null);
    try {
      await selectionCenterApi.deleteRuns(ids);
      setSelectedRuns({});
      const pageRows = await selectionCenterApi.listRunsPage({ page: runPage, pageSize: 20 });
      setRuns(pageRows.runs);
      setRunPagination(pageRows.pagination);
      if (run && ids.includes(run.run_id)) {
        setRun(null);
        setExcluded(null);
      }
    } catch (exc) {
      setError(exc);
    } finally {
      setRunning(false);
    }
  }

  const excludedFlat = Object.entries(excluded || {}).flatMap(([packageId, rows]) => rows.map((row) => ({ packageId, row: row as JsonObject })));
  const resultRows = run?.aggregate_results || [];
  const visibleResultRows = resultRows.slice(0, topK);
  const runPageSize = 20;
  const runTotalPages = Math.max(1, Number(runPagination.total_pages || 1));
  const runPageSafe = Math.min(runPage, runTotalPages);
  const visibleRuns = runs;
  const visibleRunIds = visibleRuns.map((item) => item.run_id);
  const selectedVisibleRunCount = visibleRunIds.filter((runId) => selectedRuns[runId]).length;
  const allVisibleRunsSelected = visibleRunIds.length > 0 && selectedVisibleRunCount === visibleRunIds.length;

  function setCurrentPageRunSelection(checked: boolean) {
    setSelectedRuns((current) => {
      const next = { ...current };
      for (const runId of visibleRunIds) {
        next[runId] = checked;
      }
      return next;
    });
  }

  const workflowSteps = paperV2WorkflowSteps({
    hasPackages: packages.length > 0,
    hasSelectionEnabledPackage: packages.length > 0,
    hasPaperEnabledPackage: packages.length > 0,
    hasSelectionRun: runs.length > 0,
    hasPortfolio: false,
    hasReadyRun: false,
  }, "selection");

  return (
    <main>
      <WorkflowStepper steps={workflowSteps} compact />
      <ErrorPanel error={error} title="选股操作失败" />
      <div className="pv2-grid pv2-grid-main" style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <SectionCard title="策略包选择器" eyebrow={`${selectedPackages.length} 个已选择`}>
          <PaperTable
            rows={packages}
            empty="No asset-eligible StrategyPackage. Create a QE package and pass asset checks first."
            columns={[
              { key: "pick", header: "选择", render: (row) => <input data-testid={`selection-package-${row.package_id}`} type="radio" name="selection-package" checked={Boolean(selected[row.package_id])} onChange={() => updatePackageSelection(row.package_id)} /> },
              { key: "name", header: "策略包", render: (row) => <><strong>{row.package_name}</strong><br /><span className="pv2-muted pv2-mono">{shortHash(row.package_id, 7)}</span></> },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.package_status} /> },
              { key: "health", header: "Runtime diagnostics", render: (row) => <><StatusBadge status={packageHealthStatus(row)} /><br /><span className="pv2-muted">{packageHealthHint(row)}</span></> },
              { key: "annual", header: "年化", render: (row) => formatPercent(row.metrics_summary?.annual_return) },
              { key: "ic", header: "IC", render: (row) => formatPercent(row.metrics_summary?.ic) },
              { key: "model", header: "模型", render: (row) => <StatusBadge status={String(row.model_state?.staleness_status || "unknown")} /> },
            ]}
          />
        </SectionCard>

        <SectionCard title="选股控制" eyebrow="StrategyPackage 权威推理" action={<button className="pv2-button" onClick={loadPackages} disabled={loading} type="button">刷新策略包</button>}>
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>模式</label><div className="pv2-chip" data-testid="selection-mode-fixed">单策略包</div></div>
            <div className="pv2-field"><label>交易日期</label><input className="pv2-input" data-testid="selection-trade-date" type="date" value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} /></div>
            <div className="pv2-field"><label>时点口径</label><select className="pv2-select" data-testid="selection-pit-mode" value={pitMode} onChange={(event) => setPitMode(event.target.value)}><option value="PREVIOUS_TRADING_DAY_CLOSE">前一交易日收盘数据</option><option value="NONE">诊断：不强制截止日</option></select></div>
            <div className="pv2-field"><label>因子/选股数据源</label><select className="pv2-select" data-testid="selection-data-source" value={dataSource} onChange={(event) => setDataSource(event.target.value as SelectionDataSource)}><option value="DB_HISTORICAL">DB_HISTORICAL 权威历史/PIT选股数据</option></select></div>
            <div className="pv2-field"><label>TopK（默认 20，最高 50）</label><input className="pv2-input" data-testid="selection-top-k" type="number" min={1} max={50} value={topK} onChange={(event) => setTopK(Number(event.target.value))} /></div>
            <PaperIndustryBlacklistSelector selected={industryBlacklist} onChange={setIndustryBlacklist} />
            <div className="pv2-field"><label>可交易性</label><label className="pv2-chip"><input data-testid="selection-exclude-suspended" type="checkbox" checked={excludeSuspended} onChange={(event) => setExcludeSuspended(event.target.checked)} /> 剔除已确认停牌股票并按后续排名补位</label></div>
          </div>
          {pitMode !== "NONE" ? (
            <NoticePanel title="历史时点选股口径" tone={pitContext?.cutoff_date ? "success" : "warning"}>
              目标交易日 {tradeDate} 的信号只允许使用截止到 <strong data-testid="selection-cutoff-date">{asText(pitContext?.cutoff_date)}</strong> 的数据；参考价日期为 {asText(pitContext?.reference_price_trade_date)}。后端会再次校验交易日历，不能使用目标日收盘后的未来数据。
            </NoticePanel>
          ) : (
            <NoticePanel title="诊断模式" tone="warning">当前不强制前一交易日 cutoff，仅用于排查历史 artifact；正式选股和模拟盘应使用前一交易日收盘口径。</NoticePanel>
          )}
          <NoticePanel title="组合 alpha 路径" tone="info">
            Selection Center 手工入口只运行一个 StrategyPackage；多个 alpha 的组合请先使用 from-multi-alpha-combine-run 生成已验证、已冻结、可复现的多Alpha策略包，再在这里按单包选股。
          </NoticePanel>
          <div className="pv2-card" style={{ marginTop: 14 }}>
            <div className="pv2-row-actions">
              <label className="pv2-chip"><input data-testid="selection-hmm-enabled" type="checkbox" checked={hmmEnabled} onChange={(event) => setHmmEnabled(event.target.checked)} /> 启用 HMM</label>
              <select className="pv2-select" data-testid="selection-hmm-config" value={hmmConfigId} disabled={!hmmEnabled} onChange={(event) => setHmmConfigId(event.target.value)} style={{ maxWidth: 260 }}>
                <option value="">选择模型版本</option>
                {hmmConfigs.map((item) => <option value={item.config_id} key={item.config_id}>{item.display_name} / {item.model_type}</option>)}
              </select>
              <select className="pv2-select" data-testid="selection-hmm-preset" value={hmmPreset} disabled={!hmmEnabled} onChange={(event) => setHmmPreset(event.target.value)} style={{ maxWidth: 150 }}>
                <option value="preset_A">preset_A</option>
                <option value="preset_B">preset_B</option>
              </select>
            </div>
            {hmmEnabled ? (
              <div data-testid="selection-hmm-coverage" style={{ marginTop: 10 }}>
                <NoticePanel title="HMM 自动系数缓存" tone="info">
                  选择 HMM 模型配置后无需每天手工生成系数快照；Selection/Paper 首次运行会自动按交易日计算并写入缓存，同一模型、preset 和交易日后续直接复用缓存。
                </NoticePanel>
              </div>
            ) : null}
          </div>
          <button className="pv2-button-primary" data-testid="selection-run" disabled={running} onClick={runSelection} type="button">{running ? "运行中..." : "运行选股"}</button>
        </SectionCard>
      </div>

      <SectionCard title="选股结果" eyebrow={run ? selectionRunLabel(run) : "尚未运行"} action={<div className="pv2-row-actions"><a className="pv2-button-ghost" data-testid="selection-create-advisory" href={run ? `/paper-v2/advisory?selection_run_id=${encodeURIComponent(run.run_id)}&package_ids=${encodeURIComponent(run.package_ids.join(","))}` : "/paper-v2/advisory"}>创建荐股任务</a><button className="pv2-button" data-testid="selection-add-watchlist" onClick={addToWatchlist} disabled={!run || !resultRows.length} type="button">一键加入自选股票池</button>{tdxAvailable && <button className="pv2-button" data-testid="selection-tdx-sync" onClick={syncToTdx} disabled={!watchlistResult || tdxSyncing} type="button">📡 加入通达信自选</button>}</div>}>
        <div className="pv2-form-grid" style={{ marginBottom: 12 }}>
          <div className="pv2-field"><label>自选分类名称</label><input className="pv2-input" data-testid="selection-watchlist-name" value={watchlistCategoryName} onChange={(event) => setWatchlistCategoryName(event.target.value)} placeholder="自动创建或复用同名分类" /></div>
          <div className="pv2-field"><label>目标交易日</label><div className="pv2-chip">{run?.trade_date || tradeDate}</div></div>
          <div className="pv2-field"><label>数据截止日</label><div className="pv2-chip">{asText((run?.runtime_config?.point_in_time_context as JsonObject | undefined)?.cutoff_date || pitContext?.cutoff_date)}</div></div>
        </div>
        <PaperTable
          rows={visibleResultRows}
          empty="运行选股或点击历史记录后查看排序候选股。"
          columns={[
            { key: "rank", header: "排名", render: (row) => row.rank },
            { key: "symbol", header: "股票代码", render: (row) => row.symbol },
            { key: "name", header: "股票名称", render: (row) => row.stock_name || "-" },
            { key: "score", header: "评分", render: (row) => row.score.toFixed(6) },
            { key: "entry", header: "入池价", render: (row) => row.selection_entry_price?.toFixed(3) || row.reference_price?.toFixed(3) || "-" },
            { key: "entry_band", header: "建议买入区间", render: (row) => renderEntryBand(row) },
            { key: "stop_zone", header: "建议止损", render: (row) => renderStopZone(row) },
            { key: "limits", header: "涨跌停边界", render: (row) => renderLimitBoundary(row) },
            { key: "guidance", header: "价格建议状态", render: (row) => row.guidance_status ? <StatusBadge status={row.guidance_status} /> : "未提供" },
            { key: "weight", header: "目标权重", render: (row) => formatPercent(row.target_weight) },
            { key: "reason", header: "原因", render: (row) => row.reason || "-" },
            { key: "current", header: "当前价", render: (row) => row.current_price?.toFixed(3) || "-" },
            { key: "prev", header: "昨收", render: (row) => row.previous_close?.toFixed(3) || "-" },
            { key: "volume", header: "成交量", render: (row) => formatNumber(row.volume, 0) },
          ]}
        />
        <NoticePanel title="价格区间说明" tone="info">
          建议买入区间与止损区间仅为投顾展示，guidance_status=rule_default；未经过 QE 验证，不代表最终委托价、成交价或 validated PnL。
        </NoticePanel>
        {watchlistResult ? (
          <NoticePanel title="已加入自选股票池" tone="success">
            分类 {watchlistCategoryName || watchlistResult.category_id}，来源 {watchlistResult.entry_source}，加入 {watchlistResult.imported_symbols.length} 只股票，入池基准时间 {watchlistResult.entry_as_of}。
          </NoticePanel>
        ) : null}
        {tdxSyncResult ? (
          <NoticePanel title="已同步到通达信" tone="success">
            板块「{tdxSyncResult.display_name}」，已同步 {tdxSyncResult.count} 只股票到通达信客户端。
          </NoticePanel>
        ) : null}
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="剔除与补位追踪" eyebrow="停牌/黑名单证据">
          <PaperTable
            rows={excludedFlat}
            empty="最近一次运行没有剔除记录。"
            columns={[
              { key: "pkg", header: "策略包", render: (item) => shortHash(item.packageId, 7) },
              { key: "symbol", header: "股票代码", render: (item) => String(item.row.symbol || "-") },
              { key: "reason", header: "原因", render: (item) => <StatusBadge status={String(item.row.reason || "excluded")} /> },
              { key: "ctx", header: "上下文", render: (item) => <span className="pv2-mono">{Object.keys((item.row.context as JsonObject) || {}).join(", ") || "-"}</span> },
            ]}
          />
        </SectionCard>

        <SectionCard title="运行配置追踪" eyebrow="本次请求">
          <div className="pv2-readable-panel">
            <div className="pv2-readable-table">
              <div className="pv2-readable-row"><div className="pv2-readable-key">目标交易日</div><div className="pv2-readable-value">{tradeDate}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">数据截止日</div><div className="pv2-readable-value">{asText(pitContext?.cutoff_date)}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">选股模式</div><div className="pv2-readable-value"><StatusBadge status={mode.toUpperCase()} /></div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">数据源</div><div className="pv2-readable-value">{dataSourceLabel(dataSource)}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">TopK</div><div className="pv2-readable-value">{formatNumber(topK, 0)}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">停牌过滤</div><div className="pv2-readable-value">{excludeSuspended ? "启用，按后续排名补位" : "关闭"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">HMM</div><div className="pv2-readable-value">{hmmEnabled ? `${hmmConfigId || "not selected"} / ${hmmPreset} / auto daily cache` : "not enabled"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">策略包</div><div className="pv2-readable-value">{selectedPackages.map((item) => item.package_name).join(", ") || "-"}</div></div>
            </div>
          </div>
        </SectionCard>
      </div>

      <SectionCard title="历史选股记录" eyebrow="点击记录可显示结果；历史多包/aggregate run 只读保留">
        <div className="pv2-row-actions" style={{ marginBottom: 12 }}>
          <button className="pv2-button" onClick={loadPackages} disabled={loading} type="button">刷新记录</button>
          <button className="pv2-button" data-testid="selection-history-select-page" onClick={() => setCurrentPageRunSelection(true)} disabled={!visibleRunIds.length || allVisibleRunsSelected} type="button">全选本页</button>
          <button className="pv2-button-ghost" data-testid="selection-history-clear-page" onClick={() => setCurrentPageRunSelection(false)} disabled={!selectedVisibleRunCount} type="button">取消本页</button>
          <button className="pv2-button-danger" data-testid="selection-delete-runs" onClick={deleteSelectedRuns} disabled={running || !selectedHistoryRunIds.length} type="button">删除选中 {selectedHistoryRunIds.length || ""}</button>
          <span className="pv2-muted">本页已选 {selectedVisibleRunCount}/{visibleRunIds.length}；合计已选 {selectedHistoryRunIds.length} 条</span>
        </div>
        <PaperTable
          rows={visibleRuns}
          empty="暂无选股运行。"
          columns={[
            { key: "pick", header: "批量", render: (row) => <input data-testid={`selection-run-checkbox-${row.run_id}`} type="checkbox" checked={Boolean(selectedRuns[row.run_id])} onChange={(event) => setSelectedRuns((prev) => ({ ...prev, [row.run_id]: event.target.checked }))} /> },
            { key: "run", header: "运行记录", render: (row) => <button className="pv2-link-button" onClick={() => showHistoryRun(row.run_id)} type="button">{runLabel(row)}</button> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status || "unknown"} /> },
            { key: "source", header: "数据源", render: (row) => dataSourceLabel(row.data_source) },
            { key: "pkgs", header: "策略包", render: (row) => row.package_ids.map((item) => shortHash(item, 5)).join(", ") },
            { key: "count", header: "候选数", render: (row) => row.aggregate_results?.length || 0 },
          ]}
        />
        <div className="pv2-row-actions" style={{ marginTop: 12, justifyContent: "flex-end" }}>
          <button className="pv2-button-ghost" disabled={runPageSafe <= 1 || loading} onClick={() => setRunPage((current) => Math.max(1, current - 1))} type="button">上一页</button>
          <span className="pv2-muted">第 {runPageSafe} / {runTotalPages} 页</span>
          <button className="pv2-button-ghost" disabled={runPageSafe >= runTotalPages || loading} onClick={() => setRunPage((current) => current + 1)} type="button">下一页</button>
        </div>
      </SectionCard>
    </main>
  );
}
