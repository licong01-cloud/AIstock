"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperIndustryBlacklistSelector, { selectedIndustryCodes, selectedIndustryTrace, type Sw2Entry } from "@/components/paper-v2/PaperIndustryBlacklistSelector";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import CopyChip from "@/components/paper-v2/CopyChip";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import WorkflowStepper from "@/components/paper-v2/WorkflowStepper";
import { hmmTrainingApi, paperV2Api, strategyPackageApi } from "@/lib/paper-v2/api";
import { dataSourceLabel, formatCompact, packageDisplayLabel, paperV2WorkflowSteps, shortHash, todayIso } from "@/lib/paper-v2/format";
import type { DataSource, ExecutionPolicy, HmmConfig, JsonObject, PaperPortfolio, PaperSessionMode, PaperSessionProgress, StrategyPackage } from "@/lib/paper-v2/types";

function daysAgoIso(days: number): string {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

const SESSION_MODE_OPTIONS: Array<{ value: PaperSessionMode; label: string; description: string }> = [
  { value: "REPLAY_ONLY", label: "仅历史追赶", description: "追赶到当前最新已入库交易日后停止，不自动切实时。" },
  { value: "CATCHUP_THEN_LIVE", label: "历史追赶后自动实时", description: "先追赶历史分钟线，完成后在交易日开盘由调度器进入 TDX 实时运行。" },
  { value: "LIVE_ONLY", label: "完全实时运行", description: "不做历史追赶，直接创建 TDX 实时分钟线会话。" },
];
const LIVE_TICK_SETTLED_STATUSES = ["LIVE_WAITING_FOR_BAR", "LIVE_WAITING_NEXT_TRADING_DAY", "SUCCEEDED", "FAILED", "STOPPED"];

function todayStamp(): string {
  const now = new Date();
  const yyyy = String(now.getFullYear());
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
}

function defaultPortfolioName(packageName?: string): string {
  const stamp = todayStamp();
  return packageName ? `${packageName}-${stamp}-模拟盘` : `模拟盘-${stamp}`;
}

export default function PaperV2PortfoliosPage() {
  const [portfolios, setPortfolios] = useState<PaperPortfolio[]>([]);
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [policies, setPolicies] = useState<ExecutionPolicy[]>([]);
  const [hmmConfigs, setHmmConfigs] = useState<HmmConfig[]>([]);
  const [packageId, setPackageId] = useState("");
  const [name, setName] = useState(() => defaultPortfolioName());
  const [initialCash, setInitialCash] = useState(1000000);
  const [sessionMode, setSessionMode] = useState<PaperSessionMode>("REPLAY_ONLY");
  const [startDate, setStartDate] = useState(todayIso());
  const [replayStart, setReplayStart] = useState(daysAgoIso(10));
  const [replayEnd, setReplayEnd] = useState(todayIso());
  const [dataSource, setDataSource] = useState<DataSource>("DB_HISTORICAL");
  const [policyId, setPolicyId] = useState("");
  const [topK, setTopK] = useState(20);
  const [industryBlacklist, setIndustryBlacklist] = useState<Sw2Entry[]>([]);
  const [excludeSuspended, setExcludeSuspended] = useState(true);
  const [hmmEnabled, setHmmEnabled] = useState(false);
  const [hmmConfigId, setHmmConfigId] = useState("");
  const [hmmPreset, setHmmPreset] = useState("preset_A");
  const [portfolioPage, setPortfolioPage] = useState(1);
  const [portfolioPagination, setPortfolioPagination] = useState<JsonObject>({ page: 1, page_size: 20, total: 0, total_pages: 1 });
  const [portfolioSearch, setPortfolioSearch] = useState("");
  const [showRetired, setShowRetired] = useState(false);
  const [selectedPortfolioIds, setSelectedPortfolioIds] = useState<Record<string, boolean>>({});
  const [created, setCreated] = useState<PaperPortfolio | null>(null);
  const [sessionProgress, setSessionProgress] = useState<PaperSessionProgress | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);

  const selectedPackage = useMemo(() => packages.find((item) => item.package_id === packageId), [packages, packageId]);
  const activePortfolios = useMemo(() => portfolios.filter((item) => ["RUNNING", "PAUSED"].includes(item.status)), [portfolios]);
  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [portfolioRows, packageRows, configRows] = await Promise.all([
        paperV2Api.listPortfoliosPage({
          page: portfolioPage,
          pageSize: 20,
          search: portfolioSearch,
          statuses: showRetired ? undefined : ["READY", "RUNNING", "PAUSED", "COMPLETED"],
        }),
        strategyPackageApi.list(undefined, 300),
        hmmTrainingApi.configs(),
      ]);
      setPortfolios(portfolioRows.portfolios);
      setPortfolioPagination(portfolioRows.pagination);
      setPackages(packageRows);
      setHmmConfigs(configRows);
      const initialPackage = typeof window !== "undefined" ? new URLSearchParams(window.location.search).get("package_id") : null;
      const nextPackageId = packageId || initialPackage || packageRows[0]?.package_id || "";
      if (!packageId) setPackageId(nextPackageId);
      const pkg = packageRows.find((item) => item.package_id === nextPackageId);
      if (pkg && name === defaultPortfolioName()) setName(defaultPortfolioName(pkg.package_name || pkg.package_id));
      if (!hmmConfigId && configRows[0]) setHmmConfigId(configRows[0].config_id);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [hmmConfigId, name, packageId, portfolioPage, portfolioSearch, showRetired]);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    setDataSource(sessionMode === "LIVE_ONLY" ? "TDX_REALTIME" : "DB_HISTORICAL");
  }, [sessionMode]);

  useEffect(() => {
    let alive = true;
    const initialReplayStart = daysAgoIso(10);
    const initialToday = todayIso();
    paperV2Api.tradingDayDefaults(10).then((defaults) => {
      if (!alive) return;
      setStartDate((current) => (current === initialToday ? defaults.latest_trading_day : current));
      setReplayStart((current) => (current === initialReplayStart ? defaults.replay_start_date : current));
      setReplayEnd((current) => (current === initialToday ? defaults.replay_end_date : current));
    }).catch((exc) => {
      if (alive) setError(exc);
    });
    return () => { alive = false; };
  }, []);

  useEffect(() => {
    if (!packageId) return;
    strategyPackageApi.executionPolicies(packageId).then((rows) => {
      setPolicies(rows);
      setPolicyId(rows[0]?.policy_id || "");
    }).catch((exc) => {
      setPolicies([]);
      setError(exc);
    });
  }, [packageId]);

  function runtimeProfileConfig(): JsonObject {
    const blacklist = selectedIndustryCodes(industryBlacklist);
    return {
      top_k: topK,
      selection_artifact_config: { auto_generate: true, inference_backend: "wsl" },
      industry_blacklist_trace: selectedIndustryTrace(industryBlacklist),
      runtime_profile: {
        selection: { top_k: topK },
        tradability: { exclude_suspended: excludeSuspended },
        industry_blacklist: blacklist,
        hmm: {
          enabled: hmmEnabled,
          model_config_id: hmmEnabled ? hmmConfigId || null : null,
          model_snapshot_id: null,
          signal_preset: hmmEnabled ? hmmPreset : null,
        },
      },
    };
  }

  function sessionRuntimeConfig(): JsonObject {
    return {
      ...runtimeProfileConfig(),
      paper_v2_session: { signal_data_source: "DB_HISTORICAL", manual_tick_only: false },
    };
  }

  async function createPortfolio() {
    setError(null);
    setCreated(null);
    setSessionProgress(null);
    setBusy(true);
    try {
      if (!packageId) throw new Error("请先选择 StrategyPackage。");
      if (topK < 1 || topK > 50) throw new Error("TopK 必须在 1 到 50 之间。");
      if (hmmEnabled && !hmmConfigId) throw new Error("启用 HMM 时请选择模型配置；每日系数由平台按交易日自动计算并缓存。");
      if (sessionMode !== "LIVE_ONLY" && dataSource !== "DB_HISTORICAL") throw new Error(`历史追赶必须使用「${dataSourceLabel("DB_HISTORICAL")}」数据源。`);
      const isReplayOnly = sessionMode === "REPLAY_ONLY";
      const isCatchupThenLive = sessionMode === "CATCHUP_THEN_LIVE";
      const isLiveOnly = sessionMode === "LIVE_ONLY";
      const portfolioStartDate = isLiveOnly ? startDate : replayStart;
      const portfolio = await paperV2Api.createPortfolio({
        package_id: packageId,
        portfolio_name: name,
        initial_cash: initialCash,
        start_date: portfolioStartDate,
        data_source: dataSource,
        execution_policy: policyId ? { validated_execution_policy_id: policyId } : undefined,
      });
      setCreated(portfolio);
      const effectiveRuntimeConfig = sessionRuntimeConfig();
      const session = await paperV2Api.createSession(portfolio.portfolio_id, {
        mode: sessionMode,
        start_date: isLiveOnly ? startDate : replayStart,
        end_date: isReplayOnly || isCatchupThenLive ? replayEnd : null,
        historical_data_source: isReplayOnly || isCatchupThenLive ? "DB_HISTORICAL" : null,
        live_data_source: isCatchupThenLive || isLiveOnly ? "TDX_REALTIME" : null,
        runtime_config: effectiveRuntimeConfig,
        rerun_policy: "reject_existing",
        auto_switch_to_live: false,
        created_by: "paper_v2_ui",
      });
      setSessionProgress({ session, day_count: 0, events: [] });
      setSessionProgress(await paperV2Api.tickSessionAndWait(
        session.session_id,
        {},
        isLiveOnly || isCatchupThenLive
          ? { timeoutMs: 600_000, pollMs: 2_000, settleStatuses: LIVE_TICK_SETTLED_STATUSES }
          : { timeoutMs: 600_000, pollMs: 2_000 },
      ));
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function lifecycle(portfolioId: string, action: "pause" | "resume" | "complete" | "retire") {
    setError(null);
    try {
      await paperV2Api.lifecycle(portfolioId, action);
      await load();
    } catch (exc) {
      setError(exc);
    }
  }

  async function bulkLifecycleSelected(action: "pause" | "resume" | "complete" | "retire") {
    const ids = Object.entries(selectedPortfolioIds).filter(([, checked]) => checked).map(([id]) => id);
    if (!ids.length) return;
    if (action === "retire" && !window.confirm(`确认退役 ${ids.length} 个模拟盘？退役只归档组合，不删除账本和历史证据。`)) return;
    setBusy(true);
    setError(null);
    try {
      await paperV2Api.bulkLifecycle(ids, action);
      setSelectedPortfolioIds({});
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function deleteSelectedPortfolios() {
    const ids = Object.entries(selectedPortfolioIds).filter(([, checked]) => checked).map(([id]) => id);
    if (!ids.length) return;
    if (!window.confirm(`确认彻底删除 ${ids.length} 个模拟盘及其本地账本/运行记录？此操作不可恢复，但不会修改策略包。`)) return;
    setBusy(true);
    setError(null);
    try {
      await paperV2Api.deletePortfolios(ids);
      setSelectedPortfolioIds({});
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  function togglePortfolio(portfolioId: string, checked: boolean) {
    setSelectedPortfolioIds((current) => ({ ...current, [portfolioId]: checked }));
  }

  const portfolioPageSize = 20;
  const portfolioTotalPages = Math.max(1, Number(portfolioPagination.total_pages || 1));
  const portfolioPageSafe = Math.min(portfolioPage, portfolioTotalPages);
  const pageStart = (portfolioPageSafe - 1) * portfolioPageSize;
  const portfolioTotal = Number(portfolioPagination.total || portfolios.length);
  const pageRangeStart = portfolioTotal ? pageStart + 1 : 0;
  const visiblePortfolios = portfolios;
  const selectedPortfolioCount = Object.values(selectedPortfolioIds).filter(Boolean).length;

  const workflowSteps = paperV2WorkflowSteps({
    hasPackages: packages.length > 0,
    hasSelectionEnabledPackage: packages.length > 0,
    hasPaperEnabledPackage: packages.length > 0,
    hasSelectionRun: false,
    hasPortfolio: portfolios.length > 0,
    hasReadyRun: false,
  }, "portfolio");

  return (
    <main>
      <WorkflowStepper steps={workflowSteps} compact />
      <ErrorPanel error={error} title="组合操作失败" />
      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="从单个策略包启动模拟盘" eyebrow="单包执行主链路" action={<button className="pv2-button" onClick={load} disabled={loading} type="button">刷新</button>}>
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>StrategyPackage</label><select className="pv2-select" data-testid="portfolio-package" value={packageId} onChange={(event) => setPackageId(event.target.value)}>{packages.map((item) => <option value={item.package_id} key={item.package_id}>{item.package_name} / {item.package_status}</option>)}</select></div>
            <div className="pv2-field"><label>模拟盘名称</label><input className="pv2-input" data-testid="portfolio-name" value={name} onChange={(event) => setName(event.target.value)} /></div>
            <div className="pv2-field"><label>初始资金</label><input className="pv2-input" data-testid="portfolio-initial-cash" type="number" min={1} value={initialCash} onChange={(event) => setInitialCash(Number(event.target.value))} /></div>
            <div className="pv2-field"><label>运行场景</label><select className="pv2-select" data-testid="portfolio-start-mode" value={sessionMode} onChange={(event) => setSessionMode(event.target.value as PaperSessionMode)}>{SESSION_MODE_OPTIONS.map((item) => <option value={item.value} key={item.value}>{item.label}</option>)}</select></div>
            <div className="pv2-field"><label>数据源</label><input className="pv2-input" data-testid="portfolio-data-source" value={dataSourceLabel(dataSource)} readOnly /></div>
            <div className="pv2-field"><label>执行策略</label><select className="pv2-select" data-testid="portfolio-policy" value={policyId} onChange={(event) => setPolicyId(event.target.value)}><option value="">平台默认：使用 manifest 默认执行策略，运行前 fail-fast 校验</option>{policies.map((item) => <option value={item.policy_id} key={item.policy_id}>{item.policy_name || item.policy_id} / {item.algo_code || "-"}</option>)}</select></div>
            {sessionMode !== "LIVE_ONLY" ? <>
              <div className="pv2-field"><label>历史追赶开始</label><input className="pv2-input" data-testid="portfolio-replay-start" type="date" value={replayStart} onChange={(event) => setReplayStart(event.target.value)} /></div>
              <div className="pv2-field"><label>历史追赶结束</label><input className="pv2-input" data-testid="portfolio-replay-end" type="date" value={replayEnd} onChange={(event) => setReplayEnd(event.target.value)} /></div>
              <div className="pv2-field"><label>追赶完成后</label><input className="pv2-input" value={sessionMode === "CATCHUP_THEN_LIVE" ? "自动进入 TDX 实时模拟" : "停止，等待下一次手动或调度继续追赶"} readOnly /></div>
            </> : <div className="pv2-field"><label>实时模拟开始日期</label><input className="pv2-input" data-testid="portfolio-live-start" type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} /></div>}
          </div>
          <NoticePanel title="运行场景" tone="info">
            {SESSION_MODE_OPTIONS.find((item) => item.value === sessionMode)?.description} 创建、暂停、恢复和停止允许盘中执行；真正的下单安全由交易日历、行情、停牌、涨跌停、执行策略和 broker 运行时校验负责。
          </NoticePanel>

          <div className="pv2-card" style={{ marginTop: 14 }}>
            <div className="pv2-eyebrow">运行时选股配置（不写入策略包 Manifest）</div>
            <div className="pv2-form-grid">
              <div className="pv2-field"><label>TopK</label><input className="pv2-input" data-testid="portfolio-top-k" type="number" min={1} max={50} value={topK} onChange={(event) => setTopK(Number(event.target.value))} /></div>
              <PaperIndustryBlacklistSelector selected={industryBlacklist} onChange={setIndustryBlacklist} />
              <div className="pv2-field"><label>停牌处理</label><label className="pv2-chip"><input data-testid="portfolio-exclude-suspended" type="checkbox" checked={excludeSuspended} onChange={(event) => setExcludeSuspended(event.target.checked)} /> 剔除并补位</label></div>
              <div className="pv2-field"><label>HMM</label><label className="pv2-chip"><input data-testid="portfolio-hmm-enabled" type="checkbox" checked={hmmEnabled} onChange={(event) => setHmmEnabled(event.target.checked)} /> Enable HMM</label></div>
              <div className="pv2-field"><label>HMM Config</label><select className="pv2-select" data-testid="portfolio-hmm-config" value={hmmConfigId} disabled={!hmmEnabled} onChange={(event) => setHmmConfigId(event.target.value)}><option value="">Select HMM config</option>{hmmConfigs.map((item) => <option value={item.config_id} key={item.config_id}>{item.display_name} / {item.model_type}</option>)}</select></div>
              <div className="pv2-field"><label>HMM Preset</label><select className="pv2-select" data-testid="portfolio-hmm-preset" value={hmmPreset} disabled={!hmmEnabled} onChange={(event) => setHmmPreset(event.target.value)}><option value="preset_A">preset_A</option><option value="preset_B">preset_B</option></select></div>
              {hmmEnabled ? <div className="pv2-field" data-testid="portfolio-hmm-coverage"><label>HMM Coefficients</label><NoticePanel title="HMM 自动系数缓存" tone="info">组合创建不再要求手工选择每日系数 artifact；运行时按模型配置、preset 和交易日自动计算，后续复用平台缓存。</NoticePanel></div> : null}
            </div>
          </div>

          <div className="pv2-card" style={{ marginTop: 14 }}>
            <div className="pv2-eyebrow">创建前复核</div>
            <div className="pv2-chip-row">
              <span className="pv2-chip">策略包: {selectedPackage ? packageDisplayLabel(selectedPackage) : "-"}</span>
              <span className="pv2-chip">数据源: {dataSourceLabel(dataSource)}</span>
              <span className="pv2-chip">初始资金: {formatCompact(initialCash)}</span>
              <span className="pv2-chip">运行场景: {SESSION_MODE_OPTIONS.find((item) => item.value === sessionMode)?.label}</span>
              <span className="pv2-chip">数据源角色: {sessionMode === "LIVE_ONLY" ? dataSourceLabel("TDX_REALTIME") : sessionMode === "CATCHUP_THEN_LIVE" ? `${dataSourceLabel("DB_HISTORICAL")} → ${dataSourceLabel("TDX_REALTIME")}` : dataSourceLabel("DB_HISTORICAL")}</span>
              {selectedPackage?.manifest_sha256 ? <CopyChip label={`manifest ${shortHash(selectedPackage.manifest_sha256, 6)}`} value={selectedPackage.manifest_sha256} title={`完整 manifest_sha256：${selectedPackage.manifest_sha256}`} /> : null}
            </div>
          </div>
          <button className="pv2-button-primary" data-testid="portfolio-create" onClick={createPortfolio} disabled={busy} type="button">{busy ? "处理中..." : sessionMode === "LIVE_ONLY" ? "创建完全实时模拟盘" : sessionMode === "CATCHUP_THEN_LIVE" ? "创建并追赶后自动实时" : "创建并仅历史追赶"}</button>
          {created ? <NoticePanel title="模拟盘已创建" tone="success">组合 {created.portfolio_name} 已创建；当前只是组合/会话记录已生成，真实运行是否成功以 session/run 证据为准。{sessionProgress ? ` 当前会话状态：${sessionProgress.session.status}` : ""}</NoticePanel> : null}
        </SectionCard>

      </div>

      <SectionCard
        title="当前模拟盘"
        eyebrow={loading ? "加载中" : `${pageRangeStart}-${Math.min(pageStart + portfolioPageSize, portfolioTotal)} / ${portfolioTotal} 个模拟盘`}
        action={
          <div className="pv2-row-actions">
            <input className="pv2-input" style={{ maxWidth: 180 }} value={portfolioSearch} onChange={(event) => { setPortfolioSearch(event.target.value); setPortfolioPage(1); }} placeholder="搜索名称/ID/策略包" />
            <label className="pv2-chip"><input type="checkbox" checked={showRetired} onChange={(event) => { setShowRetired(event.target.checked); setPortfolioPage(1); }} /> 显示退役</label>
            <button className="pv2-button" onClick={load} type="button">刷新</button>
            <button className="pv2-button" onClick={() => bulkLifecycleSelected("pause")} disabled={!selectedPortfolioCount || busy} type="button">批量暂停</button>
            <button className="pv2-button" onClick={() => bulkLifecycleSelected("resume")} disabled={!selectedPortfolioCount || busy} type="button">批量恢复</button>
            <button className="pv2-button" onClick={() => bulkLifecycleSelected("retire")} disabled={!selectedPortfolioCount || busy} type="button">批量退役</button>
            <button className="pv2-button-danger" onClick={deleteSelectedPortfolios} disabled={!selectedPortfolioCount || busy} type="button">删除选中 {selectedPortfolioCount || ""}</button>
          </div>
        }
      >
        <PaperTable
          rows={visiblePortfolios}
          empty="暂无模拟盘 v2 组合。"
          columns={[
            { key: "select", header: "选择", render: (row) => <input type="checkbox" checked={Boolean(selectedPortfolioIds[row.portfolio_id])} onChange={(event) => togglePortfolio(row.portfolio_id, event.target.checked)} /> },
            { key: "name", header: "名称", render: (row) => <Link href={`/paper-v2/portfolios/${row.portfolio_id}`}>{row.portfolio_name}</Link> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "package", header: "策略包", render: (row) => {
              const pkg = packages.find((item) => item.package_id === row.package_id);
              const label = pkg ? packageDisplayLabel(pkg) : shortHash(row.package_id, 7);
              return <span title={String(row.package_id)}>{label}</span>;
            } },
            { key: "cash", header: "初始资金", render: (row) => formatCompact(row.initial_cash) },
            { key: "source", header: "数据源", render: (row) => dataSourceLabel(row.data_source) },
            { key: "start", header: "开始", render: (row) => row.start_date },
            { key: "actions", header: "操作", render: (row) => <div className="pv2-row-actions"><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio_id}/run-console`}>运行控制台</Link><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio_id}/ledger`}>账本</Link><button className="pv2-link-button" onClick={() => lifecycle(row.portfolio_id, row.status === "PAUSED" ? "resume" : "pause")} type="button">{row.status === "PAUSED" ? "恢复" : "暂停"}</button><button className="pv2-link-button" onClick={() => lifecycle(row.portfolio_id, "retire")} type="button">退役</button></div> },
          ]}
        />
        <div className="pv2-row-actions" style={{ marginTop: 12, justifyContent: "flex-end" }}>
          <button className="pv2-button-ghost" disabled={portfolioPageSafe <= 1 || loading} onClick={() => setPortfolioPage((current) => Math.max(1, current - 1))} type="button">上一页</button>
          <span className="pv2-muted">第 {portfolioPageSafe} / {portfolioTotalPages} 页</span>
          <button className="pv2-button-ghost" disabled={portfolioPageSafe >= portfolioTotalPages || loading} onClick={() => setPortfolioPage((current) => current + 1)} type="button">下一页</button>
        </div>
      </SectionCard>
    </main>
  );
}
