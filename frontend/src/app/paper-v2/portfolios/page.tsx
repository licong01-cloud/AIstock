"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import CopyChip from "@/components/paper-v2/CopyChip";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import WorkflowStepper from "@/components/paper-v2/WorkflowStepper";
import { hmmTrainingApi, paperV2Api, strategyPackageApi } from "@/lib/paper-v2/api";
import { dataSourceLabel, formatCompact, hmmSnapshotLabel, packageDisplayLabel, paperV2WorkflowSteps, shortHash, todayIso } from "@/lib/paper-v2/format";
import type { DataSource, ExecutionPolicy, HmmConfig, HmmSnapshot, JsonObject, PaperPortfolio, PaperSessionMode, PaperSessionProgress, RuntimeProfileVersion, StrategyPackage } from "@/lib/paper-v2/types";

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
  const [hmmSnapshots, setHmmSnapshots] = useState<HmmSnapshot[]>([]);
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
  const [industryBlacklist, setIndustryBlacklist] = useState("");
  const [excludeSuspended, setExcludeSuspended] = useState(true);
  const [hmmEnabled, setHmmEnabled] = useState(false);
  const [hmmConfigId, setHmmConfigId] = useState("");
  const [hmmSnapshotId, setHmmSnapshotId] = useState("");
  const [hmmPreset, setHmmPreset] = useState("preset_A");
  const [created, setCreated] = useState<PaperPortfolio | null>(null);
  const [createdRuntimeVersion, setCreatedRuntimeVersion] = useState<RuntimeProfileVersion | null>(null);
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
        paperV2Api.listPortfolios(300),
        strategyPackageApi.list(undefined, 300),
        hmmTrainingApi.configs(),
      ]);
      setPortfolios(portfolioRows);
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
  }, [hmmConfigId, name, packageId]);

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
      const paperReady = rows.find((item) => item.paper_enabled);
      setPolicyId(paperReady?.policy_id || "");
    }).catch((exc) => {
      setPolicies([]);
      setError(exc);
    });
  }, [packageId]);

  useEffect(() => {
    if (!hmmConfigId) {
      setHmmSnapshots([]);
      setHmmSnapshotId("");
      return;
    }
    let alive = true;
    hmmTrainingApi.snapshots(hmmConfigId).then((rows) => {
      if (!alive) return;
      const ready = rows.filter((item) => ["completed", "ready", "success", "succeeded"].includes(String(item.status || "").toLowerCase()));
      setHmmSnapshots(ready);
      setHmmSnapshotId((current) => (
        ready.find((item) => item.snapshot_id === current) ? current : ready[0]?.snapshot_id || ""
      ));
    }).catch((exc) => {
      if (alive) setError(exc);
    });
    return () => { alive = false; };
  }, [hmmConfigId]);

  function runtimeProfileConfig(): JsonObject {
    const blacklist = industryBlacklist.split(",").map((item) => item.trim()).filter(Boolean);
    return {
      top_k: topK,
      selection_artifact_config: { auto_generate: true, inference_backend: "wsl" },
      runtime_profile: {
        selection: { top_k: topK },
        tradability: { exclude_suspended: excludeSuspended },
        industry_blacklist: blacklist,
        hmm: {
          enabled: hmmEnabled,
          model_snapshot_id: hmmEnabled ? hmmSnapshotId : null,
          signal_preset: hmmEnabled ? hmmPreset : null,
        },
      },
    };
  }

  function sessionRuntimeConfig(): JsonObject {
    return { paper_v2_session: { signal_data_source: "DB_HISTORICAL", manual_tick_only: false } };
  }

  async function createPortfolio() {
    setError(null);
    setCreated(null);
    setCreatedRuntimeVersion(null);
    setSessionProgress(null);
    setBusy(true);
    try {
      if (!packageId) throw new Error("请先选择 StrategyPackage。");
      if (topK < 1 || topK > 50) throw new Error("TopK 必须在 1 到 50 之间。");
      if (hmmEnabled && (!hmmConfigId || !hmmSnapshotId)) throw new Error("启用 HMM 时必须选择模型版本和已完成快照。");
      if (!policyId) throw new Error("Select a paper-enabled validated execution policy before creating portfolio.");
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
      const runtimeProfile = await paperV2Api.createRuntimeProfile(portfolio.portfolio_id, {
        profile_name: `${name} 运行配置`,
        config_json: runtimeProfileConfig(),
        created_by: "paper_v2_ui",
        reason: "创建模拟盘时保存可变运行配置",
      });
      setCreatedRuntimeVersion(runtimeProfile.version);
      await paperV2Api.activateRuntimeConfig(portfolio.portfolio_id, {
        trade_date: portfolioStartDate,
        profile_version_id: runtimeProfile.version.profile_version_id,
        activated_by: "paper_v2_ui",
          reason: "创建模拟盘时按开始日期激活运行配置",
        });
      const session = await paperV2Api.createSession(portfolio.portfolio_id, {
        mode: sessionMode,
        start_date: isLiveOnly ? startDate : replayStart,
        end_date: isReplayOnly || isCatchupThenLive ? replayEnd : null,
        historical_data_source: isReplayOnly || isCatchupThenLive ? "DB_HISTORICAL" : null,
        live_data_source: isCatchupThenLive || isLiveOnly ? "TDX_REALTIME" : null,
        runtime_config: sessionRuntimeConfig(),
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

  const workflowSteps = paperV2WorkflowSteps({
    hasPackages: packages.length > 0,
    hasSelectionEnabledPackage: packages.length > 0,
    hasPaperEnabledPackage: packages.some((item) => ["PAPER_ENABLED", "PAPER_RUNNING", "PAPER_PASSED"].includes(item.package_status)),
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
            <div className="pv2-field"><label>已验证执行策略</label><select className="pv2-select" data-testid="portfolio-policy" value={policyId} onChange={(event) => setPolicyId(event.target.value)}><option value="">Select paper-enabled validated policy (required; no manifest auto-import)</option>{policies.map((item) => <option value={item.policy_id} key={item.policy_id}>{item.policy_name || item.policy_id} / {item.algo_code} / {item.paper_enabled ? "可用于模拟盘" : "未启用"}</option>)}</select></div>
            {sessionMode !== "LIVE_ONLY" ? <>
              <div className="pv2-field"><label>历史追赶开始</label><input className="pv2-input" data-testid="portfolio-replay-start" type="date" value={replayStart} onChange={(event) => setReplayStart(event.target.value)} /></div>
              <div className="pv2-field"><label>历史追赶结束</label><input className="pv2-input" data-testid="portfolio-replay-end" type="date" value={replayEnd} onChange={(event) => setReplayEnd(event.target.value)} /></div>
              <div className="pv2-field"><label>追赶完成后</label><input className="pv2-input" value={sessionMode === "CATCHUP_THEN_LIVE" ? "自动进入 TDX 实时模拟" : "停止，等待下一次非交易时段继续追赶"} readOnly /></div>
            </> : <div className="pv2-field"><label>实时模拟开始日期</label><input className="pv2-input" data-testid="portfolio-live-start" type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} /></div>}
          </div>
          <NoticePanel title="运行场景" tone="info">
            {SESSION_MODE_OPTIONS.find((item) => item.value === sessionMode)?.description} 创建、暂停、恢复、停止和场景切换由后端限制在非交易时间执行；实时 Tick 和后台调度仍可在交易时间处理行情。
          </NoticePanel>

          <div className="pv2-card" style={{ marginTop: 14 }}>
            <div className="pv2-eyebrow">运行时选股配置（不写入策略包 Manifest）</div>
            <div className="pv2-form-grid">
              <div className="pv2-field"><label>TopK</label><input className="pv2-input" data-testid="portfolio-top-k" type="number" min={1} max={50} value={topK} onChange={(event) => setTopK(Number(event.target.value))} /></div>
              <div className="pv2-field"><label>行业黑名单</label><input className="pv2-input" data-testid="portfolio-industry-blacklist" value={industryBlacklist} placeholder="银行, 房地产" onChange={(event) => setIndustryBlacklist(event.target.value)} /></div>
              <div className="pv2-field"><label>停牌处理</label><label className="pv2-chip"><input data-testid="portfolio-exclude-suspended" type="checkbox" checked={excludeSuspended} onChange={(event) => setExcludeSuspended(event.target.checked)} /> 剔除并补位</label></div>
              <div className="pv2-field"><label>HMM</label><label className="pv2-chip"><input data-testid="portfolio-hmm-enabled" type="checkbox" checked={hmmEnabled} onChange={(event) => setHmmEnabled(event.target.checked)} /> 启用 HMM</label></div>
              <div className="pv2-field"><label>HMM 模型版本</label><select className="pv2-select" data-testid="portfolio-hmm-config" value={hmmConfigId} disabled={!hmmEnabled} onChange={(event) => setHmmConfigId(event.target.value)}><option value="">选择模型版本</option>{hmmConfigs.map((item) => <option value={item.config_id} key={item.config_id}>{item.display_name} / {item.model_type}</option>)}</select></div>
              <div className="pv2-field"><label>HMM 快照</label><select className="pv2-select" data-testid="portfolio-hmm-snapshot" value={hmmSnapshotId} disabled={!hmmEnabled || !hmmConfigId} onChange={(event) => setHmmSnapshotId(event.target.value)}><option value="">选择已完成快照</option>{hmmSnapshots.map((item) => <option value={item.snapshot_id} key={item.snapshot_id}>{hmmSnapshotLabel(item)}</option>)}</select></div>
              <div className="pv2-field"><label>HMM Preset</label><select className="pv2-select" data-testid="portfolio-hmm-preset" value={hmmPreset} disabled={!hmmEnabled} onChange={(event) => setHmmPreset(event.target.value)}><option value="preset_A">preset_A</option><option value="preset_B">preset_B</option></select></div>
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
          <button className="pv2-button-primary" data-testid="portfolio-create" onClick={createPortfolio} disabled={busy || !policyId} type="button">{busy ? "处理中..." : sessionMode === "LIVE_ONLY" ? "创建完全实时模拟盘" : sessionMode === "CATCHUP_THEN_LIVE" ? "创建并追赶后自动实时" : "创建并仅历史追赶"}</button>
          {created ? (
            <NoticePanel title="模拟盘组合已创建" tone="success">
              组合 {created.portfolio_name} 已绑定策略包 {shortHash(created.package_id, 7)}
              ，manifest {shortHash(created.manifest_sha256, 7)}
              ，运行配置版本 {createdRuntimeVersion?.profile_version_id ? shortHash(createdRuntimeVersion.profile_version_id, 7) : "-"}
              ，会话状态 {sessionProgress?.session_status ?? "-"}。
            </NoticePanel>
          ) : null}
        </SectionCard>

        <SectionCard title="组合生命周期规则" eyebrow="防止假成功">
          <ul>
            <li>组合会冻结 package_id、manifest hash、初始资金、开始日期、数据源、费用、风控和已验证执行策略。</li>
            <li>HMM、黑名单、TopK、停牌剔除是每日运行时配置，允许开盘前调整。</li>
            <li>历史回放使用 Paper v2 主链路和分钟线撮合，不是 QE 回测兜底。</li>
            <li>重置回放必须到运行控制台输入完整 portfolio_id 确认。</li>
          </ul>
          <NoticePanel title="实时模拟说明" tone="info">非交易时段创建实时模拟盘只会生成 READY 组合；实际单日运行仍需交易日历、分钟线、涨跌停、停牌和权威选股 artifact 全部就绪。</NoticePanel>
        </SectionCard>
      </div>

      <SectionCard title="当前模拟盘" eyebrow={loading ? "加载中" : `${activePortfolios.length}/${portfolios.length} 个运行/暂停模拟盘`} action={<button className="pv2-button" onClick={load} type="button">刷新</button>}>
        <PaperTable
          rows={portfolios}
          empty="暂无模拟盘 v2 组合。"
          columns={[
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
      </SectionCard>
    </main>
  );
}
