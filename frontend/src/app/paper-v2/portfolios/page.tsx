"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { hmmTrainingApi, paperV2Api, strategyPackageApi } from "@/lib/paper-v2/api";
import { formatCompact, hmmSnapshotLabel, shortHash, todayIso } from "@/lib/paper-v2/format";
import type { DataSource, ExecutionPolicy, HmmConfig, HmmSnapshot, JsonObject, PaperPortfolio, PaperSessionProgress, StrategyPackage } from "@/lib/paper-v2/types";

function daysAgoIso(days: number): string {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

export default function PaperV2PortfoliosPage() {
  const [portfolios, setPortfolios] = useState<PaperPortfolio[]>([]);
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [policies, setPolicies] = useState<ExecutionPolicy[]>([]);
  const [hmmConfigs, setHmmConfigs] = useState<HmmConfig[]>([]);
  const [hmmSnapshots, setHmmSnapshots] = useState<HmmSnapshot[]>([]);
  const [packageId, setPackageId] = useState("");
  const [name, setName] = useState("模拟盘 v2 组合");
  const [initialCash, setInitialCash] = useState(1000000);
  const [startMode, setStartMode] = useState<"replay" | "realtime">("replay");
  const [startDate, setStartDate] = useState(todayIso());
  const [replayStart, setReplayStart] = useState(daysAgoIso(10));
  const [replayEnd, setReplayEnd] = useState(todayIso());
  const [autoSwitchToLive, setAutoSwitchToLive] = useState(false);
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
  const [sessionProgress, setSessionProgress] = useState<PaperSessionProgress | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);

  const selectedPackage = useMemo(() => packages.find((item) => item.package_id === packageId), [packages, packageId]);
  const activePortfolios = useMemo(() => portfolios.filter((item) => ["READY", "RUNNING", "PAUSED", "FAILED"].includes(item.status)), [portfolios]);

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
      if (pkg && name === "模拟盘 v2 组合") setName(`${pkg.package_name}-模拟盘`);
      if (!hmmConfigId && configRows[0]) setHmmConfigId(configRows[0].config_id);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [hmmConfigId, name, packageId]);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    setDataSource(startMode === "realtime" ? "TDX_REALTIME" : "DB_HISTORICAL");
  }, [startMode]);

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
      if (!ready.find((item) => item.snapshot_id === hmmSnapshotId)) setHmmSnapshotId(ready[0]?.snapshot_id || "");
    }).catch((exc) => {
      if (alive) setError(exc);
    });
    return () => { alive = false; };
  }, [hmmConfigId, hmmSnapshotId]);

  function runtimeConfig(): JsonObject {
    const blacklist = industryBlacklist.split(",").map((item) => item.trim()).filter(Boolean);
    return {
      top_k: topK,
      selection_artifact_config: { auto_generate: true, inference_backend: "wsl" },
      paper_v2_session: { signal_data_source: "DB_HISTORICAL" },
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

  async function createPortfolio() {
    setError(null);
    setCreated(null);
    setSessionProgress(null);
    setBusy(true);
    try {
      if (!packageId) throw new Error("请先选择 StrategyPackage。");
      if (topK < 1 || topK > 50) throw new Error("TopK 必须在 1 到 50 之间。");
      if (hmmEnabled && (!hmmConfigId || !hmmSnapshotId)) throw new Error("启用 HMM 时必须选择模型版本和已完成快照。");
      if (startMode === "replay" && dataSource !== "DB_HISTORICAL") throw new Error("历史回放必须使用 DB_HISTORICAL 数据源。");
      const portfolioStartDate = startMode === "replay" ? replayStart : startDate;
      const portfolio = await paperV2Api.createPortfolio({
        package_id: packageId,
        portfolio_name: name,
        initial_cash: initialCash,
        start_date: portfolioStartDate,
        data_source: dataSource,
        execution_policy: policyId ? { validated_execution_policy_id: policyId } : undefined,
      });
      setCreated(portfolio);
      if (startMode === "replay") {
        const session = await paperV2Api.createSession(portfolio.portfolio_id, {
          mode: "REPLAY_ONLY",
          start_date: replayStart,
          end_date: replayEnd,
          historical_data_source: "DB_HISTORICAL",
          live_data_source: autoSwitchToLive ? "TDX_REALTIME" : null,
          runtime_config: runtimeConfig(),
          rerun_policy: "reject_existing",
          auto_switch_to_live: autoSwitchToLive,
          created_by: "paper_v2_ui",
        });
        setSessionProgress(await paperV2Api.tickSession(session.session_id));
      } else {
        const session = await paperV2Api.createSession(portfolio.portfolio_id, {
          mode: "LIVE_ONLY",
          start_date: startDate,
          live_data_source: "TDX_REALTIME",
          runtime_config: runtimeConfig(),
          rerun_policy: "reject_existing",
          created_by: "paper_v2_ui",
        });
        setSessionProgress(await paperV2Api.tickSession(session.session_id));
      }
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

  return (
    <main>
      <ErrorPanel error={error} title="组合操作失败" />
      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="从单个策略包启动模拟盘" eyebrow="单包执行主链路" action={<button className="pv2-button" onClick={load} disabled={loading} type="button">刷新</button>}>
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>StrategyPackage</label><select className="pv2-select" value={packageId} onChange={(event) => setPackageId(event.target.value)}>{packages.map((item) => <option value={item.package_id} key={item.package_id}>{item.package_name} / {item.package_status}</option>)}</select></div>
            <div className="pv2-field"><label>组合名称</label><input className="pv2-input" value={name} onChange={(event) => setName(event.target.value)} /></div>
            <div className="pv2-field"><label>初始资金</label><input className="pv2-input" type="number" min={1} value={initialCash} onChange={(event) => setInitialCash(Number(event.target.value))} /></div>
            <div className="pv2-field"><label>启动模式</label><select className="pv2-select" value={startMode} onChange={(event) => setStartMode(event.target.value as "replay" | "realtime")}><option value="replay">历史分钟回放</option><option value="realtime">直接进入实时模拟盘</option></select></div>
            <div className="pv2-field"><label>数据源</label><input className="pv2-input" value={dataSource} readOnly /></div>
            <div className="pv2-field"><label>已验证执行策略</label><select className="pv2-select" value={policyId} onChange={(event) => setPolicyId(event.target.value)}><option value="">Manifest 默认策略（后端会导入并校验）</option>{policies.map((item) => <option value={item.policy_id} key={item.policy_id}>{item.policy_name || item.policy_id} / {item.algo_code} / {item.paper_enabled ? "可用于模拟盘" : "未启用"}</option>)}</select></div>
            {startMode === "replay" ? <>
              <div className="pv2-field"><label>回放开始日期</label><input className="pv2-input" type="date" value={replayStart} onChange={(event) => setReplayStart(event.target.value)} /></div>
              <div className="pv2-field"><label>回放结束日期</label><input className="pv2-input" type="date" value={replayEnd} onChange={(event) => setReplayEnd(event.target.value)} /></div>
              <div className="pv2-field"><label>追赶后切换实时</label><label className="pv2-chip"><input type="checkbox" checked={autoSwitchToLive} onChange={(event) => setAutoSwitchToLive(event.target.checked)} /> 回放追赶完成后自动进入 TDX 实时模拟</label></div>
            </> : <div className="pv2-field"><label>实时模拟开始日期</label><input className="pv2-input" type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} /></div>}
          </div>

          <div className="pv2-card" style={{ marginTop: 14 }}>
            <div className="pv2-eyebrow">运行时选股配置（不写入策略包 Manifest）</div>
            <div className="pv2-form-grid">
              <div className="pv2-field"><label>TopK</label><input className="pv2-input" type="number" min={1} max={50} value={topK} onChange={(event) => setTopK(Number(event.target.value))} /></div>
              <div className="pv2-field"><label>行业黑名单</label><input className="pv2-input" value={industryBlacklist} placeholder="银行, 房地产" onChange={(event) => setIndustryBlacklist(event.target.value)} /></div>
              <div className="pv2-field"><label>停牌处理</label><label className="pv2-chip"><input type="checkbox" checked={excludeSuspended} onChange={(event) => setExcludeSuspended(event.target.checked)} /> 剔除并补位</label></div>
              <div className="pv2-field"><label>HMM</label><label className="pv2-chip"><input type="checkbox" checked={hmmEnabled} onChange={(event) => setHmmEnabled(event.target.checked)} /> 启用 HMM</label></div>
              <div className="pv2-field"><label>HMM 模型版本</label><select className="pv2-select" value={hmmConfigId} disabled={!hmmEnabled} onChange={(event) => setHmmConfigId(event.target.value)}><option value="">选择模型版本</option>{hmmConfigs.map((item) => <option value={item.config_id} key={item.config_id}>{item.display_name} / {item.model_type}</option>)}</select></div>
              <div className="pv2-field"><label>HMM 快照</label><select className="pv2-select" value={hmmSnapshotId} disabled={!hmmEnabled || !hmmConfigId} onChange={(event) => setHmmSnapshotId(event.target.value)}><option value="">选择已完成快照</option>{hmmSnapshots.map((item) => <option value={item.snapshot_id} key={item.snapshot_id}>{hmmSnapshotLabel(item)}</option>)}</select></div>
              <div className="pv2-field"><label>HMM Preset</label><select className="pv2-select" value={hmmPreset} disabled={!hmmEnabled} onChange={(event) => setHmmPreset(event.target.value)}><option value="preset_A">preset_A</option><option value="preset_B">preset_B</option></select></div>
            </div>
          </div>

          <div className="pv2-card" style={{ marginTop: 14 }}>
            <div className="pv2-eyebrow">创建前复核</div>
            <div className="pv2-chip-row">
              <span className="pv2-chip">package: {selectedPackage?.package_name || "-"}</span>
              <span className="pv2-chip">manifest: {shortHash(selectedPackage?.manifest_sha256)}</span>
              <span className="pv2-chip">data: {dataSource}</span>
              <span className="pv2-chip">cash: {formatCompact(initialCash)}</span>
              <span className="pv2-chip">mode: {startMode === "replay" ? "历史回放" : "实时模拟"}</span>
              {startMode === "replay" ? <span className="pv2-chip">追赶切实时: {autoSwitchToLive ? "TDX_REALTIME" : "关闭"}</span> : null}
            </div>
          </div>
          <button className="pv2-button-primary" onClick={createPortfolio} disabled={busy} type="button">{busy ? "处理中..." : startMode === "replay" ? (autoSwitchToLive ? "创建组合并回放追赶到实时" : "创建组合并开始历史回放") : "创建实时模拟盘"}</button>
          {created ? <JsonPanel value={{ created_portfolio_id: created.portfolio_id, package_id: created.package_id, manifest_sha256: created.manifest_sha256, session_progress: sessionProgress }} /> : null}
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

      <SectionCard title="当前正在运行或已创建的模拟盘" eyebrow={loading ? "加载中" : `${activePortfolios.length}/${portfolios.length} 个活跃组合`} action={<button className="pv2-button" onClick={load} type="button">刷新</button>}>
        <PaperTable
          rows={portfolios}
          empty="暂无模拟盘 v2 组合。"
          columns={[
            { key: "name", header: "名称", render: (row) => <Link href={`/paper-v2/portfolios/${row.portfolio_id}`}>{row.portfolio_name}</Link> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "package", header: "策略包", render: (row) => <span className="pv2-mono">{shortHash(row.package_id, 7)}</span> },
            { key: "cash", header: "初始资金", render: (row) => formatCompact(row.initial_cash) },
            { key: "source", header: "数据源", render: (row) => row.data_source },
            { key: "start", header: "开始", render: (row) => row.start_date },
            { key: "actions", header: "操作", render: (row) => <div className="pv2-row-actions"><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio_id}/run-console`}>运行控制台</Link><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio_id}/ledger`}>账本</Link><button className="pv2-link-button" onClick={() => lifecycle(row.portfolio_id, row.status === "PAUSED" ? "resume" : "pause")} type="button">{row.status === "PAUSED" ? "恢复" : "暂停"}</button><button className="pv2-link-button" onClick={() => lifecycle(row.portfolio_id, "retire")} type="button">退役</button></div> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
