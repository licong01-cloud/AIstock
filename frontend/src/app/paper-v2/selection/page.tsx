"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { hmmTrainingApi, paperV2Api, selectionCenterApi } from "@/lib/paper-v2/api";
import { formatPercent, shortHash, todayIso } from "@/lib/paper-v2/format";
import type { DataSource, HmmConfig, HmmSnapshot, JsonObject, SelectablePackage, SelectionMode, SelectionRun, SelectionWatchlistImportResult } from "@/lib/paper-v2/types";

function runLabel(run: SelectionRun): string {
  return `${run.trade_date} / ${run.mode} / ${run.package_ids.map((item) => shortHash(item, 5)).join(", ")}`;
}

export default function PaperV2SelectionPage() {
  const [packages, setPackages] = useState<SelectablePackage[]>([]);
  const [runs, setRuns] = useState<SelectionRun[]>([]);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [selectedRuns, setSelectedRuns] = useState<Record<string, boolean>>({});
  const [weights, setWeights] = useState<Record<string, number>>({});
  const [mode, setMode] = useState<SelectionMode>("single_package");
  const [tradeDate, setTradeDate] = useState(todayIso());
  const [dataSource, setDataSource] = useState<DataSource>("DB_HISTORICAL");
  const [topK, setTopK] = useState(20);
  const [industryBlacklist, setIndustryBlacklist] = useState("");
  const [excludeSuspended, setExcludeSuspended] = useState(true);
  const [hmmEnabled, setHmmEnabled] = useState(false);
  const [hmmConfigs, setHmmConfigs] = useState<HmmConfig[]>([]);
  const [hmmSnapshots, setHmmSnapshots] = useState<HmmSnapshot[]>([]);
  const [hmmConfigId, setHmmConfigId] = useState("");
  const [hmmSnapshotId, setHmmSnapshotId] = useState("");
  const [hmmPreset, setHmmPreset] = useState("preset_A");
  const [run, setRun] = useState<SelectionRun | null>(null);
  const [excluded, setExcluded] = useState<Record<string, unknown[]> | null>(null);
  const [watchlistCategoryName, setWatchlistCategoryName] = useState("");
  const [watchlistResult, setWatchlistResult] = useState<SelectionWatchlistImportResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const selectedPackages = useMemo(() => packages.filter((item) => selected[item.package_id]), [packages, selected]);
  const sourceRunIds = useMemo(() => runs.filter((item) => selectedRuns[item.run_id]).map((item) => item.run_id), [runs, selectedRuns]);
  const singlePackageMode = mode === "single_package";
  const selectedPackageName = selectedPackages[0]?.package_name || "策略包";

  const loadPackages = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [rows, runRows, configRows] = await Promise.all([
        selectionCenterApi.selectablePackages(300),
        selectionCenterApi.listRuns(200),
        hmmTrainingApi.configs(),
      ]);
      setPackages(rows);
      setRuns(runRows);
      setHmmConfigs(configRows);
      setWeights((prev) => Object.fromEntries(rows.map((item) => [item.package_id, prev[item.package_id] ?? 1])));
      setSelected((prev) => Object.fromEntries(rows.map((item, index) => [item.package_id, prev[item.package_id] ?? index === 0])));
      if (!hmmConfigId && configRows[0]) setHmmConfigId(configRows[0].config_id);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [hmmConfigId]);

  useEffect(() => { loadPackages(); }, [loadPackages]);

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
      if (alive) {
        setHmmSnapshots([]);
        setHmmSnapshotId("");
        setError(exc);
      }
    });
    return () => { alive = false; };
  }, [hmmConfigId, hmmSnapshotId]);

  function updateMode(nextMode: SelectionMode) {
    setMode(nextMode);
    if (nextMode !== "single_package") return;
    setSelected((prev) => {
      const firstSelected = packages.find((item) => prev[item.package_id]);
      const keep = firstSelected?.package_id || packages[0]?.package_id;
      return Object.fromEntries(packages.map((item) => [item.package_id, item.package_id === keep]));
    });
  }

  function updatePackageSelection(packageId: string, checked: boolean) {
    if (singlePackageMode && checked) {
      setSelected(Object.fromEntries(packages.map((item) => [item.package_id, item.package_id === packageId])));
      return;
    }
    setSelected((prev) => ({ ...prev, [packageId]: checked }));
  }

  function runtimeConfig(): JsonObject {
    const blacklist = industryBlacklist.split(",").map((item) => item.trim()).filter(Boolean);
    const runtimeProfile: JsonObject = {
      industry_blacklist: blacklist,
      tradability: { exclude_suspended: excludeSuspended },
      selection: { top_k: topK },
      hmm: {
        enabled: hmmEnabled,
        model_snapshot_id: hmmEnabled ? hmmSnapshotId : null,
        signal_preset: hmmEnabled ? hmmPreset : null,
      },
    };
    const config: JsonObject = {
      top_k: topK,
      selection_artifact_config: { auto_generate: true, inference_backend: "wsl" },
      runtime_profile: runtimeProfile,
    };
    if (mode === "weighted_fusion") {
      config.package_weights = Object.fromEntries(selectedPackages.map((item) => [item.package_id, weights[item.package_id] ?? 1]));
    }
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
      if (hmmEnabled && (!hmmConfigId || !hmmSnapshotId)) throw new Error("启用 HMM 时必须选择模型版本和已完成快照。");
      if (singlePackageMode && packageIds.length !== 1) throw new Error("单策略包模式必须且只能选择一个 StrategyPackage。");
      if (!singlePackageMode && packageIds.length < 2) throw new Error("多策略包聚合至少需要两个 StrategyPackage。");
      const next = await selectionCenterApi.runSelection({ package_ids: packageIds, trade_date: tradeDate, data_source: dataSource, mode, runtime_config: runtimeConfig() });
      await hydrateRun(next);
      setRuns(await selectionCenterApi.listRuns(200));
    } catch (exc) {
      setError(exc);
    } finally {
      setRunning(false);
    }
  }

  async function aggregateSelectedRuns() {
    setRunning(true);
    setError(null);
    setRun(null);
    setExcluded(null);
    setWatchlistResult(null);
    try {
      if (sourceRunIds.length < 2) throw new Error("请至少选择两个已完成的单策略包选股记录进行聚合。");
      if (mode === "single_package") throw new Error("聚合已有选股记录必须选择交集、并集或加权融合模式。");
      const next = await selectionCenterApi.aggregateRuns({ source_run_ids: sourceRunIds, mode, runtime_config: runtimeConfig() });
      await hydrateRun(next);
      setRuns(await selectionCenterApi.listRuns(200));
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

  const excludedFlat = Object.entries(excluded || {}).flatMap(([packageId, rows]) => rows.map((row) => ({ packageId, row: row as JsonObject })));
  const aggregateEnabled = !running && mode !== "single_package" && sourceRunIds.length >= 2;
  const resultRows = run?.aggregate_results || [];

  return (
    <main>
      <ErrorPanel error={error} title="选股操作失败" />
      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="选股控制" eyebrow="StrategyPackage 权威推理" action={<button className="pv2-button" onClick={loadPackages} disabled={loading} type="button">刷新策略包</button>}>
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>模式</label><select className="pv2-select" value={mode} onChange={(event) => updateMode(event.target.value as SelectionMode)}><option value="single_package">单策略包</option><option value="weighted_fusion">加权融合</option><option value="intersection">交集</option><option value="union">并集</option></select></div>
            <div className="pv2-field"><label>交易日期</label><input className="pv2-input" type="date" value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} /></div>
            <div className="pv2-field"><label>数据源</label><select className="pv2-select" value={dataSource} onChange={(event) => setDataSource(event.target.value as DataSource)}><option value="DB_HISTORICAL">DB_HISTORICAL 历史分钟回放</option><option value="TDX_REALTIME">TDX_REALTIME 实时行情</option></select></div>
            <div className="pv2-field"><label>TopK（默认 20，最高 50）</label><input className="pv2-input" type="number" min={1} max={50} value={topK} onChange={(event) => setTopK(Number(event.target.value))} /></div>
            <div className="pv2-field"><label>行业黑名单</label><input className="pv2-input" placeholder="银行, 房地产" value={industryBlacklist} onChange={(event) => setIndustryBlacklist(event.target.value)} /></div>
            <div className="pv2-field"><label>可交易性</label><label className="pv2-chip"><input type="checkbox" checked={excludeSuspended} onChange={(event) => setExcludeSuspended(event.target.checked)} /> 剔除已确认停牌股票并按后续排名补位</label></div>
          </div>
          <div className="pv2-card" style={{ marginTop: 14 }}>
            <div className="pv2-row-actions">
              <label className="pv2-chip"><input type="checkbox" checked={hmmEnabled} onChange={(event) => setHmmEnabled(event.target.checked)} /> 启用 HMM</label>
              <select className="pv2-select" value={hmmConfigId} disabled={!hmmEnabled} onChange={(event) => setHmmConfigId(event.target.value)} style={{ maxWidth: 260 }}>
                <option value="">选择模型版本</option>
                {hmmConfigs.map((item) => <option value={item.config_id} key={item.config_id}>{item.display_name} / {item.model_type}</option>)}
              </select>
              <select className="pv2-select" value={hmmSnapshotId} disabled={!hmmEnabled || !hmmConfigId} onChange={(event) => setHmmSnapshotId(event.target.value)} style={{ maxWidth: 280 }}>
                <option value="">选择已完成快照</option>
                {hmmSnapshots.map((item) => <option value={item.snapshot_id} key={item.snapshot_id}>{item.snapshot_id} / {item.trained_at}</option>)}
              </select>
              <select className="pv2-select" value={hmmPreset} disabled={!hmmEnabled} onChange={(event) => setHmmPreset(event.target.value)} style={{ maxWidth: 150 }}>
                <option value="preset_A">preset_A</option>
                <option value="preset_B">preset_B</option>
              </select>
            </div>
          </div>
          <button className="pv2-button-primary" disabled={running} onClick={runSelection} type="button">{running ? "运行中..." : "运行选股"}</button>
        </SectionCard>

        <SectionCard title="策略包选择器" eyebrow={`${selectedPackages.length} 个已选择`}>
          <PaperTable
            rows={packages}
            empty="暂无可选 StrategyPackage。请先启用策略包选股。"
            columns={[
              { key: "pick", header: "选择", render: (row) => <input type="checkbox" checked={Boolean(selected[row.package_id])} onChange={(event) => updatePackageSelection(row.package_id, event.target.checked)} /> },
              { key: "name", header: "策略包", render: (row) => <><strong>{row.package_name}</strong><br /><span className="pv2-muted pv2-mono">{shortHash(row.package_id, 7)}</span></> },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.package_status} /> },
              { key: "annual", header: "年化", render: (row) => formatPercent(row.metrics_summary?.annual_return) },
              { key: "ic", header: "IC", render: (row) => formatPercent(row.metrics_summary?.ic) },
              { key: "model", header: "模型", render: (row) => <StatusBadge status={String(row.model_state?.staleness_status || "unknown")} /> },
              { key: "weight", header: "权重", render: (row) => <input className="pv2-input" type="number" step="0.1" value={weights[row.package_id] ?? 1} disabled={mode !== "weighted_fusion"} onChange={(event) => setWeights((prev) => ({ ...prev, [row.package_id]: Number(event.target.value) }))} /> },
            ]}
          />
        </SectionCard>
      </div>

      {dataSource === "TDX_REALTIME" ? <NoticePanel title="实时数据源提示" tone="warning">当前权威 artifact 推理仍要求 DB_HISTORICAL；选择 TDX_REALTIME 时后端会明确失败，不会静默回退。</NoticePanel> : null}
      {mode !== "single_package" ? <NoticePanel title="多策略包边界" tone="warning">多策略包当前只用于统一选股研究；不能直接创建模拟盘执行组合。</NoticePanel> : null}

      <SectionCard title="选股结果" eyebrow={run ? `run_id ${shortHash(run.run_id)}` : "尚未运行"} action={<button className="pv2-button" onClick={addToWatchlist} disabled={!run || !resultRows.length} type="button">一键加入自选股票池</button>}>
        <div className="pv2-form-grid" style={{ marginBottom: 12 }}>
          <div className="pv2-field"><label>自选分类名称</label><input className="pv2-input" value={watchlistCategoryName} onChange={(event) => setWatchlistCategoryName(event.target.value)} placeholder="自动创建或复用同名分类" /></div>
        </div>
        <PaperTable
          rows={resultRows}
          empty="运行选股或点击历史记录后查看排序候选股。"
          columns={[
            { key: "rank", header: "排名", render: (row) => row.rank },
            { key: "symbol", header: "股票代码", render: (row) => row.symbol },
            { key: "score", header: "评分", render: (row) => row.score.toFixed(6) },
            { key: "price", header: "选股参考价", render: (row) => row.reference_price?.toFixed(3) || "-" },
            { key: "weight", header: "目标权重", render: (row) => formatPercent(row.target_weight) },
            { key: "reason", header: "原因", render: (row) => row.reason || "-" },
            { key: "trace", header: "追踪", render: (row) => row.component_scores ? <span className="pv2-mono">{Object.keys(row.component_scores).join(", ")}</span> : "-" },
          ]}
        />
        {watchlistResult ? <JsonPanel value={watchlistResult as unknown as JsonObject} /> : null}
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
          <JsonPanel value={{ tradeDate, dataSource, mode, topK, selected_package_ids: selectedPackages.map((item) => item.package_id), runtime_config: runtimeConfig() }} />
        </SectionCard>
      </div>

      <SectionCard title="历史选股记录与动态聚合" eyebrow="点击记录可显示结果">
        <div className="pv2-row-actions" style={{ marginBottom: 12 }}>
          <button className="pv2-button" onClick={loadPackages} disabled={loading} type="button">刷新记录</button>
          <button className="pv2-button-primary" onClick={aggregateSelectedRuns} disabled={!aggregateEnabled} type="button">聚合已选股票</button>
          <span className="pv2-muted">已选 {sourceRunIds.length} 条；聚合模式不能为单策略包。</span>
        </div>
        <PaperTable
          rows={runs}
          empty="暂无选股运行。"
          columns={[
            { key: "pick", header: "聚合", render: (row) => <input type="checkbox" checked={Boolean(selectedRuns[row.run_id])} onChange={(event) => setSelectedRuns((prev) => ({ ...prev, [row.run_id]: event.target.checked }))} /> },
            { key: "run", header: "运行记录", render: (row) => <button className="pv2-link-button" onClick={() => showHistoryRun(row.run_id)} type="button">{runLabel(row)}</button> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status || "unknown"} /> },
            { key: "source", header: "数据源", render: (row) => row.data_source },
            { key: "pkgs", header: "策略包", render: (row) => row.package_ids.map((item) => shortHash(item, 5)).join(", ") },
            { key: "count", header: "候选数", render: (row) => row.aggregate_results?.length || 0 },
          ]}
        />
      </SectionCard>
    </main>
  );
}
