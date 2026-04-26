"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { selectionCenterApi } from "@/lib/paper-v2/api";
import { formatPercent, shortHash, todayIso } from "@/lib/paper-v2/format";
import type { DataSource, JsonObject, SelectablePackage, SelectionMode, SelectionRun } from "@/lib/paper-v2/types";

export default function PaperV2SelectionPage() {
  const [packages, setPackages] = useState<SelectablePackage[]>([]);
  const [runs, setRuns] = useState<SelectionRun[]>([]);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [selectedRuns, setSelectedRuns] = useState<Record<string, boolean>>({});
  const [weights, setWeights] = useState<Record<string, number>>({});
  const [mode, setMode] = useState<SelectionMode>("single_package");
  const [tradeDate, setTradeDate] = useState(todayIso());
  const [dataSource, setDataSource] = useState<DataSource>("DB_HISTORICAL");
  const [topK, setTopK] = useState(50);
  const [industryBlacklist, setIndustryBlacklist] = useState("");
  const [excludeSuspended, setExcludeSuspended] = useState(true);
  const [hmmEnabled, setHmmEnabled] = useState(false);
  const [hmmSnapshotId, setHmmSnapshotId] = useState("");
  const [hmmPreset, setHmmPreset] = useState("preset_A");
  const [run, setRun] = useState<SelectionRun | null>(null);
  const [excluded, setExcluded] = useState<Record<string, unknown[]> | null>(null);
  const [portfolioName, setPortfolioName] = useState("模拟盘 v2 选股组合");
  const [initialCash, setInitialCash] = useState(1000000);
  const [startDate, setStartDate] = useState(todayIso());
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const selectedPackages = useMemo(() => packages.filter((item) => selected[item.package_id]), [packages, selected]);
  const singlePackageMode = mode === "single_package";

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

  const loadPackages = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [rows, runRows] = await Promise.all([
        selectionCenterApi.selectablePackages(300),
        selectionCenterApi.listRuns(200),
      ]);
      setPackages(rows);
      setRuns(runRows);
      setWeights((prev) => Object.fromEntries(rows.map((item) => [item.package_id, prev[item.package_id] ?? 1])));
      setSelected((prev) => Object.fromEntries(rows.map((item, index) => [item.package_id, prev[item.package_id] ?? index === 0])));
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadPackages(); }, [loadPackages]);

  function runtimeConfig(): JsonObject {
    const blacklist = industryBlacklist.split(",").map((item) => item.trim()).filter(Boolean);
    const runtimeProfile: JsonObject = {
      industry_blacklist: blacklist,
      tradability: { exclude_suspended: excludeSuspended },
      selection: { top_k: topK },
      hmm: { enabled: hmmEnabled, model_snapshot_id: hmmSnapshotId || null, signal_preset: hmmPreset || null },
    };
    const config: JsonObject = { top_k: topK, runtime_profile: runtimeProfile };
    if (mode === "weighted_fusion") {
      config.package_weights = Object.fromEntries(selectedPackages.map((item) => [item.package_id, weights[item.package_id] ?? 1]));
    }
    return config;
  }

  async function runSelection() {
    setRunning(true);
    setError(null);
    setRun(null);
    setExcluded(null);
    try {
      const packageIds = selectedPackages.map((item) => item.package_id);
      if (singlePackageMode && packageIds.length !== 1) throw new Error("单策略包模式必须且只能选择一个 StrategyPackage。");
      if (!singlePackageMode && packageIds.length < 2) throw new Error("多策略包聚合至少需要两个 StrategyPackage。");
      const next = await selectionCenterApi.runSelection({ package_ids: packageIds, trade_date: tradeDate, data_source: dataSource, mode, runtime_config: runtimeConfig() });
      setRun(next);
      const excludedRows = await selectionCenterApi.excludedResults(next.run_id).catch(() => null);
      setExcluded(excludedRows);
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
    try {
      const sourceRunIds = runs.filter((item) => selectedRuns[item.run_id]).map((item) => item.run_id);
      if (sourceRunIds.length < 2) throw new Error("请至少选择两个已完成的选股运行进行聚合。");
      if (mode === "single_package") throw new Error("已有运行聚合必须使用交集、并集或加权融合模式。");
      const next = await selectionCenterApi.aggregateRuns({ source_run_ids: sourceRunIds, mode, runtime_config: runtimeConfig() });
      setRun(next);
      const excludedRows = await selectionCenterApi.excludedResults(next.run_id).catch(() => null);
      setExcluded(excludedRows);
      await loadPackages();
    } catch (exc) {
      setError(exc);
    } finally {
      setRunning(false);
    }
  }

  async function createPaperPortfolio() {
    if (!run) return;
    setError(null);
    try {
      await selectionCenterApi.createPaperPortfolio(run.run_id, {
        portfolio_name: portfolioName,
        initial_cash: initialCash,
        start_date: startDate,
        data_source: dataSource,
      });
      alert("模拟盘 v2 组合已创建。请打开组合中心继续就绪检查和单日运行。");
    } catch (exc) {
      setError(exc);
    }
  }

  const excludedFlat = Object.entries(excluded || {}).flatMap(([packageId, rows]) => rows.map((row) => ({ packageId, row: row as JsonObject })));
  const canCreatePaper = Boolean(run && run.package_ids.length === 1 && run.mode === "single_package");

  return (
    <main>
      <ErrorPanel error={error} title="选股操作失败" />
      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="选股控制" eyebrow="策略包运行时" action={<button className="pv2-button" onClick={loadPackages} disabled={loading} type="button">刷新策略包</button>}>
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>模式</label><select className="pv2-select" value={mode} onChange={(event) => updateMode(event.target.value as SelectionMode)}><option value="single_package">单策略包</option><option value="weighted_fusion">加权融合</option><option value="intersection">交集</option><option value="union">并集</option></select></div>
            <div className="pv2-field"><label>交易日期</label><input className="pv2-input" type="date" value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} /></div>
            <div className="pv2-field"><label>数据源</label><select className="pv2-select" value={dataSource} onChange={(event) => setDataSource(event.target.value as DataSource)}><option value="DB_HISTORICAL">DB_HISTORICAL</option><option value="TDX_REALTIME">TDX_REALTIME</option></select></div>
            <div className="pv2-field"><label>Top K</label><input className="pv2-input" type="number" min={1} max={500} value={topK} onChange={(event) => setTopK(Number(event.target.value))} /></div>
            <div className="pv2-field"><label>行业黑名单</label><input className="pv2-input" placeholder="银行, 房地产" value={industryBlacklist} onChange={(event) => setIndustryBlacklist(event.target.value)} /></div>
            <div className="pv2-field"><label>可交易性</label><label className="pv2-chip"><input type="checkbox" checked={excludeSuspended} onChange={(event) => setExcludeSuspended(event.target.checked)} /> 剔除已确认停牌</label></div>
          </div>
          <div className="pv2-card" style={{ marginTop: 14 }}>
            <div className="pv2-row-actions">
              <label className="pv2-chip"><input type="checkbox" checked={hmmEnabled} onChange={(event) => setHmmEnabled(event.target.checked)} /> 启用 HMM</label>
              <input className="pv2-input" style={{ maxWidth: 260 }} placeholder="model_snapshot_id" value={hmmSnapshotId} onChange={(event) => setHmmSnapshotId(event.target.value)} />
              <input className="pv2-input" style={{ maxWidth: 160 }} placeholder="preset_A" value={hmmPreset} onChange={(event) => setHmmPreset(event.target.value)} />
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
              { key: "ic", header: "IC", render: (row) => formatPercent(row.metrics_summary?.ic) },
              { key: "model", header: "模型", render: (row) => <StatusBadge status={String(row.model_state?.staleness_status || "unknown")} /> },
              { key: "weight", header: "权重", render: (row) => <input className="pv2-input" type="number" step="0.1" value={weights[row.package_id] ?? 1} disabled={mode !== "weighted_fusion"} onChange={(event) => setWeights((prev) => ({ ...prev, [row.package_id]: Number(event.target.value) }))} /> },
            ]}
          />
        </SectionCard>
      </div>

      {mode !== "single_package" ? (
        <NoticePanel title="模拟盘执行边界" tone="warning">
          多策略包聚合目前仅支持研究选股。只有升级为组合 StrategyPackage 或未来 SelectionBundle 合约后，才能创建模拟盘 v2 执行组合。
        </NoticePanel>
      ) : null}

      <SectionCard title="选股结果" eyebrow={run ? `run_id ${shortHash(run.run_id)}` : "尚未运行"}>
        <PaperTable
          rows={run?.aggregate_results || []}
          empty="运行选股后查看排序候选股。"
          columns={[
            { key: "rank", header: "排名", render: (row) => row.rank },
            { key: "symbol", header: "股票代码", render: (row) => row.symbol },
            { key: "score", header: "评分", render: (row) => row.score.toFixed(6) },
            { key: "weight", header: "目标权重", render: (row) => formatPercent(row.target_weight) },
            { key: "reason", header: "原因", render: (row) => row.reason || "-" },
            { key: "trace", header: "追踪", render: (row) => row.component_scores ? <span className="pv2-mono">{Object.keys(row.component_scores).join(", ")}</span> : "-" },
          ]}
        />
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="剔除与补位追踪" eyebrow="Fail-fast 证据">
          <PaperTable
            rows={excludedFlat}
            empty="最近一次运行没有记录剔除结果。"
            columns={[
              { key: "pkg", header: "策略包", render: (item) => shortHash(item.packageId, 7) },
              { key: "symbol", header: "股票代码", render: (item) => String(item.row.symbol || "-") },
              { key: "reason", header: "原因", render: (item) => <StatusBadge status={String(item.row.reason || "excluded")} /> },
              { key: "ctx", header: "上下文", render: (item) => <span className="pv2-mono">{Object.keys(item.row.context as JsonObject || {}).join(", ") || "-"}</span> },
            ]}
          />
        </SectionCard>

        <SectionCard title="创建模拟盘 v2 组合" eyebrow="仅支持单策略包">
          {!canCreatePaper ? <div className="pv2-muted">请先完成一次成功的单策略包选股。多策略包聚合运行不能创建模拟盘 v2 组合。</div> : null}
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>名称</label><input className="pv2-input" value={portfolioName} onChange={(event) => setPortfolioName(event.target.value)} /></div>
            <div className="pv2-field"><label>初始资金</label><input className="pv2-input" type="number" value={initialCash} onChange={(event) => setInitialCash(Number(event.target.value))} /></div>
            <div className="pv2-field"><label>开始日期</label><input className="pv2-input" type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} /></div>
          </div>
          <button className="pv2-button-primary" disabled={!canCreatePaper} onClick={createPaperPortfolio} type="button" style={{ marginTop: 12 }}>从选股运行创建</button>
          {run ? <JsonPanel value={{ run_id: run.run_id, package_ids: run.package_ids, manifest_sha256_by_package: run.manifest_sha256_by_package }} /> : null}
        </SectionCard>
      </div>

      <SectionCard title="聚合已有选股运行" eyebrow="动态多策略包研究">
        <NoticePanel title="已有运行聚合规则" tone="info">
          选择交易日期和数据源相同的已完成单策略包运行，再选择并集/交集/加权融合进行聚合；聚合不会冻结新的交易合约。后端会拒绝不兼容运行。
        </NoticePanel>
        <div className="pv2-row-actions" style={{ marginBottom: 12 }}>
          <button className="pv2-button" onClick={loadPackages} disabled={loading} type="button">刷新运行</button>
          <button className="pv2-button-primary" onClick={aggregateSelectedRuns} disabled={running || mode === "single_package"} type="button">聚合已选运行</button>
        </div>
        <PaperTable
          rows={runs}
          empty="暂无选股运行。"
          columns={[
            { key: "pick", header: "选择", render: (row) => <input type="checkbox" checked={Boolean(selectedRuns[row.run_id])} onChange={(event) => setSelectedRuns((prev) => ({ ...prev, [row.run_id]: event.target.checked }))} /> },
            { key: "run", header: "运行", render: (row) => <span className="pv2-mono">{shortHash(row.run_id)}</span> },
            { key: "mode", header: "模式", render: (row) => <StatusBadge status={row.mode} /> },
            { key: "date", header: "日期", render: (row) => row.trade_date },
            { key: "source", header: "数据源", render: (row) => row.data_source },
            { key: "pkgs", header: "策略包", render: (row) => row.package_ids.map((item) => shortHash(item, 5)).join(", ") },
            { key: "count", header: "候选数", render: (row) => row.aggregate_results?.length || 0 },
          ]}
        />
      </SectionCard>
    </main>
  );
}
