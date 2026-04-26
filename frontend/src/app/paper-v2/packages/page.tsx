"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { strategyPackageApi } from "@/lib/paper-v2/api";
import { formatPercent, shortHash } from "@/lib/paper-v2/format";
import type { ExecutionPolicy, JsonObject, QEPackagingSource, StrategyPackage } from "@/lib/paper-v2/types";

function metricText(source: QEPackagingSource): string {
  const m = source.metrics_summary || {};
  return `年化 ${formatPercent(m.annual_return)} / IC ${formatPercent(m.ic)} / 回撤 ${formatPercent(m.max_drawdown)}`;
}

export default function PaperV2PackagesPage() {
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [sources, setSources] = useState<QEPackagingSource[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [sourceKind, setSourceKind] = useState<"all" | "qe_experiment" | "qe_evolution_loop">("all");
  const [sourceKey, setSourceKey] = useState("");
  const [resolveRuntimeAssets, setResolveRuntimeAssets] = useState(false);
  const [policies, setPolicies] = useState<ExecutionPolicy[]>([]);
  const [events, setEvents] = useState<JsonObject[]>([]);
  const [modelState, setModelState] = useState<JsonObject | null>(null);
  const [sourcePreview, setSourcePreview] = useState<JsonObject | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const selected = packages.find((item) => item.package_id === selectedId) || packages[0];
  const selectedSource = useMemo(() => sources.find((item) => `${item.source_kind}:${item.experiment_id}:${item.qe_task_id || ""}:${item.qe_loop_id || ""}` === sourceKey) || sources[0], [sources, sourceKey]);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [packageRows, sourceRows] = await Promise.all([
        strategyPackageApi.list(undefined, 300),
        strategyPackageApi.qeSources(sourceKind, 300),
      ]);
      setPackages(packageRows);
      setSources(sourceRows);
      if (!selectedId && packageRows[0]) setSelectedId(packageRows[0].package_id);
      if (!sourceRows.find((item) => `${item.source_kind}:${item.experiment_id}:${item.qe_task_id || ""}:${item.qe_loop_id || ""}` === sourceKey)) {
        const first = sourceRows[0];
        setSourceKey(first ? `${first.source_kind}:${first.experiment_id}:${first.qe_task_id || ""}:${first.qe_loop_id || ""}` : "");
      }
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [selectedId, sourceKind, sourceKey]);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    if (!selected) return;
    let alive = true;
    async function loadDetail() {
      setError(null);
      try {
        const [policyRows, eventRows, state] = await Promise.all([
          strategyPackageApi.executionPolicies(selected.package_id).catch(() => []),
          strategyPackageApi.statusEvents(selected.package_id).catch(() => []),
          strategyPackageApi.modelState(selected.package_id).catch(() => null),
        ]);
        if (alive) {
          setPolicies(policyRows);
          setEvents(eventRows);
          setModelState(state);
        }
      } catch (exc) {
        if (alive) setError(exc);
      }
    }
    loadDetail();
    return () => { alive = false; };
  }, [selected]);

  async function transition(action: "enableSelection" | "enablePaper" | "retire") {
    if (!selected) return;
    setError(null);
    setBusy(true);
    try {
      if (action === "enableSelection") await strategyPackageApi.enableSelection(selected.package_id);
      if (action === "enablePaper") await strategyPackageApi.enablePaper(selected.package_id);
      if (action === "retire") await strategyPackageApi.retire(selected.package_id);
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function sourceAction(action: "preview" | "readiness" | "create") {
    setError(null);
    setSourcePreview(null);
    if (!selectedSource) {
      setError(new Error("请先选择一个未打包的 QE 来源。"));
      return;
    }
    setBusy(true);
    try {
      if (action === "preview") {
        setSourcePreview(await strategyPackageApi.qeExperimentManifest(selectedSource.experiment_id));
      }
      if (action === "readiness") {
        setSourcePreview(await strategyPackageApi.qeExperimentPaperReadiness(selectedSource.experiment_id));
      }
      if (action === "create") {
        const created = selectedSource.source_kind === "qe_evolution_loop"
          ? await strategyPackageApi.createFromQEEvolutionLoop({
              qe_task_id: selectedSource.qe_task_id || "",
              qe_loop_id: selectedSource.qe_loop_id || "",
              resolve_runtime_assets: resolveRuntimeAssets,
            })
          : await strategyPackageApi.createFromQEExperiment({
              experiment_id: selectedSource.experiment_id,
              resolve_runtime_assets: resolveRuntimeAssets,
            });
        setSelectedId(created.package_id);
        setSourcePreview({
          created_package_id: created.package_id,
          package_name: created.package_name,
          manifest_sha256: created.manifest_sha256,
          package_status: created.package_status,
        });
        await load();
      }
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  const metrics = selected?.metrics_summary || {};

  return (
    <main>
      <ErrorPanel error={error} title="策略包操作失败" />
      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="从 QE 创建策略包" eyebrow="只显示未打包来源" action={<button className="pv2-button" onClick={load} disabled={loading} type="button">刷新来源</button>}>
          <div className="pv2-form-grid">
            <div className="pv2-field">
              <label>来源类型</label>
              <select className="pv2-select" value={sourceKind} onChange={(event) => setSourceKind(event.target.value as typeof sourceKind)}>
                <option value="all">全部来源</option>
                <option value="qe_experiment">QE 单次实验</option>
                <option value="qe_evolution_loop">QE 演进 Loop</option>
              </select>
            </div>
            <div className="pv2-field pv2-field-wide">
              <label>QE 来源（名称后显示年化、IC、最大回撤）</label>
              <select className="pv2-select" value={sourceKey} onChange={(event) => setSourceKey(event.target.value)}>
                {sources.map((item) => {
                  const key = `${item.source_kind}:${item.experiment_id}:${item.qe_task_id || ""}:${item.qe_loop_id || ""}`;
                  return <option value={key} key={key}>{item.display_name || `${item.experiment_name} | ${metricText(item)}`}</option>;
                })}
              </select>
            </div>
          </div>
          <label className="pv2-chip" style={{ marginTop: 12 }}>
            <input type="checkbox" checked={resolveRuntimeAssets} onChange={(event) => setResolveRuntimeAssets(event.target.checked)} />
            创建时解析并复制运行时资产
          </label>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" onClick={() => sourceAction("preview")} disabled={busy || !selectedSource} type="button">预览 Manifest</button>
            <button className="pv2-button" onClick={() => sourceAction("readiness")} disabled={busy || !selectedSource} type="button">验证模拟盘就绪</button>
            <button className="pv2-button-primary" onClick={() => sourceAction("create")} disabled={busy || !selectedSource} type="button">创建 StrategyPackage</button>
          </div>
          {!sources.length ? <NoticePanel title="暂无可打包 QE 来源" tone="info">符合条件且尚未加入策略包的 QE 单次实验或演进 Loop 为空。</NoticePanel> : null}
          {sourcePreview ? <JsonPanel value={sourcePreview} /> : null}
        </SectionCard>

        <SectionCard title="策略包晋级规则" eyebrow="冻结 Manifest">
          <ul>
            <li>只允许 QE 单次实验或 QE 演进 Loop 创建 StrategyPackage。</li>
            <li>manifest JSON 与 manifest_sha256 创建后冻结；状态流转不进入 hash。</li>
            <li>HMM、行业黑名单、TopK、停牌剔除是运行时配置，不锁定在策略包中。</li>
            <li>启用模拟盘会校验分钟线执行策略，不能使用未回测验证的模拟盘独有配置。</li>
          </ul>
        </SectionCard>
      </div>

      <SectionCard title="StrategyPackage 策略包中心" eyebrow={loading ? "加载中" : `${packages.length} 个策略包`} action={<button className="pv2-button" onClick={load} type="button">刷新</button>}>
        <PaperTable
          rows={packages}
          empty="暂无 StrategyPackage。请先从 QE 创建策略包。"
          columns={[
            { key: "name", header: "名称", render: (row) => <button className="pv2-link-button" onClick={() => setSelectedId(row.package_id)} type="button">{row.package_name}</button> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.package_status} /> },
            { key: "source", header: "来源", render: (row) => <span>{row.source_type}<br /><span className="pv2-muted pv2-mono">{shortHash(row.source_id, 8)}</span></span> },
            { key: "annual", header: "年化", render: (row) => formatPercent(row.metrics_summary?.annual_return) },
            { key: "ic", header: "IC", render: (row) => formatPercent(row.metrics_summary?.ic) },
            { key: "rank", header: "RankIC", render: (row) => formatPercent(row.metrics_summary?.rank_ic) },
            { key: "sharpe", header: "夏普", render: (row) => row.metrics_summary?.sharpe?.toFixed(2) || "-" },
            { key: "mdd", header: "最大回撤", render: (row) => formatPercent(row.metrics_summary?.max_drawdown) },
            { key: "hash", header: "Manifest", render: (row) => <span className="pv2-mono">{shortHash(row.manifest_sha256)}</span> },
          ]}
        />
      </SectionCard>

      {selected ? (
        <div className="pv2-grid pv2-grid-main">
          <SectionCard
            title={selected.package_name}
            eyebrow="当前策略包"
            action={
              <div className="pv2-row-actions">
                <button className="pv2-button" onClick={() => transition("enableSelection")} disabled={busy} type="button">启用选股</button>
                <button className="pv2-button" onClick={() => transition("enablePaper")} disabled={busy} type="button">启用模拟盘</button>
                <Link className="pv2-button-primary" href={`/paper-v2/portfolios?package_id=${selected.package_id}`}>从此包启动模拟盘</Link>
                <button className="pv2-button-danger" onClick={() => transition("retire")} disabled={busy} type="button">退役</button>
              </div>
            }
          >
            <div className="pv2-grid pv2-grid-3">
              <MetricCard label="年化收益" value={formatPercent(metrics.annual_return)} />
              <MetricCard label="IC" value={formatPercent(metrics.ic)} />
              <MetricCard label="最大回撤" value={formatPercent(metrics.max_drawdown)} />
            </div>
            <div className="pv2-chip-row" style={{ marginTop: 14 }}>
              <span className="pv2-chip">package_id: {shortHash(selected.package_id)}</span>
              <span className="pv2-chip">manifest: {shortHash(selected.manifest_sha256)}</span>
              <span className="pv2-chip">模拟组合数: {selected.paper_portfolio_count || 0}</span>
            </div>
            <h3>模型状态</h3>
            {modelState ? <JsonPanel value={modelState} /> : <div className="pv2-muted">尚未获取模型状态。</div>}
          </SectionCard>

          <SectionCard title="执行策略与状态事件" eyebrow="仅允许回测验证策略">
            <PaperTable
              rows={policies}
              empty="暂无已验证执行策略；创建模拟盘时会尝试使用 manifest 默认策略并进行校验。"
              columns={[
                { key: "name", header: "名称", render: (row) => row.policy_name || row.policy_id },
                { key: "algo", header: "算法", render: (row) => row.algo_code || "-" },
                { key: "validation", header: "验证状态", render: (row) => <StatusBadge status={row.validation_status || "unknown"} /> },
                { key: "paper", header: "模拟盘", render: (row) => <StatusBadge status={row.paper_enabled ? "READY" : "DISABLED"} /> },
                { key: "hash", header: "策略 Hash", render: (row) => <span className="pv2-mono">{shortHash(row.policy_sha256)}</span> },
              ]}
            />
            <h3>状态事件</h3>
            <PaperTable
              rows={events}
              empty="暂无状态事件。"
              columns={[
                { key: "status", header: "状态", render: (row) => <StatusBadge status={String(row.to_status || row.status || "event")} /> },
                { key: "reason", header: "原因", render: (row) => String(row.reason || "-") },
                { key: "at", header: "时间", render: (row) => String(row.created_at || row.transitioned_at || "-") },
              ]}
            />
          </SectionCard>
        </div>
      ) : null}
    </main>
  );
}
