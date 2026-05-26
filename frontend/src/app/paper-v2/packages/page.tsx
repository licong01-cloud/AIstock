"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ConfirmAction from "@/components/paper-v2/ConfirmAction";
import CopyChip from "@/components/paper-v2/CopyChip";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import WorkflowStepper from "@/components/paper-v2/WorkflowStepper";
import { strategyPackageApi } from "@/lib/paper-v2/api";
import { formatPercent, packageDisplayLabel, paperV2WorkflowSteps, shortHash } from "@/lib/paper-v2/format";
import type { ExecutionPolicy, JsonObject, QEPackagingSource, StrategyPackage } from "@/lib/paper-v2/types";

function sourceKey(source: QEPackagingSource): string {
  return `${source.source_kind}:${source.experiment_id}:${source.qe_task_id || ""}:${source.qe_loop_id || ""}`;
}

function metricText(source: QEPackagingSource): string {
  const m = source.metrics_summary || {};
  return `年化 ${formatPercent(m.annual_return)} / IC ${formatPercent(m.ic)} / 回撤 ${formatPercent(m.max_drawdown)}`;
}

function lifecycleText(status: string): string {
  const labels: Record<string, string> = {
    DRAFT: "草稿或资产尚未完成校验",
    ASSET_VALIDATED: "资产已校验",
    BACKTEST_APPROVED: "回测已批准，资产合格即可进入选股/模拟盘",
    RETIRED: "已退役，只保留历史审计，不再进入新流程",
  };
  return labels[status] || "未知状态，请查看状态事件";
}

function dependencyCount(deps: JsonObject | null): number {
  if (!deps) return 0;
  return Object.values(deps).reduce<number>((total, value) => {
    if (Array.isArray(value)) return total + value.length;
    if (typeof value === "number") return total + value;
    return total;
  }, 0);
}

export default function PaperV2PackagesPage() {
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [sources, setSources] = useState<QEPackagingSource[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [sourceKind, setSourceKind] = useState<"all" | "qe_experiment" | "qe_evolution_loop">("all");
  const [sourceKeyValue, setSourceKeyValue] = useState("");
  const [resolveRuntimeAssets, setResolveRuntimeAssets] = useState(true);
  const [policies, setPolicies] = useState<ExecutionPolicy[]>([]);
  const [events, setEvents] = useState<JsonObject[]>([]);
  const [modelState, setModelState] = useState<JsonObject | null>(null);
  const [deleteDependencies, setDeleteDependencies] = useState<JsonObject | null>(null);
  const [sourcePreview, setSourcePreview] = useState<JsonObject | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const selected = useMemo(
    () => packages.find((item) => item.package_id === selectedId) || packages[0],
    [packages, selectedId],
  );
  const selectedSource = useMemo(
    () => sources.find((item) => sourceKey(item) === sourceKeyValue) || sources[0],
    [sources, sourceKeyValue],
  );

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
      if (!sourceRows.some((item) => sourceKey(item) === sourceKeyValue)) {
        setSourceKeyValue(sourceRows[0] ? sourceKey(sourceRows[0]) : "");
      }
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [selectedId, sourceKind, sourceKeyValue]);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    if (!selected) return;
    let alive = true;
    async function loadDetail() {
      setError(null);
      try {
        const [policyRows, eventRows, state, deps] = await Promise.all([
          strategyPackageApi.executionPolicies(selected.package_id).catch(() => []),
          strategyPackageApi.statusEvents(selected.package_id).catch(() => []),
          strategyPackageApi.modelState(selected.package_id).catch(() => null),
          strategyPackageApi.deleteDependencies(selected.package_id).catch(() => null),
        ]);
        if (!alive) return;
        setPolicies(policyRows);
        setEvents(eventRows);
        setModelState(state);
        const depPayload = deps && typeof deps === "object" ? (deps.dependencies as JsonObject | undefined) : undefined;
        setDeleteDependencies(depPayload || null);
      } catch (exc) {
        if (alive) setError(exc);
      }
    }
    loadDetail();
    return () => { alive = false; };
  }, [selected]);

  async function retireSelected() {
    if (!selected) return;
    setBusy(true);
    setError(null);
    try {
      await strategyPackageApi.retire(selected.package_id);
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function deleteSelected() {
    if (!selected) return;
    setBusy(true);
    setError(null);
    try {
      await strategyPackageApi.deletePackage(selected.package_id);
      setSelectedId("");
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
      setError(new Error("请先选择一个 QE 来源。"));
      return;
    }
    setBusy(true);
    try {
      if (action === "preview") {
        setSourcePreview(await strategyPackageApi.qeExperimentManifest(selectedSource.experiment_id));
      } else if (action === "readiness") {
        setSourcePreview(await strategyPackageApi.qeExperimentPaperReadiness(selectedSource.experiment_id));
      } else {
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

  const selectedStatus = selected?.package_status || "";
  const assetEligibility = (selected?.asset_eligibility || {}) as JsonObject;
  const assetEligible = selectedStatus !== "RETIRED" && assetEligibility.eligible !== false;
  const usablePackages = packages.filter((item) => String(item.package_status || "").toUpperCase() !== "RETIRED").length;
  const depsCount = dependencyCount(deleteDependencies);
  const canDelete = Boolean(selected && depsCount === 0);
  const metrics = selected?.metrics_summary || {};

  const workflowSteps = paperV2WorkflowSteps({
    hasPackages: packages.length > 0,
    hasSelectionEnabledPackage: usablePackages > 0,
    hasPaperEnabledPackage: usablePackages > 0,
    hasSelectionRun: false,
    hasPortfolio: false,
    hasReadyRun: false,
  }, packages.length === 0 ? "packages" : "enable");

  return (
    <main>
      <WorkflowStepper steps={workflowSteps} compact />
      <ErrorPanel error={error} title="策略包操作失败" />

      <SectionCard title="从 QE 创建策略包" eyebrow="StrategyPackage 只能来自 QE" action={<button className="pv2-button" onClick={load} disabled={loading} type="button">刷新来源</button>}>
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
            <label>QE 来源</label>
            <select className="pv2-select" value={sourceKeyValue} onChange={(event) => setSourceKeyValue(event.target.value)}>
              {sources.map((item) => {
                const key = sourceKey(item);
                return <option value={key} key={key}>{item.display_name || `${item.experiment_name} | ${metricText(item)}`}</option>;
              })}
            </select>
          </div>
        </div>
        <label className="pv2-chip" style={{ marginTop: 12 }}>
          <input type="checkbox" checked={resolveRuntimeAssets} onChange={(event) => setResolveRuntimeAssets(event.target.checked)} />
          创建时解析并复制运行资产
        </label>
        <div className="pv2-row-actions" style={{ marginTop: 12 }}>
          <button className="pv2-button" onClick={() => sourceAction("preview")} disabled={busy || !selectedSource} type="button">预览 Manifest</button>
          <button className="pv2-button" onClick={() => sourceAction("readiness")} disabled={busy || !selectedSource} type="button">检查资产合格性</button>
          <button className="pv2-button-primary" onClick={() => sourceAction("create")} disabled={busy || !selectedSource} type="button">创建 StrategyPackage</button>
        </div>
        <div className="pv2-help">策略包入场只检查资产与 manifest 完整性；HMM、黑名单、TopK、停牌剔除、交易日和行情源都属于运行时平台能力。</div>
        {!sources.length ? <NoticePanel title="暂无可打包 QE 来源" tone="info">没有找到尚未打包的 QE 实验或演进 Loop。</NoticePanel> : null}
        {sourcePreview ? <JsonPanel value={sourcePreview} /> : null}
      </SectionCard>

      <SectionCard title="StrategyPackage 列表" eyebrow={loading ? "加载中" : `${packages.length} 个策略包`} action={<button className="pv2-button" onClick={load} type="button">刷新</button>}>
        <PaperTable
          rows={packages}
          empty="暂无 StrategyPackage。请先从 QE 创建策略包。"
          columns={[
            { key: "name", header: "名称", render: (row) => <button className="pv2-link-button" onClick={() => setSelectedId(row.package_id)} type="button">{row.package_name}</button> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.package_status} /> },
            { key: "eligibility", header: "资产合格", render: (row) => <StatusBadge status={((row.asset_eligibility || {}) as JsonObject).eligible === false ? "BLOCKED" : "ELIGIBLE"} /> },
            { key: "source", header: "来源", render: (row) => <span>{row.source_type}<br /><span className="pv2-muted pv2-mono">{shortHash(row.source_id, 8)}</span></span> },
            { key: "annual", header: "年化", render: (row) => formatPercent(row.metrics_summary?.annual_return) },
            { key: "ic", header: "IC", render: (row) => formatPercent(row.metrics_summary?.ic) },
            { key: "rank", header: "RankIC", render: (row) => formatPercent(row.metrics_summary?.rank_ic) },
            { key: "hash", header: "Manifest", render: (row) => <span className="pv2-mono">{shortHash(row.manifest_sha256)}</span> },
          ]}
        />
      </SectionCard>

      {selected ? (
        <div className="pv2-grid pv2-grid-main">
          <SectionCard title={selected.package_name} eyebrow="当前策略包">
            <div className="pv2-grid pv2-grid-3" style={{ marginBottom: 14 }}>
              <MetricCard label="生命周期状态" value={selectedStatus || "-"} hint={lifecycleText(selectedStatus)} tone={selectedStatus === "RETIRED" ? "danger" : assetEligible ? "success" : "warning"} />
              <MetricCard label="选股准入" value={assetEligible ? "可进入选股" : "不可进入"} hint="只看资产合格性；平台数据问题只影响本次 run。" tone={assetEligible ? "success" : "warning"} />
              <MetricCard label="模拟盘准入" value={assetEligible ? "可创建模拟盘" : "不可创建"} hint="不再需要旧的模拟盘启用状态或治理就绪状态。" tone={assetEligible ? "success" : "warning"} />
            </div>
            <div className="pv2-row-actions" style={{ marginBottom: 12 }}>
              <Link className={assetEligible ? "pv2-button-primary" : "pv2-button"} href={`/paper-v2/portfolios?package_id=${selected.package_id}`}>用此包创建模拟盘</Link>
              <ConfirmAction label="退役策略包" confirmText={selected.package_id} onConfirm={retireSelected} danger disabled={busy || selectedStatus === "RETIRED"} testId="strategy-package-retire" mode="dialog" />
              <ConfirmAction label="彻底删除策略包" confirmText={selected.package_id} onConfirm={deleteSelected} danger disabled={busy || !canDelete} testId="strategy-package-delete" mode="dialog" />
            </div>
            <NoticePanel title="退役与删除的区别" tone={depsCount ? "warning" : "info"}>
              退役只归档策略包，不删除历史组合和证据；彻底删除会物理删除没有任何运行时引用的策略包。当前删除依赖数量：{depsCount}。
            </NoticePanel>
            {deleteDependencies ? <JsonPanel value={deleteDependencies} /> : null}
            <div className="pv2-grid pv2-grid-3" style={{ marginTop: 14 }}>
              <MetricCard label="年化收益" value={formatPercent(metrics.annual_return)} />
              <MetricCard label="IC" value={formatPercent(metrics.ic)} />
              <MetricCard label="最大回撤" value={formatPercent(metrics.max_drawdown)} />
            </div>
            <div className="pv2-chip-row" style={{ marginTop: 14 }}>
              <span className="pv2-chip">{packageDisplayLabel(selected)}</span>
              <span className="pv2-chip">模拟盘数: {selected.paper_portfolio_count || 0}</span>
              <span className="pv2-chip">执行策略: {policies.length}</span>
              <CopyChip label={`package ${shortHash(selected.package_id, 6)}`} value={selected.package_id} title={`完整 package_id：${selected.package_id}`} />
              <CopyChip label={`manifest ${shortHash(selected.manifest_sha256, 6)}`} value={selected.manifest_sha256} title={`完整 manifest_sha256：${selected.manifest_sha256}`} />
            </div>
            <h3>模型状态</h3>
            {modelState ? <JsonPanel value={modelState} /> : <div className="pv2-muted">尚未获取模型状态。</div>}
          </SectionCard>

          <SectionCard title="执行策略与状态事件" eyebrow="运行时配置，不作为准入门禁">
            <PaperTable
              rows={policies}
              empty="暂无已验证执行策略；创建模拟盘时会尝试使用 manifest 默认策略并在运行前 fail-fast。"
              columns={[
                { key: "name", header: "名称", render: (row) => row.policy_name || row.policy_id },
                { key: "algo", header: "算法", render: (row) => row.algo_code || "-" },
                { key: "validation", header: "验证状态", render: (row) => <StatusBadge status={row.validation_status || "unknown"} /> },
                { key: "paper", header: "用途", render: () => <span className="pv2-muted">运行时可选，不作门禁</span> },
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
