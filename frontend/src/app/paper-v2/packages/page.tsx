"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ConfirmAction from "@/components/paper-v2/ConfirmAction";
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

const SELECTION_RUNNABLE_STATUSES = new Set(["BACKTEST_APPROVED", "SELECTION_ENABLED", "PAPER_ENABLED"]);
const SELECTION_MARKABLE_STATUSES = new Set(["BACKTEST_APPROVED"]);
const PAPER_MARKABLE_STATUSES = new Set(["BACKTEST_APPROVED", "SELECTION_ENABLED"]);

function packageLifecycleText(status: string): string {
  const labels: Record<string, string> = {
    DRAFT: "草稿，尚未完成资产校验",
    ASSET_VALIDATED: "资产已校验，等待回测批准",
    BACKTEST_APPROVED: "回测已批准，可进入选股/模拟盘准入",
    SELECTION_ENABLED: "已标记可用于选股",
    PAPER_ENABLED: "已标记可用于模拟盘",
    PAPER_RUNNING: "已有模拟盘运行历史，谨慎新建",
    PAPER_PASSED: "模拟盘验证通过历史状态",
    PAPER_FAILED: "模拟盘验证失败历史状态",
    RETIRED: "已退役，仅保留审计",
  };
  return labels[status] || "未知状态，请查看状态事件";
}

function selectionCapability(status: string): { ok: boolean; title: string; detail: string } {
  if (status === "RETIRED") {
    return { ok: false, title: "不可选股", detail: "策略包已退役，不应再进入新的选股流程。" };
  }
  if (status === "SELECTION_ENABLED" || status === "PAPER_ENABLED") {
    return { ok: true, title: "可以选股", detail: "已完成选股准入标记，可进入 Selection Center。" };
  }
  if (status === "BACKTEST_APPROVED") {
    return { ok: true, title: "可以选股", detail: "回测已批准；建议先点击“标记可用于选股”留下状态事件。" };
  }
  return { ok: false, title: "不可选股", detail: "需要至少达到 BACKTEST_APPROVED 状态。" };
}

function paperCapability(
  status: string,
  paperReadyPolicyCount: number,
  policyCount: number,
): { ok: boolean; title: string; detail: string } {
  if (status === "RETIRED") {
    return { ok: false, title: "不可新建模拟盘", detail: "策略包已退役，仅保留历史组合和审计记录。" };
  }
  if (status === "PAPER_ENABLED") {
    const policyText = paperReadyPolicyCount > 0
      ? `已有 ${paperReadyPolicyCount} 个可用于模拟盘的已验证执行策略。`
      : policyCount > 0
        ? "已有执行策略，但尚未启用用于模拟盘；创建时仍会 fail-fast 校验。"
        : "尚未列出执行策略；创建时会尝试导入并校验 manifest 默认策略。";
    return { ok: true, title: "可以创建模拟组合", detail: policyText };
  }
  if (PAPER_MARKABLE_STATUSES.has(status)) {
    return { ok: false, title: "未完成模拟盘准入", detail: "先点击“标记可用于模拟盘”，再创建具体模拟组合。" };
  }
  return { ok: false, title: "不可新建模拟盘", detail: "需要至少达到 BACKTEST_APPROVED，并完成模拟盘准入。" };
}

export default function PaperV2PackagesPage() {
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [sources, setSources] = useState<QEPackagingSource[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [sourceKind, setSourceKind] = useState<"all" | "qe_experiment" | "qe_evolution_loop">("all");
  const [sourceKey, setSourceKey] = useState("");
  const [resolveRuntimeAssets, setResolveRuntimeAssets] = useState(true);
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
  const selectedStatus = selected?.package_status || "";
  const paperReadyPolicies = policies.filter((item) => item.paper_enabled);
  const canMarkSelection = SELECTION_MARKABLE_STATUSES.has(selectedStatus);
  const canMarkPaper = PAPER_MARKABLE_STATUSES.has(selectedStatus);
  const canCreatePortfolio = selectedStatus === "PAPER_ENABLED";
  const canRetirePackage = Boolean(selected && selectedStatus !== "RETIRED");
  const selectionState = selectionCapability(selectedStatus);
  const paperState = paperCapability(selectedStatus, paperReadyPolicies.length, policies.length);

  return (
    <main>
      <ErrorPanel error={error} title="策略包操作失败" />
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
          创建时解析并复制运行时资产（V24/V25 等模型型执行策略需要）
        </label>
        <div className="pv2-row-actions" style={{ marginTop: 12 }}>
          <button className="pv2-button" onClick={() => sourceAction("preview")} disabled={busy || !selectedSource} type="button">预览 Manifest</button>
          <button className="pv2-button" onClick={() => sourceAction("readiness")} disabled={busy || !selectedSource} type="button">验证模拟盘就绪</button>
          <button className="pv2-button-primary" onClick={() => sourceAction("create")} disabled={busy || !selectedSource} type="button">创建 StrategyPackage</button>
        </div>
        <div className="pv2-help">
          晋级规则：仅 QE 单次实验或 QE 演进 Loop 可创建 StrategyPackage；Manifest JSON 与 manifest_sha256 创建后冻结，状态流转不进入 hash；HMM、行业黑名单、TopK、停牌剔除属于运行时配置；启用模拟盘会校验分钟线执行策略。
        </div>
        {!sources.length ? <NoticePanel title="暂无可打包 QE 来源" tone="info">符合条件且尚未加入策略包的 QE 单次实验或演进 Loop 为空。</NoticePanel> : null}
        {sourcePreview ? <JsonPanel value={sourcePreview} /> : null}
      </SectionCard>

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
          >
            <div className="pv2-grid pv2-grid-3" style={{ marginBottom: 14 }}>
              <MetricCard
                label="生命周期状态"
                value={selectedStatus || "-"}
                hint={packageLifecycleText(selectedStatus)}
                tone={selectedStatus === "RETIRED" ? "danger" : canCreatePortfolio ? "success" : SELECTION_RUNNABLE_STATUSES.has(selectedStatus) ? "info" : "warning"}
              />
              <MetricCard
                label="选股能力"
                value={selectionState.title}
                hint={selectionState.detail}
                tone={selectionState.ok ? "success" : "warning"}
              />
              <MetricCard
                label="模拟盘能力"
                value={paperState.title}
                hint={paperState.detail}
                tone={paperState.ok ? "success" : selectedStatus === "RETIRED" ? "danger" : "warning"}
              />
            </div>
            <div className="pv2-card" style={{ marginBottom: 14, padding: 14 }}>
              <div className="pv2-eyebrow">下一步操作</div>
              <div className="pv2-row-actions" style={{ marginTop: 10 }}>
                <button className="pv2-button" onClick={() => transition("enableSelection")} disabled={busy || !canMarkSelection} type="button">标记可用于选股</button>
                <button className="pv2-button" onClick={() => transition("enablePaper")} disabled={busy || !canMarkPaper} type="button">标记可用于模拟盘</button>
                {canCreatePortfolio ? (
                  <Link className="pv2-button-primary" href={`/paper-v2/portfolios?package_id=${selected.package_id}`}>用此包创建模拟组合</Link>
                ) : (
                  <button className="pv2-button-primary" disabled type="button">用此包创建模拟组合</button>
                )}
                <ConfirmAction
                  label="退役策略包"
                  confirmText={selected.package_id}
                  onConfirm={() => transition("retire")}
                  danger
                  disabled={busy || !canRetirePackage}
                  testId="strategy-package-retire"
                />
              </div>
              <div className="pv2-help">
                “标记”只推进策略包准入状态；“创建模拟组合”会进入组合创建页并冻结资金、日期、数据源和执行策略。退役不会删除历史，只会阻止新的准入使用。
              </div>
            </div>
            <div className="pv2-grid pv2-grid-3">
              <MetricCard label="年化收益" value={formatPercent(metrics.annual_return)} />
              <MetricCard label="IC" value={formatPercent(metrics.ic)} />
              <MetricCard label="最大回撤" value={formatPercent(metrics.max_drawdown)} />
            </div>
            <div className="pv2-chip-row" style={{ marginTop: 14 }}>
              <span className="pv2-chip">package_id: {shortHash(selected.package_id)}</span>
              <span className="pv2-chip">manifest: {shortHash(selected.manifest_sha256)}</span>
              <span className="pv2-chip">模拟组合数: {selected.paper_portfolio_count || 0}</span>
              <span className="pv2-chip">执行策略: {policies.length} 个 / 可模拟盘 {paperReadyPolicies.length} 个</span>
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
