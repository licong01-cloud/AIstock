"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ConfirmAction from "@/components/paper-v2/ConfirmAction";
import CopyChip from "@/components/paper-v2/CopyChip";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import WorkflowStepper from "@/components/paper-v2/WorkflowStepper";
import { strategyPackageApi } from "@/lib/paper-v2/api";
import { formatPercent, packageDisplayLabel, paperV2WorkflowSteps, shortHash } from "@/lib/paper-v2/format";
import type {
  ExecutionPolicy,
  JsonObject,
  MultiAlphaCombineRun,
  MultiAlphaCombineRunDetail,
  MultiAlphaCombineSchemeResult,
  QEPackagingSource,
  StrategyPackage,
  StrategyPackagePromotionResult,
} from "@/lib/paper-v2/types";

type PackagingSourceKind = "all" | "qe_experiment" | "qe_evolution_loop" | "multi_alpha_combine_run";

const MULTI_ALPHA_PROMOTE_CONFIRMATION = "MULTI_ALPHA_PACKAGE_PROMOTE";
const MULTI_ALPHA_SUPPORTED_SCHEME = "ic_weighted";
const MULTI_ALPHA_WEIGHT_POLICY = { mode: "frozen_backtest_terminal_weights" };

function sourceKey(source: QEPackagingSource): string {
  return `${source.source_kind}:${source.experiment_id}:${source.qe_task_id || ""}:${source.qe_loop_id || ""}`;
}

function metricText(source: QEPackagingSource): string {
  const m = source.metrics_summary || {};
  return `年化 ${formatPercent(m.annual_return)} / IC ${formatPercent(m.ic)} / 回撤 ${formatPercent(m.max_drawdown)}`;
}

function finiteNumber(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : typeof value === "string" && value.trim() ? Number(value) : NaN;
  return Number.isFinite(parsed) ? parsed : null;
}

function combineRoster(run?: MultiAlphaCombineRun | null): unknown[] {
  return Array.isArray(run?.roster_json) ? run.roster_json : [];
}

function combineRunTopk(run?: MultiAlphaCombineRun | null): number | null {
  const backtestConfig = objectValue(run?.backtest_config_json);
  const strategyParams = objectValue(backtestConfig.strategy_params || backtestConfig.strategy_kwargs);
  return finiteNumber(backtestConfig.topk ?? backtestConfig.top_k ?? strategyParams.topk ?? strategyParams.top_k);
}

function combineRunLabel(run: MultiAlphaCombineRun): string {
  const roster = combineRoster(run);
  const legNames = roster
    .map((item) => {
      const row = objectValue(item);
      const leg = row.leg_id || row.alpha_id || row.factor_name || row.name;
      return typeof leg === "string" ? leg : "";
    })
    .filter(Boolean)
    .slice(0, 3)
    .join(" + ");
  const topk = combineRunTopk(run);
  const rosterHash = run.roster_hash ? `roster ${shortHash(run.roster_hash, 8)}` : "roster -";
  return `${run.id} | ${roster.length}腿 | topk=${topk ?? "-"} | ${rosterHash}${legNames ? ` | ${legNames}` : ""}`;
}

function icWeightedScheme(detail: MultiAlphaCombineRunDetail | null): MultiAlphaCombineSchemeResult | null {
  return (detail?.scheme_results || []).find((row) => row.weighting_scheme === MULTI_ALPHA_SUPPORTED_SCHEME) || null;
}

function combineSchemeMetricText(row: MultiAlphaCombineSchemeResult | null): string {
  if (!row) return "缺少 ic_weighted scheme_result";
  return `CAGR ${formatPercent(row.cagr ?? row.annual_return)} / Sharpe ${finiteNumber(row.sharpe)?.toFixed(2) ?? "-"} / 回撤 ${formatPercent(row.max_drawdown)}`;
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


function textValue(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "-";
  if (typeof value === "boolean") return value ? "是" : "否";
  return String(value);
}

function objectValue(value: unknown): JsonObject {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonObject) : {};
}

function arrayCount(value: unknown): number {
  return Array.isArray(value) ? value.length : 0;
}

function totalDependencyCount(deps: JsonObject | null): number {
  if (!deps) return 0;
  return Object.values(deps).reduce<number>((total, value) => total + arrayCount(value), 0);
}

function PackagePayloadSummary({ value, kind }: { value: JsonObject; kind: "source" | "dependencies" | "model" }) {
  if (kind === "dependencies") {
    const rows = Object.entries(value).map(([key, item]) => ({ key, count: arrayCount(item), value: item }));
    return (
      <div className="pv2-readable-panel">
        <div className="pv2-readable-table">
          <div className="pv2-readable-row"><div className="pv2-readable-key">删除依赖</div><div className="pv2-readable-value">共 {totalDependencyCount(value)} 条运行时引用</div></div>
          {rows.map((row) => <div className="pv2-readable-row" key={row.key}><div className="pv2-readable-key">{row.key}</div><div className="pv2-readable-value">{row.count ? `${row.count} 条` : "无"}</div></div>)}
        </div>
      </div>
    );
  }
  if (kind === "model") {
    const model = objectValue(value.model_state || value.model || value);
    const hmm = objectValue(value.hmm_state || value.hmm || {});
    return (
      <div className="pv2-readable-panel">
        <div className="pv2-readable-table">
          <div className="pv2-readable-row"><div className="pv2-readable-key">模型状态</div><div className="pv2-readable-value">{textValue(model.status || value.status)}</div></div>
          <div className="pv2-readable-row"><div className="pv2-readable-key">模型资产</div><div className="pv2-readable-value">{textValue(model.model_id || model.model_path || model.artifact_uri)}</div></div>
          <div className="pv2-readable-row"><div className="pv2-readable-key">HMM 缓存</div><div className="pv2-readable-value">{textValue(hmm.latest_trade_date || hmm.cache_trade_date || hmm.status || "平台按交易日自动计算/缓存")}</div></div>
          <div className="pv2-readable-row"><div className="pv2-readable-key">诊断字段</div><div className="pv2-readable-value">{Object.keys(value).slice(0, 12).join(", ") || "-"}</div></div>
        </div>
      </div>
    );
  }
  const manifest = objectValue(value.manifest || value.package_manifest || value);
  const readiness = objectValue(value.readiness || value.paper_readiness || value.asset_eligibility || {});
  const paperAdmission = objectValue(value.paper_admission || {});
  const materialization = Array.isArray(value.auto_component_materialization) ? value.auto_component_materialization : [];
  const componentCount = finiteNumber(value.components_created_or_reused) ?? materialization.length;
  const materializedLegs = materialization
    .map((item) => {
      const row = objectValue(item);
      return textValue(row.leg_id || row.alpha_id || row.package_id);
    })
    .filter((item) => item !== "-");
  return (
    <div className="pv2-readable-panel">
      <div className="pv2-readable-table">
        <div className="pv2-readable-row"><div className="pv2-readable-key">策略包</div><div className="pv2-readable-value">{textValue(value.package_name || manifest.package_name || value.created_package_id)}</div></div>
        <div className="pv2-readable-row"><div className="pv2-readable-key">资产合格</div><div className="pv2-readable-value">{Object.keys(readiness).length ? (readiness.eligible === false || readiness.status === "BLOCKED" ? "不合格，查看诊断信息" : "已通过或可进入下一步") : textValue(value.package_status || "以策略包列表为准")}</div></div>
        {Object.keys(paperAdmission).length ? <div className="pv2-readable-row"><div className="pv2-readable-key">Paper 准入</div><div className="pv2-readable-value">{paperAdmission.eligible === true ? "eligible=true" : `eligible=false，${textValue(Array.isArray(paperAdmission.blocking) ? paperAdmission.blocking.join(", ") : paperAdmission.blocking)}`}</div></div> : null}
        {componentCount ? <div className="pv2-readable-row"><div className="pv2-readable-key">Component 单包</div><div className="pv2-readable-value">{componentCount} 个自动建/复用{materializedLegs.length ? `：${materializedLegs.slice(0, 4).join("、")}` : ""}</div></div> : null}
        <div className="pv2-readable-row"><div className="pv2-readable-key">manifest</div><div className="pv2-readable-value pv2-mono">{textValue(value.manifest_sha256 || manifest.manifest_sha256)}</div></div>
        <div className="pv2-readable-row"><div className="pv2-readable-key">来源</div><div className="pv2-readable-value">{textValue(value.source_type || manifest.source_type || (objectValue(manifest.source).source_type))}</div></div>
        {value.next_step ? <div className="pv2-readable-row"><div className="pv2-readable-key">下一步</div><div className="pv2-readable-value">{textValue(value.next_step)}</div></div> : null}
        <div className="pv2-readable-row"><div className="pv2-readable-key">诊断字段</div><div className="pv2-readable-value">{Object.keys(value).slice(0, 12).join(", ") || "-"}</div></div>
      </div>
    </div>
  );
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
  const [combineRuns, setCombineRuns] = useState<MultiAlphaCombineRun[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [sourceKind, setSourceKind] = useState<PackagingSourceKind>("all");
  const [sourceKeyValue, setSourceKeyValue] = useState("");
  const [selectedCombineRunId, setSelectedCombineRunId] = useState("");
  const [combineRunDetail, setCombineRunDetail] = useState<MultiAlphaCombineRunDetail | null>(null);
  const [multiAlphaTopk, setMultiAlphaTopk] = useState<25 | 50>(25);
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
  const isMultiAlphaSource = sourceKind === "multi_alpha_combine_run";
  const selectedCombineRun = useMemo(
    () => combineRuns.find((item) => item.id === selectedCombineRunId) || combineRuns[0],
    [combineRuns, selectedCombineRunId],
  );
  const selectedScheme = useMemo(
    () => combineRunDetail?.run?.id === selectedCombineRun?.id ? icWeightedScheme(combineRunDetail) : null,
    [combineRunDetail, selectedCombineRun?.id],
  );

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [packageRows, sourceRows, combineRows] = await Promise.all([
        strategyPackageApi.listSummary(undefined, 100),
        isMultiAlphaSource ? Promise.resolve([]) : strategyPackageApi.qeSources(sourceKind, 300),
        isMultiAlphaSource ? strategyPackageApi.listCombineRuns(200) : Promise.resolve([]),
      ]);
      setPackages(packageRows);
      setSources(sourceRows);
      setCombineRuns(combineRows);
      const requestedPackageId = typeof window !== "undefined"
        ? new URLSearchParams(window.location.search).get("package_id")
        : null;
      const requestedPackage = packageRows.find((item) => item.package_id === requestedPackageId);
      if (requestedPackage) {
        setSelectedId(requestedPackage.package_id);
      } else if ((!selectedId || !packageRows.some((item) => item.package_id === selectedId)) && packageRows[0]) {
        setSelectedId(packageRows[0].package_id);
      }
      if (!sourceRows.some((item) => sourceKey(item) === sourceKeyValue)) {
        setSourceKeyValue(sourceRows[0] ? sourceKey(sourceRows[0]) : "");
      }
      if (!combineRows.some((item) => item.id === selectedCombineRunId)) {
        setSelectedCombineRunId(combineRows[0]?.id || "");
      }
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [isMultiAlphaSource, selectedCombineRunId, selectedId, sourceKind, sourceKeyValue]);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    if (!isMultiAlphaSource || !selectedCombineRun?.id) {
      setCombineRunDetail(null);
      return;
    }
    let alive = true;
    async function loadCombineRunDetail() {
      setError(null);
      setCombineRunDetail(null);
      try {
        const detail = await strategyPackageApi.getCombineRun(selectedCombineRun.id);
        if (alive) setCombineRunDetail(detail);
      } catch (exc) {
        if (alive) {
          setCombineRunDetail(null);
          setError(exc);
        }
      }
    }
    void loadCombineRunDetail();
    return () => { alive = false; };
  }, [isMultiAlphaSource, selectedCombineRun?.id]);

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
    if (isMultiAlphaSource) {
      if (action !== "create") {
        setError(new Error("多Alpha combine 来源不支持 QE Manifest 预览或 QE 资产合格性检查；请查看下方 run 指标后直接创建 StrategyPackage。"));
        return;
      }
      if (!selectedCombineRun?.id) {
        setError(new Error("请先选择一个 succeeded 多Alpha combine run。"));
        return;
      }
      setBusy(true);
      try {
        const created: StrategyPackagePromotionResult = await strategyPackageApi.createFromMultiAlphaCombineRun({
          combine_backtest_run_id: selectedCombineRun.id,
          weighting_scheme: MULTI_ALPHA_SUPPORTED_SCHEME,
          topk: multiAlphaTopk,
          weight_policy: MULTI_ALPHA_WEIGHT_POLICY,
          confirmation: MULTI_ALPHA_PROMOTE_CONFIRMATION,
        });
        setSelectedId(created.package_id);
        setSourcePreview({
          created_package_id: created.package_id,
          package_name: created.package_name,
          manifest_sha256: created.manifest_sha256,
          package_status: created.package_status,
          source_type: "multi_alpha_combine_run",
          source_run_id: created.source_run_id || selectedCombineRun.id,
          paper_admission: created.paper_admission || {},
          components_created_or_reused: created.components?.length ?? 0,
          auto_component_materialization: created.auto_component_materialization || [],
          next_step: "多Alpha 包已建成 ASSET_VALIDATED，但 paper_admission.eligible=false。请执行 POST /strategy-packages/{id}/paper-runtime-dry-run（local_sim, top_k=25 或 50）清门，之后才会进入 selectable-packages、荐股下拉与 create_portfolio。",
        });
        await load();
      } catch (exc) {
        setError(exc);
      } finally {
        setBusy(false);
      }
      return;
    }
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
  const combineRunStatus = String(selectedCombineRun?.status || "").toLowerCase();
  const combineDetailLoaded = !selectedCombineRun?.id || combineRunDetail?.run?.id === selectedCombineRun.id;
  const combineRunReady = isMultiAlphaSource && Boolean(selectedCombineRun?.id) && combineRunStatus === "succeeded" && combineDetailLoaded && Boolean(selectedScheme);
  const combineDisabledReason = !isMultiAlphaSource
    ? ""
    : !selectedCombineRun?.id
      ? "请先选择 succeeded combine run"
      : combineRunStatus !== "succeeded"
        ? `后端只接受 succeeded run，当前状态为 ${selectedCombineRun.status || "-"}`
        : !combineDetailLoaded
          ? "正在加载选中 run 的 scheme_results"
          : !selectedScheme
          ? "选中 run 缺少 ic_weighted scheme_result，创建会被后端拒绝"
          : "";

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

      <SectionCard title="从 QE 创建策略包" eyebrow="QE 单Alpha / 多Alpha combine 统一入口" action={<button className="pv2-button" onClick={load} disabled={loading} type="button">刷新来源</button>}>
        <div className="pv2-form-grid">
          <div className="pv2-field">
            <label>来源类型</label>
            <select className="pv2-select" value={sourceKind} onChange={(event) => setSourceKind(event.target.value as typeof sourceKind)}>
              <option value="all">全部来源</option>
              <option value="qe_experiment">QE 单次实验</option>
              <option value="qe_evolution_loop">QE 演进 Loop</option>
              <option value="multi_alpha_combine_run">多Alpha 组合回测</option>
            </select>
          </div>
          {isMultiAlphaSource ? (
            <div className="pv2-field pv2-field-wide">
              <label>Combine run（仅 succeeded）</label>
              <select className="pv2-select" value={selectedCombineRun?.id || ""} onChange={(event) => setSelectedCombineRunId(event.target.value)}>
                {combineRuns.map((item) => <option value={item.id} key={item.id}>{combineRunLabel(item)}</option>)}
              </select>
            </div>
          ) : (
            <div className="pv2-field pv2-field-wide">
              <label>QE 来源</label>
              <select className="pv2-select" value={sourceKeyValue} onChange={(event) => setSourceKeyValue(event.target.value)}>
                {sources.map((item) => {
                  const key = sourceKey(item);
                  return <option value={key} key={key}>{item.display_name || `${item.experiment_name} | ${metricText(item)}`}</option>;
                })}
              </select>
            </div>
          )}
        </div>
        {isMultiAlphaSource ? (
          <>
            <div className="pv2-form-grid" style={{ marginTop: 12 }}>
              <div className="pv2-field">
                <label>Weighting scheme</label>
                <select className="pv2-select" value={MULTI_ALPHA_SUPPORTED_SCHEME} disabled>
                  <option value={MULTI_ALPHA_SUPPORTED_SCHEME}>ic_weighted（S1/P0 唯一支持）</option>
                  <option value="equal">equal（置灰，后端拒绝）</option>
                  <option value="orthogonality_aware">orthogonality_aware（置灰，后端拒绝）</option>
                  <option value="risk_parity">risk_parity（置灰，后端拒绝）</option>
                </select>
                <div className="pv2-help">多Alpha StrategyPackage S1 仅支持 ic_weighted；其它 scheme 不做前端兜底，后端会 fail-fast。</div>
              </div>
              <div className="pv2-field">
                <label>TopK</label>
                <select className="pv2-select" value={multiAlphaTopk} onChange={(event) => setMultiAlphaTopk(Number(event.target.value) as 25 | 50)}>
                  <option value={25}>25</option>
                  <option value={50}>50</option>
                </select>
                <div className="pv2-help">默认 25，可切 50；创建 payload 严格传 topk，不从 QE 来源推断。</div>
              </div>
            </div>
            <div className="pv2-grid pv2-grid-3" style={{ marginTop: 12 }}>
              <MetricCard label="Run 状态" value={selectedCombineRun?.status || "-"} hint={`run_id=${selectedCombineRun?.id || "-"}`} tone={combineRunReady ? "success" : "warning"} />
              <MetricCard label="Roster" value={`${combineRoster(selectedCombineRun).length || "-"} 腿`} hint={`roster_hash=${selectedCombineRun?.roster_hash || "-"}`} />
              <MetricCard label="ic_weighted 指标" value={selectedScheme ? "可创建" : "缺失"} hint={combineSchemeMetricText(selectedScheme)} tone={selectedScheme ? "success" : "warning"} />
            </div>
            <NoticePanel title="多Alpha 创建后的下一步" tone="warning">
              创建只冻结 multi_alpha 父包并自动建/复用 component 单包。父包会是 ASSET_VALIDATED，但 paper_admission.eligible=false；需 POST /strategy-packages/{`<package_id>`}/paper-runtime-dry-run（local_sim，runtime_variant=top_k={multiAlphaTopk}）通过后，才会进入 selectable-packages、荐股下拉和模拟盘 create_portfolio。
            </NoticePanel>
          </>
        ) : (
          <label className="pv2-chip" style={{ marginTop: 12 }}>
            <input type="checkbox" checked={resolveRuntimeAssets} onChange={(event) => setResolveRuntimeAssets(event.target.checked)} />
            创建时解析并复制运行资产
          </label>
        )}
        <div className="pv2-row-actions" style={{ marginTop: 12 }}>
          <button className="pv2-button" onClick={() => sourceAction("preview")} disabled={busy || isMultiAlphaSource || !selectedSource} type="button">预览 Manifest</button>
          <button className="pv2-button" onClick={() => sourceAction("readiness")} disabled={busy || isMultiAlphaSource || !selectedSource} type="button">检查资产合格性</button>
          <button className="pv2-button-primary" onClick={() => sourceAction("create")} disabled={busy || (isMultiAlphaSource ? !combineRunReady : !selectedSource)} title={isMultiAlphaSource ? combineDisabledReason || "创建多Alpha StrategyPackage" : "创建 StrategyPackage"} type="button">创建 StrategyPackage</button>
        </div>
        <div className="pv2-help">
          {isMultiAlphaSource
            ? "多Alpha combine 不能走 /strategy-packages/qe-sources，也不调用 QE experiment-only 预览/合格性端点；所有失败原因直接透传后端。"
            : "策略包入场只检查资产与 manifest 完整性；HMM、黑名单、TopK、停牌剔除、交易日和行情源都属于运行时平台能力。"}
        </div>
        {isMultiAlphaSource && combineDisabledReason ? <NoticePanel title="当前多Alpha来源不可创建" tone="warning">{combineDisabledReason}</NoticePanel> : null}
        {!isMultiAlphaSource && !sources.length ? <NoticePanel title="暂无可打包 QE 来源" tone="info">没有找到尚未打包的 QE 实验或演进 Loop。</NoticePanel> : null}
        {isMultiAlphaSource && !combineRuns.length ? <NoticePanel title="暂无 succeeded combine run" tone="info">/multi-alpha/combine-backtest/runs?status=succeeded 未返回可创建来源。</NoticePanel> : null}
        {sourcePreview ? <PackagePayloadSummary value={sourcePreview} kind="source" /> : null}
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
            {deleteDependencies ? <PackagePayloadSummary value={deleteDependencies} kind="dependencies" /> : null}
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
            {modelState ? <PackagePayloadSummary value={modelState} kind="model" /> : <div className="pv2-muted">尚未获取模型状态。</div>}
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
