"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { strategyPackageApi } from "@/lib/paper-v2/api";
import { formatPercent, shortHash } from "@/lib/paper-v2/format";
import type { ExecutionPolicy, JsonObject, StrategyPackage } from "@/lib/paper-v2/types";

export default function PaperV2PackagesPage() {
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [policies, setPolicies] = useState<ExecutionPolicy[]>([]);
  const [events, setEvents] = useState<JsonObject[]>([]);
  const [modelState, setModelState] = useState<JsonObject | null>(null);
  const [qeExperimentId, setQeExperimentId] = useState("");
  const [qeTaskId, setQeTaskId] = useState("");
  const [qeLoopId, setQeLoopId] = useState("");
  const [resolveRuntimeAssets, setResolveRuntimeAssets] = useState(false);
  const [sourcePreview, setSourcePreview] = useState<JsonObject | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const selected = packages.find((item) => item.package_id === selectedId) || packages[0];

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const rows = await strategyPackageApi.list(undefined, 300);
      setPackages(rows);
      if (!selectedId && rows[0]) setSelectedId(rows[0].package_id);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [selectedId]);

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
    try {
      if (action === "enableSelection") await strategyPackageApi.enableSelection(selected.package_id);
      if (action === "enablePaper") await strategyPackageApi.enablePaper(selected.package_id);
      if (action === "retire") await strategyPackageApi.retire(selected.package_id);
      await load();
    } catch (exc) {
      setError(exc);
    }
  }

  async function sourceAction(action: "previewExperiment" | "paperReadiness" | "createExperiment" | "createLoop") {
    setError(null);
    setSourcePreview(null);
    try {
      if (action === "previewExperiment") {
        if (!qeExperimentId) throw new Error("请输入 QE 实验 ID。");
        setSourcePreview(await strategyPackageApi.qeExperimentManifest(qeExperimentId));
      }
      if (action === "paperReadiness") {
        if (!qeExperimentId) throw new Error("请输入 QE 实验 ID。");
        setSourcePreview(await strategyPackageApi.qeExperimentPaperReadiness(qeExperimentId));
      }
      if (action === "createExperiment") {
        if (!qeExperimentId) throw new Error("请输入 QE 实验 ID。");
        const created = await strategyPackageApi.createFromQEExperiment({ experiment_id: qeExperimentId, resolve_runtime_assets: resolveRuntimeAssets });
        setSelectedId(created.package_id);
        setSourcePreview({ created_package_id: created.package_id, manifest_sha256: created.manifest_sha256, package_status: created.package_status });
        await load();
      }
      if (action === "createLoop") {
        if (!qeTaskId || !qeLoopId) throw new Error("请输入 QE 任务 ID 和 Loop ID。");
        const created = await strategyPackageApi.createFromQEEvolutionLoop({ qe_task_id: qeTaskId, qe_loop_id: qeLoopId, resolve_runtime_assets: resolveRuntimeAssets });
        setSelectedId(created.package_id);
        setSourcePreview({ created_package_id: created.package_id, manifest_sha256: created.manifest_sha256, package_status: created.package_status });
        await load();
      }
    } catch (exc) {
      setError(exc);
    }
  }

  const metrics = selected?.metrics_summary || {};

  return (
    <main>
      <ErrorPanel error={error} title="策略包操作失败" />
      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="从 QE 创建策略包" eyebrow="仅允许权威来源">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>QE 实验 ID</label><input className="pv2-input" value={qeExperimentId} onChange={(event) => setQeExperimentId(event.target.value)} placeholder="qe_20260416_002701" /></div>
            <div className="pv2-field"><label>QE 任务 ID</label><input className="pv2-input" value={qeTaskId} onChange={(event) => setQeTaskId(event.target.value)} placeholder="演进任务 ID" /></div>
            <div className="pv2-field"><label>QE Loop ID</label><input className="pv2-input" value={qeLoopId} onChange={(event) => setQeLoopId(event.target.value)} placeholder="Loop ID" /></div>
          </div>
          <label className="pv2-chip" style={{ marginTop: 12 }}><input type="checkbox" checked={resolveRuntimeAssets} onChange={(event) => setResolveRuntimeAssets(event.target.checked)} /> 解析运行时资产</label>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" onClick={() => sourceAction("previewExperiment")} type="button">预览实验 Manifest</button>
            <button className="pv2-button" onClick={() => sourceAction("paperReadiness")} type="button">验证模拟盘就绪度</button>
            <button className="pv2-button-primary" onClick={() => sourceAction("createExperiment")} type="button">从单次实验创建</button>
            <button className="pv2-button-primary" onClick={() => sourceAction("createLoop")} type="button">从演进 Loop 创建</button>
          </div>
          {sourcePreview ? <JsonPanel value={sourcePreview} /> : null}
        </SectionCard>

        <SectionCard title="策略包晋级规则" eyebrow="禁止修改 Manifest">
          <ul>
            <li>只有 QE 单次实验和 QE 演进 Loop 可以创建 StrategyPackage。</li>
            <li>冻结的 manifest JSON 与 manifest_sha256 会被保留；可变状态不进入 manifest hash。</li>
            <li>已被模拟盘 v2 使用的策略包不能静默修改；需要创建新的策略包版本。</li>
            <li>启用选股和启用模拟盘都会触发后端校验，并在这里透出 fail-fast 错误。</li>
          </ul>
        </SectionCard>
      </div>

      <SectionCard title="StrategyPackage 策略包中心" eyebrow={loading ? "加载中" : "权威策略包"} action={<button className="pv2-button" onClick={load} type="button">刷新</button>}>
        <PaperTable
          rows={packages}
          empty="暂无 StrategyPackage。请先从 QE 创建策略包。"
          columns={[
            { key: "name", header: "名称", render: (row) => <button className="pv2-link-button" onClick={() => setSelectedId(row.package_id)} type="button">{row.package_name}</button> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.package_status} /> },
            { key: "source", header: "来源", render: (row) => <span>{row.source_type}<br /><span className="pv2-muted pv2-mono">{shortHash(row.source_id, 8)}</span></span> },
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
          <SectionCard title={selected.package_name} eyebrow="当前策略包" action={<div className="pv2-row-actions"><button className="pv2-button" onClick={() => transition("enableSelection")} type="button">启用选股</button><button className="pv2-button" onClick={() => transition("enablePaper")} type="button">启用模拟盘</button><button className="pv2-button-danger" onClick={() => transition("retire")} type="button">退役</button></div>}>
            <div className="pv2-grid pv2-grid-3">
              <MetricCard label="IC" value={formatPercent(metrics.ic)} />
              <MetricCard label="RankIC" value={formatPercent(metrics.rank_ic)} />
              <MetricCard label="年化收益" value={formatPercent(metrics.annual_return)} />
            </div>
            <div className="pv2-chip-row" style={{ marginTop: 14 }}>
              <span className="pv2-chip">package_id: {shortHash(selected.package_id)}</span>
              <span className="pv2-chip">manifest: {shortHash(selected.manifest_sha256)}</span>
              <span className="pv2-chip">模拟组合数: {selected.paper_portfolio_count || 0}</span>
            </div>
            <h3>模型状态</h3>
            {modelState ? <JsonPanel value={modelState} /> : <div className="pv2-muted">尚未获取模型状态。</div>}
          </SectionCard>

          <SectionCard title="执行策略与血缘" eyebrow="仅模拟盘安全策略" action={<Link className="pv2-button" href={`/paper-v2/portfolios?package_id=${selected.package_id}`}>创建组合</Link>}>
            <PaperTable
              rows={policies}
              empty="暂无已验证执行策略。只有 manifest 默认策略通过校验时，模拟盘 v2 才会导入。"
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
                { key: "status", header: "状态", render: (row) => <StatusBadge status={row.to_status || row.status || "event"} /> },
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
