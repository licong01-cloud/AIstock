"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { formatCompact, shortHash } from "@/lib/paper-v2/format";
import { API_BASE, type QETemplate, qeTemplatesApi } from "@/lib/qe-templates/api";

function kindLabel(kind: string): string {
  if (kind === "single_experiment") return "QE 单次实验";
  if (kind === "custom_evo") return "自定义演进";
  return kind || "未知类型";
}

function formatDateTime(value: unknown): string {
  const text = String(value || "");
  return text ? text.replace("T", " ").slice(0, 19) : "-";
}

function countByStatus(rows: QETemplate[], status: string): number {
  return rows.filter((row) => row.status === status).length;
}

function extractConfigHint(row: QETemplate): string {
  const config = row.config_json || {};
  const model = typeof config.model_id === "string" ? config.model_id : typeof config.model === "string" ? config.model : "-";
  const factors = Array.isArray(config.factor_names)
    ? config.factor_names.length
    : Array.isArray(config.factor_keys)
      ? config.factor_keys.length
      : Array.isArray(config.loops)
        ? config.loops.length
        : 0;
  const horizon = typeof config.label_horizon === "number" || typeof config.label_horizon === "string" ? config.label_horizon : "-";
  return `模型 ${model} / 因子或Loop ${factors} / horizon ${horizon}`;
}

export default function QETemplatesPage() {
  const [templates, setTemplates] = useState<QETemplate[]>([]);
  const [status, setStatus] = useState("");
  const [kind, setKind] = useState("");
  const [creator, setCreator] = useState("agent");
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setTemplates(await qeTemplatesApi.list({
        status: status || undefined,
        template_kind: kind || undefined,
        created_by_type: creator || undefined,
        search: search.trim() || undefined,
        limit: 200,
      }));
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [creator, kind, search, status]);

  useEffect(() => {
    void load();
  }, [load]);

  const pendingCount = useMemo(
    () => templates.filter((item) => ["draft", "ready_for_review", "approved", "materialized"].includes(item.status)).length,
    [templates],
  );
  const mcpCount = templates.filter((item) => item.created_by_type === "agent").length;

  return (
    <main className="pv2-shell">
      <section className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">QE Pending Templates</div>
            <h1>QE 待执行实验管理台</h1>
            <p>
              统一查看 MCP 创建的 QE 单次实验和自定义演进模板。模板必须先落库，人工审查或修改后才会通过现有 QE 执行层正式运行。
              <span className="pv2-mono"> {API_BASE}/qe-templates </span>
            </p>
          </div>
          <div className="pv2-row-actions">
            <button className="pv2-button-primary" type="button" onClick={() => void load()} disabled={loading}>
              {loading ? "刷新中" : "刷新模板"}
            </button>
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="QE 模板加载失败" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="模板总数" value={formatCompact(templates.length, 0)} hint="当前筛选结果" tone="info" />
        <MetricCard label="MCP 创建" value={formatCompact(mcpCount, 0)} hint="created_by_type=agent" tone="success" />
        <MetricCard label="待审查/待执行" value={formatCompact(pendingCount, 0)} hint="未正式运行" tone={pendingCount ? "warning" : "success"} />
        <MetricCard label="已请求执行" value={formatCompact(countByStatus(templates, "run_requested"), 0)} hint="已进入现有 QE 执行层" />
      </div>

      <SectionCard title="筛选模板" eyebrow="mcp proposals / manual review">
        <div className="pv2-form-grid">
          <label className="pv2-field">
            <span>状态</span>
            <select className="pv2-select" value={status} onChange={(event) => setStatus(event.target.value)} aria-label="template status filter">
              <option value="">全部状态</option>
              <option value="draft">草稿</option>
              <option value="ready_for_review">待审查</option>
              <option value="approved">已审批</option>
              <option value="materialized">已物化待执行</option>
              <option value="run_requested">已请求执行</option>
              <option value="superseded">已废弃</option>
            </select>
          </label>
          <label className="pv2-field">
            <span>实验类型</span>
            <select className="pv2-select" value={kind} onChange={(event) => setKind(event.target.value)} aria-label="template kind filter">
              <option value="">全部类型</option>
              <option value="single_experiment">QE 单次实验</option>
              <option value="custom_evo">自定义演进</option>
            </select>
          </label>
          <label className="pv2-field">
            <span>来源</span>
            <select className="pv2-select" value={creator} onChange={(event) => setCreator(event.target.value)} aria-label="template creator filter">
              <option value="agent">MCP/Agent 创建</option>
              <option value="user">人工创建</option>
              <option value="">全部来源</option>
            </select>
          </label>
          <label className="pv2-field">
            <span>搜索</span>
            <input className="pv2-input" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="模板名、ID、实验ID、任务ID" aria-label="template search" />
          </label>
          <div className="pv2-field">
            <span>&nbsp;</span>
            <button className="pv2-button" type="button" onClick={() => void load()} disabled={loading}>应用筛选</button>
          </div>
        </div>
      </SectionCard>

      <SectionCard title="待执行实验列表" eyebrow="review before run">
        <PaperTable
          rows={templates}
          empty="暂无 MCP 创建的待执行 QE 实验模板"
          columns={[
            {
              key: "template",
              header: "模板",
              render: (row) => (
                <>
                  <Link className="pv2-link-button" href={`/quantevolver/templates/${encodeURIComponent(row.template_id)}`}>{row.title}</Link>
                  <div className="pv2-muted pv2-mono">{row.template_id}</div>
                  <div className="pv2-muted">{row.description || "无描述"}</div>
                </>
              ),
            },
            { key: "kind", header: "类型", render: (row) => <><div>{kindLabel(row.template_kind)}</div><div className="pv2-muted">{extractConfigHint(row)}</div></> },
            { key: "status", header: "状态", render: (row) => <><StatusBadge status={row.status} /><div className="pv2-muted">数仓策略 {row.archive_policy}</div></> },
            { key: "source", header: "来源", render: (row) => <><div>{row.created_by_type || "-"}</div><div className="pv2-muted">{row.created_by_name || "-"}</div></> },
            { key: "runtime", header: "运行关联", render: (row) => <><div className="pv2-mono">实验 {shortHash(row.submitted_experiment_id)}</div><div className="pv2-mono pv2-muted">任务 {shortHash(row.submitted_task_id)}</div></> },
            { key: "time", header: "更新时间", render: (row) => <><div>{formatDateTime(row.updated_at)}</div><div className="pv2-muted">创建 {formatDateTime(row.created_at)}</div></> },
            {
              key: "actions",
              header: "操作",
              render: (row) => (
                <div className="pv2-row-actions">
                  <Link className="pv2-button-ghost" href={`/quantevolver/templates/${encodeURIComponent(row.template_id)}`}>打开详情</Link>
                  {row.submitted_experiment_id ? <Link className="pv2-button-ghost" href={`/quantevolver/experiments/${encodeURIComponent(row.submitted_experiment_id)}`}>实验历史</Link> : null}
                  {row.submitted_task_id ? <Link className="pv2-button-ghost" href={`/quantevolver/evolution/${encodeURIComponent(row.submitted_task_id)}`}>演进任务</Link> : null}
                </div>
              ),
            },
          ]}
        />
      </SectionCard>
    </main>
  );
}
