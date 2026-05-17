"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { shortHash } from "@/lib/paper-v2/format";
import {
  type ArchivePolicy,
  type JsonObject,
  type QETemplate,
  type QETemplateValidation,
  qeTemplatesApi,
} from "@/lib/qe-templates/api";

const EDITABLE_STATUSES = new Set(["draft", "ready_for_review", "approved"]);
const ARCHIVE_POLICIES: ArchivePolicy[] = ["AUTO", "SKIP", "MANUAL_ONLY"];

type LoopRow = {
  index: number;
  label: string;
  model_id: string;
  node_id: string;
  factor_count: number;
  seed: string;
};

function isRecord(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function prettyJson(value: unknown): string {
  return JSON.stringify(value || {}, null, 2);
}

function parseObject(text: string): JsonObject {
  const parsed = JSON.parse(text) as unknown;
  if (!isRecord(parsed)) throw new Error("配置 JSON 必须是对象，不能是数组或空值。 ");
  return parsed;
}

function asString(value: unknown): string {
  if (value === null || value === undefined) return "";
  return String(value);
}

function getArrayText(config: JsonObject, key: string): string {
  const value = config[key];
  return Array.isArray(value) ? value.map(String).join(", ") : "";
}

function setConfigValue(config: JsonObject, key: string, value: unknown): JsonObject {
  return { ...config, [key]: value };
}

function splitList(value: string): string[] {
  return value.split(/[\n,]/).map((item) => item.trim()).filter(Boolean);
}

function kindLabel(kind: string): string {
  return kind === "custom_evo" ? "自定义演进" : "QE 单次实验";
}

function formatDateTime(value: unknown): string {
  const text = String(value || "");
  return text ? text.replace("T", " ").slice(0, 19) : "-";
}

function validationFromTemplate(template: QETemplate | null): QETemplateValidation | null {
  const validation = template?.validation_json;
  if (!isRecord(validation)) return null;
  return {
    valid: Boolean(validation.valid),
    errors: Array.isArray(validation.errors) ? validation.errors.map(String) : [],
    warnings: Array.isArray(validation.warnings) ? validation.warnings.map(String) : [],
  };
}

function extractLoopRows(template: QETemplate | null): LoopRow[] {
  const loops = template?.config_json?.loops;
  if (!Array.isArray(loops)) return [];
  return loops.map((loop, index) => {
    const row = isRecord(loop) ? loop : {};
    const factors = row.factor_keys || row.factor_names;
    return {
      index: index + 1,
      label: asString(row.label || row.name || `Loop ${index + 1}`),
      model_id: asString(row.model_id),
      node_id: asString(row.node_id),
      factor_count: Array.isArray(factors) ? factors.length : 0,
      seed: asString(row.seed || row.random_seed),
    };
  });
}

function targetHref(template: QETemplate | null): string | null {
  if (!template) return null;
  if (template.template_kind === "custom_evo" && template.submitted_task_id) {
    return `/quantevolver/evolution/${encodeURIComponent(template.submitted_task_id)}`;
  }
  if (template.template_kind === "single_experiment" && template.submitted_experiment_id) {
    return `/quantevolver/experiments/${encodeURIComponent(template.submitted_experiment_id)}`;
  }
  return null;
}

export default function QETemplateDetailPage({ params }: { params: { templateId: string } }) {
  const templateId = decodeURIComponent(params.templateId);
  const [template, setTemplate] = useState<QETemplate | null>(null);
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [archivePolicy, setArchivePolicy] = useState<ArchivePolicy>("AUTO");
  const [archiveReason, setArchiveReason] = useState("");
  const [analysisSummary, setAnalysisSummary] = useState("");
  const [riskSummary, setRiskSummary] = useState("");
  const [configText, setConfigText] = useState("{}");
  const [configError, setConfigError] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [validation, setValidation] = useState<QETemplateValidation | null>(null);
  const [runMessage, setRunMessage] = useState<string | null>(null);

  const load = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      const row = await qeTemplatesApi.get(templateId);
      setTemplate(row);
      setTitle(row.title || "");
      setDescription(row.description || "");
      setArchivePolicy(row.archive_policy || "AUTO");
      setArchiveReason(row.archive_reason || "");
      setAnalysisSummary(row.analysis_summary_md || "");
      setRiskSummary(row.risk_summary_md || "");
      setConfigText(prettyJson(row.config_json));
      setValidation(validationFromTemplate(row));
      setConfigError(null);
      setDirty(false);
    } catch (err) {
      setError(err);
    } finally {
      setBusy(false);
    }
  }, [templateId]);

  useEffect(() => {
    void load();
  }, [load]);

  const canEdit = template ? EDITABLE_STATUSES.has(template.status) : false;
  const parsedConfig = useMemo(() => {
    try {
      return parseObject(configText);
    } catch {
      return template?.config_json || {};
    }
  }, [configText, template]);
  const loops = useMemo(() => extractLoopRows(template ? { ...template, config_json: parsedConfig } : null), [parsedConfig, template]);
  const resultHref = targetHref(template);

  function updateConfig(next: JsonObject) {
    setConfigText(prettyJson(next));
    setConfigError(null);
    setDirty(true);
  }

  function editField(field: string, value: string) {
    updateConfig(setConfigValue(parsedConfig, field, value));
  }

  function editNumberField(field: string, value: string) {
    const trimmed = value.trim();
    updateConfig(setConfigValue(parsedConfig, field, trimmed === "" ? "" : Number(trimmed)));
  }

  function editListField(field: string, value: string) {
    updateConfig(setConfigValue(parsedConfig, field, splitList(value)));
  }

  async function saveTemplate(): Promise<QETemplate | null> {
    if (!template || !canEdit) return template;
    let config: JsonObject;
    try {
      config = parseObject(configText);
      setConfigError(null);
    } catch (err) {
      setConfigError(err instanceof Error ? err.message : String(err));
      return null;
    }
    setBusy(true);
    setError(null);
    try {
      const row = await qeTemplatesApi.update(template.template_id, {
        title,
        description: description || null,
        config_json: config,
        archive_policy: archivePolicy,
        archive_reason: archiveReason || null,
        analysis_summary_md: analysisSummary || null,
        risk_summary_md: riskSummary || null,
      });
      setTemplate(row);
      setValidation(validationFromTemplate(row));
      setDirty(false);
      setRunMessage("模板配置已保存，尚未执行。 ");
      return row;
    } catch (err) {
      setError(err);
      return null;
    } finally {
      setBusy(false);
    }
  }

  async function validateTemplate(rowOverride?: QETemplate): Promise<QETemplateValidation | null> {
    const row = rowOverride || template;
    if (!row) return null;
    setBusy(true);
    setError(null);
    try {
      const result = await qeTemplatesApi.validate(row.template_id);
      setTemplate(result.template);
      setValidation(result.validation);
      return result.validation;
    } catch (err) {
      setError(err);
      return null;
    } finally {
      setBusy(false);
    }
  }

  async function executeTemplate() {
    if (!template) return;
    if (!window.confirm("确认使用当前模板配置正式执行 QE 实验？执行后请到实验历史或自动演进页面查看运行详情。")) return;
    setRunMessage(null);
    const saved = dirty ? await saveTemplate() : template;
    if (!saved) return;
    const nextValidation = await validateTemplate(saved);
    if (!nextValidation?.valid) {
      setRunMessage("校验未通过，已阻止执行。请修正配置后重新校验。 ");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      let current = await qeTemplatesApi.approve(saved.template_id, { approval_note: "UI 人工审查确认执行" });
      if (!current.submitted_experiment_id && !current.submitted_task_id) {
        const materialized = await qeTemplatesApi.materialize(current.template_id);
        current = materialized.template || await qeTemplatesApi.get(current.template_id);
      }
      const runResult = await qeTemplatesApi.run(current);
      const refreshed = await qeTemplatesApi.get(current.template_id);
      setTemplate(refreshed);
      setValidation(validationFromTemplate(refreshed));
      setDirty(false);
      const target = targetHref(refreshed);
      setRunMessage(`执行请求已提交：${runResult.template_id}${target ? "，可跳转现有 QE 页面查看详情。" : "。"}`);
    } catch (err) {
      setError(err);
    } finally {
      setBusy(false);
    }
  }

  async function supersedeTemplate() {
    if (!template) return;
    if (!window.confirm("确认废弃该待执行模板？不会删除已经创建的 QE 实验或演进任务。")) return;
    setBusy(true);
    setError(null);
    try {
      const row = await qeTemplatesApi.supersede(template.template_id);
      setTemplate(row);
      setValidation(validationFromTemplate(row));
      setDirty(false);
    } catch (err) {
      setError(err);
    } finally {
      setBusy(false);
    }
  }

  if (!template && !error) {
    return <main className="pv2-shell"><div className="pv2-notice pv2-notice-info">正在加载模板...</div></main>;
  }

  return (
    <main className="pv2-shell">
      <section className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">QE Template Review</div>
            <h1>{template?.title || "QE 待执行实验详情"}</h1>
            <p>
              这里仅修改数据库中的待执行模板。点击执行前不会启动 QE；执行后会复用现有单次实验或自定义演进执行层，并在现有页面展示运行详情。
            </p>
          </div>
          <div className="pv2-row-actions">
            <Link className="pv2-button-ghost" href="/quantevolver/templates">返回列表</Link>
            <button className="pv2-button-ghost" type="button" onClick={() => void load()} disabled={busy}>刷新</button>
            {resultHref ? <Link className="pv2-button-primary" href={resultHref}>查看运行详情</Link> : null}
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="QE 模板操作失败" />
      {configError ? <div className="pv2-error-panel"><div className="pv2-error-kicker">配置 JSON 错误</div><div>{configError}</div></div> : null}
      {runMessage ? <div className="pv2-notice pv2-notice-success"><div className="pv2-notice-title">操作结果</div><div className="pv2-notice-body">{runMessage}</div></div> : null}

      {template ? (
        <>
          <div className="pv2-grid pv2-grid-4">
            <MetricCard label="模板类型" value={kindLabel(template.template_kind)} hint={template.template_id} tone="info" />
            <MetricCard label="模板状态" value={template.status} hint="执行前可审查" tone={canEdit ? "warning" : "neutral"} />
            <MetricCard label="数仓策略" value={archivePolicy} hint={archiveReason || "默认策略"} />
            <MetricCard label="运行关联" value={shortHash(template.submitted_experiment_id || template.submitted_task_id)} hint="现有 QE 页面负责运行详情" />
          </div>

          <SectionCard title="人工审查信息" eyebrow="database template / no execution on save">
            <div className="pv2-form-grid">
              <label className="pv2-field"><span>标题</span><input className="pv2-input" value={title} disabled={!canEdit} onChange={(event) => { setTitle(event.target.value); setDirty(true); }} /></label>
              <label className="pv2-field"><span>模板类型</span><input className="pv2-input" value={kindLabel(template.template_kind)} disabled /></label>
              <label className="pv2-field"><span>来源</span><input className="pv2-input" value={`${template.created_by_type || "-"} / ${template.created_by_name || "-"}`} disabled /></label>
              <label className="pv2-field"><span>数仓策略</span><select className="pv2-select" value={archivePolicy} disabled={!canEdit} onChange={(event) => { setArchivePolicy(event.target.value as ArchivePolicy); setDirty(true); }}>{ARCHIVE_POLICIES.map((policy) => <option key={policy} value={policy}>{policy}</option>)}</select></label>
              <label className="pv2-field"><span>数仓策略原因</span><input className="pv2-input" value={archiveReason} disabled={!canEdit} onChange={(event) => { setArchiveReason(event.target.value); setDirty(true); }} /></label>
              <label className="pv2-field"><span>更新时间</span><input className="pv2-input" value={formatDateTime(template.updated_at)} disabled /></label>
            </div>
            <div className="pv2-form-grid" style={{ marginTop: 12 }}>
              <label className="pv2-field"><span>描述</span><textarea className="pv2-textarea" value={description} disabled={!canEdit} onChange={(event) => { setDescription(event.target.value); setDirty(true); }} /></label>
              <label className="pv2-field"><span>MCP 分析摘要</span><textarea className="pv2-textarea" value={analysisSummary} disabled={!canEdit} onChange={(event) => { setAnalysisSummary(event.target.value); setDirty(true); }} /></label>
              <label className="pv2-field"><span>风险说明</span><textarea className="pv2-textarea" value={riskSummary} disabled={!canEdit} onChange={(event) => { setRiskSummary(event.target.value); setDirty(true); }} /></label>
            </div>
            {!canEdit ? <div className="pv2-help">当前状态不可原地修改配置。如需调整已物化或已执行模板，请由 MCP 或后续复制功能创建新的待执行模板。</div> : null}
          </SectionCard>

          <SectionCard title="统一配置层编辑" eyebrow="same config payload as manual QE">
            <div className="pv2-notice pv2-notice-info">
              <div className="pv2-notice-title">编辑边界</div>
              <div className="pv2-notice-body">
                下方字段直接更新模板的 <span className="pv2-mono">config_json</span>。保存只写数据库，不执行；点击执行会先保存、校验、审批、物化，再调用现有 QE 执行层。
                当前只支持 QE 单次实验和自定义演进，多 alpha 架构与自动演进 LLM 决策暂不接入。
              </div>
            </div>
            {template.template_kind === "single_experiment" ? (
              <div className="pv2-form-grid">
                <label className="pv2-field"><span>实验名</span><input className="pv2-input" disabled={!canEdit} value={asString(parsedConfig.experiment_name)} onChange={(event) => editField("experiment_name", event.target.value)} /></label>
                <label className="pv2-field"><span>模型 ID</span><input className="pv2-input" disabled={!canEdit} aria-label="single model id" value={asString(parsedConfig.model_id)} onChange={(event) => editField("model_id", event.target.value)} /></label>
                <label className="pv2-field"><span>策略 ID</span><input className="pv2-input" disabled={!canEdit} value={asString(parsedConfig.strategy_id)} onChange={(event) => editField("strategy_id", event.target.value)} /></label>
                <label className="pv2-field"><span>标签周期</span><input className="pv2-input" disabled={!canEdit} value={asString(parsedConfig.label_horizon)} onChange={(event) => editNumberField("label_horizon", event.target.value)} /></label>
                <label className="pv2-field"><span>节点</span><input className="pv2-input" disabled={!canEdit} value={asString(parsedConfig.node_id)} onChange={(event) => editField("node_id", event.target.value)} placeholder="local / wsl / remote" /></label>
                <label className="pv2-field"><span>因子列表</span><textarea className="pv2-textarea" disabled={!canEdit} value={getArrayText(parsedConfig, "factor_names")} onChange={(event) => editListField("factor_names", event.target.value)} placeholder="逗号或换行分隔" /></label>
              </div>
            ) : (
              <>
                <div className="pv2-form-grid">
                  <label className="pv2-field"><span>任务名</span><input className="pv2-input" disabled={!canEdit} value={asString(parsedConfig.task_name)} onChange={(event) => editField("task_name", event.target.value)} /></label>
                  <label className="pv2-field"><span>基础实验</span><input className="pv2-input" disabled={!canEdit} value={asString(parsedConfig.base_experiment_id)} onChange={(event) => editField("base_experiment_id", event.target.value)} /></label>
                  <label className="pv2-field"><span>节点并行度</span><input className="pv2-input" disabled={!canEdit} value={asString(parsedConfig.node_parallelism)} onChange={(event) => editNumberField("node_parallelism", event.target.value)} /></label>
                  <label className="pv2-field"><span>演进目标</span><textarea className="pv2-textarea" disabled={!canEdit} value={asString(parsedConfig.target_desc)} onChange={(event) => editField("target_desc", event.target.value)} /></label>
                </div>
                <div style={{ marginTop: 12 }}>
                  <PaperTable
                    rows={loops}
                    empty="暂无 Loop 配置，请在完整 JSON 中补充 loops。"
                    columns={[
                      { key: "index", header: "Loop", render: (row) => `#${row.index}` },
                      { key: "label", header: "说明", render: (row) => row.label },
                      { key: "model", header: "模型", render: (row) => row.model_id || "-" },
                      { key: "node", header: "节点", render: (row) => row.node_id || "继承任务" },
                      { key: "factor", header: "因子数", render: (row) => row.factor_count },
                      { key: "seed", header: "Seed", render: (row) => row.seed || "-" },
                    ]}
                  />
                </div>
              </>
            )}
            <div className="pv2-field" style={{ marginTop: 12 }}>
              <span>完整配置 JSON（可修改所有人工实验配置字段）</span>
              <textarea
                className="pv2-textarea"
                style={{ minHeight: 360 }}
                value={configText}
                disabled={!canEdit}
                onChange={(event) => { setConfigText(event.target.value); setDirty(true); setConfigError(null); }}
                aria-label="full template config json"
              />
            </div>
          </SectionCard>

          <SectionCard title="校验与执行" eyebrow="validate / approve / materialize / run">
            <div className="pv2-grid pv2-grid-2">
              <div className="pv2-readable-panel" style={{ marginTop: 0 }}>
                <div className="pv2-readable-table">
                  <div className="pv2-readable-row"><div className="pv2-readable-key">校验状态</div><div className="pv2-readable-value"><StatusBadge status={validation?.valid ? "PASSED" : validation ? "FAILED" : "NOT_RUN"} /></div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">错误</div><div className="pv2-readable-value">{validation?.errors?.length ? validation.errors.join("；") : "无"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">警告</div><div className="pv2-readable-value">{validation?.warnings?.length ? validation.warnings.join("；") : "无"}</div></div>
                  <div className="pv2-readable-row"><div className="pv2-readable-key">执行去向</div><div className="pv2-readable-value">{template.template_kind === "custom_evo" ? "现有自动演进页面" : "现有实验历史页面"}</div></div>
                </div>
              </div>
              <div className="pv2-confirm-box">
                <div className="pv2-help">保存不会执行；执行按钮会先保存当前配置，并复用已有后端统一执行层。</div>
                <div className="pv2-row-actions">
                  <button className="pv2-button-ghost" type="button" onClick={() => void saveTemplate()} disabled={busy || !canEdit || !dirty}>保存配置</button>
                  <button className="pv2-button" type="button" onClick={() => void validateTemplate()} disabled={busy || dirty}>校验模板</button>
                  <button className="pv2-button-danger" type="button" onClick={() => void executeTemplate()} disabled={busy || !template || template.status === "run_requested" || template.status === "superseded"}>保存并执行</button>
                  <button className="pv2-button-ghost" type="button" onClick={() => void supersedeTemplate()} disabled={busy || template.status === "superseded"}>废弃模板</button>
                </div>
                {dirty ? <div className="pv2-help">当前存在未保存修改；点击执行会先保存再校验。</div> : null}
              </div>
            </div>
          </SectionCard>
        </>
      ) : null}
    </main>
  );
}
