"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { shortHash } from "@/lib/paper-v2/format";
import {
  API_BASE,
  type ArchivePolicy,
  type JsonObject,
  type QETemplate,
  type QETemplateValidation,
  qeTemplatesApi,
} from "@/lib/qe-templates/api";

const EDITABLE_STATUSES = new Set(["draft", "ready_for_review", "approved"]);
const ARCHIVE_POLICIES: ArchivePolicy[] = ["AUTO", "SKIP", "MANUAL_ONLY"];
const LABEL_HORIZONS = [1, 3, 5, 10, 20];
const SPLIT_FIELDS = [
  "train_start",
  "train_end",
  "valid_start",
  "valid_end",
  "test_start",
  "test_end",
  "backtest_end",
];
const SINGLE_RUNTIME_PARAM_KEYS = new Set([
  "archive_policy",
  "archive_reason",
  "backtest_freq",
  "disable_alpha158",
  "enable_sector_hmm",
  "execution_algo",
  "execution_algo_params",
  "filter_suspended_on_signal",
  "hmm_config_json",
  "hmm_model_version_id",
  "hmm_signal_preset",
  "hmm_signal_presets",
  "label_horizon",
  "sector_blacklist",
  "sector_hmm_model_path",
  "stock_pool",
  "suspend_filter_strict",
  "unfilled_backup_depth",
  "unfilled_handler",
  "unfilled_trigger_minute",
]);

type CatalogItem = Record<string, unknown>;

type FieldSchema = {
  type?: string;
  default?: unknown;
  minimum?: number;
  maximum?: number;
  min?: number;
  max?: number;
  enum?: unknown[];
  options?: unknown[];
  description?: string;
  desc?: string;
  title?: string;
};

function isRecord(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asObject(value: unknown): JsonObject {
  return isRecord(value) ? value : {};
}

function asString(value: unknown): string {
  if (value === null || value === undefined) return "";
  return String(value);
}

function asBoolean(value: unknown, fallback = false): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes", "y"].includes(normalized)) return true;
    if (["false", "0", "no", "n"].includes(normalized)) return false;
  }
  return fallback;
}

function toOptionalNumber(value: string): number | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function splitList(value: string): string[] {
  return value.split(/[\n,]/).map((item) => item.trim()).filter(Boolean);
}

function listText(value: unknown): string {
  return Array.isArray(value) ? value.map(String).join("\n") : "";
}

function parseScalar(value: string): unknown {
  const trimmed = value.trim();
  if (!trimmed) return "";
  const lowered = trimmed.toLowerCase();
  if (lowered === "true") return true;
  if (lowered === "false") return false;
  if (lowered === "null") return null;
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed);
  if (trimmed.includes(",") && !trimmed.includes("://")) return splitList(trimmed);
  return value;
}

function scalarText(value: unknown): string {
  if (Array.isArray(value)) return value.map(String).join(", ");
  if (isRecord(value)) {
    return Object.entries(value).map(([key, item]) => `${key}: ${asString(item)}`).join("; ");
  }
  return asString(value);
}

function parseJsonish(value: unknown, fallback: unknown): unknown {
  if (typeof value !== "string") return value ?? fallback;
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return fallback;
  }
}

function withoutKeys(value: JsonObject, keys: Set<string>): JsonObject {
  return Object.fromEntries(Object.entries(value).filter(([key]) => !keys.has(key)));
}

function pickKeys(value: JsonObject, keys: Set<string>): JsonObject {
  return Object.fromEntries(Object.entries(value).filter(([key]) => keys.has(key)));
}

function setField(config: JsonObject, key: string, value: unknown): JsonObject {
  return { ...config, [key]: value };
}

function setObjectField(config: JsonObject, key: string, value: JsonObject): JsonObject {
  return { ...config, [key]: value };
}

function formatDateTime(value: unknown): string {
  const text = String(value || "");
  return text ? text.replace("T", " ").slice(0, 19) : "-";
}

function kindLabel(kind: string): string {
  return kind === "custom_evo" ? "自定义演进" : "QE 单次实验";
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

async function fetchCatalogArray(path: string): Promise<CatalogItem[]> {
  try {
    const response = await fetch(`${API_BASE}${path}`);
    if (!response.ok) return [];
    const payload = await response.json() as unknown;
    if (Array.isArray(payload)) return payload.filter(isRecord);
    if (isRecord(payload)) {
      if (Array.isArray(payload.items)) return payload.items.filter(isRecord);
      if (Array.isArray(payload.data)) return payload.data.filter(isRecord);
      if (Array.isArray(payload.nodes)) return payload.nodes.filter(isRecord);
    }
  } catch {
    return [];
  }
  return [];
}

function catalogValue(item: CatalogItem, primary: string, fallback?: string): string {
  return asString(item[primary] ?? (fallback ? item[fallback] : undefined));
}

function strategyId(item: CatalogItem): string {
  return catalogValue(item, "strategy_id");
}

function strategyLabel(item: CatalogItem): string {
  const id = strategyId(item);
  return asString(item.display_name || item.name || id);
}

function executionAlgoCode(item: CatalogItem): string {
  return catalogValue(item, "algo_code", "code");
}

function executionAlgoLabel(item: CatalogItem): string {
  const code = executionAlgoCode(item);
  return asString(item.algo_name || item.display_name || item.name || code);
}

function nodeId(item: CatalogItem): string {
  return catalogValue(item, "node_id", "id");
}

function nodeLabel(item: CatalogItem): string {
  const id = nodeId(item);
  return asString(item.display_name || item.name || id);
}

function hmmConfigId(item: CatalogItem): string {
  return catalogValue(item, "config_id", "id");
}

function hmmConfigLabel(item: CatalogItem): string {
  const id = hmmConfigId(item);
  return asString(item.display_name || item.name || id);
}

function normalizeFieldSchema(raw: unknown, fallbackType = "string"): FieldSchema {
  if (!isRecord(raw)) return { type: fallbackType };
  const type = asString(raw.type || fallbackType);
  return {
    type,
    default: raw.default,
    minimum: typeof raw.minimum === "number" ? raw.minimum : undefined,
    maximum: typeof raw.maximum === "number" ? raw.maximum : undefined,
    min: typeof raw.min === "number" ? raw.min : undefined,
    max: typeof raw.max === "number" ? raw.max : undefined,
    enum: Array.isArray(raw.enum) ? raw.enum : undefined,
    options: Array.isArray(raw.options) ? raw.options : undefined,
    description: typeof raw.description === "string" ? raw.description : undefined,
    desc: typeof raw.desc === "string" ? raw.desc : undefined,
    title: typeof raw.title === "string" ? raw.title : undefined,
  };
}

function inferSchemaType(value: unknown): string {
  if (typeof value === "boolean") return "boolean";
  if (typeof value === "number") return Number.isInteger(value) ? "integer" : "number";
  return "string";
}

function schemaFromCatalogItem(item: CatalogItem | undefined): Record<string, FieldSchema> {
  if (!item) return {};
  const defaults = asObject(parseJsonish(item.default_kwargs ?? item.default_config ?? {}, {}));
  const rawSchema = parseJsonish(item.param_schema ?? item.schema ?? item.config_schema ?? {}, {});
  const schema: Record<string, FieldSchema> = {};
  if (Array.isArray(rawSchema)) {
    rawSchema.forEach((rawField) => {
      if (!isRecord(rawField)) return;
      const name = asString(rawField.name || rawField.key);
      if (!name) return;
      const field = normalizeFieldSchema(rawField, inferSchemaType(defaults[name]));
      schema[name] = { ...field, default: field.default ?? defaults[name], title: field.title || name };
    });
  } else if (isRecord(rawSchema) && isRecord(rawSchema.properties)) {
    Object.entries(rawSchema.properties).forEach(([key, field]) => {
      schema[key] = normalizeFieldSchema(field, inferSchemaType(defaults[key]));
    });
  } else if (isRecord(rawSchema)) {
    Object.entries(rawSchema).forEach(([key, field]) => {
      schema[key] = normalizeFieldSchema(field, inferSchemaType(defaults[key]));
    });
  }
  Object.entries(defaults).forEach(([key, value]) => {
    if (!schema[key]) schema[key] = { type: inferSchemaType(value), default: value, title: key };
  });
  return schema;
}

function TextField({
  label,
  value,
  onChange,
  disabled,
  placeholder,
  ariaLabel,
  list,
  type = "text",
}: {
  label: string;
  value: unknown;
  onChange: (value: string) => void;
  disabled?: boolean;
  placeholder?: string;
  ariaLabel?: string;
  list?: string;
  type?: string;
}) {
  return (
    <label className="pv2-field">
      <span>{label}</span>
      <input
        aria-label={ariaLabel}
        className="pv2-input"
        disabled={disabled}
        list={list}
        placeholder={placeholder}
        type={type}
        value={asString(value)}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

function TextAreaField({
  label,
  value,
  onChange,
  disabled,
  placeholder,
  ariaLabel,
}: {
  label: string;
  value: unknown;
  onChange: (value: string) => void;
  disabled?: boolean;
  placeholder?: string;
  ariaLabel?: string;
}) {
  return (
    <label className="pv2-field">
      <span>{label}</span>
      <textarea
        aria-label={ariaLabel}
        className="pv2-textarea"
        disabled={disabled}
        placeholder={placeholder}
        value={asString(value)}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

function ToggleField({
  label,
  checked,
  onChange,
  disabled,
}: {
  label: string;
  checked: boolean;
  onChange: (value: boolean) => void;
  disabled?: boolean;
}) {
  return (
    <label className="pv2-chip" style={{ alignItems: "center", display: "inline-flex", gap: 8, width: "fit-content" }}>
      <input checked={checked} disabled={disabled} type="checkbox" onChange={(event) => onChange(event.target.checked)} />
      {label}
    </label>
  );
}

function SelectField({
  label,
  value,
  onChange,
  disabled,
  children,
  ariaLabel,
}: {
  label: string;
  value: unknown;
  onChange: (value: string) => void;
  disabled?: boolean;
  children: React.ReactNode;
  ariaLabel?: string;
}) {
  return (
    <label className="pv2-field">
      <span>{label}</span>
      <select
        aria-label={ariaLabel}
        className="pv2-select"
        disabled={disabled}
        value={asString(value)}
        onChange={(event) => onChange(event.target.value)}
      >
        {children}
      </select>
    </label>
  );
}

function CatalogDatalists({
  strategies,
  executionAlgorithms,
  nodes,
  hmmConfigs,
}: {
  strategies: CatalogItem[];
  executionAlgorithms: CatalogItem[];
  nodes: CatalogItem[];
  hmmConfigs: CatalogItem[];
}) {
  return (
    <>
      <datalist id="qe-template-strategy-options">
        {strategies.map((item) => {
          const id = strategyId(item);
          return id ? <option key={id} value={id}>{strategyLabel(item)}</option> : null;
        })}
      </datalist>
      <datalist id="qe-template-execution-options">
        {executionAlgorithms.map((item) => {
          const code = executionAlgoCode(item);
          return code ? <option key={code} value={code}>{executionAlgoLabel(item)}</option> : null;
        })}
      </datalist>
      <datalist id="qe-template-node-options">
        {nodes.map((item) => {
          const id = nodeId(item);
          return id ? <option key={id} value={id}>{nodeLabel(item)}</option> : null;
        })}
      </datalist>
      <datalist id="qe-template-hmm-config-options">
        {hmmConfigs.map((item) => {
          const id = hmmConfigId(item);
          return id ? <option key={id} value={id}>{hmmConfigLabel(item)}</option> : null;
        })}
      </datalist>
    </>
  );
}

function KeyValueEditor({
  title,
  value,
  onChange,
  disabled,
  addLabel = "添加参数",
}: {
  title: string;
  value: JsonObject;
  onChange: (value: JsonObject) => void;
  disabled?: boolean;
  addLabel?: string;
}) {
  const entries = Object.entries(value);
  const updateKey = (oldKey: string, newKey: string) => {
    const cleanKey = newKey.trim();
    if (!cleanKey || cleanKey === oldKey) return;
    const next: JsonObject = {};
    entries.forEach(([key, item]) => {
      next[key === oldKey ? cleanKey : key] = item;
    });
    onChange(next);
  };
  const updateValue = (key: string, text: string) => {
    onChange({ ...value, [key]: parseScalar(text) });
  };
  const removeKey = (key: string) => {
    onChange(Object.fromEntries(entries.filter(([itemKey]) => itemKey !== key)));
  };
  const addKey = () => {
    let index = entries.length + 1;
    let key = `param_${index}`;
    while (Object.prototype.hasOwnProperty.call(value, key)) {
      index += 1;
      key = `param_${index}`;
    }
    onChange({ ...value, [key]: "" });
  };

  return (
    <div className="pv2-readable-panel">
      <div className="pv2-readable-table">
        <div className="pv2-readable-row">
          <div className="pv2-readable-key">{title}</div>
          <div className="pv2-readable-value">
            <div style={{ display: "grid", gap: 8 }}>
              {entries.length === 0 ? <div className="pv2-help">暂无参数；可按需添加键值字段。</div> : null}
              {entries.map(([key, item]) => (
                <div key={key} style={{ display: "grid", gap: 8, gridTemplateColumns: "minmax(120px, 0.35fr) minmax(180px, 1fr) auto" }}>
                  <input
                    aria-label={`${title} key ${key}`}
                    className="pv2-input"
                    defaultValue={key}
                    disabled={disabled}
                    onBlur={(event) => updateKey(key, event.target.value)}
                    readOnly={disabled}
                  />
                  <input
                    aria-label={`${title} value ${key}`}
                    className="pv2-input"
                    disabled={disabled}
                    value={scalarText(item)}
                    onChange={(event) => updateValue(key, event.target.value)}
                  />
                  <button className="pv2-button-ghost" disabled={disabled} type="button" onClick={() => removeKey(key)}>移除</button>
                </div>
              ))}
              <button className="pv2-button-ghost" disabled={disabled} type="button" onClick={addKey}>{addLabel}</button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function SchemaParamEditor({
  title,
  schema,
  values,
  onChange,
  disabled,
}: {
  title: string;
  schema: Record<string, FieldSchema>;
  values: JsonObject;
  onChange: (value: JsonObject) => void;
  disabled?: boolean;
}) {
  const schemaEntries = Object.entries(schema);
  if (schemaEntries.length === 0) {
    return <KeyValueEditor title={title} value={values} onChange={onChange} disabled={disabled} />;
  }
  const schemaKeys = new Set(schemaEntries.map(([key]) => key));
  const extraParams = Object.fromEntries(Object.entries(values).filter(([key]) => !schemaKeys.has(key)));
  const updateSchemaValue = (key: string, value: unknown) => {
    onChange({ ...values, [key]: value });
  };
  const updateExtra = (nextExtra: JsonObject) => {
    const known = Object.fromEntries(Object.entries(values).filter(([key]) => schemaKeys.has(key)));
    onChange({ ...known, ...nextExtra });
  };

  return (
    <div style={{ display: "grid", gap: 12 }}>
      <div className="pv2-readable-panel">
        <div className="pv2-readable-table">
          <div className="pv2-readable-row">
            <div className="pv2-readable-key">{title}</div>
            <div className="pv2-readable-value">
              <div className="pv2-form-grid">
                {schemaEntries.map(([key, field]) => {
                  const normalizedType = field.type === "bool" ? "boolean" : field.type === "int" ? "integer" : field.type === "float" ? "number" : field.type || "string";
                  const current = values[key] ?? field.default ?? "";
                  const options = field.enum || field.options;
                  const label = field.title || key;
                  if (normalizedType === "boolean") {
                    return (
                      <div className="pv2-field" key={key}>
                        <span>{label}</span>
                        <ToggleField checked={asBoolean(current)} disabled={disabled} label={field.description || field.desc || key} onChange={(next) => updateSchemaValue(key, next)} />
                      </div>
                    );
                  }
                  if (options?.length) {
                    return (
                      <SelectField key={key} label={label} value={current} disabled={disabled} onChange={(next) => updateSchemaValue(key, next)}>
                        {options.map((option) => <option key={asString(option)} value={asString(option)}>{asString(option)}</option>)}
                      </SelectField>
                    );
                  }
                  if (normalizedType === "integer" || normalizedType === "number") {
                    return (
                      <TextField
                        key={key}
                        disabled={disabled}
                        label={label}
                        placeholder={asString(field.default)}
                        type="number"
                        value={current}
                        onChange={(next) => updateSchemaValue(key, normalizedType === "integer" ? toOptionalNumber(next) : toOptionalNumber(next))}
                      />
                    );
                  }
                  return (
                    <TextField
                      key={key}
                      disabled={disabled}
                      label={label}
                      placeholder={asString(field.default)}
                      value={current}
                      onChange={(next) => updateSchemaValue(key, next || undefined)}
                    />
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      </div>
      <KeyValueEditor title={`${title}附加字段`} value={extraParams} onChange={updateExtra} disabled={disabled} />
    </div>
  );
}

function DataSplitEditor({
  value,
  onChange,
  disabled,
}: {
  value: JsonObject;
  onChange: (value: JsonObject) => void;
  disabled?: boolean;
}) {
  const extra = Object.fromEntries(Object.entries(value).filter(([key]) => !SPLIT_FIELDS.includes(key)));
  const update = (key: string, text: string) => {
    onChange({ ...value, [key]: text || undefined });
  };
  const updateExtra = (nextExtra: JsonObject) => {
    const known = Object.fromEntries(Object.entries(value).filter(([key]) => SPLIT_FIELDS.includes(key)));
    onChange({ ...known, ...nextExtra });
  };
  return (
    <div style={{ display: "grid", gap: 12 }}>
      <div className="pv2-form-grid">
        {SPLIT_FIELDS.map((field) => (
          <TextField
            key={field}
            disabled={disabled}
            label={field}
            placeholder="YYYY-MM-DD"
            type="text"
            value={value[field]}
            onChange={(next) => update(field, next)}
          />
        ))}
      </div>
      <KeyValueEditor title="数据切分附加字段" value={extra} onChange={updateExtra} disabled={disabled} />
    </div>
  );
}

function LabelHorizonField({
  label,
  value,
  onChange,
  disabled,
  ariaLabel,
}: {
  label: string;
  value: unknown;
  onChange: (value: number | undefined) => void;
  disabled?: boolean;
  ariaLabel?: string;
}) {
  const text = asString(value);
  const hasCustom = text && !LABEL_HORIZONS.map(String).includes(text);
  return (
    <SelectField ariaLabel={ariaLabel} disabled={disabled} label={label} value={text} onChange={(next) => onChange(toOptionalNumber(next))}>
      <option value="">继承默认</option>
      {LABEL_HORIZONS.map((item) => <option key={item} value={item}>{item} 日</option>)}
      {hasCustom ? <option value={text}>{text} 日</option> : null}
    </SelectField>
  );
}

function PlatformRuntimeNotice() {
  return (
    <div className="pv2-notice pv2-notice-info">
      <div className="pv2-notice-title">平台运行选项</div>
      <div className="pv2-notice-body">
        HMM、ST/PIT 风险过滤、停牌过滤和尾盘未成交处理属于 QE 平台运行配置；这里仅写入待执行模板，保存不会启动实验，也不会写入策略包绑定。
      </div>
    </div>
  );
}

function SingleExperimentEditor({
  config,
  onChange,
  canEdit,
  strategies,
  executionAlgorithms,
}: {
  config: JsonObject;
  onChange: (config: JsonObject) => void;
  canEdit: boolean;
  strategies: CatalogItem[];
  executionAlgorithms: CatalogItem[];
}) {
  const customParams = asObject(config.custom_params);
  const strategyParams = withoutKeys(customParams, SINGLE_RUNTIME_PARAM_KEYS);
  const runtimeParams = pickKeys(customParams, SINGLE_RUNTIME_PARAM_KEYS);
  const strategy = strategies.find((item) => strategyId(item) === asString(config.strategy_id));
  const executionAlgo = executionAlgorithms.find((item) => executionAlgoCode(item) === asString(customParams.execution_algo));
  const strategySchema = schemaFromCatalogItem(strategy);
  const executionSchema = schemaFromCatalogItem(executionAlgo);
  const updateConfig = (key: string, value: unknown) => onChange(setField(config, key, value));
  const updateCustomParams = (next: JsonObject) => onChange(setObjectField(config, "custom_params", next));
  const updateStrategyParams = (nextStrategyParams: JsonObject) => updateCustomParams({ ...runtimeParams, ...nextStrategyParams });
  const updateRuntime = (key: string, value: unknown) => updateCustomParams({ ...customParams, [key]: value });
  const updateLabelHorizon = (value: number | undefined) => {
    onChange({
      ...config,
      label_horizon: value,
      custom_params: { ...customParams, label_horizon: value },
    });
  };

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <div className="pv2-subsection-head">基础实验配置</div>
      <div className="pv2-form-grid">
        <TextField disabled={!canEdit} label="实验名" value={config.experiment_name} onChange={(value) => updateConfig("experiment_name", value || undefined)} />
        <TextField ariaLabel="single model id" disabled={!canEdit} label="模型 ID" value={config.model_id} onChange={(value) => updateConfig("model_id", value || undefined)} />
        <TextField disabled={!canEdit} label="策略 ID" list="qe-template-strategy-options" value={config.strategy_id} onChange={(value) => updateConfig("strategy_id", value || undefined)} />
        <TextField disabled={!canEdit} label="调度模式" value={config.dispatch_mode} placeholder="normal / evolution" onChange={(value) => updateConfig("dispatch_mode", value || undefined)} />
        <LabelHorizonField ariaLabel="single label horizon" disabled={!canEdit} label="标签周期" value={customParams.label_horizon ?? config.label_horizon} onChange={updateLabelHorizon} />
        <TextField disabled={!canEdit} label="执行节点" list="qe-template-node-options" value={config.node_id} placeholder="local / wsl / remote" onChange={(value) => updateConfig("node_id", value || undefined)} />
      </div>
      <TextAreaField
        ariaLabel="single factor names"
        disabled={!canEdit}
        label="因子列表"
        placeholder="每行一个因子，或用逗号分隔"
        value={listText(config.factor_names)}
        onChange={(value) => updateConfig("factor_names", splitList(value))}
      />
      <KeyValueEditor title="因子来源" value={asObject(config.factor_sources)} onChange={(next) => updateConfig("factor_sources", next)} disabled={!canEdit} />

      <div className="pv2-subsection-head">数据切分</div>
      <DataSplitEditor value={asObject(config.data_split)} onChange={(next) => updateConfig("data_split", next)} disabled={!canEdit} />

      <div className="pv2-subsection-head">策略参数</div>
      <SchemaParamEditor title="策略参数" schema={strategySchema} values={strategyParams} onChange={updateStrategyParams} disabled={!canEdit} />

      <div className="pv2-subsection-head">执行算法</div>
      <div className="pv2-form-grid">
        <TextField
          ariaLabel="single execution algo"
          disabled={!canEdit}
          label="执行算法"
          list="qe-template-execution-options"
          value={customParams.execution_algo}
          placeholder="TWAP / CLOSE_PRICE / ..."
          onChange={(value) => updateRuntime("execution_algo", value || undefined)}
        />
        <SelectField disabled={!canEdit} label="尾盘未成交处理" value={config.unfilled_handler} onChange={(value) => updateConfig("unfilled_handler", value || undefined)}>
          <option value="">不启用</option>
          <option value="TAIL_BOOST">TAIL_BOOST</option>
          <option value="TAIL_SUBSTITUTE">TAIL_SUBSTITUTE</option>
        </SelectField>
      </div>
      <SchemaParamEditor title="执行算法参数" schema={executionSchema} values={asObject(customParams.execution_algo_params)} onChange={(next) => updateRuntime("execution_algo_params", next)} disabled={!canEdit} />
      <KeyValueEditor title="尾盘处理参数" value={asObject(config.unfilled_handler_params)} onChange={(next) => updateConfig("unfilled_handler_params", next)} disabled={!canEdit} />

      <div className="pv2-subsection-head">平台风险与 HMM</div>
      <PlatformRuntimeNotice />
      <div className="pv2-form-grid">
        <div className="pv2-field">
          <span>HMM 开关</span>
          <ToggleField disabled={!canEdit} label="启用行业 HMM 热度调整" checked={asBoolean(customParams.enable_sector_hmm)} onChange={(value) => updateRuntime("enable_sector_hmm", value)} />
        </div>
        <TextField disabled={!canEdit} label="HMM 配置" list="qe-template-hmm-config-options" value={customParams.hmm_config_id} onChange={(value) => updateRuntime("hmm_config_id", value || undefined)} />
        <TextField disabled={!canEdit} label="HMM 快照 / 版本" value={customParams.hmm_model_version_id} onChange={(value) => updateRuntime("hmm_model_version_id", value || undefined)} />
        <SelectField disabled={!canEdit} label="HMM 信号档位" value={customParams.hmm_signal_preset} onChange={(value) => updateRuntime("hmm_signal_preset", value || undefined)}>
          <option value="">继承默认</option>
          <option value="preset_A">preset_A</option>
          <option value="preset_B">preset_B</option>
        </SelectField>
        <TextField disabled={!canEdit} label="股票池 / 风险文件" value={customParams.stock_pool} onChange={(value) => updateRuntime("stock_pool", value || undefined)} />
        <TextAreaField disabled={!canEdit} label="行业黑名单" value={listText(customParams.sector_blacklist)} placeholder="每行一个行业或板块" onChange={(value) => updateRuntime("sector_blacklist", splitList(value))} />
      </div>
      <div className="pv2-chip-row">
        <ToggleField disabled={!canEdit} label="信号日过滤停牌/ST/PIT 风险" checked={asBoolean(customParams.filter_suspended_on_signal)} onChange={(value) => updateRuntime("filter_suspended_on_signal", value)} />
        <ToggleField disabled={!canEdit} label="严格停牌过滤" checked={asBoolean(customParams.suspend_filter_strict, true)} onChange={(value) => updateRuntime("suspend_filter_strict", value)} />
        <ToggleField disabled={!canEdit} label="禁用 Alpha158 基线" checked={asBoolean(customParams.disable_alpha158)} onChange={(value) => updateRuntime("disable_alpha158", value)} />
      </div>
    </div>
  );
}

function loopsFromConfig(config: JsonObject): JsonObject[] {
  return Array.isArray(config.loops) ? config.loops.map(asObject) : [];
}

function CustomEvoEditor({
  config,
  onChange,
  canEdit,
  strategies,
  executionAlgorithms,
}: {
  config: JsonObject;
  onChange: (config: JsonObject) => void;
  canEdit: boolean;
  strategies: CatalogItem[];
  executionAlgorithms: CatalogItem[];
}) {
  const loops = loopsFromConfig(config);
  const updateConfig = (key: string, value: unknown) => onChange(setField(config, key, value));
  const updateLoop = (index: number, updates: JsonObject) => {
    const nextLoops = loops.map((loop, loopIndex) => loopIndex === index ? { ...loop, ...updates } : loop);
    onChange({ ...config, loops: nextLoops });
  };
  const addLoop = () => {
    const nextLoop: JsonObject = {
      label: `Loop ${loops.length + 1}`,
      factor_keys: [],
      model_id: "",
      strategy_params: {},
      execution_algo_params: {},
      label_horizon: 1,
      suspend_filter_strict: true,
      hmm_signal_preset: "preset_A",
    };
    onChange({ ...config, loops: [...loops, nextLoop] });
  };
  const removeLoop = (index: number) => {
    onChange({ ...config, loops: loops.filter((_, loopIndex) => loopIndex !== index) });
  };

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <div className="pv2-subsection-head">任务级配置</div>
      <div className="pv2-form-grid">
        <TextField disabled={!canEdit} label="任务名" value={config.task_name} onChange={(value) => updateConfig("task_name", value || undefined)} />
        <TextField disabled={!canEdit} label="基础实验" value={config.base_experiment_id} onChange={(value) => updateConfig("base_experiment_id", value || undefined)} />
        <TextField disabled={!canEdit} label="克隆来源任务" value={config.clone_from_task_id} onChange={(value) => updateConfig("clone_from_task_id", value || undefined)} />
        <SelectField disabled={!canEdit} label="执行模式" value={config.execution_mode || "serial"} onChange={(value) => updateConfig("execution_mode", value || undefined)}>
          <option value="serial">serial</option>
          <option value="parallel_2">parallel_2</option>
          <option value="parallel_4">parallel_4</option>
          <option value="parallel_8">parallel_8</option>
        </SelectField>
        <TextField disabled={!canEdit} label="默认节点" list="qe-template-node-options" value={config.node_id} onChange={(value) => updateConfig("node_id", value || undefined)} />
        <TextField disabled={!canEdit} label="节点并行度" type="number" value={config.node_parallelism} onChange={(value) => updateConfig("node_parallelism", toOptionalNumber(value))} />
      </div>
      <TextAreaField disabled={!canEdit} label="演进目标" value={config.target_desc} onChange={(value) => updateConfig("target_desc", value || undefined)} />

      <div className="pv2-subsection-head">Loop 配置</div>
      <div className="pv2-notice pv2-notice-info">
        <div className="pv2-notice-title">Loop 可读编辑</div>
        <div className="pv2-notice-body">每个 Loop 可独立调整因子、模型、策略、执行算法、标签周期、数据切分、平台 HMM 和 ST/PIT 过滤；保存只更新待执行模板。</div>
      </div>
      <div style={{ display: "grid", gap: 12 }}>
        {loops.length === 0 ? <div className="pv2-help">暂无 Loop。点击“添加 Loop”创建第一条配置。</div> : null}
        {loops.map((loop, index) => (
          <LoopEditor
            key={index}
            index={index}
            loop={loop}
            canEdit={canEdit}
            strategies={strategies}
            executionAlgorithms={executionAlgorithms}
            onChange={(updates) => updateLoop(index, updates)}
            onRemove={() => removeLoop(index)}
            canRemove={loops.length > 1}
          />
        ))}
        <button className="pv2-button-ghost" disabled={!canEdit} type="button" onClick={addLoop}>添加 Loop</button>
      </div>
    </div>
  );
}

function LoopEditor({
  index,
  loop,
  onChange,
  onRemove,
  canRemove,
  canEdit,
  strategies,
  executionAlgorithms,
}: {
  index: number;
  loop: JsonObject;
  onChange: (updates: JsonObject) => void;
  onRemove: () => void;
  canRemove: boolean;
  canEdit: boolean;
  strategies: CatalogItem[];
  executionAlgorithms: CatalogItem[];
}) {
  const loopNo = index + 1;
  const strategy = strategies.find((item) => strategyId(item) === asString(loop.strategy_id));
  const executionAlgo = executionAlgorithms.find((item) => executionAlgoCode(item) === asString(loop.execution_algo));
  const strategySchema = schemaFromCatalogItem(strategy);
  const executionSchema = schemaFromCatalogItem(executionAlgo);
  return (
    <details className="pv2-readable-item" open>
      <summary>Loop {loopNo}: {asString(loop.label) || asString(loop.model_id) || "未命名"}</summary>
      <div style={{ display: "grid", gap: 14, marginTop: 12 }}>
        <div className="pv2-form-grid">
          <TextField ariaLabel={`loop ${loopNo} label`} disabled={!canEdit} label="Loop 说明" value={loop.label} onChange={(value) => onChange({ label: value || undefined })} />
          <TextField ariaLabel={`loop ${loopNo} model id`} disabled={!canEdit} label="模型 ID" value={loop.model_id} onChange={(value) => onChange({ model_id: value || undefined })} />
          <TextField disabled={!canEdit} label="策略 ID" list="qe-template-strategy-options" value={loop.strategy_id} onChange={(value) => onChange({ strategy_id: value || undefined })} />
          <TextField disabled={!canEdit} label="执行算法" list="qe-template-execution-options" value={loop.execution_algo} onChange={(value) => onChange({ execution_algo: value || undefined })} />
          <TextField disabled={!canEdit} label="节点" list="qe-template-node-options" value={loop.node_id} onChange={(value) => onChange({ node_id: value || undefined })} />
          <TextField disabled={!canEdit} label="Seed" type="number" value={loop.seed ?? loop.random_seed} onChange={(value) => onChange({ seed: toOptionalNumber(value) })} />
          <SelectField disabled={!canEdit} label="标签类型" value={loop.label_type} onChange={(value) => onChange({ label_type: value || undefined })}>
            <option value="">默认</option>
            <option value="return">return</option>
            <option value="excess_return">excess_return</option>
            <option value="rank_return">rank_return</option>
          </SelectField>
          <LabelHorizonField ariaLabel={`loop ${loopNo} label horizon`} disabled={!canEdit} label="标签周期" value={loop.label_horizon} onChange={(value) => onChange({ label_horizon: value })} />
          <TextField disabled={!canEdit} label="股票池 / 风险文件" value={loop.stock_pool} onChange={(value) => onChange({ stock_pool: value || undefined })} />
        </div>
        <TextAreaField
          ariaLabel={`loop ${loopNo} factor keys`}
          disabled={!canEdit}
          label="因子列表"
          placeholder="每行一个 factor_key，或用逗号分隔"
          value={listText(loop.factor_keys ?? loop.factor_names)}
          onChange={(value) => onChange({ factor_keys: splitList(value) })}
        />
        <div className="pv2-chip-row">
          <ToggleField disabled={!canEdit} label="禁用 Alpha158 基线" checked={asBoolean(loop.disable_alpha158)} onChange={(value) => onChange({ disable_alpha158: value })} />
          <ToggleField disabled={!canEdit} label="信号日过滤停牌/ST/PIT 风险" checked={asBoolean(loop.filter_suspended_on_signal)} onChange={(value) => onChange({ filter_suspended_on_signal: value })} />
          <ToggleField disabled={!canEdit} label="严格停牌过滤" checked={asBoolean(loop.suspend_filter_strict, true)} onChange={(value) => onChange({ suspend_filter_strict: value })} />
          <ToggleField disabled={!canEdit} label="仅回测复用既有模型" checked={asBoolean(loop.backtest_only)} onChange={(value) => onChange({ backtest_only: value })} />
        </div>

        <div className="pv2-subsection-head">Loop 数据切分</div>
        <DataSplitEditor value={asObject(loop.data_split)} onChange={(next) => onChange({ data_split: next })} disabled={!canEdit} />

        <div className="pv2-subsection-head">Loop 策略与执行参数</div>
        <SchemaParamEditor title={`Loop ${loopNo} 策略参数`} schema={strategySchema} values={asObject(loop.strategy_params)} onChange={(next) => onChange({ strategy_params: next })} disabled={!canEdit} />
        <SchemaParamEditor title={`Loop ${loopNo} 执行参数`} schema={executionSchema} values={asObject(loop.execution_algo_params)} onChange={(next) => onChange({ execution_algo_params: next })} disabled={!canEdit} />

        <div className="pv2-subsection-head">Loop 平台运行选项</div>
        <PlatformRuntimeNotice />
        <div className="pv2-form-grid">
          <div className="pv2-field">
            <span>HMM 开关</span>
            <ToggleField disabled={!canEdit} label="启用行业 HMM 热度调整" checked={asBoolean(loop.enable_sector_hmm)} onChange={(value) => onChange({ enable_sector_hmm: value })} />
          </div>
          <TextField disabled={!canEdit} label="HMM 配置" list="qe-template-hmm-config-options" value={loop.hmm_config_id} onChange={(value) => onChange({ hmm_config_id: value || undefined })} />
          <TextField disabled={!canEdit} label="HMM 快照 / 版本" value={loop.hmm_model_version_id} onChange={(value) => onChange({ hmm_model_version_id: value || undefined })} />
          <SelectField disabled={!canEdit} label="HMM 信号档位" value={loop.hmm_signal_preset} onChange={(value) => onChange({ hmm_signal_preset: value || undefined })}>
            <option value="">继承默认</option>
            <option value="preset_A">preset_A</option>
            <option value="preset_B">preset_B</option>
          </SelectField>
          <SelectField disabled={!canEdit} label="尾盘未成交处理" value={loop.unfilled_handler} onChange={(value) => onChange({ unfilled_handler: value || undefined })}>
            <option value="">不启用</option>
            <option value="TAIL_BOOST">TAIL_BOOST</option>
            <option value="TAIL_SUBSTITUTE">TAIL_SUBSTITUTE</option>
          </SelectField>
          <TextAreaField disabled={!canEdit} label="行业黑名单" value={listText(loop.sector_blacklist)} onChange={(value) => onChange({ sector_blacklist: splitList(value) })} />
        </div>
        <KeyValueEditor title={`Loop ${loopNo} 尾盘处理参数`} value={asObject(loop.unfilled_handler_params)} onChange={(next) => onChange({ unfilled_handler_params: next })} disabled={!canEdit} />

        {asBoolean(loop.backtest_only) ? (
          <div className="pv2-form-grid">
            <TextField disabled={!canEdit} label="源模型任务 ID" value={loop.model_source_task_id} onChange={(value) => onChange({ model_source_task_id: value || undefined })} />
            <TextField disabled={!canEdit} label="源模型 Loop 序号" type="number" value={loop.model_source_loop_index} onChange={(value) => onChange({ model_source_loop_index: toOptionalNumber(value) })} />
          </div>
        ) : null}

        <div className="pv2-row-actions">
          <button className="pv2-button-ghost" disabled={!canEdit || !canRemove} type="button" onClick={onRemove}>删除 Loop</button>
        </div>
      </div>
    </details>
  );
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
  const [config, setConfig] = useState<JsonObject>({});
  const [dirty, setDirty] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [validation, setValidation] = useState<QETemplateValidation | null>(null);
  const [runMessage, setRunMessage] = useState<string | null>(null);
  const [strategies, setStrategies] = useState<CatalogItem[]>([]);
  const [executionAlgorithms, setExecutionAlgorithms] = useState<CatalogItem[]>([]);
  const [nodes, setNodes] = useState<CatalogItem[]>([]);
  const [hmmConfigs, setHmmConfigs] = useState<CatalogItem[]>([]);

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
      setConfig(row.config_json || {});
      setValidation(validationFromTemplate(row));
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

  useEffect(() => {
    let cancelled = false;
    async function loadCatalogs() {
      const [strategyRows, executionRows, nodeRows, hmmRows] = await Promise.all([
        fetchCatalogArray("/quantevolver/strategies?limit=100"),
        fetchCatalogArray("/quantevolver/execution-algorithms"),
        fetchCatalogArray("/dispatch/nodes"),
        fetchCatalogArray("/hmm-training/configs?model_type=sector_hmm"),
      ]);
      if (cancelled) return;
      setStrategies(strategyRows);
      setExecutionAlgorithms(executionRows);
      setNodes(nodeRows);
      setHmmConfigs(hmmRows);
    }
    void loadCatalogs();
    return () => { cancelled = true; };
  }, []);

  const canEdit = template ? EDITABLE_STATUSES.has(template.status) : false;
  const resultHref = targetHref(template);
  const loopCount = useMemo(() => loopsFromConfig(config).length, [config]);

  function updateConfig(next: JsonObject) {
    setConfig(next);
    setDirty(true);
  }

  async function saveTemplate(): Promise<QETemplate | null> {
    if (!template || !canEdit) return template;
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
      setConfig(row.config_json || {});
      setValidation(validationFromTemplate(row));
      setDirty(false);
      setRunMessage("模板配置已保存，尚未执行。");
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
      setConfig(result.template.config_json || {});
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
      setRunMessage("校验未通过，已阻止执行。请修正配置后重新校验。");
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
      setConfig(refreshed.config_json || {});
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
      setConfig(row.config_json || {});
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
      <CatalogDatalists strategies={strategies} executionAlgorithms={executionAlgorithms} nodes={nodes} hmmConfigs={hmmConfigs} />
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
      {runMessage ? <div className="pv2-notice pv2-notice-success"><div className="pv2-notice-title">操作结果</div><div className="pv2-notice-body">{runMessage}</div></div> : null}

      {template ? (
        <>
          <div className="pv2-grid pv2-grid-4">
            <MetricCard label="模板类型" value={kindLabel(template.template_kind)} hint={template.template_id} tone="info" />
            <MetricCard label="模板状态" value={template.status} hint="执行前可审查" tone={canEdit ? "warning" : "neutral"} />
            <MetricCard label="数仓策略" value={archivePolicy} hint={archiveReason || "默认策略"} />
            <MetricCard label="运行关联" value={shortHash(template.submitted_experiment_id || template.submitted_task_id)} hint={template.template_kind === "custom_evo" ? `${loopCount} loops` : "single"} />
          </div>

          <SectionCard title="人工审查信息" eyebrow="database template / no execution on save">
            <div className="pv2-form-grid">
              <TextField label="标题" value={title} disabled={!canEdit} onChange={(value) => { setTitle(value); setDirty(true); }} />
              <TextField label="模板类型" value={kindLabel(template.template_kind)} disabled onChange={() => undefined} />
              <TextField label="来源" value={`${template.created_by_type || "-"} / ${template.created_by_name || "-"}`} disabled onChange={() => undefined} />
              <SelectField label="数仓策略" value={archivePolicy} disabled={!canEdit} onChange={(value) => { setArchivePolicy(value as ArchivePolicy); setDirty(true); }}>
                {ARCHIVE_POLICIES.map((policy) => <option key={policy} value={policy}>{policy}</option>)}
              </SelectField>
              <TextField label="数仓策略原因" value={archiveReason} disabled={!canEdit} onChange={(value) => { setArchiveReason(value); setDirty(true); }} />
              <TextField label="更新时间" value={formatDateTime(template.updated_at)} disabled onChange={() => undefined} />
            </div>
            <div className="pv2-form-grid" style={{ marginTop: 12 }}>
              <TextAreaField label="描述" value={description} disabled={!canEdit} onChange={(value) => { setDescription(value); setDirty(true); }} />
              <TextAreaField label="MCP 分析摘要" value={analysisSummary} disabled={!canEdit} onChange={(value) => { setAnalysisSummary(value); setDirty(true); }} />
              <TextAreaField label="风险说明" value={riskSummary} disabled={!canEdit} onChange={(value) => { setRiskSummary(value); setDirty(true); }} />
            </div>
            {!canEdit ? <div className="pv2-help">当前状态不可原地修改配置。如需调整已物化或已执行模板，请由 MCP 或后续复制功能创建新的待执行模板。</div> : null}
          </SectionCard>

          <SectionCard title="结构化配置编辑" eyebrow="human-readable template controls">
            <div className="pv2-notice pv2-notice-info">
              <div className="pv2-notice-title">编辑边界</div>
              <div className="pv2-notice-body">
                下方字段直接更新待执行模板的配置内容。保存只写模板表，不校验、不审批、不物化、不执行；点击执行才会按保存、校验、审批、物化、运行的顺序调用现有 QE 执行层。
              </div>
            </div>
            {template.template_kind === "single_experiment" ? (
              <SingleExperimentEditor
                config={config}
                onChange={updateConfig}
                canEdit={canEdit}
                strategies={strategies}
                executionAlgorithms={executionAlgorithms}
              />
            ) : (
              <CustomEvoEditor
                config={config}
                onChange={updateConfig}
                canEdit={canEdit}
                strategies={strategies}
                executionAlgorithms={executionAlgorithms}
              />
            )}
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
