"use client";

import { useState, type ReactNode } from "react";

import type { JsonObject } from "@/lib/research-assistant/api";

const FIELD_LABELS: Record<string, string> = {
  action_proposal_id: "动作提案",
  approval_id: "审批",
  approval_required: "需要审批",
  as_of: "截至时间",
  capability_key: "能力",
  created_at: "创建时间",
  duration_ms: "耗时",
  error: "错误",
  error_message: "错误消息",
  event_type: "事件",
  exception_type: "异常类型",
  message: "消息",
  missing_confirmations: "缺少确认",
  next_step: "下一步",
  operator_action: "操作建议",
  provenance: "来源",
  reason: "原因",
  reason_code: "原因",
  risk_level: "风险",
  server_key: "服务",
  side_effect: "副作用",
  side_effect_level: "副作用",
  source: "来源",
  source_ref: "来源引用",
  server: "服务",
  status: "状态",
  task_id: "任务",
  title: "标题",
  tool: "工具",
  tool_name: "工具",
  trace_id: "Trace ID",
  updated_at: "更新时间",
};

const HUMAN_MAX_DEPTH = 3;
const HUMAN_MAX_FIELDS = 24;
const HUMAN_MAX_ARRAY_ITEMS = 8;
const HUMAN_MAX_TEXT = 180;

export function display(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "number" || typeof value === "bigint") return String(value);
  if (value instanceof Date) return formatDateTime(value.toISOString());
  if (typeof value === "string") return normalizeDisplayString(value);
  if (Array.isArray(value)) {
    if (!value.length) return "-";
    if (value.every(isPrimitive)) {
      const shown = value.slice(0, HUMAN_MAX_ARRAY_ITEMS).map((item) => display(item)).join(" / ");
      return value.length > HUMAN_MAX_ARRAY_ITEMS ? `${shown} / 其余 ${value.length - HUMAN_MAX_ARRAY_ITEMS} 项已省略` : shown;
    }
    return `${value.length} 项`;
  }
  if (value instanceof Error) return value.message || value.name;
  if (typeof value === "object") return `${Object.keys(value as Record<string, unknown>).length} 个字段`;
  return String(value);
}

export function formatDateTime(value: unknown): string {
  const text = String(value || "");
  return text ? text.replace("T", " ").replace(/Z$/, "").slice(0, 19) : "-";
}

export function humanizeValue(value: unknown, key = "", depth = 0): ReactNode {
  try {
    if (isPrimitive(value)) return displayValueForKey(value, key);
    if (typeof value === "bigint") return String(value);
    if (value instanceof Date) return formatDateTime(value.toISOString());
    if (value instanceof Error) return <HumanizedFields value={errorToRecord(value)} depth={depth + 1} />;
    if (Array.isArray(value)) return humanizeArray(value, key, depth);
    if (value && typeof value === "object") {
      if (depth >= HUMAN_MAX_DEPTH) return display(value);
      return <HumanizedFields value={value as Record<string, unknown>} depth={depth + 1} />;
    }
    return display(value);
  } catch {
    return <span>无法预览，请查看原始数据/开发者。</span>;
  }
}

function displayValueForKey(value: string | number | boolean | null | undefined, key: string): string {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "number") return String(value);
  const text = normalizeDisplayString(value);
  return isDateLikeKey(key) || isIsoDateString(text) ? formatDateTime(text) : text;
}

function normalizeDisplayString(value: string): string {
  const text = String(value);
  if (isIsoDateString(text)) return formatDateTime(text);
  return truncateText(text);
}

function truncateText(text: string): string {
  return text.length > HUMAN_MAX_TEXT ? `${text.slice(0, HUMAN_MAX_TEXT)}…` : text;
}

function humanizeArray(value: unknown[], key: string, depth: number): ReactNode {
  if (!value.length) return "-";
  if (value.every(isPrimitive)) {
    const shown = value.slice(0, HUMAN_MAX_ARRAY_ITEMS).map((item) => displayValueForKey(item, key)).join(" / ");
    return value.length > HUMAN_MAX_ARRAY_ITEMS ? `${shown} / 其余 ${value.length - HUMAN_MAX_ARRAY_ITEMS} 项已省略` : shown;
  }
  const rows = value.slice(0, HUMAN_MAX_ARRAY_ITEMS);
  return (
    <div className="ra-json-summary">
      {rows.map((item, index) => (
        <div className="ra-json-summary-row" key={`${key || "item"}-${index}`}>
          <span>{`第 ${index + 1} 项`}</span>
          <div>{humanizeValue(item, key, depth + 1)}</div>
        </div>
      ))}
      {value.length > rows.length ? (
        <div className="ra-json-summary-row">
          <span>已省略</span>
          <strong>{`其余 ${value.length - rows.length} 项`}</strong>
        </div>
      ) : null}
    </div>
  );
}

function HumanizedFields({ value, depth }: { value: Record<string, unknown>; depth: number }) {
  const entries = Object.entries(value).slice(0, HUMAN_MAX_FIELDS);
  const extra = Object.keys(value).length - entries.length;
  if (!entries.length) return <span>-</span>;
  return (
    <div className="ra-json-summary">
      {entries.map(([key, item]) => {
        const primitive = isPrimitive(item) || typeof item === "bigint" || item instanceof Date;
        return (
          <div className="ra-json-summary-row" key={key}>
            <span>{labelForKey(key)}</span>
            {primitive ? <strong>{displayValueForKey(item as string | number | boolean | null | undefined, key)}</strong> : <div>{humanizeValue(item, key, depth)}</div>}
          </div>
        );
      })}
      {extra > 0 ? (
        <div className="ra-json-summary-row">
          <span>已省略</span>
          <strong>{`其余 ${extra} 个字段`}</strong>
        </div>
      ) : null}
    </div>
  );
}

function summarizeJson(value: unknown, depth = 0, seen = new WeakSet<object>()): unknown {
  if (depth >= HUMAN_MAX_DEPTH) return "...";
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Error) return errorToRecord(value);
  if (Array.isArray(value)) {
    const preview = value.slice(0, HUMAN_MAX_ARRAY_ITEMS).map((item) => summarizeJson(item, depth + 1, seen));
    return value.length > HUMAN_MAX_ARRAY_ITEMS ? [...preview, `其余 ${value.length - HUMAN_MAX_ARRAY_ITEMS} 项已省略`] : preview;
  }
  if (value && typeof value === "object") {
    if (seen.has(value)) return "[Circular]";
    seen.add(value);
    const entries = Object.entries(value as Record<string, unknown>).slice(0, HUMAN_MAX_FIELDS);
    const result: Record<string, unknown> = {};
    for (const [key, item] of entries) result[key] = summarizeJson(item, depth + 1, seen);
    const extra = Object.keys(value as Record<string, unknown>).length - entries.length;
    if (extra > 0) result.__omitted__ = `其余 ${extra} 个字段已省略`;
    return result;
  }
  return value;
}

function safeStringify(value: unknown): string {
  try {
    const seen = new WeakSet<object>();
    const text = JSON.stringify(value, (_key, item: unknown) => {
      if (typeof item === "bigint") return item.toString();
      if (item instanceof Error) return errorToRecord(item);
      if (item && typeof item === "object") {
        if (seen.has(item)) return "[Circular]";
        seen.add(item);
      }
      return item;
    }, 2);
    return text === undefined ? display(value) : text;
  } catch {
    return String(value);
  }
}

function RawJsonDetails({ value }: { value: unknown }) {
  const [open, setOpen] = useState(false);
  return (
    <details className="ra-detail-drawer" onToggle={(event) => setOpen(event.currentTarget.open)}>
      <summary>查看原始数据/开发者</summary>
      {open ? <pre className="ra-json-preview">{safeStringify(value)}</pre> : <p className="ra-muted">展开后显示原始数据。</p>}
    </details>
  );
}

function JsonPreview({ value }: { value: unknown }) {
  return (
    <div>
      <div className="ra-json-summary">
        <div className="ra-json-summary-row">
          <span>内容</span>
          <div>{humanizeValue(value)}</div>
        </div>
      </div>
      <RawJsonDetails value={value} />
    </div>
  );
}

function labelForKey(key: string): string {
  const normalized = key.trim();
  if (FIELD_LABELS[normalized]) return FIELD_LABELS[normalized];
  return normalized
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function isDateLikeKey(key: string): boolean {
  return /(^|_)(as_of|created_at|updated_at|timestamp|time|date|loaded_at|started_at|finished_at|closed_at)$/i.test(key);
}

function isIsoDateString(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value);
}

function errorToRecord(error: Error): Record<string, unknown> {
  return {
    name: error.name,
    message: error.message,
    stack: error.stack,
  };
}

export function EmptyState({ title = "暂无数据", hint = "当前 API 返回空数据；这不是静态 mock，后端写入后会在此展示。" }: { title?: string; hint?: string }) {
  return (
    <div className="ra-empty">
      <strong>{title}</strong>
      <p>{hint}</p>
    </div>
  );
}

export function DetailDrawer({ title, data }: { title: string; data: unknown }) {
  return (
    <details className="ra-detail-drawer">
      <summary>{title}</summary>
      <JsonPreview value={data} />
    </details>
  );
}

export function DiagnosticLogBlock({ title, data, lines, testId = "ra-diagnostic-log" }: { title: string; data?: unknown; lines?: string[]; testId?: string }) {
  const visibleLines = (lines || []).filter(Boolean);
  const hasData = data !== undefined && data !== null && data !== "";
  if (!visibleLines.length && !hasData) return null;
  return (
    <section className="ra-error ra-diagnostic-log" data-testid={testId} aria-label={title}>
      <strong>{title}</strong>
      {visibleLines.length ? (
        <div className="ra-json-summary">
          {visibleLines.map((line, index) => (
            <div className="ra-json-summary-row" key={`${line}-${index}`}>
              <span>{`日志 ${index + 1}`}</span>
              <strong>{truncateText(line)}</strong>
            </div>
          ))}
        </div>
      ) : null}
      {hasData ? <JsonPreview value={data} /> : null}
    </section>
  );
}

export function KeyValueGrid({ rows }: { rows: Array<[string, unknown]> }) {
  return (
    <div className="ra-key-value-grid">
      {rows.map(([key, value]) => (
        <div className="ra-key-value-row" key={key}>
          <div className="ra-key-value-key">{key}</div>
          <div className="ra-key-value-value">{isPrimitive(value) ? display(value) : <JsonPreview value={value} />}</div>
        </div>
      ))}
    </div>
  );
}

function isPrimitive(value: unknown): value is string | number | boolean | null | undefined {
  return value === null || value === undefined || ["string", "number", "boolean"].includes(typeof value);
}

function statusTone(status: string): string {
  const value = status.toLowerCase();
  if (["ready", "enabled", "approved", "ok", "success", "succeeded", "done", "current"].includes(value)) return "success";
  if (["failed", "error", "blocked", "disabled", "danger", "rejected"].includes(value)) return "danger";
  if (["pending", "waiting", "warning", "initializing", "unknown"].includes(value)) return "warning";
  return "neutral";
}

export function StatusPill({ status, children }: { status: unknown; children?: ReactNode }) {
  const fallback = display(status);
  return <span className={`ra-status-pill ra-status-pill-${statusTone(String(status || fallback || "unknown"))}`}>{children ?? fallback}</span>;
}

export function StatusChips({ counts }: { counts?: Record<string, number> }) {
  const entries = Object.entries(counts || {});
  if (!entries.length) return <span className="ra-muted">暂无状态计数</span>;
  return (
    <div className="ra-chip-row">
      {entries.map(([status, count]) => (
        <StatusPill status={status} key={status}>{`${status}: ${count}`}</StatusPill>
      ))}
    </div>
  );
}

export function ApiErrorBox({ error, title = "研究助理 API 读取失败" }: { error: unknown; title?: string }) {
  if (!error) return null;
  const message = error instanceof Error ? error.message : String(error);
  return (
    <div className="ra-error" role="alert">
      <strong>{title}</strong>
      <p>{message}</p>
      <p className="ra-muted">如果是 schema_missing，请先在明确授权后执行数据库迁移；页面不会静默使用 mock 数据冒充成功。</p>
    </div>
  );
}

export function asObject(value: unknown): JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as JsonObject : {};
}
