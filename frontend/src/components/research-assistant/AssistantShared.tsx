"use client";

import type { ReactNode } from "react";

import type { JsonObject } from "@/lib/research-assistant/api";

export function display(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "boolean") return value ? "是" : "否";
  if (Array.isArray(value)) return value.length ? value.map((item) => display(item)).join(" / ") : "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

export function formatDateTime(value: unknown): string {
  const text = String(value || "");
  return text ? text.replace("T", " ").slice(0, 19) : "-";
}

function summarizeJson(value: unknown, depth = 0): unknown {
  if (depth >= 3) return "...";
  if (Array.isArray(value)) {
    const preview = value.slice(0, 8).map((item) => summarizeJson(item, depth + 1));
    return value.length > 8 ? [...preview, `... 其余 ${value.length - 8} 项已省略`] : preview;
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).slice(0, 24);
    const result: Record<string, unknown> = {};
    for (const [key, item] of entries) result[key] = summarizeJson(item, depth + 1);
    const extra = Object.keys(value as Record<string, unknown>).length - entries.length;
    if (extra > 0) result.__omitted__ = `其余 ${extra} 个字段已省略`;
    return result;
  }
  return value;
}

function JsonPreview({ value }: { value: unknown }) {
  const summaryEntries = value && typeof value === "object" && !Array.isArray(value)
    ? Object.entries(value as Record<string, unknown>).slice(0, 24)
    : [];
  return (
    <div>
      {summaryEntries.length ? (
        <div className="ra-json-summary">
          {summaryEntries.map(([key, item]) => (
            <div className="ra-json-summary-row" key={key}>
              <span>{labelForKey(key)}</span>
              <strong>{isPrimitive(item) ? display(item) : Array.isArray(item) ? `${item.length} 项` : "对象"}</strong>
            </div>
          ))}
        </div>
      ) : null}
      <pre className="ra-json-preview">
        {JSON.stringify(summarizeJson(value), null, 2)}
      </pre>
    </div>
  );
}

function diagnosticLogText(data: unknown): string {
  if (typeof data === "string") return data;
  try {
    return JSON.stringify(data, null, 2);
  } catch {
    return String(data);
  }
}

function labelForKey(key: string): string {
  return key
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
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
  const lineText = (lines || []).filter(Boolean).join("\n");
  const dataText = data === undefined ? "" : diagnosticLogText(data);
  const logText = [lineText, dataText].filter(Boolean).join("\n");
  if (!logText) return null;
  return (
    <section className="ra-error ra-diagnostic-log" data-testid={testId} aria-label={title}>
      <strong>{title}</strong>
      <pre className="ra-json-preview">{logText}</pre>
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
