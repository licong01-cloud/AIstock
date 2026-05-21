"use client";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import StatusBadge from "@/components/paper-v2/StatusBadge";

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
      <JsonPanel value={data} />
    </details>
  );
}

export function KeyValueGrid({ rows }: { rows: Array<[string, unknown]> }) {
  return (
    <div className="pv2-readable-panel">
      <div className="pv2-readable-table">
        {rows.map(([key, value]) => (
          <div className="pv2-readable-row" key={key}>
            <div className="pv2-readable-key">{key}</div>
            <div className="pv2-readable-value">{typeof value === "string" || typeof value === "number" || typeof value === "boolean" ? display(value) : <JsonPanel value={value} />}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

export function StatusChips({ counts }: { counts?: Record<string, number> }) {
  const entries = Object.entries(counts || {});
  if (!entries.length) return <span className="pv2-muted">暂无状态计数</span>;
  return (
    <div className="pv2-chip-row">
      {entries.map(([status, count]) => (
        <span className="pv2-chip" key={status}><StatusBadge status={status} /> {count}</span>
      ))}
    </div>
  );
}

export function ApiErrorBox({ error, title = "研究助理 API 读取失败" }: { error: unknown; title?: string }) {
  if (!error) return null;
  const message = error instanceof Error ? error.message : String(error);
  return (
    <div className="pv2-error-panel ra-error" role="alert">
      <strong>{title}</strong>
      <p>{message}</p>
      <p className="pv2-muted">如果是 schema_missing，请先在明确授权后执行数据库迁移；页面不会静默使用 mock 数据冒充成功。</p>
    </div>
  );
}

export function asObject(value: unknown): JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as JsonObject : {};
}
