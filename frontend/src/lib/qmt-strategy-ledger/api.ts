import { PaperV2ApiError } from "@/lib/paper-v2/api";
import type { JsonObject } from "@/lib/paper-v2/types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export type QmtVirtualStrategySummary = JsonObject & {
  account_id?: string | null;
  trade_date?: string;
  strategy_count?: number;
  strategies?: JsonObject[];
  overlap_symbols?: string[];
  unattributed_orders?: number;
  unattributed_trades?: number;
};

export type QmtLedgerResponse = JsonObject & { success?: boolean };

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseError(payload: unknown, status: number): PaperV2ApiError {
  if (isObject(payload)) {
    const detail = payload.detail;
    if (isObject(detail)) {
      const errorCode = typeof detail.error_code === "string" ? detail.error_code : undefined;
      const message = typeof detail.message === "string" ? detail.message : JSON.stringify(detail);
      const context = isObject(detail.context) ? detail.context : undefined;
      return new PaperV2ApiError(message, status, payload, errorCode, context);
    }
    if (typeof detail === "string") return new PaperV2ApiError(detail, status, payload);
    if (typeof payload.error === "string") return new PaperV2ApiError(payload.error, status, payload);
    if (typeof payload.message === "string") return new PaperV2ApiError(payload.message, status, payload);
  }
  return new PaperV2ApiError(`HTTP ${status}`, status, payload);
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  });
  const text = await response.text();
  const payload = text ? JSON.parse(text) as unknown : {};
  if (!response.ok) throw parseError(payload, response.status);
  return payload as T;
}

function body(payload: unknown): RequestInit {
  return { method: "POST", body: JSON.stringify(payload) };
}

export const qmtStrategyLedgerApi = {
  async summary(params: { account_id?: string; trade_date?: string } = {}): Promise<QmtVirtualStrategySummary> {
    const qs = new URLSearchParams();
    if (params.account_id) qs.set("account_id", params.account_id);
    if (params.trade_date) qs.set("trade_date", params.trade_date);
    const suffix = qs.toString() ? `?${qs.toString()}` : "";
    const data = await apiFetch<{ summary: QmtVirtualStrategySummary }>(`/qmt/virtual-strategies/summary${suffix}`);
    return data.summary || {};
  },
  async syncSnapshot(payload: JsonObject): Promise<QmtLedgerResponse> {
    return apiFetch<QmtLedgerResponse>("/qmt/virtual-strategies/sync-snapshot", body(payload));
  },
  async reconcile(payload: JsonObject): Promise<QmtLedgerResponse> {
    return apiFetch<QmtLedgerResponse>("/qmt/virtual-strategies/reconciliation", body(payload));
  },
  async bindPackage(payload: JsonObject): Promise<QmtLedgerResponse> {
    return apiFetch<QmtLedgerResponse>("/qmt/virtual-strategies/package-bindings", body(payload));
  },
  async previewOrdersFromBinding(bindingId: string, payload: JsonObject): Promise<QmtLedgerResponse> {
    return apiFetch<QmtLedgerResponse>(
      `/qmt/virtual-strategies/package-bindings/${encodeURIComponent(bindingId)}/orders/preview`,
      body(payload),
    );
  },
  async previewOrder(payload: JsonObject): Promise<QmtLedgerResponse> {
    return apiFetch<QmtLedgerResponse>("/qmt/virtual-strategies/orders/preview", body(payload));
  },
};
