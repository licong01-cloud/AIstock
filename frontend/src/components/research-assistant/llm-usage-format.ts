import type { AssistantLlmUsageTotals, JsonObject } from "@/lib/research-assistant/api";

export function asRecord(value: unknown): JsonObject {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonObject) : {};
}

export function formatUsageNumber(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) return value.toLocaleString("zh-CN");
  if (typeof value === "string" && value.trim()) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toLocaleString("zh-CN") : value;
  }
  return "-";
}

export function formatUsageUsd(value: unknown, reason?: unknown): string {
  if (value === null || value === undefined || value === "") {
    const detail = reason ? `(${String(reason)})` : "";
    return `成本不可用${detail}`;
  }
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return `$${number.toFixed(number > 0 && number < 0.01 ? 6 : 4)}`;
}

export function usageStatusText(summary?: AssistantLlmUsageTotals | null): string {
  const usageStatus = String(summary?.usage_status || "unavailable");
  const costStatus = String(summary?.cost_status || "unavailable");
  return `usage=${usageStatus} / cost=${costStatus}`;
}

export function usageTotalCost(summary?: AssistantLlmUsageTotals | null): string {
  return formatUsageUsd(summary?.total_cost_usd, summary?.cost_reason_code || summary?.reason_code);
}
