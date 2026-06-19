import { API_BASE } from "@/lib/qe-archive/api";

export type MatrixValue = number | null;

export type OrthogonalityLegMeta = {
  run_id: string;
  model?: string | null;
  factor_set?: string | null;
  n_dates?: number | null;
  row_count?: number | null;
  instrument_count?: number | null;
};

export type MultiAlphaOrthogonalityResult = {
  schema_version?: string;
  legs: string[];
  k: number;
  pred_corr_matrix: MatrixValue[][];
  jaccard_matrix: MatrixValue[][];
  n_common_dates: number;
  common_date_start?: string | null;
  common_date_end?: string | null;
  per_leg: Record<string, OrthogonalityLegMeta>;
};

export class MultiAlphaApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "MultiAlphaApiError";
    this.status = status;
    this.raw = raw;
  }
}

function messageFromPayload(payload: unknown, status: number): string {
  if (payload && typeof payload === "object") {
    const record = payload as Record<string, unknown>;
    if (typeof record.detail === "string") return record.detail;
    if (typeof record.message === "string") return record.message;
    if (typeof record.error === "string") return record.error;
  }
  return `HTTP ${status}`;
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, { method: "GET" });
  const text = await response.text();
  const payload = text ? JSON.parse(text) as unknown : {};
  if (!response.ok) {
    throw new MultiAlphaApiError(messageFromPayload(payload, response.status), response.status, payload);
  }
  return payload as T;
}

export const multiAlphaApi = {
  async orthogonality(runIds: string[], k = 25): Promise<MultiAlphaOrthogonalityResult> {
    const qs = new URLSearchParams();
    runIds.forEach((runId) => qs.append("run_ids", runId));
    qs.set("k", String(Math.max(1, Math.min(500, Math.trunc(k)))));
    const response = await getJson<{ status: string; data: MultiAlphaOrthogonalityResult }>(
      `/multi-alpha/orthogonality?${qs.toString()}`,
    );
    return response.data;
  },
};
