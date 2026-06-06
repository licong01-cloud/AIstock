export type JsonObject = Record<string, unknown>;

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export type SelectionRunSummary = {
  run_id: string;
  mode: string;
  trade_date: string;
  package_ids: string[];
  status?: string;
};

export type FusionDiagnosticRow = {
  symbol: string;
  rank: number;
  score: number;
  fusion_score?: number;
  source_package_ids?: string[];
  package_raw_scores?: Record<string, number>;
  package_ranks?: Record<string, number>;
  package_rank_scores?: Record<string, number>;
  package_presence?: Record<string, string>;
  support_count?: number;
  rank_dispersion?: number;
  fusion_policy_sha256?: string;
};

export type FusionDiagnostics = {
  run_id: string;
  mode: string;
  package_ids: string[];
  fusion_method?: string;
  fusion_policy_sha256?: string;
  diagnostics: FusionDiagnosticRow[];
};

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  });
  const text = await response.text();
  const payload = text ? JSON.parse(text) as unknown : {};
  if (!response.ok) {
    const detail = typeof payload === "object" && payload && "detail" in payload ? (payload as JsonObject).detail : null;
    const message = typeof detail === "object" && detail && "message" in detail
      ? String((detail as JsonObject).message)
      : `HTTP ${response.status}`;
    throw new Error(message);
  }
  return payload as T;
}

function body(payload: unknown): RequestInit {
  return { method: "POST", body: globalThis.JSON["stringify"](payload) };
}

export const selectionCenterAdvisoryApi = {
  async listRuns(limit = 20): Promise<SelectionRunSummary[]> {
    const data = await apiFetch<{ runs: SelectionRunSummary[] }>(`/selection-center/runs?limit=${limit}`);
    return data.runs || [];
  },
  async fusionDiagnostics(runId: string): Promise<FusionDiagnostics> {
    const data = await apiFetch<FusionDiagnostics & { ok?: boolean }>(
      `/selection-center/runs/${encodeURIComponent(runId)}/fusion-diagnostics`,
    );
    return data;
  },
  post<T>(path: string, payload: unknown): Promise<T> {
    return apiFetch<T>(path, body(payload));
  },
};
