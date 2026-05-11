export type RLModelVersion = {
  id: number;
  dev_version: string;
  roll_tag: string;
  version_tag: string;
  dev_description?: string | null;
  parent_dev?: string | null;
  policy_path: string;
  train_type: string;
  train_start?: string | null;
  train_end?: string | null;
  purge_end?: string | null;
  train_epochs?: number | null;
  train_duration_sec?: number | null;
  train_config?: Record<string, unknown> | null;
  state_dim?: number | null;
  action_dim?: number | null;
  network_arch?: string | null;
  eval_oracle_gap_bps?: number | null;
  eval_pa_bps?: number | null;
  eval_ffr?: number | null;
  eval_vs_twap_bps?: number | null;
  eval_urgency_cost_bps?: number | null;
  eval_details?: Record<string, unknown> | null;
  status: string;
  created_at: string;
  activated_at?: string | null;
};

export type RLDevLineage = {
  dev_version: string;
  dev_description?: string | null;
  parent_dev?: string | null;
  roll_count: number;
  latest_train_end?: string | null;
  roll_tags: string[];
};

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export class RLExecutionApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "RLExecutionApiError";
    this.status = status;
    this.raw = raw;
  }
}

async function jsonFetch<T>(input: string, init?: RequestInit): Promise<T> {
  const response = await fetch(input, {
    ...init,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  });
  const text = await response.text();
  let parsed: unknown = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = text;
  }
  if (!response.ok) {
    const detail =
      typeof parsed === "object" && parsed && "detail" in parsed
        ? String((parsed as { detail: unknown }).detail)
        : `HTTP ${response.status}`;
    throw new RLExecutionApiError(detail, response.status, parsed);
  }
  return parsed as T;
}

function buildQuery(params: Record<string, string | undefined>): string {
  const usp = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") usp.append(key, value);
  }
  const qs = usp.toString();
  return qs ? `?${qs}` : "";
}

export const rlExecutionApi = {
  models: (params: { dev_version?: string; status?: string } = {}) =>
    jsonFetch<RLModelVersion[]>(`${API_BASE}/rl-execution/models${buildQuery(params)}`),
  devVersions: () => jsonFetch<RLDevLineage[]>(`${API_BASE}/rl-execution/dev-versions`),
  rolls: (dev: string) =>
    jsonFetch<RLModelVersion[]>(
      `${API_BASE}/rl-execution/dev-versions/${encodeURIComponent(dev)}/rolls`,
    ),
  activate: (versionTag: string) =>
    jsonFetch<{ status: string; version_tag: string }>(
      `${API_BASE}/rl-execution/models/${encodeURIComponent(versionTag)}/activate`,
      { method: "POST" },
    ),
  deactivate: (versionTag: string) =>
    jsonFetch<{ status: string; version_tag: string }>(
      `${API_BASE}/rl-execution/models/${encodeURIComponent(versionTag)}/deactivate`,
      { method: "POST" },
    ),
};
