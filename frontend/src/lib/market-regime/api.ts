export const SOURCE_METHODS = [
  "simple_quadrant",
  "hmm_viterbi",
  "bbq",
  "ensemble",
] as const;

export type SourceMethod = (typeof SOURCE_METHODS)[number];

export const REGIME_VALUES = ["bull", "bear", "oscillation", "high_vol", "low_vol"] as const;
export type Regime = (typeof REGIME_VALUES)[number];

export type RegimeLabel = {
  trade_date: string;
  regime: Regime;
  regime_confidence: number | null;
  source_method: string;
  source_signal_json: Record<string, unknown> | null;
  labeled_at: string | null;
};

export type RegimeTimelineResponse = {
  source_method: string;
  items: RegimeLabel[];
};

export type RegimeDistributionItem = {
  regime: string;
  count: number;
  pct: number;
};

export type RegimeDistributionResponse = {
  source_method: string;
  start_date: string | null;
  end_date: string | null;
  total: number;
  items: RegimeDistributionItem[];
};

export type RegimeMethodsResponse = {
  supported: string[];
  available: string[];
};

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export class MarketRegimeApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "MarketRegimeApiError";
    this.status = status;
    this.raw = raw;
  }
}

async function jsonFetch<T>(input: string): Promise<T> {
  const response = await fetch(input, { headers: { Accept: "application/json" } });
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
    throw new MarketRegimeApiError(detail, response.status, parsed);
  }
  return parsed as T;
}

function buildQuery(params: Record<string, string | number | undefined>): string {
  const usp = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      usp.append(key, String(value));
    }
  }
  const qs = usp.toString();
  return qs ? `?${qs}` : "";
}

export const marketRegimeApi = {
  methods: () => jsonFetch<RegimeMethodsResponse>(`${API_BASE}/market/regime-label/methods`),
  timeline: (params: {
    source_method: string;
    start_date?: string;
    end_date?: string;
    limit?: number;
  }) =>
    jsonFetch<RegimeTimelineResponse>(
      `${API_BASE}/market/regime-label/timeline${buildQuery(params)}`,
    ),
  distribution: (params: {
    source_method: string;
    start_date?: string;
    end_date?: string;
  }) =>
    jsonFetch<RegimeDistributionResponse>(
      `${API_BASE}/market/regime-label/distribution${buildQuery(params)}`,
    ),
  current: (source_method: string) =>
    jsonFetch<RegimeLabel | null>(
      `${API_BASE}/market/regime-label/current${buildQuery({ source_method })}`,
    ),
};

export const REGIME_LABELS: Record<Regime, string> = {
  bull: "牛市",
  bear: "熊市",
  oscillation: "震荡",
  high_vol: "高波动",
  low_vol: "低波动",
};
