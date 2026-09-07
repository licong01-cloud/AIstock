const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1").replace(/\/+$/, "");

export type RotationState = "fading" | "neutral" | "trending";

export interface RotationL1Row {
  prediction_id: string;
  trade_date: string;
  as_of_date: string;
  sector_code: string;
  sector_name: string;
  rotation_score: number | null;
  forecast_state: RotationState | null;
  availability: "available" | "unavailable";
  reason_code: string | null;
  model_hash: string;
  input_hash: string;
  mapping_snapshot_hash: string;
  revision: number;
}

export interface RotationOverview {
  model_hash: string;
  trade_date: string;
  as_of_date: string;
  sector_count: number;
  available_count: number;
  binding_mbe_rank_ic: number;
  research_surface_status: "NOT_AVAILABLE" | "AVAILABLE_EXPERIMENTAL";
  rotation_l1_capability_status:
    | "NOT_AVAILABLE"
    | "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
    | "ADVISORY_PREDICTION_AVAILABLE";
  forward_power_status: "UNAVAILABLE" | "INSUFFICIENT" | "SUFFICIENT";
  forward_confirmation:
    | "NOT_STARTED"
    | "PENDING_INSUFFICIENT_POWER"
    | "PENDING_INCONCLUSIVE"
    | "PASSED"
    | "FAILED";
  advisory_status: "NOT_AVAILABLE" | "AVAILABLE";
  validation_basis: "development_causal_oof" | "single_date_frozen_model";
  development_oof_rank_ic: number | null;
  development_oof_rank_ic_hac_lower: number | null;
  development_oof_rank_ic_hac_upper: number | null;
  input_hash: string;
  mapping_snapshot_hash: string;
  tail_accessed: boolean;
}

export class HMMRiskApiError extends Error {
  constructor(
    message: string,
    readonly reasonCode: string,
    readonly httpStatus: number,
  ) {
    super(message);
    this.name = "HMMRiskApiError";
  }
}

async function request<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}/hmm-risk${path}`, { cache: "no-store" });
  const payload = (await response.json()) as {
    status?: string;
    data?: T;
    detail?: { reason_code?: string; message?: string };
  };
  if (!response.ok || payload.status !== "ok" || payload.data === undefined) {
    throw new HMMRiskApiError(
      payload.detail?.message || `HMM Risk 请求失败（HTTP ${response.status}）`,
      payload.detail?.reason_code || "hmm_risk_client_invalid_response",
      response.status,
    );
  }
  return payload.data;
}

export function getRotationOverview(): Promise<RotationOverview> {
  return request<RotationOverview>("/overview");
}

export function getRotationL1(tradeDate: string, modelHash: string): Promise<{
  model_hash: string;
  trade_date: string;
  rows: RotationL1Row[];
}> {
  const query = new URLSearchParams({ trade_date: tradeDate, model_hash: modelHash });
  return request(`/rotation-l1?${query.toString()}`);
}
