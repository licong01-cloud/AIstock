import { API_BASE } from "@/lib/qe-archive/api";

export type JsonObject = Record<string, unknown>;

export class PredictionStoreApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "PredictionStoreApiError";
    this.status = status;
    this.raw = raw;
  }
}

export type PredictionStoreDisk = {
  total_bytes?: number;
  used_bytes?: number;
  free_bytes?: number;
  error?: string;
};

export type PredictionStoreHealth = {
  status?: string;
  tracking_backend?: string;
  mlflow_pg_enabled?: boolean;
  artifact_store?: {
    store_root?: string;
    store_root_env?: string;
    exists?: boolean;
    scheme?: string;
    disk?: PredictionStoreDisk;
    policy?: JsonObject;
  };
  [key: string]: unknown;
};

export type PredictionStoreArtifact = {
  artifact_type?: string | null;
  artifact_name?: string | null;
  uri?: string | null;
  artifact_uri?: string | null;
  sha256?: string | null;
  size_bytes?: number | null;
  row_count?: number | null;
  symbol_count?: number | null;
  date_start?: string | null;
  date_end?: string | null;
  parser_status?: string | null;
  parser_error?: string | null;
  collection_status?: string | null;
  collected_status?: string | null;
  source_api?: string | null;
  source_uri?: string | null;
  source_node_id?: string | null;
  metadata?: JsonObject | null;
};

export type PredictionStoreManifest = {
  schema_version?: string | null;
  run_key?: string | null;
  run_key_safe?: string | null;
  uri?: string | null;
  mlflow_artifact_uri?: string | null;
  storage_tier?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  metadata?: JsonObject | null;
  artifacts?: PredictionStoreArtifact[];
  [key: string]: unknown;
};

export type PredictionStorePointer = {
  pointer_status?: string;
  warehouse_found?: boolean;
  run_id?: string | null;
  experiment_id?: string | null;
  run?: JsonObject | null;
  source?: JsonObject | null;
  artifacts?: PredictionStoreArtifact[];
  mlflow_artifact_uri?: string | null;
  prediction_store_manifest?: PredictionStoreManifest | null;
  manifest_error?: string | null;
  reason?: string | null;
};

export type PredictionPreview = {
  run_id?: string;
  artifact_type?: string;
  row_count?: number;
  columns?: string[];
  head?: JsonObject[];
  head_count?: number;
  size_bytes?: number;
  pointer?: PredictionStorePointer;
};

export type ParamsPreview = {
  run_id?: string;
  artifact_type?: string;
  artifact_path?: string;
  size_bytes?: number;
  pointer?: PredictionStorePointer;
};

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(payload: unknown, status: number): string {
  if (isObject(payload)) {
    if (typeof payload.detail === "string") return payload.detail;
    if (isObject(payload.detail) && typeof payload.detail.message === "string") return payload.detail.message;
    if (typeof payload.message === "string") return payload.message;
    if (typeof payload.error === "string") return payload.error;
  }
  return `HTTP ${status}`;
}

async function apiFetch<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, { method: "GET" });
  const text = await response.text();
  const payload = text ? JSON.parse(text) as unknown : {};
  if (!response.ok) {
    throw new PredictionStoreApiError(errorMessage(payload, response.status), response.status, payload);
  }
  return payload as T;
}

export const predictionStoreApi = {
  async health(): Promise<PredictionStoreHealth> {
    const response = await apiFetch<{ status: string; data: PredictionStoreHealth }>("/prediction-store/health");
    return response.data;
  },
  async pointer(runId: string, experimentId?: string | null): Promise<PredictionStorePointer> {
    const qs = new URLSearchParams();
    if (experimentId) qs.set("experiment_id", experimentId);
    const suffix = qs.toString() ? `?${qs.toString()}` : "";
    const response = await apiFetch<{ status: string; data: PredictionStorePointer }>(
      `/prediction-store/pointers/${encodeURIComponent(runId)}${suffix}`,
    );
    return response.data;
  },
  async pointerByExperiment(experimentId: string): Promise<PredictionStorePointer> {
    const response = await apiFetch<{ status: string; data: PredictionStorePointer }>(
      `/prediction-store/pointers/by-experiment/${encodeURIComponent(experimentId)}`,
    );
    return response.data;
  },
  async previewPred(runId: string, head = 5): Promise<PredictionPreview> {
    const qs = new URLSearchParams({ head: String(Math.max(0, Math.min(1000, head))) });
    const response = await apiFetch<{ status: string; data: PredictionPreview }>(
      `/prediction-store/pred/${encodeURIComponent(runId)}?${qs.toString()}`,
    );
    return response.data;
  },
  async params(runId: string): Promise<ParamsPreview> {
    const response = await apiFetch<{ status: string; data: ParamsPreview }>(
      `/prediction-store/params/${encodeURIComponent(runId)}`,
    );
    return response.data;
  },
  downloadUrl(runId: string, artifactType: "prediction" | "model_params"): string {
    return `${API_BASE}/prediction-store/artifacts/${encodeURIComponent(runId)}/${artifactType}`;
  },
};
