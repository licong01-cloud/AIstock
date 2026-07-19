import type {
  ApiEnvelope,
  ApiFailure,
  BatchDetail,
  BatchSummary,
  CandidatePreview,
  CandidateRecord,
  CandidateSourcePayload,
  EvaluationDetail,
  EvaluationSummary,
  EvaluationSpecPayload,
  QEAssetCatalog,
  QEAssetEntry,
  QEAssetTextContent,
} from "@/lib/hmm-research/contracts";

const API_BASE = (
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1"
).replace(/\/+$/, "");

const REQUEST_TIMEOUT_MS = 20_000;
const IDEMPOTENCY_STORAGE_PREFIX = "aistock:hmm-evolution:intent:";

export class HMMApiError extends Error {
  readonly errorCode: string;
  readonly reasonCode: string;
  readonly context: Record<string, unknown>;
  readonly traceId?: string;
  readonly httpStatus: number;

  constructor(failure: ApiFailure, httpStatus: number) {
    super(failure.message);
    this.name = "HMMApiError";
    this.errorCode = failure.error_code;
    this.reasonCode = failure.reason_code;
    this.context = failure.context || {};
    this.traceId = failure.trace_id;
    this.httpStatus = httpStatus;
  }
}

async function request<T>(
  path: string,
  init: RequestInit = {},
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<T> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`${API_BASE}/hmm-evolution${path}`, {
      ...init,
      cache: "no-store",
      headers: {
        Accept: "application/json",
        ...(init.body ? { "Content-Type": "application/json" } : {}),
        ...init.headers,
      },
      signal: controller.signal,
    });
    const payload = (await response.json()) as ApiEnvelope<T> | ApiFailure;
    if (!response.ok || !("status" in payload)) {
      const failure = payload as ApiFailure;
      throw new HMMApiError(
        {
          error_code: failure.error_code || "HMM_EVOLUTION_CLIENT_ERROR",
          reason_code: failure.reason_code || "hmm_evolution_invalid_response",
          message: failure.message || `HMM API 请求失败（HTTP ${response.status}）`,
          context: failure.context || {},
          trace_id: failure.trace_id,
        },
        response.status,
      );
    }
    return payload.data;
  } catch (error) {
    if (error instanceof HMMApiError) throw error;
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new HMMApiError(
        {
          error_code: "HMM_EVOLUTION_CLIENT_TIMEOUT",
          reason_code: "hmm_evolution_client_timeout",
          message: "HMM 研究接口请求超时，未使用旧结果替代。",
          context: { timeout_ms: timeoutMs },
        },
        504,
      );
    }
    throw new HMMApiError(
      {
        error_code: "HMM_EVOLUTION_CLIENT_ERROR",
        reason_code: "hmm_evolution_client_request_failed",
        message: error instanceof Error ? error.message : "HMM 研究接口请求失败。",
        context: {},
      },
      0,
    );
  } finally {
    window.clearTimeout(timer);
  }
}

function canonicalizeIntent(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalizeIntent);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, nested]) => [key, canonicalizeIntent(nested)]),
    );
  }
  return value;
}

async function idempotencyKeyForIntent(scope: string, payload: unknown): Promise<string> {
  const canonical = JSON.stringify({ scope, payload: canonicalizeIntent(payload) });
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(canonical));
  const fingerprint = Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
  const storageKey = `${IDEMPOTENCY_STORAGE_PREFIX}${fingerprint}`;
  const existing = window.sessionStorage.getItem(storageKey);
  if (existing) return existing;
  const created = crypto.randomUUID();
  window.sessionStorage.setItem(storageKey, created);
  return created;
}

export function listCandidates(): Promise<CandidateRecord[]> {
  return request("/candidates?limit=200");
}

export function getCandidate(candidateId: string): Promise<{
  candidate: CandidateRecord;
  recent_evaluations: EvaluationSummary[];
}> {
  return request(`/candidates/${encodeURIComponent(candidateId)}`);
}

export function previewCandidate(payload: CandidateSourcePayload): Promise<CandidatePreview> {
  return request("/candidates/preview", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function registerCandidate(
  payload: CandidateSourcePayload & {
    display_name: string;
    description?: string;
    created_by?: string;
  },
): Promise<{ candidate: CandidateRecord; created: boolean }> {
  return request("/candidates", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function listQEAssets(taskId: string, loopName: string): Promise<QEAssetCatalog> {
  return request(
    `/qe-assets/${encodeURIComponent(taskId)}/${encodeURIComponent(loopName)}?require_complete=false`,
  );
}

export function statQEAsset(
  taskId: string,
  loopName: string,
  relativePath: string,
): Promise<QEAssetEntry> {
  return request(
    `/qe-assets/${encodeURIComponent(taskId)}/${encodeURIComponent(loopName)}/stat?path=${encodeURIComponent(relativePath)}`,
  );
}

export function listBatches(): Promise<BatchSummary[]> {
  return request("/batches?limit=100");
}

export function getBatch(batchId: string): Promise<BatchDetail> {
  return request(`/batches/${encodeURIComponent(batchId)}`);
}

export function getEvaluation(evalId: string): Promise<EvaluationDetail> {
  return request(`/evaluations/${encodeURIComponent(evalId)}`);
}

export async function createBatch(payload: {
  candidate_ids: string[];
  evaluation_spec: EvaluationSpecPayload;
  created_by?: string;
}): Promise<{ batch: BatchSummary; created: boolean }> {
  const idempotencyKey = await idempotencyKeyForIntent("create-batch", payload);
  return request("/batch", {
    method: "POST",
    body: JSON.stringify(payload),
    headers: { "Idempotency-Key": idempotencyKey },
  });
}

export function readQEAssetText(
  taskId: string,
  loopName: string,
  relativePath: string,
  range?: { start: number; end: number },
): Promise<QEAssetTextContent> {
  return request(
    `/qe-assets/${encodeURIComponent(taskId)}/${encodeURIComponent(loopName)}/content?path=${encodeURIComponent(relativePath)}`,
    range
      ? { headers: { Range: `bytes=${range.start}-${range.end}` } }
      : {},
  );
}

export function cancelBatch(batchId: string): Promise<BatchSummary> {
  return request(`/batches/${encodeURIComponent(batchId)}/cancel`, {
    method: "POST",
    body: JSON.stringify({ requested_by: "hmm_research_ui" }),
  });
}

export async function retryFailedBatch(batchId: string): Promise<BatchSummary> {
  const payload = { created_by: "hmm_research_ui" };
  const idempotencyKey = await idempotencyKeyForIntent(`retry-batch:${batchId}`, payload);
  return request(`/batches/${encodeURIComponent(batchId)}/retry-failed`, {
    method: "POST",
    body: JSON.stringify(payload),
    headers: { "Idempotency-Key": idempotencyKey },
  });
}
