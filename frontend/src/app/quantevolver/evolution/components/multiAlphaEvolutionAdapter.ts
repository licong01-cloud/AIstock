export type MultiAlphaLegDraft = {
  leg_id: string;
  seed_run_ids: string[];
  metadata: Record<string, unknown>;
};

export type MultiAlphaScenarioDraft = {
  scenario_id: string;
  scenario_name: string;
  initial_cash: number;
  topk: number;
  node_id: string;
  node_parallelism: number;
  n_drop: number;
  max_n_drop: number;
  min_n_drop: number;
  hold_thresh: number;
  oos_start?: string;
  oos_end?: string;
  scheme_timeout_seconds?: number;
  run_timeout_seconds?: number;
};

export type MultiAlphaCreateRequest = {
  task_id?: string | null;
  roster: MultiAlphaLegDraft[];
  oos_start: string;
  oos_end: string;
  weighting_schemes: string[];
  normalize_method: string;
  walk_forward: Record<string, unknown>;
  rank_fusion: Record<string, unknown>;
  backtest_config: Record<string, unknown>;
  baseline_leg_id: string;
  topk: number;
  min_date_coverage: number;
  run_async: boolean;
  scheme_timeout_seconds?: number | null;
  run_timeout_seconds?: number | null;
  wait_timeout_seconds?: number | null;
};

export type MultiAlphaCreateResult = {
  scenario_id: string;
  scenario_name: string;
  status: "succeeded" | "failed";
  payload: MultiAlphaCreateRequest;
  data?: Record<string, unknown>;
  error?: MultiAlphaApiErrorShape;
};

export type MultiAlphaApiErrorShape = {
  status: number;
  reason_code: string;
  message: string;
  context: Record<string, unknown>;
};

export class MultiAlphaApiError extends Error {
  readonly status: number;
  readonly reasonCode: string;
  readonly context: Record<string, unknown>;

  constructor(shape: MultiAlphaApiErrorShape) {
    super(shape.message);
    this.name = "MultiAlphaApiError";
    this.status = shape.status;
    this.reasonCode = shape.reason_code;
    this.context = shape.context;
  }

  toShape(): MultiAlphaApiErrorShape {
    return { status: this.status, reason_code: this.reasonCode, message: this.message, context: this.context };
  }
}

export type DurableAttempt = {
  attempt_id: string;
  child_id?: string;
  run_id?: string;
  attempt_no?: number | string | null;
  retry_mode?: string | null;
  retry_of_attempt_id?: string | null;
  source_attempt_id?: string | null;
  execution_kind?: string | null;
  node_id?: string | null;
  qe_task_id?: string | null;
  qe_loop_id?: string | number | null;
  submission_intent_hash?: string | null;
  status?: string | null;
  phase?: string | null;
  selected?: boolean;
  heartbeat_at?: string | null;
  lease_expires_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  remote_submission_json?: Record<string, unknown> | null;
  process_identity_json?: Record<string, unknown> | null;
  process_identity_hash?: string | null;
  environment_identity_json?: Record<string, unknown> | null;
  environment_identity_hash?: string | null;
  dataset_identity_json?: Record<string, unknown> | null;
  dataset_identity_hash?: string | null;
  artifact_manifest_json?: Record<string, unknown> | null;
  result_manifest_json?: Record<string, unknown> | null;
  result_manifest_hash?: string | null;
  error_code?: string | null;
  error_json?: Record<string, unknown> | null;
};

export type DurableChild = {
  child_id: string;
  run_id?: string;
  child_key: string;
  child_kind?: string | null;
  ordinal?: number | string | null;
  status?: string | null;
  phase?: string | null;
  selected_attempt_id?: string | null;
  execution_disposition?: string | null;
  source_child_id?: string | null;
  dependency_json?: Record<string, unknown> | null;
  artifact_manifest_json?: Record<string, unknown> | null;
  artifact_manifest_hash?: string | null;
  result_manifest_json?: Record<string, unknown> | null;
  result_manifest_hash?: string | null;
  row_version?: number | string | null;
  created_at?: string | null;
  updated_at?: string | null;
  finished_at?: string | null;
  error_code?: string | null;
  error_json?: Record<string, unknown> | null;
  attempts?: DurableAttempt[];
};

export type DurableEvent = {
  event_id: number;
  run_id: string;
  child_id?: string | null;
  attempt_id?: string | null;
  event_type?: string | null;
  phase?: string | null;
  reason_code?: string | null;
  payload_json?: Record<string, unknown> | null;
  created_at?: string | null;
};

export type DurableEventsPage = {
  run_id: string;
  events: DurableEvent[];
  count: number;
  after_event_id: number;
  next_event_id: number;
  has_more: boolean;
};

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function apiErrorShape(status: number, payload: unknown): MultiAlphaApiErrorShape {
  const root = asRecord(payload);
  const detail = typeof root.detail === "string" ? { message: root.detail } : asRecord(root.detail);
  const context = asRecord(detail.context);
  const reason = String(detail.reason_code || context.reason_code || root.reason_code || "multi_alpha_http_error");
  const message = String(detail.message || detail.detail || root.message || `HTTP ${status}`);
  return { status, reason_code: reason, message, context };
}

export async function multiAlphaRequest<T>(url: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(url, init);
  } catch (error) {
    throw new MultiAlphaApiError({
      status: 0,
      reason_code: "multi_alpha_network_error",
      message: error instanceof Error ? error.message : String(error),
      context: { url },
    });
  }
  const payload = await response.json().catch(() => null);
  if (!response.ok || !payload || payload.status !== "success") {
    throw new MultiAlphaApiError(apiErrorShape(response.status, payload));
  }
  return payload.data as T;
}

export async function submitMultiAlphaScenarios(
  apiBase: string,
  scenarios: Array<{ scenario: MultiAlphaScenarioDraft; payload: MultiAlphaCreateRequest }>,
  onResult?: (result: MultiAlphaCreateResult) => void,
): Promise<MultiAlphaCreateResult[]> {
  const results: MultiAlphaCreateResult[] = [];
  for (const { scenario, payload } of scenarios) {
    let result: MultiAlphaCreateResult;
    try {
      const data = await multiAlphaRequest<Record<string, unknown>>(`${apiBase}/multi-alpha/combine-backtest/run`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": `multi-alpha-create-${scenario.scenario_id}`,
        },
        body: JSON.stringify(payload),
      });
      result = {
        scenario_id: scenario.scenario_id,
        scenario_name: scenario.scenario_name,
        status: "succeeded",
        payload,
        data,
      };
    } catch (error) {
      const shape = error instanceof MultiAlphaApiError
        ? error.toShape()
        : { status: 0, reason_code: "multi_alpha_submit_unknown_error", message: String(error), context: {} };
      result = {
        scenario_id: scenario.scenario_id,
        scenario_name: scenario.scenario_name,
        status: "failed",
        payload,
        error: shape,
      };
    }
    results.push(result);
    onResult?.(result);
  }
  return results;
}

export function canonicalMultiAlphaEvolutionUrl(taskId?: string, query?: URLSearchParams): string {
  const params = new URLSearchParams(query?.toString() || "");
  params.set("task_type", "multi_alpha_combine");
  if (taskId) params.set("task_id", taskId);
  else params.delete("task_id");
  return `/quantevolver/evolution?${params.toString()}`;
}
