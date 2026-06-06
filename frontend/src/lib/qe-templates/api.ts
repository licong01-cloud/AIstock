export type JsonObject = Record<string, unknown>;

export const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1").replace(/\/+$/, "");

export class QETemplateApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "QETemplateApiError";
    this.status = status;
    this.raw = raw;
  }
}

export type QETemplateKind = "single_experiment" | "custom_evo";
export type QETemplateStatus =
  | "draft"
  | "ready_for_review"
  | "approved"
  | "materialized"
  | "run_requested"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "superseded"
  | "expired";
export type ArchivePolicy = "AUTO" | "SKIP" | "MANUAL_ONLY";

export type QETemplateValidation = {
  valid: boolean;
  errors: string[];
  warnings: string[];
};

export type QETemplate = {
  template_id: string;
  template_kind: QETemplateKind;
  status: QETemplateStatus;
  title: string;
  description?: string | null;
  config_json: JsonObject;
  config_sha256?: string | null;
  archive_policy: ArchivePolicy;
  archive_reason?: string | null;
  source_context_json?: JsonObject;
  analysis_summary_md?: string | null;
  risk_summary_md?: string | null;
  validation_json?: QETemplateValidation | JsonObject;
  approval_json?: JsonObject;
  parent_template_id?: string | null;
  proposed_metrics_json?: JsonObject;
  created_by_type?: string | null;
  created_by_name?: string | null;
  data_versions_json?: JsonObject;
  submitted_experiment_id?: string | null;
  submitted_task_id?: string | null;
  runtime_config_sha256?: string | null;
  runtime_diff_json?: JsonObject;
  actual_metrics_json?: JsonObject;
  metric_delta_json?: JsonObject;
  created_at?: string | null;
  updated_at?: string | null;
};

export type QETemplateUpdatePayload = Partial<{
  title: string;
  description: string | null;
  config_json: JsonObject;
  archive_policy: ArchivePolicy;
  archive_reason: string | null;
  source_context_json: JsonObject;
  analysis_summary_md: string | null;
  risk_summary_md: string | null;
  proposed_metrics_json: JsonObject;
  data_versions_json: JsonObject;
}>;

export type QETemplateListParams = {
  status?: string;
  template_kind?: string;
  created_by_type?: string;
  search?: string;
  limit?: number;
  offset?: number;
};

export type QETemplateMaterializeResult = {
  template?: QETemplate;
  materialized?: JsonObject;
};

export type QETemplateRunResult = {
  template_id: string;
  run_result?: JsonObject;
};

export type QETemplateDeleteResult = {
  deleted_template: QETemplate;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(payload: unknown, status: number): string {
  if (isRecord(payload)) {
    if (typeof payload.detail === "string") return payload.detail;
    if (isRecord(payload.detail)) return JSON.stringify(payload.detail);
    if (typeof payload.message === "string") return payload.message;
    if (typeof payload.error === "string") return payload.error;
  }
  return `HTTP ${status}`;
}

function buildQuery(params: QETemplateListParams): string {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") qs.set(key, String(value));
  });
  const query = qs.toString();
  return query ? `?${query}` : "";
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
  if (!response.ok) throw new QETemplateApiError(errorMessage(payload, response.status), response.status, payload);
  return payload as T;
}

function body(payload: unknown): RequestInit {
  return { method: "POST", body: JSON.stringify(payload) };
}

function putBody(payload: unknown): RequestInit {
  return { method: "PUT", body: JSON.stringify(payload) };
}

export const qeTemplatesApi = {
  async list(params: QETemplateListParams = {}): Promise<QETemplate[]> {
    const response = await apiFetch<{ status: string; data: QETemplate[] }>(`/qe-templates${buildQuery({ limit: 100, ...params })}`);
    return response.data || [];
  },
  async get(templateId: string): Promise<QETemplate> {
    const response = await apiFetch<{ status: string; data: QETemplate }>(`/qe-templates/${encodeURIComponent(templateId)}`);
    return response.data;
  },
  async update(templateId: string, payload: QETemplateUpdatePayload): Promise<QETemplate> {
    const response = await apiFetch<{ status: string; data: QETemplate }>(`/qe-templates/${encodeURIComponent(templateId)}`, putBody(payload));
    return response.data;
  },
  async validate(templateId: string): Promise<{ template: QETemplate; validation: QETemplateValidation }> {
    const response = await apiFetch<{ status: string; data: { template: QETemplate; validation: QETemplateValidation } }>(`/qe-templates/${encodeURIComponent(templateId)}/validate`, body({}));
    return response.data;
  },
  async approve(templateId: string, payload: { approved_by?: string; approval_note?: string | null } = {}): Promise<QETemplate> {
    const response = await apiFetch<{ status: string; data: QETemplate }>(`/qe-templates/${encodeURIComponent(templateId)}/approve`, body({ approved_by: "ui_operator", ...payload }));
    return response.data;
  },
  async materialize(templateId: string): Promise<QETemplateMaterializeResult> {
    const response = await apiFetch<{ status: string; data: QETemplateMaterializeResult }>(`/qe-templates/${encodeURIComponent(templateId)}/materialize`, body({ confirm_template: "QE_TEMPLATE_MATERIALIZE" }));
    return response.data;
  },
  async run(template: QETemplate, payload: { node_id?: string | null; force_full_train?: boolean } = {}): Promise<QETemplateRunResult> {
    const confirm_run = template.template_kind === "custom_evo" ? "QE_CUSTOM_EVO_RUN" : "QE_EXPERIMENT_RUN";
    const response = await apiFetch<{ status: string; data: QETemplateRunResult }>(
      `/qe-templates/${encodeURIComponent(template.template_id)}/run`,
      body({ confirm_run, node_id: payload.node_id || null, force_full_train: Boolean(payload.force_full_train) }),
    );
    return response.data;
  },
  async supersede(templateId: string): Promise<QETemplate> {
    const response = await apiFetch<{ status: string; data: QETemplate }>(`/qe-templates/${encodeURIComponent(templateId)}/supersede`, body({}));
    return response.data;
  },
  async deletePending(templateId: string): Promise<QETemplateDeleteResult> {
    const response = await apiFetch<{ status: string; data: QETemplateDeleteResult }>(
      `/qe-templates/${encodeURIComponent(templateId)}`,
      { method: "DELETE", body: JSON.stringify({ confirm_delete: "QE_TEMPLATE_DELETE" }) },
    );
    return response.data;
  },
};
