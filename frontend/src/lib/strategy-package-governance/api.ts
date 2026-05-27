export type EvidenceCheckStatus = "pass" | "fail" | "missing" | "skipped" | "pending";

export type EvidenceCheck = {
  status: EvidenceCheckStatus;
  reason?: string | null;
  detail?: Record<string, unknown> | null;
};

export type GovernanceEligibility = {
  package_id: string;
  paper_ready: boolean;
  paper_ready_block_reason?: string | null;
  evaluated_at?: string | null;
  manifest_identity: EvidenceCheck;
  original_fixed_weight_retest: EvidenceCheck;
  validation_stability: EvidenceCheck;
  protected_asset_ledger: EvidenceCheck;
  runtime_variant_paper_candidate: EvidenceCheck;
};

export type StrategyPackageSummary = {
  package_id: string;
  package_name?: string | null;
  status: string;
  paper_status?: string | null;
  selection_status?: string | null;
  manifest_version?: string | null;
  source_system?: string | null;
  source_id?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export class GovernanceApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "GovernanceApiError";
    this.status = status;
    this.raw = raw;
  }
}

async function jsonFetch<T>(input: string, init?: RequestInit): Promise<T> {
  const response = await fetch(input, {
    ...init,
    headers: {
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
    throw new GovernanceApiError(detail, response.status, parsed);
  }
  return parsed as T;
}

export const governanceApi = {
  listPackages: async (): Promise<StrategyPackageSummary[]> => {
    const data = await jsonFetch<{ items?: StrategyPackageSummary[] } | StrategyPackageSummary[]>(
      `${API_BASE}/strategy-packages`,
    );
    if (Array.isArray(data)) return data;
    return data?.items ?? [];
  },
  eligibility: async (packageId: string): Promise<GovernanceEligibility> => {
    const data = await jsonFetch<{ data?: GovernanceEligibility } | GovernanceEligibility>(
      `${API_BASE}/strategy-packages/${encodeURIComponent(packageId)}/governance-eligibility`,
    );
    if (data && typeof data === "object" && "data" in data && (data as { data?: GovernanceEligibility }).data) {
      return (data as { data: GovernanceEligibility }).data;
    }
    return data as GovernanceEligibility;
  },
};

export const EVIDENCE_KEYS = [
  "manifest_identity",
  "original_fixed_weight_retest",
  "validation_stability",
  "protected_asset_ledger",
  "runtime_variant_paper_candidate",
] as const;

export type EvidenceKey = (typeof EVIDENCE_KEYS)[number];

export const EVIDENCE_LABELS: Record<EvidenceKey, string> = {
  manifest_identity: "Manifest 身份",
  original_fixed_weight_retest: "原始固定权重复测",
  validation_stability: "验证稳定性",
  protected_asset_ledger: "受保护资产账本",
  runtime_variant_paper_candidate: "历史 runtime 候选证据",
};
