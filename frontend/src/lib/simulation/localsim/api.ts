import type {
  JsonObject,
  LocalSimAccount,
  LocalSimAccountStatus,
  LocalSimControlResponse,
  LocalSimCutoverReadiness,
  LocalSimListResponse,
  LocalSimReplay,
  LocalSimRuntimeProfile,
  LocalSimRuntimeProfileVersion,
  LocalSimSchedulerVerification,
  RuntimeProfileConfigRequest,
  TradingCalendarStatus,
} from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export class LocalSimApiError extends Error {
  constructor(
    message: string,
    public status: number,
    public code?: string,
    public context?: JsonObject,
  ) {
    super(message);
    this.name = "LocalSimApiError";
  }
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    cache: "no-store",
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
  });
  const payload = await response.json().catch(() => ({})) as JsonObject;
  if (!response.ok) {
    const detail = (payload.detail && typeof payload.detail === "object" ? payload.detail : payload) as JsonObject;
    throw new LocalSimApiError(
      String(detail.message || `HTTP ${response.status}`),
      response.status,
      String(detail.code || detail.error_code || "LOCALSIM_API_ERROR"),
      (detail.context && typeof detail.context === "object" ? detail.context : undefined) as JsonObject | undefined,
    );
  }
  return payload as T;
}

const post = (payload: unknown): RequestInit => ({ method: "POST", body: JSON.stringify(payload) });

export const localSimApi = {
  async tradingCalendarStatus(asOfDate?: string): Promise<TradingCalendarStatus> {
    const suffix = asOfDate ? `?as_of_date=${encodeURIComponent(asOfDate)}` : "";
    return request(`/trading-calendar/status${suffix}`);
  },

  async listPackages(): Promise<Array<{ package_id: string; display_name?: string; package_name?: string; package_status: string; created_at?: string }>> {
    const data = await request<{ packages: Array<{ package_id: string; display_name?: string; package_name?: string; package_status: string; created_at?: string }> }>(
      "/strategy-packages?view=summary&limit=200",
    );
    return data.packages || [];
  },

  async listExecutionPolicies(packageId: string): Promise<Array<{ policy_id: string; policy_name: string; algo_code: string }>> {
    const data = await request<{ execution_policies: Array<{ policy_id: string; policy_name: string; algo_code: string }> }>(
      `/strategy-packages/${encodeURIComponent(packageId)}/execution-policies`,
    );
    return data.execution_policies || [];
  },

  async readiness(signal?: AbortSignal): Promise<LocalSimCutoverReadiness> {
    const data = await request<{ readiness: LocalSimCutoverReadiness }>(
      "/simulation-runtime/localsim/cutover-readiness",
      { signal },
    );
    const value = data.readiness;
    if (!value || value.schema_version !== "localsim_cutover_readiness_v1"
      || typeof value.ready !== "boolean" || !Array.isArray(value.blockers)
      || !value.blockers.every((reason) => typeof reason === "string")
      || !Number.isInteger(value.in_flight_economic_run_count) || value.in_flight_economic_run_count < 0
      || !Number.isFinite(Date.parse(value.checked_at))
      || (value.ready !== (value.blockers.length === 0))
      || (value.ready && value.in_flight_economic_run_count > 0)) {
      throw new LocalSimApiError("切流状态返回不完整或相互矛盾，请重新核验", 200, "LOCALSIM_READINESS_INVALID");
    }
    return value;
  },

  async schedulerVerification(signal?: AbortSignal): Promise<LocalSimSchedulerVerification> {
    const data = await request<{ scheduler: LocalSimSchedulerVerification }>(
      "/simulation-runtime/scheduler/verification-status?broker_backend=local_sim", { signal },
    );
    if (!data.scheduler || typeof data.scheduler.running !== "boolean" || typeof data.scheduler.thread_alive !== "boolean") {
      throw new LocalSimApiError("调度核验返回不完整，请重新读取", 200, "LOCALSIM_SCHEDULER_VERIFICATION_INVALID");
    }
    return data.scheduler;
  },

  async listAccounts(params: {
    packageId?: string;
    status?: LocalSimAccountStatus;
    cursor?: string;
    limit?: number;
    signal?: AbortSignal;
  } = {}): Promise<LocalSimListResponse<LocalSimAccount>> {
    const query = new URLSearchParams({ limit: String(params.limit || 100) });
    if (params.packageId) query.set("package_id", params.packageId);
    if (params.status) query.set("status", params.status);
    if (params.cursor) query.set("cursor", params.cursor);
    const data = await request<LocalSimListResponse<LocalSimAccount>>(`/simulation-runtime/localsim/accounts?${query.toString()}`, { signal: params.signal });
    if (!Array.isArray(data.items)) throw new LocalSimApiError("账户列表返回不完整，请重新读取", 200, "LOCALSIM_ACCOUNTS_INVALID");
    return data;
  },

  async getAccount(accountId: string, signal?: AbortSignal): Promise<LocalSimControlResponse> {
    return request(`/simulation-runtime/localsim/accounts/${encodeURIComponent(accountId)}`, {signal});
  },

  async createAccount(payload: {
    account_name: string;
    package_id: string;
    initial_capital: string;
    runtime_profile_version_id: string;
    execution_policy_version_id: string;
    effective_from: string;
    effective_to?: string | null;
    created_reason?: string | null;
  }): Promise<LocalSimControlResponse> {
    return request("/simulation-runtime/localsim/accounts", post({
      schema_version: "localsim_account_create_request_v1",
      ...payload,
    }));
  },

  async transitionAccount(accountId: string, action: "pause" | "resume" | "retire", expectedVersion: number) {
    return request<LocalSimControlResponse>(
      `/simulation-runtime/localsim/accounts/${encodeURIComponent(accountId)}/${action}`,
      post({ schema_version: "localsim_lifecycle_request_v1", expected_version: expectedVersion }),
    );
  },

  async listProfiles(packageId?: string): Promise<LocalSimListResponse<LocalSimRuntimeProfile>> {
    const query = new URLSearchParams({ limit: "200" });
    if (packageId) query.set("package_id", packageId);
    return request(`/simulation-runtime/localsim/runtime-profiles?${query.toString()}`);
  },

  async createProfile(packageId: string, profileName: string): Promise<{ profile: LocalSimRuntimeProfile }> {
    return request("/simulation-runtime/localsim/runtime-profiles", post({
      schema_version: "localsim_runtime_profile_create_request_v1",
      package_id: packageId,
      profile_name: profileName,
    }));
  },

  async listProfileVersions(profileId: string): Promise<LocalSimListResponse<LocalSimRuntimeProfileVersion>> {
    return request(`/simulation-runtime/localsim/runtime-profiles/${encodeURIComponent(profileId)}/versions?limit=200`);
  },

  async createProfileVersion(
    profileId: string,
    expectedProfileVersion: number,
    config: RuntimeProfileConfigRequest,
  ): Promise<{ profile: LocalSimRuntimeProfile; version: LocalSimRuntimeProfileVersion }> {
    return request(
      `/simulation-runtime/localsim/runtime-profiles/${encodeURIComponent(profileId)}/versions`,
      post({
        schema_version: "localsim_runtime_profile_version_create_request_v1",
        expected_profile_version: expectedProfileVersion,
        config,
      }),
    );
  },

  async listReplays(cursor?: string, signal?: AbortSignal): Promise<LocalSimListResponse<LocalSimReplay>> {
    const query = new URLSearchParams({ limit: "100" });
    if (cursor) query.set("cursor", cursor);
    const data = await request<LocalSimListResponse<LocalSimReplay>>(`/simulation-runtime/localsim/replays?${query.toString()}`, { signal });
    if (!Array.isArray(data.items)) throw new LocalSimApiError("回放列表返回不完整，请重新读取", 200, "LOCALSIM_REPLAYS_INVALID");
    return data;
  },

  async createReplay(payload: {
    account_name: string;
    package_id: string;
    initial_capital: string;
    runtime_profile_version_id: string;
    execution_policy_version_id: string;
    effective_from: string;
    effective_to: string;
    start_trade_date: string;
    end_trade_date: string;
    historical_source_id?: string;
  }): Promise<LocalSimControlResponse> {
    return request("/simulation-runtime/localsim/replays", post({
      schema_version: "localsim_replay_create_request_v1",
      historical_source_id: "dataset_files.frozen_market.v1",
      ...payload,
    }));
  },

  async getReplay(replayJobId: string, signal?: AbortSignal): Promise<LocalSimControlResponse> {
    return request(`/simulation-runtime/localsim/replays/${encodeURIComponent(replayJobId)}`, { signal });
  },

  async cancelReplay(replayJobId: string, expectedVersion: number): Promise<LocalSimControlResponse> {
    return request(
      `/simulation-runtime/localsim/replays/${encodeURIComponent(replayJobId)}/cancel`,
      post({ schema_version: "localsim_replay_cancel_request_v1", expected_version: expectedVersion }),
    );
  },

  async accountRuns(accountId: string): Promise<JsonObject[]> {
    const data = await request<{ runs: JsonObject[] }>(
      `/simulation-runtime/localsim/accounts/${encodeURIComponent(accountId)}/runs`,
    );
    return data.runs;
  },

  async accountLedger(accountId: string): Promise<JsonObject> {
    const data = await request<{ ledger: JsonObject }>(
      `/simulation-runtime/localsim/accounts/${encodeURIComponent(accountId)}/ledger`,
    );
    return data.ledger;
  },

  async accountPerformance(accountId: string): Promise<JsonObject> {
    const data = await request<{ performance: JsonObject }>(
      `/simulation-runtime/localsim/accounts/${encodeURIComponent(accountId)}/performance`,
    );
    return data.performance;
  },
};
