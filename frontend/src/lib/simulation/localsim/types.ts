export type JsonObject = Record<string, unknown>;

export type LocalSimAccountStatus = "ACTIVE" | "PAUSED" | "RETIRED";
export type LocalSimReplayStatus =
  | "CREATED"
  | "RUNNING_HISTORICAL"
  | "CAUGHT_UP"
  | "READY_FOR_LIVE"
  | "ACTIVATION_PENDING_SAFE_BOUNDARY"
  | "LIVE_ACTIVE"
  | "FAILED_RETRYABLE"
  | "FAILED_TERMINAL"
  | "CANCELLED";

export type LocalSimAccount = {
  schema_version: "simulation_account_v1";
  account_id: string;
  account_hash: string;
  account_name: string;
  broker_backend: "local_sim";
  package_id: string;
  manifest_sha256: string;
  admission_receipt_id: string;
  initial_capital: number;
  status: LocalSimAccountStatus;
  version: number;
  created_at: string;
  updated_at: string;
};

export type LocalSimLedgerScope = {
  schema_version: "simulation_ledger_scope_v1";
  ledger_scope_id: string;
  ledger_scope_hash: string;
  scope_kind: "LEGACY_PORTFOLIO" | "SUCCESSOR_NATIVE";
  source_identity: string;
  native_account_id?: string | null;
};

export type LocalSimRuntimeRelease = {
  release_id: string;
  release_hash: string;
  package_id: string;
  manifest_sha256: string;
  runtime_profile_id: string;
  runtime_profile_version_id: string;
  execution_policy_version_id: string;
  execution_policy_sha256: string;
  tail_policy_version_id: string;
  tail_policy_sha256: string;
  effective_from?: string | null;
  effective_to?: string | null;
  validation_state: string;
  validation_evidence: JsonObject;
};

export type LocalSimBinding = {
  binding_id: string;
  binding_hash: string;
  strategy_id: string;
  release_id: string;
  package_id: string;
  broker_backend: "local_sim";
  capital_allocation: number;
  effective_from?: string | null;
  effective_to?: string | null;
  approval_state: string;
};

export type LocalSimReplay = {
  schema_version: "localsim_replay_job_v1";
  replay_job_id: string;
  replay_hash: string;
  simulation_account_id: string;
  release_id: string;
  binding_id: string;
  start_trade_date: string;
  end_trade_date: string;
  historical_source_id: string;
  status: LocalSimReplayStatus;
  next_trade_date?: string | null;
  completed_trade_date?: string | null;
  activation_trade_date?: string | null;
  version: number;
  failure_code?: string | null;
  failure_context?: JsonObject | null;
  created_at: string;
  updated_at: string;
};

export type LocalSimRuntimeProfile = {
  profile_id: string;
  profile_hash: string;
  package_id: string;
  manifest_sha256: string;
  profile_name: string;
  status: "ACTIVE" | "RETIRED";
  version: number;
  created_at: string;
  updated_at: string;
};

export type LocalSimRuntimeProfileVersion = {
  profile_version_id: string;
  profile_version_hash: string;
  profile_id: string;
  package_id: string;
  manifest_sha256: string;
  version_no: number;
  config_json: JsonObject;
  config_sha256: string;
  daily_strategy_profile_version_id: string;
  validation_status: "VALIDATED" | "INVALID" | "RETIRED";
  validation_evidence: JsonObject;
  created_at: string;
};

export type LocalSimControlResponse = {
  ok: true;
  schema_version: "localsim_control_response_v1";
  account?: LocalSimAccount | null;
  ledger_scope?: LocalSimLedgerScope | null;
  release?: LocalSimRuntimeRelease | null;
  binding?: LocalSimBinding | null;
  replay?: LocalSimReplay | null;
};

export type LocalSimListResponse<T> = {
  ok: true;
  schema_version: "localsim_list_response_v1";
  items: T[];
  next_cursor?: string | null;
  limit: number;
};

export type LocalSimCutoverReadiness = {
  schema_version: "localsim_cutover_readiness_v1";
  ready: boolean;
  checked_at: string;
  blockers: string[];
  retained_legacy_account_ids: string[];
  missing_lineage_account_ids: string[];
  legacy_active_session_count: number;
  legacy_auto_run_count: number;
  legacy_sentinel_count: number;
  in_flight_economic_run_count: number;
};

export type TradingCalendarStatus = {
  ok: true;
  as_of_date: string;
  is_trading_day: boolean;
  latest_completed_trading_day?: string | null;
  previous_trading_day?: string | null;
  next_trading_day?: string | null;
};

export type RuntimeProfileConfigRequest = {
  schema_version: "localsim_runtime_profile_config_request_v1";
  daily_strategy: {
    strategy_id: string;
    strategy_version: string;
    top_k: number;
    industry_filters: string[];
    sector_filters: string[];
    parameters: JsonObject;
  };
  hmm: {
    enabled: boolean;
    snapshot_id: string | null;
    model_version: string | null;
    preset: string | null;
    state_mapping: JsonObject;
  };
  risk_policy: JsonObject;
  fee_policy: JsonObject;
  runtime_variant_id?: string | null;
  runtime_variant_hash?: string | null;
  notes?: string | null;
  metadata: JsonObject;
};
