export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = Record<string, unknown>;

export type DataSource = "DB_HISTORICAL" | "TDX_REALTIME";
export type SelectionMode = "single_package" | "intersection" | "union" | "weighted_fusion";
export type PaperSessionMode = "REPLAY_ONLY" | "LIVE_ONLY" | "CATCHUP_THEN_LIVE";

export type TradingDayDefaults = {
  as_of_date: string;
  lookback_trading_days: number;
  require_minute_data?: boolean;
  data_ready_latest_date?: string | null;
  latest_trading_day: string;
  replay_start_date: string;
  replay_end_date: string;
  available_trading_day_count: number;
  next_trading_day?: string | null;
};

export type BackendErrorDetail = {
  error_code?: string;
  message?: string;
  context?: JsonObject;
  [key: string]: unknown;
};

export type MetricsSummary = {
  ic?: number | null;
  rank_ic?: number | null;
  icir?: number | null;
  sharpe?: number | null;
  annual_return?: number | null;
  max_drawdown?: number | null;
  final_nav?: number | null;
  turnover?: number | null;
  n_trading_days?: number | null;
  sample_start?: string | null;
  sample_end?: string | null;
  missing_metrics?: string[];
  raw_metric_keys?: string[];
};

export type ModelState = {
  package_id?: string;
  active_model_version_id?: string | null;
  train_start_date?: string | null;
  train_end_date?: string | null;
  trained_at?: string | null;
  last_retrain_job_id?: string | null;
  last_retrained_at?: string | null;
  stale_after_days?: number | null;
  staleness_status?: string;
  last_checked_at?: string | null;
  [key: string]: unknown;
};

export type StrategyPackage = {
  package_id: string;
  package_name: string;
  package_version?: string;
  source_type?: string;
  source_id?: string;
  loop_id?: string | null;
  run_id?: string | null;
  package_status: string;
  manifest_sha256: string;
  paper_portfolio_count?: number;
  created_at?: string;
  updated_at?: string;
  metrics_summary?: MetricsSummary;
  manifest?: JsonObject;
};

export type QEPackagingSource = {
  source_kind: "qe_experiment" | "qe_evolution_loop";
  experiment_id: string;
  experiment_name: string;
  qe_task_id?: string | null;
  qe_loop_id?: string | null;
  loop_index?: number | null;
  alpha_mode?: string | null;
  display_name: string;
  metrics_summary?: MetricsSummary;
  created_at?: string | null;
  completed_at?: string | null;
};

export type LatestSelectionRun = {
  run_id: string;
  mode: string;
  trade_date: string;
  data_source: string;
  status: string;
  candidate_count: number;
  completed_at?: string | null;
};

export type SelectablePackage = {
  package_id: string;
  package_name: string;
  package_version?: string;
  package_status: string;
  source_type?: string;
  source_id?: string;
  manifest_sha256: string;
  alpha_mode?: string;
  alpha_count?: number;
  portfolio_topk?: number;
  metrics_summary?: MetricsSummary;
  model_state?: ModelState | JsonObject;
  latest_selection_run?: LatestSelectionRun | null;
};

export type SelectionCandidate = {
  symbol: string;
  score: number;
  rank: number;
  target_weight?: number | null;
  reference_price?: number | null;
  component_scores?: JsonObject;
  reason?: string | null;
  context?: JsonObject;
};

export type SelectionRun = {
  run_id: string;
  mode: string;
  trade_date: string;
  data_source: string;
  package_ids: string[];
  runtime_config: JsonObject;
  status?: string;
  aggregate_results: SelectionCandidate[];
  manifest_sha256_by_package?: Record<string, string>;
  package_results?: Record<string, SelectionCandidate[]>;
  excluded_results?: Record<string, SelectionCandidate[]>;
};

export type SelectionWatchlistImportResult = {
  run_id: string;
  category_id: number;
  entry_source: string;
  entry_as_of: string;
  requested_top_k: number;
  imported_symbols: string[];
  added?: number;
  skipped?: number;
  moved?: number;
  item_ids_by_code?: Record<string, number>;
};

export type ExecutionPolicy = {
  validated_execution_policy_id?: string;
  policy_id?: string;
  package_id?: string;
  manifest_sha256?: string;
  policy_name?: string;
  policy_json?: JsonObject;
  policy_sha256?: string;
  algo_code?: string;
  algo_config?: JsonObject;
  validation_status?: string;
  paper_enabled?: boolean;
  is_portfolio_default?: boolean;
  source_backtest_id?: string;
  source_backtest_status?: string;
  validated_at?: string | null;
  [key: string]: unknown;
};

export type PaperPortfolio = {
  portfolio_id: string;
  portfolio_name: string;
  package_id: string;
  manifest_sha256: string;
  frozen_manifest?: JsonObject;
  initial_cash: number;
  start_date: string;
  data_source: DataSource | string;
  fee_policy?: JsonObject;
  risk_policy?: JsonObject;
  execution_policy?: JsonObject;
  status: string;
  created_at?: string;
  updated_at?: string;
};

export type PaperRun = {
  run_id: string;
  portfolio_id: string;
  trade_date: string;
  status: string;
  data_source?: string;
  runtime_config?: JsonObject;
  started_at?: string;
  completed_at?: string | null;
  error?: JsonObject | null;
};

export type ReadinessCheck = {
  check_name: string;
  status: string;
  context?: JsonObject;
};

export type ReadinessResult = {
  portfolio_id: string;
  trade_date: string;
  data_source: string;
  checks: ReadinessCheck[];
  raw_candidate_count: number;
  tradable_candidate_count: number;
  excluded_candidate_count: number;
  target_count: number;
  order_intent_count: number;
  checked_symbols: string[];
  runtime_config_keys: string[];
};

export type ReplayResult = {
  portfolio_id: string;
  start_date: string;
  end_date: string;
  data_source: string;
  trading_days: string[];
  day_results: Array<JsonObject>;
  reset_audit?: JsonObject | null;
};

export type PaperSession = {
  session_id: string;
  portfolio_id: string;
  mode: PaperSessionMode;
  status: string;
  phase: string;
  start_date: string;
  end_date?: string | null;
  historical_data_source?: DataSource | string | null;
  live_data_source?: DataSource | string | null;
  runtime_config?: JsonObject;
  validated_execution_policy?: JsonObject;
  created_by?: string | null;
  created_at?: string;
  updated_at?: string;
  started_at?: string | null;
  completed_at?: string | null;
  last_error?: JsonObject | null;
};

export type PaperSessionProgress = {
  session: PaperSession;
  current_trade_date?: string | null;
  last_processed_bar_time?: string | null;
  latest_available_bar_time?: string | null;
  next_expected_bar_time?: string | null;
  day_count: number;
  events: JsonObject[];
};

export type PaperSessionCapabilities = {
  portfolio_id: string;
  portfolio_data_source: string;
  algo_code?: string | null;
  validated_execution_policy_id?: string | null;
  modes: Record<string, { can_start: boolean; errors: JsonObject[] }>;
};

export type PaperSchedulerStatus = {
  running: boolean;
  thread_alive: boolean;
  interval_seconds: number;
  tickable_statuses: string[];
  last_run_at?: string | null;
  last_result?: JsonObject | null;
};

export type PaperSchedulerRunResult = {
  started_at: string;
  completed_at?: string;
  session_count: number;
  processed: JsonObject[];
  errors: JsonObject[];
};

export type Activation = {
  activation_id: string;
  portfolio_id: string;
  trade_date: string;
  policy_id: string;
  policy_sha256?: string;
  policy_name?: string | null;
  policy_json?: JsonObject;
  status: string;
  activated_at?: string;
  activated_by?: string | null;
  reason?: string | null;
};

export type RuntimeProfile = {
  profile_id: string;
  portfolio_id: string;
  package_id: string;
  profile_name: string;
  status: string;
  current_version_id?: string | null;
  created_by?: string | null;
  created_at?: string;
  updated_at?: string;
};

export type RuntimeProfileVersion = {
  profile_version_id: string;
  profile_id: string;
  version_no: number;
  config_json: JsonObject;
  config_sha256: string;
  validation_status: string;
  validation_errors?: JsonObject[];
  created_by?: string | null;
  reason?: string | null;
  created_at?: string;
  supersedes_version_id?: string | null;
};

export type RuntimeConfigActivation = {
  activation_id: string;
  portfolio_id: string;
  trade_date: string;
  profile_version_id: string;
  status: string;
  activated_at?: string;
  activated_by?: string | null;
  reason?: string | null;
  context?: JsonObject;
  superseded_at?: string | null;
};

export type ConfigChangeAudit = {
  audit_id?: number;
  portfolio_id?: string | null;
  package_id?: string | null;
  object_type: string;
  object_id: string;
  change_type: string;
  before_json?: JsonObject | null;
  after_json?: JsonObject | null;
  before_sha256?: string | null;
  after_sha256?: string | null;
  reason?: string | null;
  created_by?: string | null;
  request_id?: string | null;
  code_version?: string | null;
  created_at?: string;
};

export type HmmConfig = {
  config_id: string;
  model_type: string;
  display_name: string;
  config_json: JsonObject;
  snapshot_count?: number;
  cron_expression?: string | null;
  cron_enabled?: boolean;
  created_at?: string;
};

export type HmmJob = {
  job_id: string;
  config_id: string;
  snapshot_id?: string | null;
  status: string;
  started_at?: string | null;
  completed_at?: string | null;
  error_message?: string | null;
  rolling_training_preview?: JsonObject | null;
};

export type HmmDailyCoefficientJob = {
  job_id: string;
  snapshot_id: string;
  config_id: string;
  signal_preset: string;
  as_of_trade_date: string;
  effective_trade_date: string;
  generation_mode: string;
  status: string;
  result_status?: string | null;
  requested_at?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  input_data_max_dates?: JsonObject | null;
  output_path?: string | null;
  artifact_sha256?: string | null;
  plan_json?: JsonObject | null;
  result_json?: JsonObject | null;
  error_message?: string | null;
  error_context?: JsonObject | null;
};

export type HmmSnapshot = {
  snapshot_id: string;
  config_id: string;
  display_name?: string | null;
  config_display_name?: string | null;
  trained_at: string;
  model_path: string;
  sector_count: number;
  status: string;
  metrics_json?: JsonObject | null;
  coefficient_artifacts?: Array<{
    filename?: string;
    path?: string;
    preset?: string;
    start_date?: string;
    end_date?: string;
    covered_trade_dates?: string[];
    date_count?: number;
    parse_error?: string;
    generation_mode?: string;
    as_of_trade_date?: string;
    effective_trade_date?: string;
    generated_at?: string;
    snapshot_id?: string;
    config_id?: string;
    artifact_sha256?: string;
    input_data_max_dates?: JsonObject;
  }>;
};
