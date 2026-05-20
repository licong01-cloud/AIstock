export type JsonObject = Record<string, unknown>;

export type ValidationPage<T> = {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  has_more: boolean;
};

export type ValidationEnvelope<T> = {
  status: string;
  data: T;
};

export type ValidationHealth = {
  status?: string;
  mode?: string;
  history?: {
    mode?: string;
    history_root?: string;
    exists?: boolean;
    run_count?: number;
    coverage_snapshot_count?: number;
    evidence_manifest_count?: number;
  };
  plan_catalog?: {
    catalog_path?: string;
    missing?: boolean;
    plan_count?: number;
  };
  quality?: {
    mode?: string;
    finding_count?: number;
    bug_count?: number;
    parse_errors?: JsonObject[];
  };
  runner?: ValidationRunnerHealth;
  production_8001_touched?: boolean;
};

export type ValidationPlan = JsonObject & {
  plan_key: string;
  title?: string;
  module?: string;
  level?: string;
  command_key?: string;
  nox_session?: string;
  enabled?: boolean;
  requires_backend?: boolean;
  requires_frontend?: boolean;
  allowed_backend_ports?: number[];
  allowed_frontend_ports?: number[];
  writes_database?: boolean;
  writes_artifacts?: boolean;
  writes_business_state?: boolean;
  runner_enabled?: boolean;
};

export type ValidationPlanCatalog = {
  catalog_path?: string;
  missing?: boolean;
  plans: ValidationPlan[];
};

export type ValidationPassScope = JsonObject & {
  level?: string;
  real_backend?: boolean;
  real_database?: boolean;
  real_node_api?: boolean;
  real_frontend_click?: boolean;
  writes_business_state?: boolean;
  positive_business_success?: boolean;
  negative_failfast_only?: boolean;
  mock_api_used?: boolean;
  production_8001_touched?: boolean;
};

export type ValidationBusinessAssertion = JsonObject & {
  can_user_complete_operation?: boolean;
  operation_name?: string;
  evidence?: JsonObject;
  unresolved_blockers?: string[];
};

export type ValidationCoverageSummary = JsonObject & {
  snapshot_id: string;
  schema_version?: string;
  module?: string;
  level?: string;
  title?: string;
  run_id?: string | null;
  generated_at?: string;
  git_commit?: string;
  status?: string;
  snapshot_path?: string;
  totals?: JsonObject;
  diff?: JsonObject;
  quality_gates?: JsonObject[];
  failed_gates?: JsonObject[];
};

export type ValidationEvidenceSummary = JsonObject & {
  manifest_id: string;
  schema_version?: string;
  module?: string;
  level?: string;
  title?: string;
  run_id?: string | null;
  generated_at?: string;
  git_commit?: string;
  manifest_path?: string;
  missing_count?: number;
  evidence_count?: number;
  missing?: unknown[];
};

export type ValidationRunSummary = JsonObject & {
  run_id: string;
  module?: string;
  module_slug?: string;
  level?: string;
  title?: string;
  status?: string;
  git_commit?: string | null;
  operator?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  markdown_path?: string;
  metadata_path?: string | null;
  metadata_missing?: boolean;
  metadata_parse_error?: string | null;
  source_type?: string;
  coverage?: JsonObject | null;
  coverage_snapshot_id?: string | null;
  coverage_missing?: boolean;
  evidence_manifest_id?: string | null;
  evidence_missing?: boolean;
  pass_scope?: ValidationPassScope | null;
  business_assertion?: ValidationBusinessAssertion | null;
  success_scope_recorded?: boolean;
  quality_gates?: JsonObject[];
  parse_error?: string | null;
};

export type ValidationRunDetail = ValidationRunSummary & {
  markdown_text?: string | null;
  metadata?: JsonObject | null;
  coverage_snapshot?: ValidationCoverageSummary | null;
  evidence_manifest?: ValidationEvidenceSummary | null;
};

export type ValidationCoverageDetail = {
  summary: ValidationCoverageSummary;
  snapshot: JsonObject;
};

export type ValidationEvidenceDetail = {
  summary: ValidationEvidenceSummary;
  manifest: JsonObject;
};

export type ValidationSummary = {
  history_root?: string;
  run_count?: number;
  coverage_snapshot_count?: number;
  evidence_manifest_count?: number;
  plan_count?: number;
  quality?: {
    finding_count?: number;
    bug_count?: number;
  };
  runner?: ValidationRunnerHealth;
  runs_by_status?: Record<string, number>;
  modules?: Array<JsonObject & { module?: string; run_count?: number; latest_run?: ValidationRunSummary }>;
  latest_runs?: ValidationRunSummary[];
  latest_coverage?: ValidationCoverageSummary | null;
};

export type ValidationRunnerHealth = JsonObject & {
  mode?: string;
  execution_root?: string;
  exists?: boolean;
  job_count?: number;
  jobs_by_status?: Record<string, number>;
  allowed_command_type?: string;
  arbitrary_shell_allowed?: boolean;
  production_8001_touched?: boolean;
};

export type ValidationExecutionJob = JsonObject & {
  schema_version?: string;
  job_id: string;
  status?: string;
  plan_key?: string;
  title?: string;
  module?: string;
  level?: string;
  command_key?: string;
  nox_session?: string;
  command?: string[];
  cwd?: string;
  requested_by?: string;
  requested_at?: string;
  started_at?: string | null;
  finished_at?: string | null;
  timeout_seconds?: number;
  return_code?: number | null;
  backend_port?: number | null;
  frontend_port?: number | null;
  writes_database?: boolean;
  writes_artifacts?: boolean;
  writes_business_state?: boolean;
  production_8001_touched?: boolean;
  arbitrary_shell_allowed?: boolean;
  log_path?: string;
  evidence_path?: string;
  error?: string | null;
  archive?: JsonObject & {
    status?: string;
    run_id?: string;
    run_record_path?: string;
    metadata_path?: string;
    evidence_manifest_path?: string;
    runner_log_archive_path?: string;
    coverage_snapshot_path?: string | null;
    artifact_paths?: string[];
  };
};

export type ValidationExecutionQuery = {
  status?: string;
  plan_key?: string;
  module?: string;
  page?: number;
  page_size?: number;
};

export type ValidationExecutionStartRequest = {
  plan_key: string;
  requested_by?: string;
  backend_port?: number;
  frontend_port?: number;
  timeout_seconds?: number;
  confirm_text?: string;
};

export type ValidationExecutionLog = JsonObject & {
  job_id: string;
  exists?: boolean;
  path?: string;
  content?: string;
  tail_lines?: number;
  truncated?: boolean;
  size_bytes?: number | null;
  sha256?: string | null;
  archive_path?: string | null;
};

export type ValidationExecutionEvidence = JsonObject & {
  job_id: string;
  job?: ValidationExecutionJob;
  runner_evidence?: JsonObject | null;
  standard_evidence?: JsonObject | null;
  runner_evidence_path?: string;
  standard_evidence_path?: string | null;
};

export type ValidationQualityFinding = JsonObject & {
  finding_id: string;
  source_type?: string;
  source_schema?: string;
  module?: string;
  severity?: string;
  status?: string;
  title?: string;
  description?: string;
  rule_id?: string | null;
  category?: string | null;
  file_path?: string | null;
  line?: number | null;
  fingerprint?: string;
  first_seen_at?: string | null;
  last_seen_at?: string | null;
  evidence_uri?: string | null;
  remediation?: string | null;
  baseline_policy?: string | null;
  lifecycle_status?: string | null;
  risk?: string | null;
  confidence?: string | null;
  allowed_write_scope?: string[];
  required_verification?: string[];
  linked_issue?: string | null;
  agent_context?: ValidationAgentContext;
};

export type ValidationBug = JsonObject & {
  bug_id: string;
  title?: string;
  description?: string | null;
  module?: string;
  severity?: string;
  risk_area?: string | null;
  status?: string;
  trigger_condition?: JsonObject;
  reproduce_command?: string | null;
  failing_run_id?: string | null;
  evidence_uris?: string[];
  fingerprint?: string;
  github_issue_number?: number | null;
  github_issue_url?: string | null;
  assigned_agent?: string | null;
  fix_branch?: string | null;
  fix_commit?: string | null;
  verification_run_id?: string | null;
  first_seen_at?: string | null;
  last_seen_at?: string | null;
  allowed_write_scope?: string[];
  suspected_modules?: string[];
  required_verification?: string[];
  closure_requirements?: string[];
  source_path?: string;
  agent_context?: ValidationAgentContext;
};

export type ValidationAgentContext = JsonObject & {
  schema_version?: string;
  context_type?: "bug" | "quality_finding" | string;
  bug_id?: string;
  finding_id?: string;
  problem_statement?: string | null;
  finding_source?: string;
  severity?: string;
  status?: string;
  reproduce_command?: string | null;
  evidence_uris?: Array<string | null | undefined>;
  allowed_write_scope?: string[];
  suspected_modules?: Array<string | null | undefined>;
  required_verification?: string[];
  closure_requirements?: string[];
  github_issue_url?: string | null;
  verification_run_id?: string | null;
};

export type ValidationFindingSummary = {
  finding_count?: number;
  by_source_type?: Record<string, number>;
  by_severity?: Record<string, number>;
  by_status?: Record<string, number>;
  by_module?: Record<string, number>;
  latest_findings?: ValidationQualityFinding[];
  parse_errors?: Array<JsonObject>;
};

export type ValidationBugSummary = {
  bug_count?: number;
  by_severity?: Record<string, number>;
  by_status?: Record<string, number>;
  by_module?: Record<string, number>;
  latest_bugs?: ValidationBug[];
  parse_errors?: Array<JsonObject>;
};

export type ValidationRunQuery = {
  module?: string;
  level?: string;
  status?: string;
  search?: string;
  include_markdown_only?: boolean;
  page?: number;
  page_size?: number;
};

export type ValidationListQuery = {
  module?: string;
  status?: string;
  page?: number;
  page_size?: number;
};

export type ValidationFindingQuery = ValidationListQuery & {
  source_type?: string;
  severity?: string;
  search?: string;
};

export type ValidationBugQuery = ValidationListQuery & {
  severity?: string;
  agent?: string;
  search?: string;
};

export type ValidationGitWorkspaceSummary = JsonObject & {
  changed_files?: number;
  staged_files?: number;
  unstaged_files?: number;
  untracked_files?: number;
  conflicted_files?: number;
  deleted_files?: number;
  renamed_files?: number;
  unmapped_files?: number;
  ambiguous_files?: number;
  critical_risk_files?: number;
};

export type ValidationGitWorkspaceFile = JsonObject & {
  path: string;
  old_path?: string | null;
  status?: string;
  git_xy?: string | null;
  staged?: boolean;
  unstaged?: boolean;
  untracked?: boolean;
  conflicted?: boolean;
  primary_module?: string | null;
  impact_modules?: string[];
  layer?: string | null;
  risk_level?: string | null;
  ownership_status?: string;
  matched_rule_ids?: string[];
  reason_codes?: string[];
  recommended_action?: string;
};

export type ValidationGitWorkspaceModule = JsonObject & {
  module_id: string;
  changed_file_count?: number;
  max_risk_level?: string | null;
  statuses?: Record<string, number>;
};

export type ValidationGitWorkspaceStatus = JsonObject & {
  schema_version?: string;
  generated_at?: string;
  repo_root?: string;
  branch?: string | null;
  upstream?: string | null;
  head_commit?: string | null;
  short_head_commit?: string | null;
  ahead_count?: number;
  behind_count?: number;
  dirty?: boolean;
  summary?: ValidationGitWorkspaceSummary;
  by_status?: Record<string, number>;
  by_module?: ValidationGitWorkspaceModule[];
  files?: ValidationGitWorkspaceFile[];
  reason_codes?: string[];
  git_command_mode?: string;
  arbitrary_shell_allowed?: boolean;
  production_8001_touched?: boolean;
};

export type ValidationGitBranchStatus = JsonObject & {
  schema_version?: string;
  generated_at?: string;
  repo_root?: string;
  branch?: string | null;
  detached?: boolean;
  upstream?: string | null;
  head_commit?: string | null;
  short_head_commit?: string | null;
  ahead_count?: number;
  behind_count?: number;
  upstream_known?: boolean;
  git_command_mode?: string;
  arbitrary_shell_allowed?: boolean;
  production_8001_touched?: boolean;
};

export type ValidationGitCommitFile = JsonObject & {
  path: string;
  old_path?: string | null;
  change_type?: string;
  primary_module?: string | null;
  impact_modules?: string[];
  layer?: string | null;
  risk_level?: string | null;
  ownership_status?: string;
  matched_rule_ids?: string[];
  reason_codes?: string[];
};

export type ValidationGitCommit = JsonObject & {
  commit_hash: string;
  short_hash?: string;
  author_name?: string;
  author_email?: string;
  authored_at?: string;
  subject?: string;
  changed_file_count?: number;
  file_status_counts?: Record<string, number>;
  module_ids?: string[];
  ownership_summary?: Record<string, number>;
  max_risk_level?: string | null;
  files?: ValidationGitCommitFile[];
};

export type ValidationGitCommitModule = JsonObject & {
  module_id: string;
  display_name?: string;
  commit_count?: number;
  changed_file_count?: number;
  latest_commit?: JsonObject | null;
  max_risk_level?: string | null;
  file_status_counts?: Record<string, number>;
  required_test_plans?: string[];
  recommended_test_plans?: string[];
};

export type ValidationGitCommitActivity = JsonObject & {
  schema_version?: string;
  generated_at?: string;
  repo_root?: string;
  branch?: string | null;
  upstream?: string | null;
  head_commit?: string | null;
  short_head_commit?: string | null;
  limit?: number;
  summary?: JsonObject & {
    commit_count?: number;
    changed_file_count?: number;
    unmapped_commit_count?: number;
    ambiguous_commit_count?: number;
    latest_commit?: ValidationGitCommit | null;
  };
  by_day?: Array<JsonObject & { period?: string; commit_count?: number }>;
  by_week?: Array<JsonObject & { period?: string; commit_count?: number }>;
  by_month?: Array<JsonObject & { period?: string; commit_count?: number }>;
  by_module?: ValidationGitCommitModule[];
  commits?: ValidationGitCommit[];
  git_command_mode?: string;
  arbitrary_shell_allowed?: boolean;
  production_8001_touched?: boolean;
};

export type ValidationModuleQualityItem = JsonObject & {
  module_id: string;
  display_name?: string;
  parent_module?: string | null;
  module_type?: string;
  registry_risk_level?: string;
  description?: string;
  description_zh?: string;
  ui_routes?: string[];
  api_routes?: string[];
  test_plans?: JsonObject & {
    required_on_change?: string[];
    recommended?: string[];
  };
  workspace?: JsonObject & {
    changed_file_count?: number;
    staged_file_count?: number;
    unstaged_file_count?: number;
    untracked_file_count?: number;
    max_risk_level?: string | null;
    files?: JsonObject[];
  };
  commits?: JsonObject & {
    commit_count?: number;
    changed_file_count?: number;
    latest_commit?: JsonObject | null;
    max_risk_level?: string | null;
  };
  coverage?: JsonObject & {
    snapshot_id?: string | null;
    status?: string | null;
    line_percent?: number | null;
    branch_percent?: number | null;
    generated_at?: string | null;
  };
  quality?: JsonObject & {
    finding_count?: number;
    bug_count?: number;
    by_severity?: Record<string, number>;
    by_status?: Record<string, number>;
  };
  priority?: JsonObject & {
    score?: number;
    level?: string;
    reason_codes?: string[];
  };
};

export type ValidationModuleQualitySummary = JsonObject & {
  schema_version?: string;
  generated_at?: string;
  repo_root?: string;
  summary?: JsonObject & {
    module_count?: number;
    modules_with_workspace_changes?: number;
    modules_with_recent_commits?: number;
    modules_needing_validation?: number;
    unmapped_workspace_files?: number;
    ambiguous_workspace_files?: number;
    recent_commit_count?: number;
  };
  modules?: ValidationModuleQualityItem[];
  workspace_summary?: ValidationGitWorkspaceSummary;
  commit_summary?: ValidationGitCommitActivity["summary"];
  global_reason_codes?: string[];
  git_command_mode?: string;
  arbitrary_shell_allowed?: boolean;
  production_8001_touched?: boolean;
};

export type ValidationUiTarget = JsonObject & {
  route_id: string;
  href: string;
  label?: string;
  nav_group?: string;
  primary_module?: string;
  impact_modules?: string[];
  risk_level?: string;
  required_test_plans?: string[];
  recommended_test_plans?: string[];
  business_operations?: string[];
  coverage_status?: string;
  exclusion_reason?: string;
  module_quality?: ValidationModuleQualityItem | null;
  latest_run?: ValidationRunSummary | null;
  warnings?: string[];
  proven_by_real_business_evidence?: boolean;
};

export type ValidationUiTargetPage = ValidationPage<ValidationUiTarget> & {
  schema_version?: string;
  catalog_path?: string;
  missing?: boolean;
};

export type ValidationUiTargetSummary = JsonObject & {
  schema_version?: string;
  generated_at?: string;
  catalog_path?: string;
  missing?: boolean;
  target_count?: number;
  nav_group_count?: number;
  warning_count?: number;
  targets_requiring_action?: number;
  by_nav_group?: Array<JsonObject & { nav_group?: string; target_count?: number; warning_count?: number }>;
  by_coverage_status?: Record<string, number>;
  by_risk_level?: Record<string, number>;
  production_8001_touched?: boolean;
};

export type ValidationUiTargetDetail = JsonObject & {
  schema_version?: string;
  catalog_path?: string;
  missing?: boolean;
  target: ValidationUiTarget;
};

export type ValidationUiTargetQuery = {
  nav_group?: string;
  module?: string;
  coverage_status?: string;
  risk_level?: string;
  search?: string;
  page?: number;
  page_size?: number;
};

export type ValidationPhase1Card = JsonObject & {
  card_id: string;
  title?: string;
  primary_route?: string;
  health_tone?: string;
  risk_score?: number;
  summary?: JsonObject;
  reason_codes?: string[];
};

export type ValidationPhase1CardsSummary = JsonObject & {
  schema_version?: string;
  generated_at?: string;
  repo?: JsonObject;
  cards?: ValidationPhase1Card[];
  data_state?: string;
  production_8001_touched?: boolean;
};

export type ValidationMergeGate = JsonObject & {
  decision?: string;
  decision_label?: string;
  source_branch?: string | null;
  target_branch?: string;
  head_commit?: string | null;
  base_commit?: string | null;
  change_class?: string;
  changed_files?: string[];
  touched_modules?: string[];
  checks?: JsonObject[];
  blocking_reasons?: string[];
  warnings?: string[];
  manual_confirmations?: string[];
  recommended_next_actions?: string[];
  evidence_bundles?: string[];
  risk_score?: number;
  health_tone?: string;
  data_state?: string;
  detail?: JsonObject;
};

export type ValidationIssueWorkflowItem = JsonObject & {
  bug_id: string;
  title?: string;
  workflow_state?: string;
  severity?: string;
  module_id?: string;
  gate_state?: string;
  next_action?: string;
  allowed_write_scope_state?: string;
  required_verification_state?: string;
  closure_requirements_state?: string;
  github_issue_url?: string | null;
};

export type ValidationIssueWorkflowSummary = JsonObject & {
  open_count?: number;
  triaged_count?: number;
  triage_only_count?: number;
  in_progress_count?: number;
  review_ready_count?: number;
  missing_scope_count?: number;
  missing_required_verification_count?: number;
  by_workflow_state?: Record<string, number>;
  reason_codes?: string[];
};

export type ValidationPipelineTestItem = JsonObject & {
  test_id: string;
  title?: string;
  module?: string;
  level?: string;
  test_level?: string;
  status?: string;
  nox_session?: string;
  fast_path_eligible?: boolean;
  evidence_bundle_id?: string | null;
  rerun_cost_level?: string;
  recommended_command?: string | null;
};

export type ValidationPipelineTestSummary = JsonObject & {
  test_count?: number;
  blocking_count?: number;
  failed_count?: number;
  missing_evidence_count?: number;
  by_status?: Record<string, number>;
  reason_codes?: string[];
};

export type ValidationGithubIssueSync = JsonObject & {
  bug_id: string;
  title?: string;
  module_id?: string;
  severity?: string;
  workflow_state?: string;
  github_issue_number?: number | null;
  github_issue_url?: string | null;
  sync_state?: string;
  next_action?: string;
};

export type ValidationBranchDetailSummary = JsonObject & {
  current_branch?: string | null;
  head_commit?: string | null;
  branch_count?: number;
  worktree_count?: number;
  branches?: JsonObject[];
  worktrees?: JsonObject[];
  reason_codes?: string[];
  data_state?: string;
};

export type ValidationGithubPrSummary = JsonObject & {
  pr_count?: number;
  open_count?: number;
  by_state?: Record<string, number>;
  data_state?: string;
  reason_codes?: string[];
};

export type ValidationGithubPr = JsonObject & {
  number?: number;
  title?: string;
  head_ref?: string;
  base_ref?: string;
  state?: string;
  url?: string;
  merge_state_status?: string;
};

export type ValidationLegacyDebtSummary = JsonObject & {
  group_count?: number;
  debt_count?: number;
  p0_p1_count?: number;
  reason_codes?: string[];
};

export type ValidationLegacyDebtGroup = JsonObject & {
  debt_group_id: string;
  module?: string;
  category?: string;
  baseline_state?: string;
  count?: number;
  p0_p1_count?: number;
  sample_items?: JsonObject[];
};

export type ValidationAutomationSummary = JsonObject & {
  summary?: JsonObject;
  github_data_state?: string;
  gh_auth_status?: string;
  scripts?: JsonObject;
  actions?: JsonObject[];
  mcp_policy?: JsonObject;
  reason_codes?: string[];
};

export type ValidationDiscoverySummaryCard = JsonObject & {
  card_id: string;
  title: string;
  value?: string | number | boolean | null;
  hint?: string;
  tone?: string;
  filter?: string;
};

export type ValidationDiscoveryRun = JsonObject & {
  run_id?: string;
  title?: string;
  branch?: string | null;
  commit?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  status?: string | null;
};

export type ValidationDiscoveryModule = JsonObject & {
  module_id: string;
  display_name?: string;
  status?: string;
  coverage?: JsonObject;
  candidate_count?: number;
  p0_p1_count?: number;
  issue_count?: number;
  finding_count?: number;
  workspace_changed_file_count?: number;
  test_plans?: JsonObject;
  candidates?: ValidationDiscoveryCandidate[];
};

export type ValidationDiscoveryTask = JsonObject & {
  task_id: string;
  title?: string;
  source?: string;
  module?: string;
  risk_level?: string;
  status?: string;
  detectors?: string[];
  resource_policy_id?: string;
  requested_by?: string;
  reason?: string;
  cleanup_required?: boolean;
  cleanup_status?: string;
  created_at?: string;
  updated_at?: string;
  duration_ms?: number;
  evidence_manifest_id?: string;
  agent_runtime?: string;
  agent_name?: string;
  workspace?: string;
  branch?: string;
  result?: JsonObject;
};

export type ValidationDiscoveryExecutionNode = JsonObject & {
  node_id: string;
  label?: string;
  status?: string;
  duration_ms?: number;
  children?: ValidationDiscoveryTask[];
};

export type ValidationDiscoveryCandidate = JsonObject & {
  candidate_id: string;
  source?: string;
  source_id?: string;
  title?: string;
  module?: string;
  severity?: string;
  confidence?: number;
  review_status?: string;
  evidence_status?: string;
  deterministic_status?: string;
  github_issue_url?: string | null;
  github_issue_number?: number | null;
  evidence_types?: string[];
  evidence_manifest_id?: string;
  reproduce_command?: string | null;
  recommended_action?: string;
  created_at?: string | null;
  updated_at?: string | null;
  llm_provider_declared?: string | null;
  llm_model_declared?: string | null;
  prompt_id?: string | null;
  prompt_version?: string | number | null;
  context_pack_id?: string | null;
};

export type ValidationDiscoveryNightlyReport = JsonObject & {
  schema_version?: string;
  report_id: string;
  generated_at?: string;
  run?: ValidationDiscoveryRun;
  summary_cards?: ValidationDiscoverySummaryCard[];
  modules?: ValidationDiscoveryModule[];
  execution_tree?: ValidationDiscoveryExecutionNode[];
  llm_summary?: JsonObject;
  candidate_summary?: JsonObject & {
    total?: number;
    by_severity?: Record<string, number>;
    by_review_status?: Record<string, number>;
    needs_review?: number;
  };
  issue_sync?: JsonObject;
  cleanup?: JsonObject;
  evidence_manifest_id?: string;
};

export type ValidationDiscoveryNightlyReportSummary = JsonObject & {
  report_id: string;
  generated_at?: string;
  run?: ValidationDiscoveryRun;
  candidate_summary?: ValidationDiscoveryNightlyReport["candidate_summary"];
  llm_summary?: JsonObject;
  cleanup?: JsonObject;
};

export type ValidationDiscoveryLlmProfile = JsonObject & {
  profile_id: string;
  agent_role?: string;
  provider_id?: string;
  provider_status?: string;
  model_id?: string;
  prompt_id?: string;
  prompt_version?: string | number | null;
  prompt_management_url?: string;
  model_config_url?: string;
  temperature?: number;
  max_tokens?: number;
  enabled_for_nightly?: boolean;
  enabled_for_manual_mcp?: boolean;
  last_7_runs?: JsonObject;
  secret_visible?: boolean;
};

export type ValidationDiscoveryLlmReport = JsonObject & {
  report_id: string;
  generated_at?: string;
  profiles?: ValidationDiscoveryLlmProfile[];
  traces?: JsonObject[];
  draft_candidates?: ValidationDiscoveryCandidate[];
  eval_summary?: JsonObject;
  sensitive_payload_policy?: string;
};

export type ValidationDiscoveryEvidenceManifest = JsonObject & {
  manifest_id?: string;
  trace_id?: string;
  task_id?: string;
  generated_at?: string;
  artifacts?: JsonObject[];
  logs?: JsonObject[];
  api_responses?: JsonObject[];
  mcp_responses?: JsonObject[];
  screenshots?: JsonObject[];
  reproduce_command?: string | null;
  sensitive_payload_policy?: string;
};

export type ValidationDiscoveryToolAdapter = JsonObject & {
  adapter_id: string;
  title?: string;
  kind?: string;
  status?: string;
  config_path?: string;
  dry_run_supported?: boolean;
  writes_production?: boolean;
  requires_confirm_for_write?: boolean;
};

export type ValidationDiscoveryReviewRequest = {
  action: string;
  reviewer?: string;
  comment?: string;
  evidence_checklist?: string[];
};

export type ValidationDiscoveryPromoteRequest = {
  confirm_promote: string;
  reviewer?: string;
  comment?: string;
  evidence_checklist?: string[];
};

export type ValidationDiscoveryTaskRequest = {
  task_id?: string;
  title?: string;
  source?: string;
  module?: string;
  risk_level?: string;
  detectors?: string[];
  resource_policy_id?: string;
  requested_by?: string;
  reason?: string;
  cleanup_required?: boolean;
  confirm_schedule?: string;
};

export type ValidationDiscoveryRunTaskRequest = {
  dry_run?: boolean;
  confirm_run?: string;
};

export type ValidationDiscoveryAgentTaskRequest = {
  agent_runtime?: string;
  agent_name?: string;
  workspace?: string;
  branch?: string;
  llm_provider_declared?: string;
  llm_model_declared?: string;
  prompt_id?: string;
  prompt_version?: number;
  context_pack_id?: string;
  result_id?: string;
  candidate_title?: string;
  summary?: string;
  confidence?: number;
  requires_deterministic_verification?: boolean;
  evidence_manifest_id?: string;
  status?: string;
};

export type ValidationDiscoveryEvidenceRequest = {
  evidence_manifest_id?: string;
  artifacts?: JsonObject[];
  logs?: JsonObject[];
  api_responses?: JsonObject[];
  mcp_responses?: JsonObject[];
  screenshots?: JsonObject[];
  reproduce_command?: string;
};

const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1").replace(/\/+$/, "");

export class ValidationApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "ValidationApiError";
    this.status = status;
    this.raw = raw;
  }
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(payload: unknown, status: number): string {
  if (isObject(payload)) {
    if (typeof payload.detail === "string") return payload.detail;
    if (isObject(payload.detail)) {
      const detail = payload.detail;
      if (typeof detail.message === "string") return detail.message;
      return JSON.stringify(detail);
    }
    if (typeof payload.message === "string") return payload.message;
    if (typeof payload.error === "string") return payload.error;
  }
  return `HTTP ${status}`;
}

function appendQuery(path: string, params: Record<string, string | number | boolean | undefined | null>): string {
  const qs = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    qs.set(key, String(value));
  }
  const query = qs.toString();
  return query ? `${path}?${query}` : path;
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
  if (!response.ok) throw new ValidationApiError(errorMessage(payload, response.status), response.status, payload);
  return payload as T;
}

async function unwrap<T>(path: string): Promise<T> {
  const response = await apiFetch<ValidationEnvelope<T>>(path);
  return response.data;
}

export const validationApi = {
  health(): Promise<ValidationHealth> {
    return unwrap<ValidationHealth>("/validation/health");
  },
  platformHealth(): Promise<ValidationHealth> {
    return unwrap<ValidationHealth>("/validation/platform/health");
  },
  catalogIntegrity(): Promise<JsonObject> {
    return unwrap<JsonObject>("/validation/catalog/integrity");
  },
  nightlySummary(): Promise<JsonObject> {
    return unwrap<JsonObject>("/validation/nightly/summary");
  },
  nightlyRuns(query: { limit?: number } = {}): Promise<ValidationPage<JsonObject>> {
    return unwrap<ValidationPage<JsonObject>>(appendQuery("/validation/nightly/runs", query));
  },
  nightlyRunnerHealth(): Promise<JsonObject> {
    return unwrap<JsonObject>("/validation/nightly/runner-health");
  },
  plans(): Promise<ValidationPlanCatalog> {
    return unwrap<ValidationPlanCatalog>("/validation/plans");
  },
  plan(planKey: string): Promise<ValidationPlan> {
    return unwrap<ValidationPlan>(`/validation/plans/${encodeURIComponent(planKey)}`);
  },
  runs(query: ValidationRunQuery = {}): Promise<ValidationPage<ValidationRunSummary>> {
    return unwrap<ValidationPage<ValidationRunSummary>>(appendQuery("/validation/runs", query));
  },
  run(runId: string): Promise<ValidationRunDetail> {
    return unwrap<ValidationRunDetail>(`/validation/runs/${encodeURIComponent(runId)}`);
  },
  coverage(query: ValidationListQuery = {}): Promise<ValidationPage<ValidationCoverageSummary>> {
    return unwrap<ValidationPage<ValidationCoverageSummary>>(appendQuery("/validation/coverage", query));
  },
  coverageDetail(snapshotId: string): Promise<ValidationCoverageDetail> {
    return unwrap<ValidationCoverageDetail>(`/validation/coverage/${encodeURIComponent(snapshotId)}`);
  },
  evidence(query: Omit<ValidationListQuery, "status"> = {}): Promise<ValidationPage<ValidationEvidenceSummary>> {
    return unwrap<ValidationPage<ValidationEvidenceSummary>>(appendQuery("/validation/evidence", query));
  },
  evidenceDetail(manifestId: string): Promise<ValidationEvidenceDetail> {
    return unwrap<ValidationEvidenceDetail>(`/validation/evidence/${encodeURIComponent(manifestId)}`);
  },
  summary(): Promise<ValidationSummary> {
    return unwrap<ValidationSummary>("/validation/summary");
  },
  findings(query: ValidationFindingQuery = {}): Promise<ValidationPage<ValidationQualityFinding>> {
    return unwrap<ValidationPage<ValidationQualityFinding>>(appendQuery("/validation/findings", query));
  },
  finding(findingId: string): Promise<ValidationQualityFinding> {
    return unwrap<ValidationQualityFinding>(`/validation/findings/${encodeURIComponent(findingId)}`);
  },
  findingSummary(): Promise<ValidationFindingSummary> {
    return unwrap<ValidationFindingSummary>("/validation/findings/summary");
  },
  bugs(query: ValidationBugQuery = {}): Promise<ValidationPage<ValidationBug>> {
    return unwrap<ValidationPage<ValidationBug>>(appendQuery("/validation/bugs", query));
  },
  bug(bugId: string): Promise<ValidationBug> {
    return unwrap<ValidationBug>(`/validation/bugs/${encodeURIComponent(bugId)}`);
  },
  bugSummary(): Promise<ValidationBugSummary> {
    return unwrap<ValidationBugSummary>("/validation/bugs/summary");
  },
  bugAgentContext(bugId: string): Promise<ValidationAgentContext> {
    return unwrap<ValidationAgentContext>(`/validation/bugs/${encodeURIComponent(bugId)}/agent-context`);
  },
  workspaceStatus(): Promise<ValidationGitWorkspaceStatus> {
    return unwrap<ValidationGitWorkspaceStatus>("/validation/git/workspace-status");
  },
  branchStatus(): Promise<ValidationGitBranchStatus> {
    return unwrap<ValidationGitBranchStatus>("/validation/git/branch-status");
  },
  commitActivity(limit = 50): Promise<ValidationGitCommitActivity> {
    return unwrap<ValidationGitCommitActivity>(appendQuery("/validation/git/commit-activity", { limit }));
  },
  moduleQualitySummary(commitLimit = 50): Promise<ValidationModuleQualitySummary> {
    return unwrap<ValidationModuleQualitySummary>(appendQuery("/validation/modules/quality-summary", { commit_limit: commitLimit }));
  },
  cardsSummary(): Promise<ValidationPhase1CardsSummary> {
    return unwrap<ValidationPhase1CardsSummary>("/validation/cards/summary");
  },
  mergeGateSummary(): Promise<ValidationMergeGate> {
    return unwrap<ValidationMergeGate>("/validation/merge-gate/summary");
  },
  mergeGateDetail(): Promise<ValidationMergeGate> {
    return unwrap<ValidationMergeGate>("/validation/merge-gate/detail");
  },
  issueWorkflowSummary(): Promise<ValidationIssueWorkflowSummary> {
    return unwrap<ValidationIssueWorkflowSummary>("/validation/issues/workflow/summary");
  },
  issueWorkflow(query: ValidationListQuery & { severity?: string; workflow_state?: string } = {}): Promise<ValidationPage<ValidationIssueWorkflowItem>> {
    return unwrap<ValidationPage<ValidationIssueWorkflowItem>>(appendQuery("/validation/issues/workflow", query));
  },
  issueWorkflowDetail(bugId: string): Promise<ValidationIssueWorkflowItem> {
    return unwrap<ValidationIssueWorkflowItem>(`/validation/issues/${encodeURIComponent(bugId)}/workflow`);
  },
  moduleDetailSummary(): Promise<ValidationModuleQualitySummary> {
    return unwrap<ValidationModuleQualitySummary>("/validation/modules/detail-summary");
  },
  pipelineTestsSummary(): Promise<ValidationPipelineTestSummary> {
    return unwrap<ValidationPipelineTestSummary>("/validation/pipeline/tests/summary");
  },
  pipelineTests(query: { page?: number; page_size?: number } = {}): Promise<ValidationPage<ValidationPipelineTestItem>> {
    return unwrap<ValidationPage<ValidationPipelineTestItem>>(appendQuery("/validation/pipeline/tests", query));
  },
  featuresSummary(): Promise<ValidationUiTargetSummary> {
    return unwrap<ValidationUiTargetSummary>("/validation/features/summary");
  },
  features(query: { page?: number; page_size?: number } = {}): Promise<ValidationUiTargetPage> {
    return unwrap<ValidationUiTargetPage>(appendQuery("/validation/features", query));
  },
  githubIssuesSummary(): Promise<JsonObject> {
    return unwrap<JsonObject>("/validation/github/issues/summary");
  },
  githubIssues(query: { page?: number; page_size?: number } = {}): Promise<ValidationPage<ValidationGithubIssueSync>> {
    return unwrap<ValidationPage<ValidationGithubIssueSync>>(appendQuery("/validation/github/issues", query));
  },
  branchDetailSummary(): Promise<ValidationBranchDetailSummary> {
    return unwrap<ValidationBranchDetailSummary>("/validation/git/branches/detail-summary");
  },
  githubPrsSummary(): Promise<ValidationGithubPrSummary> {
    return unwrap<ValidationGithubPrSummary>("/validation/github/prs/summary");
  },
  githubPrs(query: { page?: number; page_size?: number } = {}): Promise<ValidationPage<ValidationGithubPr>> {
    return unwrap<ValidationPage<ValidationGithubPr>>(appendQuery("/validation/github/prs", query));
  },
  legacyDebtSummary(): Promise<ValidationLegacyDebtSummary> {
    return unwrap<ValidationLegacyDebtSummary>("/validation/legacy-debt/summary");
  },
  legacyDebtGroups(query: { page?: number; page_size?: number } = {}): Promise<ValidationPage<ValidationLegacyDebtGroup>> {
    return unwrap<ValidationPage<ValidationLegacyDebtGroup>>(appendQuery("/validation/legacy-debt/groups", query));
  },
  automationSummary(): Promise<ValidationAutomationSummary> {
    return unwrap<ValidationAutomationSummary>("/validation/automation/summary");
  },
  discoverySummary(): Promise<JsonObject> {
    return unwrap<JsonObject>("/validation/discovery/summary");
  },
  discoveryNightlyReports(query: { limit?: number } = {}): Promise<ValidationPage<ValidationDiscoveryNightlyReportSummary>> {
    return unwrap<ValidationPage<ValidationDiscoveryNightlyReportSummary>>(appendQuery("/validation/discovery/nightly-reports", query));
  },
  discoveryNightlyReport(reportId = "current"): Promise<ValidationDiscoveryNightlyReport> {
    return unwrap<ValidationDiscoveryNightlyReport>(`/validation/discovery/nightly-reports/${encodeURIComponent(reportId)}`);
  },
  discoveryNightlyLlmReport(reportId = "current"): Promise<ValidationDiscoveryLlmReport> {
    return unwrap<ValidationDiscoveryLlmReport>(`/validation/discovery/nightly-reports/${encodeURIComponent(reportId)}/llm`);
  },
  discoveryCandidates(query: { module?: string; severity?: string; review_status?: string; source?: string; search?: string; page?: number; page_size?: number } = {}): Promise<ValidationPage<ValidationDiscoveryCandidate>> {
    return unwrap<ValidationPage<ValidationDiscoveryCandidate>>(appendQuery("/validation/discovery/candidates", query));
  },
  discoveryCandidate(candidateId: string): Promise<ValidationDiscoveryCandidate> {
    return unwrap<ValidationDiscoveryCandidate>(`/validation/discovery/candidates/${encodeURIComponent(candidateId)}`);
  },
  reviewDiscoveryCandidate(candidateId: string, request: ValidationDiscoveryReviewRequest): Promise<JsonObject> {
    return apiFetch<ValidationEnvelope<JsonObject>>(`/validation/discovery/candidates/${encodeURIComponent(candidateId)}/review`, {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
  promoteDiscoveryCandidate(candidateId: string, request: ValidationDiscoveryPromoteRequest): Promise<JsonObject> {
    return apiFetch<ValidationEnvelope<JsonObject>>(`/validation/discovery/candidates/${encodeURIComponent(candidateId)}/promote`, {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
  discoveryTasks(query: { source?: string; status?: string; page?: number; page_size?: number } = {}): Promise<ValidationPage<ValidationDiscoveryTask>> {
    return unwrap<ValidationPage<ValidationDiscoveryTask>>(appendQuery("/validation/discovery/tasks", query));
  },
  scheduleDiscoveryTask(request: ValidationDiscoveryTaskRequest): Promise<ValidationDiscoveryTask> {
    return apiFetch<ValidationEnvelope<ValidationDiscoveryTask>>("/validation/discovery/tasks", {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
  runDiscoveryTask(taskId: string, request: ValidationDiscoveryRunTaskRequest): Promise<ValidationDiscoveryTask> {
    return apiFetch<ValidationEnvelope<ValidationDiscoveryTask>>(`/validation/discovery/tasks/${encodeURIComponent(taskId)}/run`, {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
  cancelDiscoveryTask(taskId: string, reason?: string): Promise<ValidationDiscoveryTask> {
    return apiFetch<ValidationEnvelope<ValidationDiscoveryTask>>(`/validation/discovery/tasks/${encodeURIComponent(taskId)}/cancel`, {
      method: "POST",
      body: JSON.stringify({ reason }),
    }).then((response) => response.data);
  },
  claimDiscoveryAgentTask(taskId: string, request: ValidationDiscoveryAgentTaskRequest): Promise<ValidationDiscoveryTask> {
    return apiFetch<ValidationEnvelope<ValidationDiscoveryTask>>(`/validation/discovery/agent-tasks/${encodeURIComponent(taskId)}/claim`, {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
  discoveryAgentContextPack(taskId: string): Promise<JsonObject> {
    return unwrap<JsonObject>(`/validation/discovery/agent-tasks/${encodeURIComponent(taskId)}/context-pack`);
  },
  submitDiscoveryAgentResult(taskId: string, request: ValidationDiscoveryAgentTaskRequest): Promise<JsonObject> {
    return apiFetch<ValidationEnvelope<JsonObject>>(`/validation/discovery/agent-tasks/${encodeURIComponent(taskId)}/results`, {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
  attachDiscoveryAgentEvidence(taskId: string, request: ValidationDiscoveryEvidenceRequest): Promise<ValidationDiscoveryEvidenceManifest> {
    return apiFetch<ValidationEnvelope<ValidationDiscoveryEvidenceManifest>>(`/validation/discovery/agent-tasks/${encodeURIComponent(taskId)}/evidence`, {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
  completeDiscoveryAgentTask(taskId: string, request: ValidationDiscoveryAgentTaskRequest): Promise<ValidationDiscoveryTask> {
    return apiFetch<ValidationEnvelope<ValidationDiscoveryTask>>(`/validation/discovery/agent-tasks/${encodeURIComponent(taskId)}/complete`, {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
  discoveryLlmProfiles(): Promise<ValidationPage<ValidationDiscoveryLlmProfile> & { prompt_management_url?: string; model_config_url?: string }> {
    return unwrap<ValidationPage<ValidationDiscoveryLlmProfile> & { prompt_management_url?: string; model_config_url?: string }>("/validation/discovery/llm-profiles");
  },
  discoveryToolAdapters(): Promise<ValidationPage<ValidationDiscoveryToolAdapter>> {
    return unwrap<ValidationPage<ValidationDiscoveryToolAdapter>>("/validation/discovery/tool-adapters");
  },
  runDiscoveryToolAdapter(adapterId: string, request: { dry_run?: boolean; confirm_run?: string; profiles?: string[] } = { dry_run: true }): Promise<JsonObject> {
    return apiFetch<ValidationEnvelope<JsonObject>>(`/validation/discovery/tool-adapters/${encodeURIComponent(adapterId)}/dry-run`, {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
  discoveryLlmEvals(): Promise<JsonObject> {
    return unwrap<JsonObject>("/validation/discovery/llm-evals");
  },
  runDiscoveryLlmEval(request: { dry_run?: boolean; profiles?: string[] } = { dry_run: true }): Promise<JsonObject> {
    return apiFetch<ValidationEnvelope<JsonObject>>("/validation/discovery/llm-evals/run", {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
  discoveryTrace(traceId: string): Promise<ValidationDiscoveryEvidenceManifest> {
    return unwrap<ValidationDiscoveryEvidenceManifest>(`/validation/discovery/traces/${encodeURIComponent(traceId)}`);
  },
  uiTargets(query: ValidationUiTargetQuery = {}): Promise<ValidationUiTargetPage> {
    return unwrap<ValidationUiTargetPage>(appendQuery("/validation/ui-targets", query));
  },
  uiTargetSummary(): Promise<ValidationUiTargetSummary> {
    return unwrap<ValidationUiTargetSummary>("/validation/ui-targets/summary");
  },
  uiTarget(routeId: string): Promise<ValidationUiTargetDetail> {
    return unwrap<ValidationUiTargetDetail>(`/validation/ui-targets/${encodeURIComponent(routeId)}`);
  },
  executions(query: ValidationExecutionQuery = {}): Promise<ValidationPage<ValidationExecutionJob>> {
    return unwrap<ValidationPage<ValidationExecutionJob>>(appendQuery("/validation/executions", query));
  },
  execution(jobId: string): Promise<ValidationExecutionJob> {
    return unwrap<ValidationExecutionJob>(`/validation/executions/${encodeURIComponent(jobId)}`);
  },
  executionLog(jobId: string, tailLines = 300): Promise<ValidationExecutionLog> {
    return unwrap<ValidationExecutionLog>(appendQuery(`/validation/executions/${encodeURIComponent(jobId)}/log`, { tail_lines: tailLines }));
  },
  executionEvidence(jobId: string): Promise<ValidationExecutionEvidence> {
    return unwrap<ValidationExecutionEvidence>(`/validation/executions/${encodeURIComponent(jobId)}/evidence`);
  },
  startExecution(request: ValidationExecutionStartRequest): Promise<ValidationExecutionJob> {
    return apiFetch<ValidationEnvelope<ValidationExecutionJob>>("/validation/executions", {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
};

export { API_BASE };
