import MetricCard from "@/components/trading-console/MetricCard";
import SectionCard from "@/components/trading-console/SectionCard";
import StatusBadge from "@/components/trading-console/StatusBadge";
import {
  type JsonObject,
  type OverviewSource,
  type PipelineOverviewSnapshot,
  type ValidationAutomationSummary,
  type ValidationBranchDetailSummary,
  type ValidationCodeIntelligenceSummary,
  type ValidationExecutionJob,
  type ValidationGithubPrSummary,
  type ValidationHealth,
  type ValidationIssueCandidateSummary,
  type ValidationIssueWorkflowSummary,
  type ValidationLegacyDebtSummary,
  type ValidationMergeGate,
  type ValidationModuleQualitySummary,
  type ValidationPage,
  type ValidationPhase1Card,
  type ValidationPhase1CardsSummary,
  type ValidationPipelineTestSummary,
  type ValidationPlan,
  type ValidationSummary,
  type ValidationUiTargetSummary,
} from "@/lib/validation/api";

export type PipelineOverviewSectionId =
  | "overview"
  | "code_intelligence"
  | "merge_gate"
  | "issue_workflow"
  | "pipeline_tests"
  | "features"
  | "modules"
  | "github_issues"
  | "branches_prs"
  | "legacy_debt"
  | "automation";

type PipelineTone = "green" | "yellow" | "orange" | "red" | "gray";

type PipelineSection = {
  id: PipelineOverviewSectionId;
  label: string;
  hint: string;
};

type PipelineCardView = {
  id: PipelineOverviewSectionId;
  label: string;
  hint: string;
  tone: PipelineTone;
  state: string;
  meta: string;
  risk: string | number;
  rows: Array<[string, unknown]>;
  reasonCodes: string[];
};

export type PipelineOverviewCardsProps = {
  snapshot?: PipelineOverviewSnapshot | null;
  activeSection: string;
  onSelect: (section: PipelineOverviewSectionId) => void;
  cardsSummary?: ValidationPhase1CardsSummary | null;
  health?: ValidationHealth | null;
  validationSummary?: ValidationSummary | null;
  codeIntelligenceSummary?: ValidationCodeIntelligenceSummary | null;
  plans?: ValidationPlan[];
  executions?: ValidationPage<ValidationExecutionJob> | null;
  mergeGate?: ValidationMergeGate | null;
  issueCandidateSummary?: ValidationIssueCandidateSummary | null;
  issueWorkflowSummary?: ValidationIssueWorkflowSummary | null;
  moduleQuality?: ValidationModuleQualitySummary | null;
  moduleDetailSummary?: ValidationModuleQualitySummary | null;
  pipelineTestSummary?: ValidationPipelineTestSummary | null;
  uiTargetSummary?: ValidationUiTargetSummary | null;
  githubIssueSummary?: JsonObject | null;
  branchDetailSummary?: ValidationBranchDetailSummary | null;
  githubPrSummary?: ValidationGithubPrSummary | null;
  legacyDebtSummary?: ValidationLegacyDebtSummary | null;
  automationSummary?: ValidationAutomationSummary | null;
  optionalApiWarnings?: Record<string, string>;
};

export const PIPELINE_OVERVIEW_SECTIONS: PipelineSection[] = [
  { id: "overview", label: "平台健康", hint: "运行环境 / catalog / 连接" },
  { id: "code_intelligence", label: "Code Intelligence", hint: "CodeGraph / UA freshness" },
  { id: "merge_gate", label: "合入门禁", hint: "只读预览" },
  { id: "issue_workflow", label: "Issue 修复流程", hint: "BUG / GitHub / 验证" },
  { id: "pipeline_tests", label: "测试计划", hint: "计划 / 证据 / Runner" },
  { id: "features", label: "功能与路由", hint: "菜单 / UI target" },
  { id: "modules", label: "模块质量", hint: "覆盖率 / Issue / 优先级" },
  { id: "github_issues", label: "GitHub Issue", hint: "镜像 / 待同步" },
  { id: "branches_prs", label: "分支与 PR", hint: "worktree / PR" },
  { id: "legacy_debt", label: "历史遗留", hint: "基线债务" },
  { id: "automation", label: "Nightly / Runner", hint: "夜间验证 / MCP" },
];

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function display(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "number") return Number.isFinite(value) ? value.toLocaleString("en-US") : "-";
  if (Array.isArray(value)) return value.length ? value.map((item) => display(item)).join(" / ") : "-";
  if (isObject(value)) return Object.entries(value).map(([key, item]) => `${key}: ${display(item)}`).join("; ");
  return String(value);
}

function numberValue(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function stringList(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string" && item.length > 0) : [];
}

function objectField(value: unknown, key: string): unknown {
  return isObject(value) ? value[key] : undefined;
}

function normalizeTone(value: unknown): PipelineTone {
  const text = String(value || "").toLowerCase();
  if (["green", "success", "healthy", "passed", "pass", "ok"].includes(text)) return "green";
  if (["yellow", "warning", "degraded", "queued", "pending"].includes(text)) return "yellow";
  if (["orange", "failed", "error"].includes(text)) return "orange";
  if (["red", "danger", "blocked", "critical"].includes(text)) return "red";
  return "gray";
}

function metricTone(tone: PipelineTone): "neutral" | "success" | "warning" | "danger" | "info" {
  if (tone === "green") return "success";
  if (tone === "yellow" || tone === "orange") return "warning";
  if (tone === "red") return "danger";
  return "info";
}

function toneFromState(state: unknown): PipelineTone {
  const text = String(state || "").toLowerCase();
  if (["blocked", "failed", "error"].includes(text)) return text === "blocked" ? "red" : "orange";
  return normalizeTone(text);
}

function cardSummaryText(card?: ValidationPhase1Card): string {
  const summary = isObject(card?.summary) ? card?.summary : {};
  const decision = summary.decision;
  const total =
    summary.test_count ??
    summary.bug_count ??
    summary.debt_count ??
    summary.worktree_count ??
    summary.target_count ??
    summary.module_count ??
    summary.open_count;
  if (decision) return `裁决 ${display(decision)}`;
  if (total !== undefined) return `数量 ${display(total)}`;
  return card?.health_tone ? `状态 ${display(card.health_tone)}` : "";
}

function findBackendCard(cardsSummary: ValidationPhase1CardsSummary | null | undefined, sectionId: string): ValidationPhase1Card | undefined {
  return (cardsSummary?.cards || []).find((card) => card.card_id === sectionId);
}

function buildSectionView(section: PipelineSection, props: PipelineOverviewCardsProps): PipelineCardView {
  const backendCard = findBackendCard(props.cardsSummary, section.id);
  const warnings = props.optionalApiWarnings || {};
  const warningKeys = Object.keys(warnings);
  const baseRows: Array<[string, unknown]> = [
    ["数据来源", backendCard ? "后端 cards summary + 页面 API" : "当前页面 API 聚合"],
    ["后端卡片状态", backendCard?.health_tone || "-"],
    ["后端风险分", backendCard?.risk_score ?? "-"],
  ];

  const required: Record<string, unknown> = {
    overview: props.health,
    code_intelligence: props.codeIntelligenceSummary?.data_state,
    merge_gate: props.mergeGate?.decision,
    issue_workflow: props.issueWorkflowSummary?.open_count,
    pipeline_tests: props.pipelineTestSummary?.test_count,
    features: props.uiTargetSummary?.target_count,
    modules: props.moduleQuality?.summary?.module_count,
    github_issues: props.githubIssueSummary?.bug_count,
    branches_prs: props.githubPrSummary?.open_count,
    legacy_debt: props.legacyDebtSummary?.debt_count,
    automation: props.snapshot?.sections.nightly?.data,
  };
  if (section.id === "overview" && props.snapshot) {
    const sources = Object.values(props.snapshot.sections);
    const complete = sources.length > 0 && sources.every((source) => source.state === "available" && !source.stale);
    return { id: section.id, label: section.label, hint: section.hint, tone: "gray",
      state: complete ? "快照已读取（不代表业务验收）" : "部分数据 / unknown",
      meta: "CI / Runner / Nightly / BUG / PR / worktree", risk: "-",
      rows: [["数据时间", props.snapshot.generated_at], ["来源", "overview/snapshot"], ["复用时间（秒）", props.snapshot.ttl_seconds]], reasonCodes: [] };
  }
  if (required[section.id] === null || required[section.id] === undefined || warningKeys.length) {
    return { id: section.id, label: section.label, hint: section.hint, tone: "gray", state: warningKeys.length ? "unavailable" : "unknown",
      meta: "未读取 / 数据不完整，不作通过判断", risk: "-", rows: baseRows,
      reasonCodes: warningKeys.length ? warningKeys : ["source_not_loaded"] };
  }

  if (section.id === "overview") {
    const repo = isObject(props.cardsSummary?.repo) ? props.cardsSummary?.repo : {};
    const state = objectField(props.health, "state") || props.health?.status || props.cardsSummary?.data_state || "unknown";
    const productionTouched = Boolean(props.health?.production_8001_touched || props.cardsSummary?.production_8001_touched);
    const planCount = props.plans?.length ?? props.health?.plan_catalog?.plan_count ?? 0;
    const tone = productionTouched ? "red" : toneFromState(state);
    const codeIntel = props.codeIntelligenceSummary || props.validationSummary?.code_intelligence;
    const codegraph = isObject(codeIntel?.codegraph) ? codeIntel?.codegraph : {};
    const ua = isObject(codeIntel?.understand_anything) ? codeIntel?.understand_anything : {};
    const codeIntelWarnings = stringList(codeIntel?.warnings);
    const codegraphFreshness =
      objectField(codegraph, "effective_freshness") || objectField(codegraph, "freshness") || objectField(codegraph, "status") || "-";
    return {
      id: section.id,
      label: section.label,
      hint: section.hint,
      tone,
      state: display(state),
      meta: `${display(repo.current_branch || objectField(props.health, "branch"))} / plans ${display(planCount)}`,
      risk: productionTouched ? 100 : warningKeys.length ? 35 : backendCard?.risk_score ?? "-",
      rows: [
        ["repo root", repo.root || objectField(objectField(props.health, "repo_context"), "repo_root")],
        ["branch", repo.current_branch || objectField(objectField(props.health, "repo_context"), "branch")],
        ["commit", repo.head_commit || objectField(objectField(props.health, "repo_context"), "commit")],
        ["baseline ref", repo.target_branch || objectField(objectField(props.health, "repo_context"), "baseline_ref") || "main"],
        ["health generated_at", objectField(props.health, "generated_at") || props.cardsSummary?.generated_at],
        ["catalog", objectField(props.health, "catalog_integrity") || props.health?.plan_catalog],
        ["runner", props.health?.runner || props.validationSummary?.runner],
        ["GitHub 连接", props.automationSummary?.gh_auth_status || props.githubIssueSummary?.data_state || props.githubPrSummary?.data_state],
        ["code intelligence", codeIntel?.data_state || "missing"],
        ["CodeGraph freshness", codegraphFreshness],
        ["CodeGraph effective_source", objectField(codegraph, "effective_source") || "-"],
        ["CodeGraph stale_metadata_warning", objectField(codegraph, "stale_metadata_warning") ?? false],
        ["UA summaries", objectField(ua, "summary_count") ?? "-"],
        ["code intelligence warnings", codeIntelWarnings.length ? codeIntelWarnings.join(" / ") : "none"],
        ["可选 API 降级", warningKeys.length ? warningKeys.join(" / ") : "无"],
      ],
      reasonCodes: [
        ...(productionTouched ? ["production_8001_touched"] : []),
        ...warningKeys.map((key) => `optional_api_unavailable:${key}`),
        ...stringList(codeIntel?.reason_codes),
      ],
    };
  }

  if (section.id === "code_intelligence") {
    const codeIntel = props.codeIntelligenceSummary || props.validationSummary?.code_intelligence;
    const codegraph = isObject(codeIntel?.codegraph) ? codeIntel?.codegraph : {};
    const ua = isObject(codeIntel?.understand_anything) ? codeIntel?.understand_anything : {};
    const warningsList = stringList(codeIntel?.warnings);
    const freshness = String(
      objectField(codegraph, "effective_freshness") || objectField(codegraph, "freshness") || objectField(codegraph, "status") || "missing",
    );
    const dataState = String(codeIntel?.data_state || "missing");
    const warningOnly = codeIntel?.blocking_for_issue_workflow === false;
    const tone = freshness === "fresh" && dataState === "complete" ? "green" : "yellow";
    return {
      id: section.id,
      label: section.label,
      hint: section.hint,
      tone,
      state: warningOnly ? "warning-only" : display(dataState),
      meta: `CodeGraph ${display(freshness)} / UA ${display(objectField(ua, "summary_count") ?? 0)}`,
      risk: warningsList.length ? Math.min(60, warningsList.length * 15) : tone === "green" ? 0 : 25,
      rows: [
        ...baseRows,
        ["data_state", dataState],
        ["blocking_for_issue_workflow", codeIntel?.blocking_for_issue_workflow],
        ["artifact_count", codeIntel?.artifact_count],
        ["artifact_roots", codeIntel?.artifact_roots],
        ["CodeGraph freshness", freshness],
        ["CodeGraph effective_source", objectField(codegraph, "effective_source")],
        ["CodeGraph stale_metadata_warning", objectField(codegraph, "stale_metadata_warning") ?? false],
        ["CodeGraph generated_at", objectField(codegraph, "generated_at")],
        ["CodeGraph artifact_path", objectField(codegraph, "artifact_path")],
        ["CodeGraph summary_ref", objectField(codegraph, "summary_ref")],
        ["Understand Anything summaries", objectField(ua, "summary_count")],
        ["warnings", warningsList.length ? warningsList.join(" / ") : "none"],
      ],
      reasonCodes: stringList(codeIntel?.reason_codes),
    };
  }

  if (section.id === "merge_gate") {
    const state = props.mergeGate?.decision || objectField(backendCard?.summary, "decision") || "unknown";
    return {
      id: section.id,
      label: section.label,
      hint: section.hint,
      tone: normalizeTone(backendCard?.health_tone || (state === "blocked" ? "red" : state === "pass" ? "green" : "yellow")),
      state: display(state),
      meta: `${display(props.mergeGate?.source_branch)} → ${display(props.mergeGate?.target_branch || "main")}`,
      risk: props.mergeGate?.risk_score ?? backendCard?.risk_score ?? "-",
      rows: [
        ...baseRows,
        ["decision", props.mergeGate?.decision_label || props.mergeGate?.decision],
        ["change_class", props.mergeGate?.change_class],
        ["blocking_reasons", props.mergeGate?.blocking_reasons],
        ["warnings", props.mergeGate?.warnings],
        ["manual_confirmations", props.mergeGate?.manual_confirmations],
      ],
      reasonCodes: stringList(props.mergeGate?.blocking_reasons),
    };
  }

  if (section.id === "issue_workflow") {
    const missing = numberValue(props.issueWorkflowSummary?.missing_scope_count) + numberValue(props.issueWorkflowSummary?.missing_required_verification_count);
    const readyDrafts = numberValue(props.issueCandidateSummary?.issue_payload_ready_count);
    const candidateWarnings = numberValue(props.issueCandidateSummary?.missing_issue_link_count);
    return {
      id: section.id,
      label: section.label,
      hint: section.hint,
      tone: missing || candidateWarnings ? "yellow" : normalizeTone(backendCard?.health_tone || "green"),
      state: missing ? "needs-scope" : readyDrafts ? "issue-ready" : "ready",
      meta: `open ${display(props.issueWorkflowSummary?.open_count ?? objectField(backendCard?.summary, "open_count") ?? 0)} / nightly ${display(props.issueCandidateSummary?.nightly_candidate_count ?? props.issueCandidateSummary?.candidate_count ?? 0)}`,
      risk: backendCard?.risk_score ?? (missing + candidateWarnings) * 12,
      rows: [
        ...baseRows,
        ["open_count", props.issueWorkflowSummary?.open_count],
        ["in_progress_count", props.issueWorkflowSummary?.in_progress_count],
        ["review_ready_count", props.issueWorkflowSummary?.review_ready_count],
        ["missing_scope_count", props.issueWorkflowSummary?.missing_scope_count],
        ["missing_required_verification_count", props.issueWorkflowSummary?.missing_required_verification_count],
        ["nightly_candidate_count", props.issueCandidateSummary?.nightly_candidate_count],
        ["issue_payload_ready_count", props.issueCandidateSummary?.issue_payload_ready_count],
        ["candidate_status", props.issueCandidateSummary?.by_status],
        ["candidate_no_submit_reasons", props.issueCandidateSummary?.no_submit_reason_counts],
        ["by_workflow_state", props.issueWorkflowSummary?.by_workflow_state],
      ],
      reasonCodes: [...stringList(props.issueWorkflowSummary?.reason_codes), ...stringList(props.issueCandidateSummary?.reason_codes)],
    };
  }

  if (section.id === "pipeline_tests") {
    const runnerEnabled = (props.plans || []).filter((plan) => plan.runner_enabled).length;
    const failed = numberValue(props.pipelineTestSummary?.failed_count);
    const missingEvidence = numberValue(props.pipelineTestSummary?.missing_evidence_count);
    return {
      id: section.id,
      label: section.label,
      hint: section.hint,
      tone: failed ? "red" : missingEvidence ? "yellow" : normalizeTone(backendCard?.health_tone || "green"),
      state: failed ? "failed" : missingEvidence ? "missing evidence" : "ready",
      meta: `plans ${display(props.plans?.length ?? props.pipelineTestSummary?.test_count ?? 0)} / runner ${display(runnerEnabled)}`,
      risk: backendCard?.risk_score ?? failed * 20 + missingEvidence * 5,
      rows: [
        ...baseRows,
        ["catalog plans", props.plans?.length],
        ["runner_enabled plans", runnerEnabled],
        ["pipeline summary", props.pipelineTestSummary],
        ["runner health", props.validationSummary?.runner || props.health?.runner],
        ["recent runner jobs", props.executions?.total ?? props.executions?.items?.length],
      ],
      reasonCodes: stringList(props.pipelineTestSummary?.reason_codes),
    };
  }

  if (section.id === "features") {
    const needsAction = numberValue(props.uiTargetSummary?.targets_requiring_action);
    return {
      id: section.id,
      label: section.label,
      hint: section.hint,
      tone: needsAction ? "yellow" : normalizeTone(backendCard?.health_tone || "green"),
      state: needsAction ? "需补证" : "已登记",
      meta: `routes ${display(props.uiTargetSummary?.target_count ?? objectField(backendCard?.summary, "target_count") ?? 0)}`,
      risk: backendCard?.risk_score ?? needsAction * 6,
      rows: [
        ...baseRows,
        ["catalog_path", props.uiTargetSummary?.catalog_path],
        ["target_count", props.uiTargetSummary?.target_count],
        ["warning_count", props.uiTargetSummary?.warning_count],
        ["targets_requiring_action", props.uiTargetSummary?.targets_requiring_action],
        ["by_coverage_status", props.uiTargetSummary?.by_coverage_status],
      ],
      reasonCodes: [],
    };
  }

  if (section.id === "modules") {
    const moduleSummary = props.moduleDetailSummary?.summary || props.moduleQuality?.summary || {};
    const risk = numberValue(moduleSummary.max_risk_score ?? backendCard?.risk_score);
    return {
      id: section.id,
      label: section.label,
      hint: section.hint,
      tone: risk >= 70 ? "red" : risk >= 40 ? "orange" : normalizeTone(backendCard?.health_tone || "green"),
      state: risk >= 70 ? "高风险" : risk >= 40 ? "需关注" : "可审查",
      meta: `modules ${display(moduleSummary.module_count ?? props.moduleQuality?.modules?.length ?? 0)}`,
      risk: risk || backendCard?.risk_score || "-",
      rows: [
        ...baseRows,
        ["summary", moduleSummary],
        ["workspace_summary", props.moduleQuality?.workspace_summary],
        ["commit_summary", props.moduleQuality?.commit_summary],
        ["global_reason_codes", props.moduleQuality?.global_reason_codes],
      ],
      reasonCodes: stringList(props.moduleQuality?.global_reason_codes),
    };
  }

  if (section.id === "github_issues") {
    const missing = numberValue(props.githubIssueSummary?.missing_link_count);
    const mismatch = numberValue(props.githubIssueSummary?.workflow_mismatch_count);
    return {
      id: section.id,
      label: section.label,
      hint: section.hint,
      tone: mismatch ? "red" : missing ? "yellow" : normalizeTone(backendCard?.health_tone || "green"),
      state: mismatch ? "不同步" : missing ? "待同步" : "已镜像",
      meta: `linked ${display(props.githubIssueSummary?.linked_count ?? 0)} / bugs ${display(props.githubIssueSummary?.bug_count ?? 0)}`,
      risk: backendCard?.risk_score ?? missing * 5 + mismatch * 20,
      rows: [
        ...baseRows,
        ["github summary", props.githubIssueSummary],
        ["GitHub auth", props.automationSummary?.gh_auth_status],
        ["GitHub data_state", props.githubPrSummary?.data_state || props.githubIssueSummary?.data_state],
      ],
      reasonCodes: stringList(props.githubIssueSummary?.reason_codes),
    };
  }

  if (section.id === "branches_prs") {
    const worktreeCount = numberValue(props.branchDetailSummary?.worktree_count);
    const dataUnavailable = props.githubPrSummary?.data_state === "unavailable";
    return {
      id: section.id,
      label: section.label,
      hint: section.hint,
      tone: dataUnavailable ? "yellow" : worktreeCount > 20 ? "orange" : normalizeTone(backendCard?.health_tone || "green"),
      state: dataUnavailable ? "GitHub unknown" : "已读取",
      meta: `${display(props.branchDetailSummary?.current_branch)} / PR ${display(props.githubPrSummary?.open_count ?? 0)}`,
      risk: backendCard?.risk_score ?? Math.min(100, worktreeCount),
      rows: [
        ...baseRows,
        ["current_branch", props.branchDetailSummary?.current_branch],
        ["head_commit", props.branchDetailSummary?.head_commit],
        ["branch_count", props.branchDetailSummary?.branch_count],
        ["worktree_count", props.branchDetailSummary?.worktree_count],
        ["pr_summary", props.githubPrSummary],
        ["reason_codes", props.branchDetailSummary?.reason_codes],
      ],
      reasonCodes: stringList(props.branchDetailSummary?.reason_codes),
    };
  }

  if (section.id === "legacy_debt") {
    const p0p1 = numberValue(props.legacyDebtSummary?.p0_p1_count);
    return {
      id: section.id,
      label: section.label,
      hint: section.hint,
      tone: p0p1 ? "yellow" : normalizeTone(backendCard?.health_tone || "green"),
      state: p0p1 ? "基线债务" : "无阻塞",
      meta: `debt ${display(props.legacyDebtSummary?.debt_count ?? 0)} / P0P1 ${display(p0p1)}`,
      risk: backendCard?.risk_score ?? Math.min(100, p0p1 * 18 + numberValue(props.legacyDebtSummary?.debt_count) / 10),
      rows: [
        ...baseRows,
        ["group_count", props.legacyDebtSummary?.group_count],
        ["debt_count", props.legacyDebtSummary?.debt_count],
        ["p0_p1_count", props.legacyDebtSummary?.p0_p1_count],
        ["reason_codes", props.legacyDebtSummary?.reason_codes],
      ],
      reasonCodes: stringList(props.legacyDebtSummary?.reason_codes),
    };
  }

  const nightly = props.snapshot?.sections.nightly;
  const runners = props.snapshot?.sections.runners;
  const run = objectField(nightly?.data, "latest_run");
  const state = objectField(run, "conclusion") || objectField(run, "status") || "unknown";
  return {
    id: section.id, label: section.label, hint: section.hint,
    tone: nightly?.state === "available" && !nightly.stale ? toneFromState(state) : "gray",
    state: nightly?.stale ? "stale" : display(state),
    meta: `Nightly ${display(state)} / Runner ${display(runners?.state)}`,
    risk: "-", rows: [
      ...baseRows, ["Nightly latest_run", run], ["Runner", runners?.data],
      ["数据时间", nightly?.collected_at], ["来源", nightly?.source],
      ["gh_auth_status（不代表业务通过）", props.automationSummary?.gh_auth_status],
      ["scripts", props.automationSummary?.scripts], ["actions", props.automationSummary?.actions],
    ], reasonCodes: nightly?.error ? [nightly.error] : [],
  };
}

function sourceTone(source?: OverviewSource): "info" | "warning" {
  return source?.state === "available" && !source.stale ? "info" : "warning";
}

export function PipelineGlobalOverview({ snapshot, error, onRefresh }: {
  snapshot: PipelineOverviewSnapshot | null; error: string | null; onRefresh: () => void;
}) {
  const sources = snapshot?.sections || {};
  const data = (name: string) => sources[name]?.data;
  const rows = (name: string, field = "items"): JsonObject[] => {
    const value = data(name)?.[field];
    return Array.isArray(value) ? value.filter(isObject) : [];
  };
  const queued = objectField(data("ci"), "queued");
  const running = objectField(data("ci"), "in_progress");
  const nightly = objectField(data("nightly"), "latest_run");
  const nightlyState = objectField(nightly, "conclusion") || objectField(nightly, "status") || "unknown";
  const nightlyUpdated = Date.parse(String(objectField(nightly, "updated_at") || ""));
  const nightlyFresh = Number.isFinite(nightlyUpdated) && Date.now() - nightlyUpdated < 36 * 3600_000 && Date.now() >= nightlyUpdated - 300_000;
  const runRows = (value: unknown): JsonObject[] => isObject(value) && Array.isArray(value.items) ? value.items.filter(isObject) : [];
  return <SectionCard title="研发流水线全局总览" eyebrow="只读观测，不代表业务验收或可安全清理"
    action={<button type="button" className="pv2-link-button" onClick={onRefresh}>重新读取快照</button>}>
    {error ? <div role="alert">快照读取失败：{error}。已有数据仅供参考，不代表最新状态。</div> : null}
    {!snapshot && !error ? <div role="status">正在读取轻量快照，数据到达后分区展示。</div> : null}
    <div className="pv2-grid pv2-grid-4">
      <MetricCard label="CI 排队工作流" value={display(objectField(queued, "total") ?? "unknown")} hint="GitHub queued；不是业务测试失败" tone={sourceTone(sources.ci)} />
      <MetricCard label="CI 执行中工作流" value={display(objectField(running, "total") ?? "unknown")} hint="in_progress；其中 job 仍可能排队" tone={sourceTone(sources.ci)} />
      <MetricCard label="最近 Nightly" value={display(nightlyState)} hint={display(objectField(nightly, "updated_at"))}
        tone={nightlyState === "success" && objectField(nightly, "status") === "completed" && nightlyFresh && sources.nightly?.state === "available" && !sources.nightly.stale && !error ? "success" : "warning"} />
      <MetricCard label="待处理 BUG" value={display(data("bugs")?.pending ?? "unknown")} hint="canonical registry；含待重启验证记录" tone={sourceTone(sources.bugs)} />
      <MetricCard label="开放 PR" value={display(data("prs")?.observed_open ?? "unknown")} hint={data("prs")?.truncated ? "至少 100，列表已截断" : "GitHub open PR"} tone={sourceTone(sources.prs)} />
      <MetricCard label="任务 Worktree" value={display(data("worktrees")?.task_worktrees ?? "unknown")} hint="仅注册数量，未判定是否可清理" tone={sourceTone(sources.worktrees)} />
    </div>
    <h3>两个 GitHub Runner</h3>
    <div className="pv2-table-wrap"><table className="pv2-table"><thead><tr><th>角色</th><th>名称</th><th>连接</th><th>占用</th></tr></thead>
      <tbody>{["ci", "security"].map((role) => {
        const matches = rows("runners").filter((runner) => runner.role === role);
        return matches.length ? matches.map((runner) => <tr key={String(runner.id)}><td>{role}</td><td>{display(runner.name)}</td><td>{display(runner.status)}</td><td>{runner.status === "online" ? runner.busy === true ? "busy" : "idle" : "unknown"}</td></tr>)
          : <tr key={role}><td>{role}</td><td colSpan={3}>unknown / 未观测到该角色</td></tr>;
      })}</tbody></table></div>
    <details><summary>CI 排队 / 执行明细（有界列表）</summary>
      <DetailRows rows={[...runRows(queued), ...runRows(running)].map((run) => [String(run.id), {
        workflow: run.name, status: run.status, branch: run.head_branch,
        queued_since: run.status === "queued" ? run.created_at : undefined,
        running_since: run.status === "in_progress" ? run.run_started_at : undefined,
        url: run.html_url,
      }])} />
    </details>
    <details><summary>最近 Nightly 原始运行身份</summary><DetailRows rows={[["latest_run", nightly ?? "unknown"]]} /></details>
    <details><summary>开放 PR 明细（最多 10 条）</summary><DetailRows rows={rows("prs").map((pr) => [String(pr.number), { title: pr.title, url: pr.html_url }])} /></details>
    <h3>数据时间与来源</h3>
    <div className="pv2-table-wrap"><table className="pv2-table"><thead><tr><th>分区</th><th>读取状态</th><th>采集时间</th><th>数据年龄（秒）</th><th>来源</th></tr></thead>
      <tbody>{["ci", "runners", "nightly", "bugs", "prs", "worktrees"].map((key) => {
        const source = sources[key];
        return <tr key={key}><td>{key}</td><td>{error ? "连接失败 / 未确认新鲜度" : source?.stale ? "stale / 刷新中" : source?.state || "loading"}{source?.error ? ` (${source.error})` : ""}</td><td>{display(source?.collected_at)}</td><td>{display(source?.age_seconds)}</td><td>{display(source?.source)}</td></tr>;
      })}</tbody></table></div>
    <p className="pv2-muted">共享缓存 {snapshot?.ttl_seconds ?? 60} 秒；各来源独立采集。只读观测不会启动测试、修改数据库或控制服务。</p>
  </SectionCard>;
}

function DetailRows({ rows }: { rows: Array<[string, unknown]> }) {
  return (
    <div className="pv2-readable-panel">
      <div className="pv2-readable-table">
        {rows.map(([key, value]) => (
          <div className="pv2-readable-row" key={key}>
            <div className="pv2-readable-key">{key}</div>
            <div className="pv2-readable-value">{display(value)}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function ReasonCodeChips({ items }: { items: string[] }) {
  if (!items.length) return <span className="pv2-muted">无 reason code</span>;
  return (
    <div className="pv2-chip-row">
      {items.map((item) => <span className="pv2-chip" key={item}>{item}</span>)}
    </div>
  );
}

export default function PipelineOverviewCards(props: PipelineOverviewCardsProps) {
  const sections = PIPELINE_OVERVIEW_SECTIONS.map((section) => buildSectionView(section, props));
  const active = sections.find((section) => section.id === props.activeSection) || sections[0];
  const optionalApiWarnings = props.optionalApiWarnings || {};
  const warningEntries = Object.entries(optionalApiWarnings);

  return (
    <SectionCard
      title="流水线入口"
      eyebrow="P0/P1 pipeline cards / graceful optional API fallback"
      action={<StatusBadge status={active.state} />}
    >
      <nav className="pv2-phase-nav" aria-label="流水线中心页面导航">
        {sections.map((section) => (
          <button
            aria-label={`打开${section.label}卡片`}
            className={`pv2-phase-tab pv2-phase-tab-${section.tone} ${active.id === section.id ? "pv2-phase-tab-active" : ""}`}
            key={section.id}
            onClick={() => props.onSelect(section.id)}
            type="button"
          >
            <span className="pv2-phase-tab-title">{section.label}</span>
            <span className="pv2-phase-tab-meta">{section.meta || section.hint}</span>
            <span className="pv2-phase-tab-risk">risk {display(section.risk)}</span>
          </button>
        ))}
      </nav>

      <details className="pv2-readable-item" open>
        <summary>展开{active.label}概要</summary>
        <div className="pv2-grid pv2-grid-4" style={{ marginTop: 12 }}>
          <MetricCard label="当前状态" value={active.state} hint={active.hint} tone={metricTone(active.tone)} />
          <MetricCard label="摘要" value={active.meta || "-"} hint={cardSummaryText(findBackendCard(props.cardsSummary, active.id)) || "当前 API 聚合"} tone="info" />
          <MetricCard label="风险分" value={active.risk} hint="后端提供或前端保守估算" tone={metricTone(active.tone)} />
          <MetricCard label="降级项" value={warningEntries.length} hint="仅统计已请求的分区；未读取不代表通过" tone={warningEntries.length ? "warning" : "info"} />
        </div>
        <div style={{ marginTop: 12 }}>
          <ReasonCodeChips items={active.reasonCodes} />
        </div>
        <DetailRows rows={active.rows} />
      </details>

      {warningEntries.length ? (
        <details className="pv2-readable-item">
          <summary>可选 API 降级详情</summary>
          <DetailRows rows={warningEntries.map(([key, message]) => [key, message])} />
        </details>
      ) : null}
    </SectionCard>
  );
}
