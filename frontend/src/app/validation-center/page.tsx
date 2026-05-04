"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import MetricCard from "@/components/paper-v2/MetricCard";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import {
  type JsonObject,
  type ValidationAgentContext,
  type ValidationBug,
  type ValidationBugSummary,
  type ValidationBusinessAssertion,
  type ValidationCoverageDetail,
  type ValidationCoverageSummary,
  type ValidationEvidenceDetail,
  type ValidationEvidenceSummary,
  type ValidationExecutionJob,
  type ValidationFindingSummary,
  type ValidationHealth,
  type ValidationPage,
  type ValidationPassScope,
  type ValidationPlan,
  type ValidationQualityFinding,
  type ValidationRunDetail,
  type ValidationRunSummary,
  type ValidationSummary,
  validationApi,
} from "@/lib/validation/api";

const DEFAULT_PAGE_SIZE = 20;

function emptyPage<T>(pageSize = DEFAULT_PAGE_SIZE): ValidationPage<T> {
  return { items: [], total: 0, page: 1, page_size: pageSize, has_more: false };
}

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

function compactId(value: unknown, size = 10): string {
  const text = String(value || "");
  if (!text) return "-";
  if (text.length <= size * 2 + 3) return text;
  return `${text.slice(0, size)}...${text.slice(-size)}`;
}

function pct(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${n.toFixed(2)}%`;
}

function arrayCount(value: unknown): number {
  return Array.isArray(value) ? value.length : 0;
}

function pageCount(page: ValidationPage<unknown>): number {
  return Math.max(1, Math.ceil((page.total || 0) / (page.page_size || DEFAULT_PAGE_SIZE)));
}

function errorText(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error || "未知错误");
}

function WarningList({ run }: { run: ValidationRunSummary | ValidationRunDetail }) {
  const warnings = [
    run.metadata_missing ? "metadata_missing：缺少 JSON run metadata，不能证明本次验证范围" : null,
    run.metadata_parse_error ? `metadata_parse_error：${run.metadata_parse_error}` : null,
    run.coverage_missing ? "coverage_missing：未发现覆盖率快照" : null,
    run.evidence_missing ? "evidence_missing：未发现 evidence manifest" : null,
    !run.success_scope_recorded ? "pass_scope / business_assertion 未记录：只能作为历史文本参考，不能当作业务成功证明" : null,
  ].filter(Boolean) as string[];
  if (!warnings.length) return <span className="pv2-muted">完整</span>;
  return <div className="pv2-readable-list">{warnings.map((item) => <span className="pv2-badge pv2-badge-warning" key={item}>{item}</span>)}</div>;
}

function KeyValuePanel({ rows }: { rows: Array<[string, unknown]> }) {
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

function BadgeList({ items, empty = "-" }: { items?: Array<string | null | undefined>; empty?: string }) {
  const validItems = (items || []).filter(Boolean) as string[];
  if (!validItems.length) return <span className="pv2-muted">{empty}</span>;
  return <div className="pv2-readable-list">{validItems.map((item) => <span className="pv2-chip" key={item}>{item}</span>)}</div>;
}

function CountChips({ counts }: { counts?: Record<string, number> }) {
  const entries = Object.entries(counts || {});
  if (!entries.length) return <span className="pv2-muted">无统计</span>;
  return <div className="pv2-chip-row">{entries.map(([key, count]) => <span className="pv2-chip" key={key}>{key}: {count}</span>)}</div>;
}

function Pagination({ page, label, onPageChange }: { page: ValidationPage<unknown>; label: string; onPageChange: (page: number) => void }) {
  return (
    <div className="pv2-pagination" aria-label={label}>
      <button className="pv2-button-ghost" disabled={page.page <= 1} onClick={() => onPageChange(Math.max(1, page.page - 1))} type="button">上一页</button>
      <span aria-label={`${label} status`}>第 {page.page} / {pageCount(page)} 页，共 {page.total} 条</span>
      <button className="pv2-button-ghost" disabled={!page.has_more} onClick={() => onPageChange(page.page + 1)} type="button">下一页</button>
    </div>
  );
}

function PassScopePanel({ passScope, businessAssertion }: { passScope?: ValidationPassScope | null; businessAssertion?: ValidationBusinessAssertion | null }) {
  if (!passScope && !businessAssertion) {
    return (
      <div className="pv2-notice pv2-notice-warning">
        <div className="pv2-notice-title">未记录 / 未证明</div>
        <div className="pv2-notice-body">该 run 没有 pass_scope 或 business_assertion，只能说明曾经产生文本记录，不能证明真实业务链路已通过。</div>
      </div>
    );
  }
  const assertionRows: Array<[string, unknown]> = businessAssertion ? [
    ["操作名称", businessAssertion.operation_name],
    ["用户是否可完成操作", businessAssertion.can_user_complete_operation],
    ["证据边界", businessAssertion.evidence],
    ["未解决阻塞", businessAssertion.unresolved_blockers],
  ] : [];
  return (
    <div className="pv2-grid pv2-grid-2">
      <SectionCard title="Pass Scope" eyebrow="mock / real proof boundary">
        {passScope ? <KeyValuePanel rows={Object.entries(passScope)} /> : <p className="pv2-muted">未记录 pass_scope。</p>}
      </SectionCard>
      <SectionCard title="Business Assertion" eyebrow="positive success gate">
        {businessAssertion ? <KeyValuePanel rows={assertionRows} /> : <p className="pv2-muted">未记录 business_assertion。</p>}
      </SectionCard>
    </div>
  );
}

function QualityGatePanel({ gates }: { gates?: JsonObject[] }) {
  const rows = gates || [];
  if (!rows.length) return <p className="pv2-muted">未记录质量门禁。</p>;
  return (
    <div className="pv2-table-wrap">
      <table className="pv2-table">
        <thead><tr><th>Metric</th><th>Status</th><th>Actual</th><th>Threshold</th></tr></thead>
        <tbody>
          {rows.map((gate, index) => (
            <tr key={`${display(gate.metric)}-${index}`}>
              <td>{display(gate.metric)}</td>
              <td><StatusBadge status={gate.status} /></td>
              <td>{display(gate.actual)}</td>
              <td>{display(gate.threshold)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DetailNotice({ detail }: { detail: ValidationRunDetail }) {
  if (!detail.metadata_missing && !detail.metadata_parse_error && !detail.coverage_missing && !detail.evidence_missing && detail.success_scope_recorded) return null;
  return (
    <div className="pv2-notice pv2-notice-warning">
      <div className="pv2-notice-title">显式缺失状态</div>
      <div className="pv2-notice-body"><WarningList run={detail} /></div>
    </div>
  );
}

function AgentContextPanel({ context }: { context?: ValidationAgentContext | null }) {
  if (!context) return <p className="pv2-muted">请选择一条 Bug 或质量发现查看 agent-context。</p>;
  return (
    <KeyValuePanel rows={[
      ["context_type", context.context_type],
      ["问题说明", context.problem_statement],
      ["复现命令", context.reproduce_command],
      ["证据", context.evidence_uris],
      ["允许修改范围", context.allowed_write_scope],
      ["疑似模块", context.suspected_modules],
      ["必须验证", context.required_verification],
      ["关闭条件", context.closure_requirements],
      ["GitHub Issue", context.github_issue_url],
      ["verification_run_id", context.verification_run_id],
    ]} />
  );
}

export default function ValidationCenterPage() {
  const [health, setHealth] = useState<ValidationHealth | null>(null);
  const [summary, setSummary] = useState<ValidationSummary | null>(null);
  const [findingSummary, setFindingSummary] = useState<ValidationFindingSummary | null>(null);
  const [bugSummary, setBugSummary] = useState<ValidationBugSummary | null>(null);
  const [plans, setPlans] = useState<ValidationPlan[]>([]);
  const [runs, setRuns] = useState<ValidationPage<ValidationRunSummary>>(emptyPage<ValidationRunSummary>());
  const [coverage, setCoverage] = useState<ValidationPage<ValidationCoverageSummary>>(emptyPage<ValidationCoverageSummary>(10));
  const [evidence, setEvidence] = useState<ValidationPage<ValidationEvidenceSummary>>(emptyPage<ValidationEvidenceSummary>(10));
  const [executions, setExecutions] = useState<ValidationPage<ValidationExecutionJob>>(emptyPage<ValidationExecutionJob>(10));
  const [findings, setFindings] = useState<ValidationPage<ValidationQualityFinding>>(emptyPage<ValidationQualityFinding>());
  const [bugs, setBugs] = useState<ValidationPage<ValidationBug>>(emptyPage<ValidationBug>());
  const [selectedRun, setSelectedRun] = useState<ValidationRunDetail | null>(null);
  const [selectedCoverage, setSelectedCoverage] = useState<ValidationCoverageDetail | null>(null);
  const [selectedEvidence, setSelectedEvidence] = useState<ValidationEvidenceDetail | null>(null);
  const [selectedFinding, setSelectedFinding] = useState<ValidationQualityFinding | null>(null);
  const [selectedBug, setSelectedBug] = useState<ValidationBug | null>(null);
  const [selectedAgentContext, setSelectedAgentContext] = useState<ValidationAgentContext | null>(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [executionBusy, setExecutionBusy] = useState<string | null>(null);
  const [executionMessage, setExecutionMessage] = useState<string | null>(null);
  const [filters, setFilters] = useState({ module: "", level: "", status: "", search: "", includeMarkdownOnly: true, page: 1, pageSize: DEFAULT_PAGE_SIZE });
  const [qualityFilters, setQualityFilters] = useState({ findingSource: "", findingSeverity: "", findingStatus: "", findingSearch: "", findingPage: 1, bugSeverity: "", bugStatus: "", bugSearch: "", bugPage: 1, pageSize: DEFAULT_PAGE_SIZE });

  const loadStatic = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [healthData, summaryData, planCatalog, coverageData, evidenceData, executionData, findingSummaryData, bugSummaryData] = await Promise.all([
        validationApi.health(),
        validationApi.summary(),
        validationApi.plans(),
        validationApi.coverage({ page: 1, page_size: 10 }),
        validationApi.evidence({ page: 1, page_size: 10 }),
        validationApi.executions({ page: 1, page_size: 10 }),
        validationApi.findingSummary(),
        validationApi.bugSummary(),
      ]);
      setHealth(healthData);
      setSummary(summaryData);
      setPlans(planCatalog.plans || []);
      setCoverage(coverageData || emptyPage<ValidationCoverageSummary>(10));
      setEvidence(evidenceData || emptyPage<ValidationEvidenceSummary>(10));
      setExecutions(executionData || emptyPage<ValidationExecutionJob>(10));
      setFindingSummary(findingSummaryData);
      setBugSummary(bugSummaryData);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setLoading(false);
    }
  }, []);

  const loadExecutions = useCallback(async () => {
    try {
      const data = await validationApi.executions({ page: 1, page_size: 10 });
      setExecutions(data || emptyPage<ValidationExecutionJob>(10));
    } catch (err) {
      setError(errorText(err));
    }
  }, []);

  const loadRuns = useCallback(async () => {
    setError(null);
    try {
      const data = await validationApi.runs({
        module: filters.module || undefined,
        level: filters.level || undefined,
        status: filters.status || undefined,
        search: filters.search || undefined,
        include_markdown_only: filters.includeMarkdownOnly,
        page: filters.page,
        page_size: filters.pageSize,
      });
      setRuns(data || emptyPage<ValidationRunSummary>());
    } catch (err) {
      setError(errorText(err));
    }
  }, [filters]);

  const loadFindings = useCallback(async () => {
    setError(null);
    try {
      const data = await validationApi.findings({
        source_type: qualityFilters.findingSource || undefined,
        severity: qualityFilters.findingSeverity || undefined,
        status: qualityFilters.findingStatus || undefined,
        search: qualityFilters.findingSearch || undefined,
        page: qualityFilters.findingPage,
        page_size: qualityFilters.pageSize,
      });
      setFindings(data || emptyPage<ValidationQualityFinding>());
    } catch (err) {
      setError(errorText(err));
    }
  }, [qualityFilters]);

  const loadBugs = useCallback(async () => {
    setError(null);
    try {
      const data = await validationApi.bugs({
        severity: qualityFilters.bugSeverity || undefined,
        status: qualityFilters.bugStatus || undefined,
        search: qualityFilters.bugSearch || undefined,
        page: qualityFilters.bugPage,
        page_size: qualityFilters.pageSize,
      });
      setBugs(data || emptyPage<ValidationBug>());
    } catch (err) {
      setError(errorText(err));
    }
  }, [qualityFilters]);

  useEffect(() => { void loadStatic(); }, [loadStatic]);
  useEffect(() => { void loadRuns(); }, [loadRuns]);
  useEffect(() => { void loadFindings(); }, [loadFindings]);
  useEffect(() => { void loadBugs(); }, [loadBugs]);

  const statusCounts = useMemo(() => summary?.runs_by_status || {}, [summary]);
  const executionCounts = useMemo(() => summary?.runner?.jobs_by_status || health?.runner?.jobs_by_status || {}, [summary, health]);
  const latestCoverageTotals = summary?.latest_coverage?.totals || {};

  async function openRun(runId: string) {
    setDetailLoading(true);
    setError(null);
    try {
      setSelectedRun(await validationApi.run(runId));
    } catch (err) {
      setError(errorText(err));
    } finally {
      setDetailLoading(false);
    }
  }

  async function openCoverage(snapshotId: string) {
    setDetailLoading(true);
    setError(null);
    try {
      setSelectedCoverage(await validationApi.coverageDetail(snapshotId));
    } catch (err) {
      setError(errorText(err));
    } finally {
      setDetailLoading(false);
    }
  }

  async function openEvidence(manifestId: string) {
    setDetailLoading(true);
    setError(null);
    try {
      setSelectedEvidence(await validationApi.evidenceDetail(manifestId));
    } catch (err) {
      setError(errorText(err));
    } finally {
      setDetailLoading(false);
    }
  }

  async function openFinding(findingId: string) {
    setDetailLoading(true);
    setError(null);
    try {
      const detail = await validationApi.finding(findingId);
      setSelectedFinding(detail);
      setSelectedBug(null);
      setSelectedAgentContext(detail.agent_context || null);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setDetailLoading(false);
    }
  }

  async function openBug(bugId: string) {
    setDetailLoading(true);
    setError(null);
    try {
      const [bug, agentContext] = await Promise.all([
        validationApi.bug(bugId),
        validationApi.bugAgentContext(bugId),
      ]);
      setSelectedBug(bug);
      setSelectedFinding(null);
      setSelectedAgentContext(agentContext || bug.agent_context || null);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setDetailLoading(false);
    }
  }

  function updateFilter(key: keyof typeof filters, value: string | number | boolean) {
    setFilters((prev) => ({ ...prev, [key]: value, page: key === "page" ? Number(value) : 1 }));
  }

  function updateQualityFilter(key: keyof typeof qualityFilters, value: string | number) {
    setQualityFilters((prev) => ({
      ...prev,
      [key]: value,
      findingPage: key === "findingPage" ? Number(value) : key.startsWith("finding") ? 1 : prev.findingPage,
      bugPage: key === "bugPage" ? Number(value) : key.startsWith("bug") ? 1 : prev.bugPage,
    }));
  }

  async function startExecution(plan: ValidationPlan) {
    setExecutionBusy(plan.plan_key);
    setExecutionMessage(null);
    setError(null);
    try {
      const job = await validationApi.startExecution({
        plan_key: plan.plan_key,
        requested_by: "ui",
        backend_port: plan.requires_backend ? plan.allowed_backend_ports?.[0] : undefined,
        frontend_port: plan.requires_frontend ? plan.allowed_frontend_ports?.[0] : undefined,
        timeout_seconds: typeof plan.max_duration_seconds === "number" ? plan.max_duration_seconds : undefined,
      });
      setExecutionMessage(`已提交 ${plan.plan_key}，job=${compactId(job.job_id, 8)}，状态=${display(job.status)}`);
      await loadExecutions();
      await loadStatic();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setExecutionBusy(null);
    }
  }

  return (
    <main className="pv2-shell">
      <header className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">Validation Center / Read Only</div>
            <h1>自动化测试流水线中心</h1>
            <p>集中查看测试计划、历史 run、覆盖率快照、evidence manifest、质量发现和 Bug registry。本阶段只读展示，不执行命令、不写数据库、不触碰生产 8001。</p>
          </div>
          <div className="pv2-chip-row">
            <span className="pv2-chip">只读 API</span>
            <span className="pv2-chip">Mock/真实证明边界</span>
            <span className="pv2-chip">质量发现与 Bug 上下文</span>
          </div>
        </div>
      </header>

      {error ? <div className="pv2-error-panel"><div className="pv2-error-kicker">Validation Center Error</div><div className="pv2-error-main">{error}</div></div> : null}
      {loading ? <div className="pv2-notice pv2-notice-info"><div className="pv2-notice-title">加载中</div><div className="pv2-notice-body">正在读取本地验证历史索引和质量问题索引。</div></div> : null}

      <section className="pv2-grid pv2-grid-4">
        <MetricCard label="历史 Run" value={summary?.run_count ?? health?.history?.run_count ?? "-"} hint={health?.history?.history_root || "tests/aistock_validation/history"} tone="info" />
        <MetricCard label="覆盖率快照" value={summary?.coverage_snapshot_count ?? health?.history?.coverage_snapshot_count ?? "-"} hint={`最新行覆盖 ${pct(latestCoverageTotals.line_percent)}`} tone="success" />
        <MetricCard label="质量发现" value={findingSummary?.finding_count ?? summary?.quality?.finding_count ?? health?.quality?.finding_count ?? "-"} hint="guardrail / legacy inventory" tone="warning" />
        <MetricCard label="Bug Registry" value={bugSummary?.bug_count ?? summary?.quality?.bug_count ?? health?.quality?.bug_count ?? "-"} hint={health?.production_8001_touched ? "异常：触碰 8001" : "read_only / agent-context"} tone={health?.production_8001_touched ? "danger" : "success"} />
      </section>

      <SectionCard
        title="测试计划目录"
        eyebrow="allowlisted nox entrypoints"
        action={<span className="pv2-chip">受控 Runner：allowlist only</span>}
      >
        <div className="pv2-notice pv2-notice-info">
          <div className="pv2-notice-title">执行边界</div>
          <div className="pv2-notice-body">只允许执行 YAML allowlist 中 `runner_enabled=true` 的 nox session；不开放任意 shell，不允许生产 8001，执行日志和 evidence 写入本地 `tmp/validation/runner/jobs`。</div>
        </div>
        {executionMessage ? <div className="pv2-notice pv2-notice-success"><div className="pv2-notice-title">Runner 已提交</div><div className="pv2-notice-body">{executionMessage}</div></div> : null}
        <div className="pv2-table-wrap">
          <table className="pv2-table">
            <thead><tr><th>Plan</th><th>模块/级别</th><th>Nox</th><th>端口</th><th>写入边界</th><th>Runner</th><th>操作</th></tr></thead>
            <tbody>
              {plans.length ? plans.map((plan) => (
                <tr key={plan.plan_key}>
                  <td><strong>{plan.title || plan.plan_key}</strong><br /><span className="pv2-muted pv2-mono">{plan.plan_key}</span></td>
                  <td>{display(plan.module)} / {display(plan.level)}</td>
                  <td><span className="pv2-mono">{display(plan.nox_session || plan.command_key)}</span></td>
                  <td>Backend {display(plan.allowed_backend_ports)}<br />Frontend {display(plan.allowed_frontend_ports)}</td>
                  <td>DB {display(plan.writes_database)} / Artifacts {display(plan.writes_artifacts)} / Business {display(plan.writes_business_state)}</td>
                  <td><StatusBadge status={plan.runner_enabled ? "RUNNER_READY" : "READ_ONLY"} /></td>
                  <td>
                    <button
                      aria-label={`run validation plan ${plan.plan_key}`}
                      className="pv2-link-button"
                      disabled={!plan.enabled || !plan.runner_enabled || Boolean(executionBusy)}
                      onClick={() => void startExecution(plan)}
                      type="button"
                    >
                      {executionBusy === plan.plan_key ? "执行中..." : "执行"}
                    </button>
                  </td>
                </tr>
              )) : <tr><td className="pv2-empty-cell" colSpan={7}>暂无测试计划</td></tr>}
            </tbody>
          </table>
        </div>
      </SectionCard>

      <SectionCard title="Runner 执行队列" eyebrow="controlled nox jobs / logs / evidence">
        <div className="pv2-notice pv2-notice-info">
          <div className="pv2-notice-title">Runner 状态</div>
          <div className="pv2-notice-body">
            执行根目录：{display(summary?.runner?.execution_root || health?.runner?.execution_root)}；
            任意 shell：{display(summary?.runner?.arbitrary_shell_allowed || health?.runner?.arbitrary_shell_allowed)}；
            生产 8001：{display(summary?.runner?.production_8001_touched || health?.runner?.production_8001_touched)}
          </div>
        </div>
        <div style={{ marginBottom: 12 }}><CountChips counts={executionCounts} /></div>
        <div className="pv2-table-wrap">
          <table className="pv2-table">
            <thead><tr><th>Job</th><th>Plan/Nox</th><th>状态</th><th>时间</th><th>端口</th><th>证据</th><th>错误</th></tr></thead>
            <tbody>
              {executions.items.length ? executions.items.map((job) => (
                <tr key={job.job_id}>
                  <td><strong>{compactId(job.job_id, 8)}</strong><br /><span className="pv2-muted">{display(job.requested_by)}</span></td>
                  <td>{display(job.plan_key)}<br /><span className="pv2-muted pv2-mono">{display(job.nox_session)}</span></td>
                  <td><StatusBadge status={job.status} /><br /><span className="pv2-muted">return={display(job.return_code)}</span></td>
                  <td>{display(job.started_at || job.requested_at)}<br /><span className="pv2-muted">{display(job.finished_at)}</span></td>
                  <td>Backend {display(job.backend_port)}<br />Frontend {display(job.frontend_port)}</td>
                  <td><span className="pv2-muted">{display(job.log_path)}</span><br /><span className="pv2-muted">{display(job.evidence_path)}</span></td>
                  <td>{display(job.error)}</td>
                </tr>
              )) : <tr><td className="pv2-empty-cell" colSpan={7}>暂无 Runner job</td></tr>}
            </tbody>
          </table>
        </div>
        <button className="pv2-button-ghost" type="button" onClick={() => void loadExecutions()}>刷新 Runner 队列</button>
      </SectionCard>

      <SectionCard title="Run 历史" eyebrow="filters / pagination / missing states">
        <div className="pv2-form-grid pv2-filter-card">
          <div className="pv2-field"><label htmlFor="validation-module">模块</label><input id="validation-module" className="pv2-input" value={filters.module} onChange={(event) => updateFilter("module", event.target.value)} placeholder="validation_center" /></div>
          <div className="pv2-field"><label htmlFor="validation-level">级别</label><select id="validation-level" className="pv2-select" value={filters.level} onChange={(event) => updateFilter("level", event.target.value)}><option value="">全部</option><option value="L0">L0</option><option value="L1">L1</option><option value="L2">L2</option><option value="L3">L3</option><option value="L4">L4</option><option value="L5">L5</option></select></div>
          <div className="pv2-field"><label htmlFor="validation-status">状态</label><input id="validation-status" className="pv2-input" value={filters.status} onChange={(event) => updateFilter("status", event.target.value)} placeholder="passed / failed" /></div>
          <div className="pv2-field"><label htmlFor="validation-search">搜索</label><input id="validation-search" className="pv2-input" value={filters.search} onChange={(event) => updateFilter("search", event.target.value)} placeholder="run id / 标题 / 路径" /></div>
          <div className="pv2-field"><label htmlFor="validation-page-size">每页</label><select id="validation-page-size" className="pv2-select" value={filters.pageSize} onChange={(event) => updateFilter("pageSize", Number(event.target.value))}><option value={10}>10</option><option value={20}>20</option><option value={50}>50</option><option value={100}>100</option></select></div>
          <label className="pv2-field"><span className="pv2-label">Markdown only</span><span><input checked={filters.includeMarkdownOnly} onChange={(event) => updateFilter("includeMarkdownOnly", event.target.checked)} type="checkbox" /> 包含无 metadata 记录</span></label>
        </div>
        <div style={{ marginBottom: 12 }}><CountChips counts={statusCounts} /></div>
        <div className="pv2-table-wrap">
          <table className="pv2-table">
            <thead><tr><th>Run</th><th>模块/级别</th><th>状态</th><th>时间</th><th>证明范围</th><th>缺失项</th><th>操作</th></tr></thead>
            <tbody>
              {runs.items.length ? runs.items.map((run) => (
                <tr key={run.run_id}>
                  <td><strong>{run.title || run.run_id}</strong><br /><span className="pv2-muted pv2-mono">{compactId(run.run_id)}</span><br /><span className="pv2-muted">{run.markdown_path}</span></td>
                  <td>{display(run.module)} / {display(run.level)}</td>
                  <td><StatusBadge status={run.status} /></td>
                  <td>{display(run.started_at)}<br /><span className="pv2-muted">{display(run.finished_at)}</span></td>
                  <td>{run.success_scope_recorded ? <span className="pv2-badge pv2-badge-success">已记录</span> : <span className="pv2-badge pv2-badge-warning">未记录/未证明</span>}</td>
                  <td><WarningList run={run} /></td>
                  <td><button className="pv2-link-button" type="button" onClick={() => void openRun(run.run_id)}>查看详情</button></td>
                </tr>
              )) : <tr><td className="pv2-empty-cell" colSpan={7}>暂无 run 记录</td></tr>}
            </tbody>
          </table>
        </div>
        <Pagination page={runs} label="validation run pagination" onPageChange={(page) => updateFilter("page", page)} />
      </SectionCard>

      <SectionCard title="质量发现与 Bug Registry" eyebrow="guardrail / legacy inventory / bug agent-context">
        <div className="pv2-grid pv2-grid-2">
          <div>
            <div className="pv2-notice pv2-notice-info">
              <div className="pv2-notice-title">质量发现统计</div>
              <div className="pv2-notice-body"><CountChips counts={findingSummary?.by_source_type} /></div>
            </div>
            <div className="pv2-form-grid pv2-filter-card">
              <div className="pv2-field"><label htmlFor="finding-source">来源</label><input id="finding-source" className="pv2-input" value={qualityFilters.findingSource} onChange={(event) => updateQualityFilter("findingSource", event.target.value)} placeholder="guardrail" /></div>
              <div className="pv2-field"><label htmlFor="finding-severity">严重级别</label><input id="finding-severity" className="pv2-input" value={qualityFilters.findingSeverity} onChange={(event) => updateQualityFilter("findingSeverity", event.target.value)} placeholder="P1" /></div>
              <div className="pv2-field"><label htmlFor="finding-search">搜索</label><input id="finding-search" className="pv2-input" value={qualityFilters.findingSearch} onChange={(event) => updateQualityFilter("findingSearch", event.target.value)} placeholder="规则 / 文件 / fingerprint" /></div>
            </div>
            <div className="pv2-table-wrap">
              <table className="pv2-table">
                <thead><tr><th>发现</th><th>级别/状态</th><th>模块</th><th>文件/证据</th><th>Agent 边界</th><th>操作</th></tr></thead>
                <tbody>
                  {findings.items.length ? findings.items.map((item) => (
                    <tr key={item.finding_id}>
                      <td><strong>{item.title || item.finding_id}</strong><br /><span className="pv2-muted pv2-mono">{compactId(item.finding_id)}</span><br /><span className="pv2-muted">{display(item.source_type)}</span></td>
                      <td><StatusBadge status={item.severity} /><br /><StatusBadge status={item.status} /></td>
                      <td>{display(item.module)}</td>
                      <td>{display(item.file_path)}{item.line ? `:${item.line}` : ""}<br /><span className="pv2-muted">{display(item.evidence_uri)}</span></td>
                      <td><BadgeList items={item.allowed_write_scope} /><span className="pv2-muted">验证 {arrayCount(item.required_verification)} 项</span></td>
                      <td><button className="pv2-link-button" type="button" onClick={() => void openFinding(item.finding_id)}>查看发现</button></td>
                    </tr>
                  )) : <tr><td className="pv2-empty-cell" colSpan={6}>暂无质量发现</td></tr>}
                </tbody>
              </table>
            </div>
            <Pagination page={findings} label="validation finding pagination" onPageChange={(page) => updateQualityFilter("findingPage", page)} />
          </div>

          <div>
            <div className="pv2-notice pv2-notice-warning">
              <div className="pv2-notice-title">Bug 状态统计</div>
              <div className="pv2-notice-body"><CountChips counts={bugSummary?.by_status} /></div>
            </div>
            <div className="pv2-form-grid pv2-filter-card">
              <div className="pv2-field"><label htmlFor="bug-severity">严重级别</label><input id="bug-severity" className="pv2-input" value={qualityFilters.bugSeverity} onChange={(event) => updateQualityFilter("bugSeverity", event.target.value)} placeholder="P2" /></div>
              <div className="pv2-field"><label htmlFor="bug-status">状态</label><input id="bug-status" className="pv2-input" value={qualityFilters.bugStatus} onChange={(event) => updateQualityFilter("bugStatus", event.target.value)} placeholder="detected" /></div>
              <div className="pv2-field"><label htmlFor="bug-search">搜索</label><input id="bug-search" className="pv2-input" value={qualityFilters.bugSearch} onChange={(event) => updateQualityFilter("bugSearch", event.target.value)} placeholder="bug id / 标题" /></div>
            </div>
            <div className="pv2-table-wrap">
              <table className="pv2-table">
                <thead><tr><th>Bug</th><th>模块/级别</th><th>状态</th><th>复现与证据</th><th>修复状态</th><th>操作</th></tr></thead>
                <tbody>
                  {bugs.items.length ? bugs.items.map((bug) => (
                    <tr key={bug.bug_id}>
                      <td><strong>{bug.title || bug.bug_id}</strong><br /><span className="pv2-muted pv2-mono">{bug.bug_id}</span></td>
                      <td>{display(bug.module)}<br /><StatusBadge status={bug.severity} /></td>
                      <td><StatusBadge status={bug.status} /></td>
                      <td><span className="pv2-mono">{display(bug.reproduce_command)}</span><br /><BadgeList items={bug.evidence_uris} /></td>
                      <td>commit {display(bug.fix_commit)}<br />verify {display(bug.verification_run_id)}<br />GitHub {display(bug.github_issue_url)}</td>
                      <td><button className="pv2-link-button" type="button" onClick={() => void openBug(bug.bug_id)}>查看 Bug</button></td>
                    </tr>
                  )) : <tr><td className="pv2-empty-cell" colSpan={6}>暂无 Bug 记录</td></tr>}
                </tbody>
              </table>
            </div>
            <Pagination page={bugs} label="validation bug pagination" onPageChange={(page) => updateQualityFilter("bugPage", page)} />
          </div>
        </div>

        <div className="pv2-grid pv2-grid-2" style={{ marginTop: 16 }}>
          <SectionCard title="质量/Bug 详情" eyebrow="readable detail">
            {detailLoading ? <p className="pv2-muted">读取详情中...</p> : null}
            {selectedFinding ? <KeyValuePanel rows={[
              ["finding_id", selectedFinding.finding_id],
              ["来源", selectedFinding.source_type],
              ["规则", selectedFinding.rule_id],
              ["标题", selectedFinding.title],
              ["描述", selectedFinding.description],
              ["文件", selectedFinding.file_path],
              ["fingerprint", selectedFinding.fingerprint],
              ["证据", selectedFinding.evidence_uri],
              ["修复建议", selectedFinding.remediation],
              ["必须验证", selectedFinding.required_verification],
            ]} /> : null}
            {selectedBug ? <KeyValuePanel rows={[
              ["bug_id", selectedBug.bug_id],
              ["标题", selectedBug.title],
              ["描述", selectedBug.description],
              ["模块", selectedBug.module],
              ["严重级别", selectedBug.severity],
              ["状态", selectedBug.status],
              ["触发条件", selectedBug.trigger_condition],
              ["失败 run", selectedBug.failing_run_id],
              ["复现命令", selectedBug.reproduce_command],
              ["证据", selectedBug.evidence_uris],
              ["关闭条件", selectedBug.closure_requirements],
            ]} /> : null}
            {!selectedFinding && !selectedBug ? <p className="pv2-muted">请选择一条质量发现或 Bug。</p> : null}
          </SectionCard>
          <SectionCard title="Agent Context" eyebrow="Codex / Claude repair input">
            <AgentContextPanel context={selectedAgentContext} />
          </SectionCard>
        </div>
      </SectionCard>

      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="Run 详情" eyebrow="metadata / markdown / evidence links">
          {detailLoading ? <p className="pv2-muted">读取详情中...</p> : null}
          {selectedRun ? (
            <>
              <DetailNotice detail={selectedRun} />
              <KeyValuePanel rows={[
                ["run_id", selectedRun.run_id],
                ["title", selectedRun.title],
                ["module", selectedRun.module],
                ["level", selectedRun.level],
                ["status", selectedRun.status],
                ["git_commit", selectedRun.git_commit],
                ["operator", selectedRun.operator],
                ["markdown_path", selectedRun.markdown_path],
                ["coverage_snapshot_id", selectedRun.coverage_snapshot_id],
                ["evidence_manifest_id", selectedRun.evidence_manifest_id],
              ]} />
              <PassScopePanel passScope={selectedRun.pass_scope} businessAssertion={selectedRun.business_assertion} />
              <SectionCard title="质量门禁" eyebrow="quality gates"><QualityGatePanel gates={selectedRun.quality_gates} /></SectionCard>
              <details className="pv2-readable-item"><summary>查看 Markdown 摘要</summary><pre className="pv2-mono" style={{ whiteSpace: "pre-wrap" }}>{(selectedRun.markdown_text || "").slice(0, 4000) || "无 Markdown 内容"}</pre></details>
            </>
          ) : <p className="pv2-muted">请在 Run 历史中选择一条记录。</p>}
        </SectionCard>

        <div>
          <SectionCard title="Coverage 快照" eyebrow="line / branch / diff gates">
            <div className="pv2-table-wrap">
              <table className="pv2-table">
                <thead><tr><th>Snapshot</th><th>状态</th><th>Line</th><th>Branch</th><th>Failed Gates</th><th>操作</th></tr></thead>
                <tbody>
                  {coverage.items.length ? coverage.items.map((item) => (
                    <tr key={item.snapshot_id}>
                      <td><strong>{item.title || item.snapshot_id}</strong><br /><span className="pv2-muted pv2-mono">{compactId(item.snapshot_id)}</span></td>
                      <td><StatusBadge status={item.status} /></td>
                      <td>{pct(item.totals?.line_percent)}</td>
                      <td>{pct(item.totals?.branch_percent)}</td>
                      <td>{arrayCount(item.failed_gates)}</td>
                      <td><button className="pv2-link-button" type="button" onClick={() => void openCoverage(item.snapshot_id)}>查看快照</button></td>
                    </tr>
                  )) : <tr><td className="pv2-empty-cell" colSpan={6}>暂无 coverage 快照</td></tr>}
                </tbody>
              </table>
            </div>
            {selectedCoverage ? <KeyValuePanel rows={[
              ["snapshot_id", selectedCoverage.summary.snapshot_id],
              ["status", selectedCoverage.summary.status],
              ["module", selectedCoverage.summary.module],
              ["level", selectedCoverage.summary.level],
              ["line_percent", selectedCoverage.summary.totals?.line_percent],
              ["branch_percent", selectedCoverage.summary.totals?.branch_percent],
              ["diff_line_percent", selectedCoverage.summary.diff?.line_percent],
              ["failed_gates", arrayCount(selectedCoverage.summary.failed_gates)],
            ]} /> : null}
          </SectionCard>

          <SectionCard title="Evidence Manifest" eyebrow="artifact proof and missing files">
            <div className="pv2-table-wrap">
              <table className="pv2-table">
                <thead><tr><th>Manifest</th><th>模块/级别</th><th>Evidence</th><th>Missing</th><th>操作</th></tr></thead>
                <tbody>
                  {evidence.items.length ? evidence.items.map((item) => (
                    <tr key={item.manifest_id}>
                      <td><strong>{item.title || item.manifest_id}</strong><br /><span className="pv2-muted pv2-mono">{compactId(item.manifest_id)}</span></td>
                      <td>{display(item.module)} / {display(item.level)}</td>
                      <td>{item.evidence_count ?? 0}</td>
                      <td>{item.missing_count ? <span className="pv2-badge pv2-badge-warning">{item.missing_count}</span> : <span className="pv2-badge pv2-badge-success">0</span>}</td>
                      <td><button className="pv2-link-button" type="button" onClick={() => void openEvidence(item.manifest_id)}>查看证据</button></td>
                    </tr>
                  )) : <tr><td className="pv2-empty-cell" colSpan={5}>暂无 evidence manifest</td></tr>}
                </tbody>
              </table>
            </div>
            {selectedEvidence ? <KeyValuePanel rows={[
              ["manifest_id", selectedEvidence.summary.manifest_id],
              ["module", selectedEvidence.summary.module],
              ["level", selectedEvidence.summary.level],
              ["evidence_count", selectedEvidence.summary.evidence_count],
              ["missing_count", selectedEvidence.summary.missing_count],
              ["missing", selectedEvidence.summary.missing],
            ]} /> : null}
          </SectionCard>
        </div>
      </div>
    </main>
  );
}
