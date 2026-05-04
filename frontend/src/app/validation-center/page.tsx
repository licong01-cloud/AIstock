"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import MetricCard from "@/components/paper-v2/MetricCard";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import {
  type JsonObject,
  type ValidationBusinessAssertion,
  type ValidationCoverageDetail,
  type ValidationCoverageSummary,
  type ValidationEvidenceDetail,
  type ValidationEvidenceSummary,
  type ValidationHealth,
  type ValidationPage,
  type ValidationPassScope,
  type ValidationPlan,
  type ValidationRunDetail,
  type ValidationRunSummary,
  type ValidationSummary,
  validationApi,
} from "@/lib/validation/api";

const DEFAULT_PAGE_SIZE = 20;
const EMPTY_PAGE = { items: [], total: 0, page: 1, page_size: DEFAULT_PAGE_SIZE, has_more: false };

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
  ].filter(Boolean);
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

function PassScopePanel({ passScope, businessAssertion }: { passScope?: ValidationPassScope | null; businessAssertion?: ValidationBusinessAssertion | null }) {
  if (!passScope && !businessAssertion) {
    return <div className="pv2-notice pv2-notice-warning"><div className="pv2-notice-title">未记录 / 未证明</div><div className="pv2-notice-body">该 run 没有 pass_scope 或 business_assertion，只能说明曾经产生文本记录，不能证明真实业务链路已通过。</div></div>;
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

export default function ValidationCenterPage() {
  const [health, setHealth] = useState<ValidationHealth | null>(null);
  const [summary, setSummary] = useState<ValidationSummary | null>(null);
  const [plans, setPlans] = useState<ValidationPlan[]>([]);
  const [runs, setRuns] = useState<ValidationPage<ValidationRunSummary>>(EMPTY_PAGE);
  const [coverage, setCoverage] = useState<ValidationPage<ValidationCoverageSummary>>(EMPTY_PAGE);
  const [evidence, setEvidence] = useState<ValidationPage<ValidationEvidenceSummary>>(EMPTY_PAGE);
  const [selectedRun, setSelectedRun] = useState<ValidationRunDetail | null>(null);
  const [selectedCoverage, setSelectedCoverage] = useState<ValidationCoverageDetail | null>(null);
  const [selectedEvidence, setSelectedEvidence] = useState<ValidationEvidenceDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState({ module: "", level: "", status: "", search: "", includeMarkdownOnly: true, page: 1, pageSize: DEFAULT_PAGE_SIZE });

  const loadStatic = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [healthData, summaryData, planCatalog, coverageData, evidenceData] = await Promise.all([
        validationApi.health(),
        validationApi.summary(),
        validationApi.plans(),
        validationApi.coverage({ page: 1, page_size: 10 }),
        validationApi.evidence({ page: 1, page_size: 10 }),
      ]);
      setHealth(healthData);
      setSummary(summaryData);
      setPlans(planCatalog.plans || []);
      setCoverage(coverageData || EMPTY_PAGE);
      setEvidence(evidenceData || EMPTY_PAGE);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setLoading(false);
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
      setRuns(data || EMPTY_PAGE);
    } catch (err) {
      setError(errorText(err));
    }
  }, [filters]);

  useEffect(() => { void loadStatic(); }, [loadStatic]);
  useEffect(() => { void loadRuns(); }, [loadRuns]);

  const statusCounts = useMemo(() => summary?.runs_by_status || {}, [summary]);
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

  function updateFilter(key: keyof typeof filters, value: string | number | boolean) {
    setFilters((prev) => ({ ...prev, [key]: value, page: key === "page" ? Number(value) : 1 }));
  }

  return (
    <main className="pv2-shell">
      <header className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">Validation Center / Read Only</div>
            <h1>自动化测试流水线中心</h1>
            <p>集中查看测试计划、历史 run、覆盖率快照和 evidence manifest。本阶段只读展示，不执行命令、不写数据库、不触碰生产 8001。</p>
          </div>
          <div className="pv2-chip-row">
            <span className="pv2-chip">只读 API</span>
            <span className="pv2-chip">Mock/真实证明边界</span>
            <span className="pv2-chip">缺失状态显式展示</span>
          </div>
        </div>
      </header>

      {error ? <div className="pv2-error-panel"><div className="pv2-error-kicker">Validation Center Error</div><div className="pv2-error-main">{error}</div></div> : null}
      {loading ? <div className="pv2-notice pv2-notice-info"><div className="pv2-notice-title">加载中</div><div className="pv2-notice-body">正在读取本地验证历史索引。</div></div> : null}

      <section className="pv2-grid pv2-grid-4">
        <MetricCard label="历史 Run" value={summary?.run_count ?? health?.history?.run_count ?? "-"} hint={health?.history?.history_root || "tests/aistock_validation/history"} tone="info" />
        <MetricCard label="覆盖率快照" value={summary?.coverage_snapshot_count ?? health?.history?.coverage_snapshot_count ?? "-"} hint={`最新行覆盖 ${pct(latestCoverageTotals.line_percent)}`} tone="success" />
        <MetricCard label="证据 Manifest" value={summary?.evidence_manifest_count ?? health?.history?.evidence_manifest_count ?? "-"} hint="missing_count 显式展示" tone="warning" />
        <MetricCard label="计划数" value={summary?.plan_count ?? health?.plan_catalog?.plan_count ?? "-"} hint={health?.production_8001_touched ? "异常：触碰 8001" : "production_8001_touched=false"} tone={health?.production_8001_touched ? "danger" : "success"} />
      </section>

      <SectionCard
        title="测试计划目录"
        eyebrow="allowlisted nox entrypoints"
        action={<button className="pv2-button-ghost" type="button" disabled aria-label="controlled execution disabled">执行测试（尚未启用）</button>}
      >
        <div className="pv2-notice pv2-notice-info">
          <div className="pv2-notice-title">执行边界</div>
          <div className="pv2-notice-body">UI 当前只展示 allowlist 计划。受控执行、队列、权限确认和 evidence 自动回写将在后续阶段启用。</div>
        </div>
        <div className="pv2-table-wrap">
          <table className="pv2-table">
            <thead><tr><th>Plan</th><th>模块/级别</th><th>Nox</th><th>端口</th><th>写入边界</th><th>状态</th></tr></thead>
            <tbody>
              {plans.length ? plans.map((plan) => (
                <tr key={plan.plan_key}>
                  <td><strong>{plan.title || plan.plan_key}</strong><br /><span className="pv2-muted pv2-mono">{plan.plan_key}</span></td>
                  <td>{display(plan.module)} / {display(plan.level)}</td>
                  <td><span className="pv2-mono">{display(plan.nox_session || plan.command_key)}</span></td>
                  <td>Backend {display(plan.allowed_backend_ports)}<br />Frontend {display(plan.allowed_frontend_ports)}</td>
                  <td>DB {display(plan.writes_database)} / Artifacts {display(plan.writes_artifacts)} / Business {display(plan.writes_business_state)}</td>
                  <td><StatusBadge status={plan.enabled ? "READY" : "DISABLED"} /></td>
                </tr>
              )) : <tr><td className="pv2-empty-cell" colSpan={6}>暂无测试计划</td></tr>}
            </tbody>
          </table>
        </div>
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
        <div className="pv2-chip-row" style={{ marginBottom: 12 }}>
          {Object.entries(statusCounts).map(([status, count]) => <span className="pv2-chip" key={status}>{status}: {count}</span>)}
        </div>
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
        <div className="pv2-pagination" aria-label="validation run pagination">
          <button className="pv2-button-ghost" disabled={filters.page <= 1} onClick={() => updateFilter("page", Math.max(1, filters.page - 1))} type="button">上一页</button>
          <span aria-label="validation run pagination status">第 {runs.page} / {pageCount(runs)} 页，共 {runs.total} 条</span>
          <button className="pv2-button-ghost" disabled={!runs.has_more} onClick={() => updateFilter("page", filters.page + 1)} type="button">下一页</button>
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
