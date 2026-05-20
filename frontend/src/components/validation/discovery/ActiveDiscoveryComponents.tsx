"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useMemo, useState } from "react";
import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { Background, Controls, ReactFlow, type Edge, type Node } from "@xyflow/react";
import { Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import {
  type JsonObject,
  type ValidationDiscoveryCandidate,
  type ValidationDiscoveryEvidenceManifest,
  type ValidationDiscoveryExecutionNode,
  type ValidationDiscoveryLlmProfile,
  type ValidationDiscoveryLlmReport,
  type ValidationDiscoveryModule,
  type ValidationDiscoveryNightlyReport,
  type ValidationDiscoveryNightlyReportSummary,
  type ValidationDiscoverySummaryCard,
  type ValidationDiscoveryTask,
  type ValidationDiscoveryToolAdapter,
  type ValidationPage,
} from "@/lib/validation/api";

export type DiscoveryFilters = {
  search?: string;
  severity?: string;
  module?: string;
  status?: string;
  source?: string;
};

const TABS = [
  ["/validation/nightly-reports", "夜间汇报", "Morning review"],
  ["/validation/discovery-candidates", "候选 Issue", "审核与晋级"],
  ["/validation/discovery-tasks", "探测任务", "夜间/变更/MCP"],
  ["/validation/business-probes", "业务探针", "QE 到 Paper v2"],
  ["/validation/discovery-llm-profiles", "LLM 配置引用", "Prompt/模型绑定"],
];

const FLOW = [
  ["qe", "QE 实验", 0],
  ["qe_archive", "QE Archive", 210],
  ["strategy_package", "StrategyPackage", 440],
  ["selection", "Selection Center", 710],
  ["paper_v2", "Paper v2", 980],
  ["data_warehouse", "数仓/证据", 1210],
] as const;

function isRecord(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function display(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "number") return Number.isFinite(value) ? value.toLocaleString("zh-CN") : "-";
  if (Array.isArray(value)) return value.length ? value.map((item) => display(item)).join(" / ") : "-";
  if (isRecord(value)) return Object.entries(value).map(([key, item]) => `${key}: ${display(item)}`).join("; ");
  return String(value);
}

function num(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function counts(value: unknown): Record<string, number> {
  if (!isRecord(value)) return {};
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, num(item)]));
}

function tone(value?: string | null): "healthy" | "warning" | "critical" | "unknown" | "running" {
  const text = String(value || "").toLowerCase();
  if (["passed", "success", "healthy", "verified", "completed", "linked"].some((item) => text.includes(item))) return "healthy";
  if (["failed", "error", "critical", "blocked", "p0", "red"].some((item) => text.includes(item))) return "critical";
  if (["warning", "needs", "missing", "p1", "pending", "degraded", "amber"].some((item) => text.includes(item))) return "warning";
  if (["running", "ready", "scheduled", "claimed", "blue"].some((item) => text.includes(item))) return "running";
  return "unknown";
}

function toneFromCard(value?: string | null): string {
  const text = String(value || "").toLowerCase();
  if (["green", "success"].includes(text)) return "healthy";
  if (["amber", "yellow"].includes(text)) return "warning";
  if (["red", "danger"].includes(text)) return "critical";
  if (["blue", "info"].includes(text)) return "running";
  return tone(text);
}

function pct(value?: number): string {
  return value === undefined || value === null ? "-" : `${Math.round(value * 100)}%`;
}

function copy(value?: string | null) {
  if (value && typeof navigator !== "undefined" && navigator.clipboard) void navigator.clipboard.writeText(value);
}

export function StatusPill({ status }: { status?: string | null }) {
  return <span className={`disc-status disc-status-${tone(status)}`}>{display(status || "unknown")}</span>;
}

export function ValidationDiscoveryShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  return (
    <main className="pv2-shell disc-shell">
      <header className="pv2-hero disc-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">Validation Active Discovery</div>
            <h1>流水线主动发现中心</h1>
            <p>展示夜间测试、候选 Issue、业务探针、MCP Agent 与 LLM 探索报告。LLM 只提供辅助 draft，正式 Issue 必须经过证据审核并同步 GitHub。</p>
          </div>
          <div className="pv2-chip-row"><span className="pv2-chip">真实 API</span><span className="pv2-chip">证据抽屉</span><span className="pv2-chip">安全确认</span></div>
        </div>
        <nav className="disc-top-tabs" aria-label="流水线主动发现顶部导航">
          {TABS.map(([href, label, hint]) => {
            const active = pathname === href || pathname?.startsWith(`${href}/`);
            return <Link className={`disc-top-tab ${active ? "disc-top-tab-active" : ""}`} href={href} key={href}><span>{label}</span><small>{hint}</small></Link>;
          })}
        </nav>
      </header>
      {children}
    </main>
  );
}

export function MetricSummaryCard({ card, onClick }: { card: ValidationDiscoverySummaryCard; onClick?: (card: ValidationDiscoverySummaryCard) => void }) {
  return (
    <button className={`disc-metric disc-metric-${toneFromCard(card.tone)}`} onClick={() => onClick?.(card)} title={display(card.hint)} type="button">
      <span className="disc-metric-label">{card.title}</span>
      <strong className="disc-metric-value">{display(card.value)}</strong>
      <span className="disc-metric-hint">{display(card.hint)}</span>
    </button>
  );
}

export function NightlyRunHeader({
  reports,
  selectedReportId,
  report,
  loading,
  onSelectReport,
  onRefresh,
}: {
  reports: ValidationDiscoveryNightlyReportSummary[];
  selectedReportId: string;
  report?: ValidationDiscoveryNightlyReport | null;
  loading?: boolean;
  onSelectReport: (reportId: string) => void;
  onRefresh: () => void;
}) {
  const run = report?.run;
  return (
    <section className="pv2-card">
      <div className="pv2-card-head">
        <div><div className="pv2-eyebrow">Nightly Run</div><h2>{display(run?.title || report?.report_id || "当前快照")}</h2><p className="pv2-muted">按日期、run_id、branch、commit 切换报告，切换后所有卡片同步刷新。</p></div>
        <div className="pv2-row-actions">
          <select className="pv2-select disc-run-select" value={selectedReportId} onChange={(event) => onSelectReport(event.target.value)}>
            <option value="current">当前快照</option>
            {reports.map((item) => <option key={item.report_id} value={item.report_id}>{display(item.run?.run_id || item.report_id)} / {display(item.run?.branch)} / {display(item.run?.commit)}</option>)}
          </select>
          <button className="pv2-button-primary" disabled={loading} onClick={onRefresh} type="button">{loading ? "刷新中" : "刷新"}</button>
        </div>
      </div>
      <div className="disc-run-grid">
        {[["report_id", report?.report_id], ["run_id", run?.run_id], ["branch", run?.branch], ["commit", run?.commit], ["开始", run?.started_at], ["结束", run?.finished_at]].map(([key, value]) => (
          <div key={key}><span>{key}</span><button className="pv2-link-button pv2-mono" onClick={() => copy(String(value || ""))} type="button">{display(value)}</button></div>
        ))}
        <div><span>状态</span><StatusPill status={run?.status || "snapshot"} /></div>
      </div>
    </section>
  );
}

export function ModuleResultCard({ module, onOpenEvidence, onFilterCandidates }: { module: ValidationDiscoveryModule; onOpenEvidence?: (id?: string) => void; onFilterCandidates?: (moduleId: string) => void }) {
  const [open, setOpen] = useState(false);
  const coverage = module.coverage || {};
  const status = module.status || String(coverage.status || "unknown");
  return (
    <article className={`disc-module-card disc-module-${tone(status)}`}>
      <button className="disc-module-head" onClick={() => setOpen((value) => !value)} type="button"><span><strong>{display(module.display_name || module.module_id)}</strong><small className="pv2-mono">{module.module_id}</small></span><StatusPill status={status} /></button>
      <div className="disc-module-metrics"><span>覆盖率 <b>{display(coverage.line_percent)}%</b></span><span>候选 <b>{display(module.candidate_count)}</b></span><span>P0/P1 <b>{display(module.p0_p1_count)}</b></span><span>Issue <b>{display(module.issue_count)}</b></span></div>
      {open ? <div className="disc-module-detail"><KeyValue rows={[["测试计划", module.test_plans], ["覆盖率详情", coverage], ["Finding", module.finding_count], ["changed files", module.workspace_changed_file_count]]} /><div className="pv2-row-actions" style={{ marginTop: 12 }}><button className="pv2-button-ghost" onClick={() => onFilterCandidates?.(module.module_id)} type="button">查看本模块候选</button><button className="pv2-button-ghost" onClick={() => onOpenEvidence?.(`evid_module_${module.module_id}`)} type="button">打开证据</button></div></div> : null}
    </article>
  );
}

export function KeyValue({ rows }: { rows: Array<[string, unknown]> }) {
  return <div className="pv2-readable-panel"><div className="pv2-readable-table">{rows.map(([key, value]) => <div className="pv2-readable-row" key={key}><div className="pv2-readable-key">{key}</div><div className="pv2-readable-value">{display(value)}</div></div>)}</div></div>;
}

function EvidenceSection({ title, items }: { title: string; items?: unknown[] }) {
  const rows = Array.isArray(items) ? items : [];
  return <details className="disc-evidence-section" open={rows.length > 0}><summary>{title} <span className="pv2-badge pv2-badge-neutral">{rows.length}</span></summary>{rows.length ? rows.map((item, index) => <pre key={index}>{JSON.stringify(item, null, 2)}</pre>) : <p className="pv2-muted">暂无该类证据。</p>}</details>;
}

export function EvidenceDrawer({ open, title, manifest, loading, error, candidate, onClose }: { open: boolean; title?: string; manifest?: ValidationDiscoveryEvidenceManifest | null; loading?: boolean; error?: string | null; candidate?: ValidationDiscoveryCandidate | null; onClose: () => void }) {
  if (!open) return null;
  const evidenceCount = [manifest?.logs, manifest?.api_responses, manifest?.mcp_responses, manifest?.screenshots, manifest?.artifacts].reduce((sum, item) => sum + (Array.isArray(item) ? item.length : 0), 0);
  return (
    <div className="disc-drawer-backdrop" role="presentation">
      <aside className="disc-drawer" aria-label="证据详情抽屉">
        <div className="disc-drawer-head"><div><div className="pv2-eyebrow">Evidence Drawer</div><h2>{title || display(manifest?.manifest_id || candidate?.candidate_id || "证据包")}</h2><p className="pv2-muted">日志、API/MCP 响应、截图/trace、artifact 与复现命令统一展示；敏感字段必须脱敏。</p></div><button className="pv2-button-ghost" onClick={onClose} type="button">关闭</button></div>
        {loading ? <div className="pv2-notice pv2-notice-info">正在加载证据...</div> : null}
        {error ? <div className="pv2-notice pv2-notice-warning">{error}</div> : null}
        {candidate ? <KeyValue rows={[["候选", `${display(candidate.title)} / ${candidate.candidate_id}`], ["级别", `${display(candidate.severity)} / ${pct(candidate.confidence)}`], ["GitHub", candidate.github_issue_url || "待审核/未同步"], ["复现", candidate.reproduce_command]]} /> : null}
        <div className="disc-evidence-count">证据条目：{evidenceCount}</div>
        <EvidenceSection title="日志" items={manifest?.logs} /><EvidenceSection title="API 响应" items={manifest?.api_responses} /><EvidenceSection title="MCP 响应" items={manifest?.mcp_responses} /><EvidenceSection title="截图 / Trace" items={manifest?.screenshots} /><EvidenceSection title="Artifacts" items={manifest?.artifacts} />
        <div className="disc-reproduce-box"><strong>复现命令</strong><code>{display(manifest?.reproduce_command || candidate?.reproduce_command)}</code><button className="pv2-button-ghost" onClick={() => copy(String(manifest?.reproduce_command || candidate?.reproduce_command || ""))} type="button">复制复现命令</button></div>
      </aside>
    </div>
  );
}

export function ExecutionTimeline({ nodes, onOpenEvidence }: { nodes?: ValidationDiscoveryExecutionNode[]; onOpenEvidence?: (id?: string) => void }) {
  return (
    <section className="pv2-card"><div className="pv2-card-head"><div><div className="pv2-eyebrow">Execution Timeline</div><h2>执行链路</h2></div></div>
      <div className="disc-timeline">{(nodes || []).map((group) => <div className={`disc-timeline-group disc-timeline-${tone(group.status)}`} key={group.node_id}><div className="disc-timeline-title"><strong>{display(group.label || group.node_id)}</strong><StatusPill status={group.status} /></div><div className="pv2-muted">耗时 {display(group.duration_ms)} ms / 任务 {group.children?.length || 0}</div><div className="disc-timeline-children">{(group.children || []).map((task) => <button className="disc-task-chip" key={task.task_id} onClick={() => onOpenEvidence?.(task.evidence_manifest_id)} type="button"><StatusPill status={task.status} /> {display(task.title || task.task_id)}</button>)}</div></div>)}{!(nodes || []).length ? <div className="pv2-empty-cell">暂无执行链路数据。</div> : null}</div>
    </section>
  );
}

export function IssueCandidateTable({ candidates, page, filters, onFilterChange, onPageChange, onOpenCandidate, onOpenEvidence, onReview, onPromote }: { candidates: ValidationPage<ValidationDiscoveryCandidate>; page: number; filters: DiscoveryFilters; onFilterChange: (filters: DiscoveryFilters) => void; onPageChange: (page: number) => void; onOpenCandidate: (candidate: ValidationDiscoveryCandidate) => void; onOpenEvidence: (candidate: ValidationDiscoveryCandidate) => void; onReview?: (candidate: ValidationDiscoveryCandidate, action: string) => void; onPromote?: (candidate: ValidationDiscoveryCandidate) => void }) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const columns = useMemo<ColumnDef<ValidationDiscoveryCandidate>[]>(() => [
    { accessorKey: "candidate_id", header: "候选", cell: ({ row }) => <button className="pv2-link-button" onClick={() => onOpenCandidate(row.original)} type="button"><strong>{display(row.original.title)}</strong><br /><span className="pv2-mono">{row.original.candidate_id}</span></button> },
    { accessorKey: "severity", header: "级别", cell: ({ row }) => <StatusPill status={row.original.severity} /> },
    { accessorKey: "module", header: "模块", cell: ({ row }) => display(row.original.module) },
    { accessorKey: "source", header: "来源", cell: ({ row }) => display(row.original.source) },
    { accessorKey: "confidence", header: "置信度", cell: ({ row }) => pct(row.original.confidence) },
    { accessorKey: "evidence_status", header: "证据", cell: ({ row }) => <button className="pv2-link-button" onClick={() => onOpenEvidence(row.original)} type="button">{display(row.original.evidence_status)} / {display(row.original.evidence_types)}</button> },
    { accessorKey: "review_status", header: "审核", cell: ({ row }) => <StatusPill status={row.original.review_status} /> },
    { accessorKey: "github_issue_url", header: "GitHub", cell: ({ row }) => row.original.github_issue_url ? <a href={row.original.github_issue_url} target="_blank" rel="noreferrer">#{display(row.original.github_issue_number)}</a> : <span className="pv2-muted">待审核/未同步</span> },
    { id: "actions", header: "操作", cell: ({ row }) => <div className="pv2-row-actions"><button className="pv2-button-ghost" onClick={() => onReview?.(row.original, "needs_evidence")} type="button">追加证据</button><button className="pv2-button-ghost" onClick={() => onReview?.(row.original, "rejected")} type="button">拒绝</button><button className="pv2-button-primary" onClick={() => onPromote?.(row.original)} type="button">晋级</button></div> },
  ], [onOpenCandidate, onOpenEvidence, onPromote, onReview]);
  const table = useReactTable({ data: candidates.items, columns, state: { sorting }, onSortingChange: setSorting, getCoreRowModel: getCoreRowModel(), getSortedRowModel: getSortedRowModel() });
  const totalPages = Math.max(1, Math.ceil(candidates.total / Math.max(1, candidates.page_size)));
  return (
    <section className="pv2-card"><div className="pv2-card-head"><div><div className="pv2-eyebrow">Issue Candidates</div><h2>候选 Issue 审核表</h2></div></div>
      <div className="disc-filter-grid"><input className="pv2-input" placeholder="搜索标题或 candidate_id" value={filters.search || ""} onChange={(event) => onFilterChange({ ...filters, search: event.target.value })} /><select className="pv2-select" value={filters.severity || ""} onChange={(event) => onFilterChange({ ...filters, severity: event.target.value })}><option value="">全部级别</option><option value="P0">P0</option><option value="P1">P1</option><option value="P2">P2</option><option value="P3">P3</option></select><input className="pv2-input" placeholder="模块过滤" value={filters.module || ""} onChange={(event) => onFilterChange({ ...filters, module: event.target.value })} /><input className="pv2-input" placeholder="审核状态过滤" value={filters.status || ""} onChange={(event) => onFilterChange({ ...filters, status: event.target.value })} /><input className="pv2-input" placeholder="来源过滤" value={filters.source || ""} onChange={(event) => onFilterChange({ ...filters, source: event.target.value })} /></div>
      <div className="pv2-table-wrap"><table className="pv2-table disc-table"><thead>{table.getHeaderGroups().map((group) => <tr key={group.id}>{group.headers.map((header) => <th key={header.id}><button className="pv2-sort-button" onClick={header.column.getToggleSortingHandler()} type="button">{flexRender(header.column.columnDef.header, header.getContext())} {display(header.column.getIsSorted() || "")}</button></th>)}</tr>)}</thead><tbody>{table.getRowModel().rows.length ? table.getRowModel().rows.map((row) => <tr key={row.id}>{row.getVisibleCells().map((cell) => <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}</tr>) : <tr><td className="pv2-empty-cell" colSpan={columns.length}>暂无候选；请调整过滤条件或等待夜间探测结果。</td></tr>}</tbody></table></div>
      <div className="pv2-pagination"><span>第 {page} / {totalPages} 页，总数 {candidates.total}</span><button className="pv2-button-ghost" disabled={page <= 1} onClick={() => onPageChange(page - 1)} type="button">上一页</button><button className="pv2-button-ghost" disabled={!candidates.has_more} onClick={() => onPageChange(page + 1)} type="button">下一页</button></div>
    </section>
  );
}

export function LlmReportPanel({ profiles, report, onRunEval }: { profiles?: ValidationDiscoveryLlmProfile[]; report?: ValidationDiscoveryLlmReport | null; onRunEval?: () => void }) {
  const chartRows = (profiles || []).map((profile) => ({ name: profile.agent_role || profile.profile_id, success: num(profile.last_7_runs?.success_rate), cost: num(profile.last_7_runs?.cost_estimate) }));
  return (
    <section className="pv2-card"><div className="pv2-card-head"><div><div className="pv2-eyebrow">LLM Report</div><h2>LLM 探索报告与配置引用</h2></div><button className="pv2-button-primary" onClick={onRunEval} type="button">运行 Eval dry-run</button></div>
      <div className="pv2-notice pv2-notice-info">LLM 输出只作为 draft；正式 Issue 必须进入候选审核，并补齐确定性证据与 GitHub 同步门禁。</div>
      <div className="disc-chart-box"><ResponsiveContainer width="100%" height={180}><BarChart data={chartRows}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="name" hide /><YAxis /><Tooltip /><Bar dataKey="success" fill="#0f766e" name="成功率" /><Bar dataKey="cost" fill="#1d4ed8" name="成本估算" /></BarChart></ResponsiveContainer></div>
      <div className="disc-profile-grid">{(profiles || []).map((profile) => <article className="disc-profile-card" key={profile.profile_id}><div className="disc-profile-head"><strong>{display(profile.agent_role)}</strong><StatusPill status={profile.provider_status} /></div><KeyValue rows={[["Provider / Model", `${display(profile.provider_id)} / ${display(profile.model_id)}`], ["Prompt", `${display(profile.prompt_id)} v${display(profile.prompt_version)}`], ["Context", `不展示 token；secret_visible=${display(profile.secret_visible)}`], ["质量", profile.last_7_runs]]} /><div className="pv2-row-actions" style={{ marginTop: 10 }}><Link className="pv2-button-ghost" href={profile.prompt_management_url || "/quantevolver/prompts"}>Prompt 管理</Link><Link className="pv2-button-ghost" href={profile.model_config_url || "/config/rdagent-llm"}>模型配置</Link></div></article>)}</div>
      <div className="disc-llm-drafts"><h3>LLM draft 候选与补证据状态</h3>{(report?.draft_candidates || []).length ? report?.draft_candidates?.map((candidate) => <div className="pv2-readable-item" key={candidate.candidate_id}><strong>{display(candidate.title)}</strong> <StatusPill status={candidate.deterministic_status || candidate.evidence_status} /><div className="pv2-muted">{display(candidate.llm_provider_declared)} / {display(candidate.llm_model_declared)} / {display(candidate.prompt_id)} / {display(candidate.context_pack_id)}</div></div>) : <p className="pv2-muted">暂无 LLM draft 候选。</p>}</div>
    </section>
  );
}

export function AgentTaskPanel({ tasks, sourceFilter, statusFilter, onSourceFilter, onStatusFilter, onClaim, onRun, onCancel, onOpenEvidence }: { tasks: ValidationPage<ValidationDiscoveryTask>; sourceFilter?: string; statusFilter?: string; onSourceFilter: (value: string) => void; onStatusFilter: (value: string) => void; onClaim?: (task: ValidationDiscoveryTask) => void; onRun?: (task: ValidationDiscoveryTask) => void; onCancel?: (task: ValidationDiscoveryTask) => void; onOpenEvidence?: (id?: string) => void }) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const columns = useMemo<ColumnDef<ValidationDiscoveryTask>[]>(() => [
    { accessorKey: "task_id", header: "任务", cell: ({ row }) => <><strong>{display(row.original.title)}</strong><br /><span className="pv2-mono">{row.original.task_id}</span></> },
    { accessorKey: "source", header: "分类", cell: ({ row }) => display(row.original.source) },
    { accessorKey: "module", header: "模块", cell: ({ row }) => display(row.original.module) },
    { accessorKey: "risk_level", header: "风险", cell: ({ row }) => <StatusPill status={row.original.risk_level} /> },
    { accessorKey: "status", header: "状态", cell: ({ row }) => <StatusPill status={row.original.status} /> },
    { accessorKey: "detectors", header: "Detector", cell: ({ row }) => display(row.original.detectors) },
    { accessorKey: "agent_runtime", header: "Agent", cell: ({ row }) => <>{display(row.original.agent_runtime)}<br /><span className="pv2-muted">{display(row.original.workspace || row.original.branch)}</span></> },
    { id: "actions", header: "操作", cell: ({ row }) => <div className="pv2-row-actions"><button className="pv2-button-ghost" onClick={() => onOpenEvidence?.(row.original.evidence_manifest_id)} type="button">证据</button><button className="pv2-button-ghost" onClick={() => onClaim?.(row.original)} type="button">Claim</button><button className="pv2-button-primary" onClick={() => onRun?.(row.original)} type="button">Dry-run</button><button className="pv2-button-ghost" onClick={() => onCancel?.(row.original)} type="button">取消</button></div> },
  ], [onCancel, onClaim, onOpenEvidence, onRun]);
  const table = useReactTable({ data: tasks.items, columns, state: { sorting }, onSortingChange: setSorting, getCoreRowModel: getCoreRowModel(), getSortedRowModel: getSortedRowModel() });
  return <section className="pv2-card"><div className="pv2-card-head"><div><div className="pv2-eyebrow">Agent Tasks</div><h2>MCP Agent 探测任务</h2></div></div><div className="disc-filter-grid disc-filter-grid-compact"><select className="pv2-select" value={sourceFilter || ""} onChange={(event) => onSourceFilter(event.target.value)}><option value="">全部分类</option><option value="nightly_baseline">nightly baseline</option><option value="change_driven">change-driven</option><option value="manual_mcp">manual MCP</option></select><input className="pv2-input" placeholder="状态过滤" value={statusFilter || ""} onChange={(event) => onStatusFilter(event.target.value)} /></div><div className="pv2-table-wrap"><table className="pv2-table disc-table"><thead>{table.getHeaderGroups().map((group) => <tr key={group.id}>{group.headers.map((header) => <th key={header.id}><button className="pv2-sort-button" onClick={header.column.getToggleSortingHandler()} type="button">{flexRender(header.column.columnDef.header, header.getContext())}</button></th>)}</tr>)}</thead><tbody>{table.getRowModel().rows.length ? table.getRowModel().rows.map((row) => <tr key={row.id}>{row.getVisibleCells().map((cell) => <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}</tr>) : <tr><td className="pv2-empty-cell" colSpan={columns.length}>暂无任务。</td></tr>}</tbody></table></div></section>;
}

export function CleanupRiskPanel({ tasks, cleanup }: { tasks?: ValidationDiscoveryTask[]; cleanup?: JsonObject }) {
  const risky = (tasks || []).filter((task) => task.cleanup_required || task.cleanup_status === "failed");
  return <section className="pv2-card"><div className="pv2-card-head"><div><div className="pv2-eyebrow">Cleanup Risk</div><h2>清理与资源风险</h2></div><StatusPill status={num(cleanup?.overdue_count) || num(cleanup?.failed_count) ? "warning" : "healthy"} /></div><div className="pv2-grid pv2-grid-4"><div className="pv2-notice pv2-notice-info"><div className="pv2-notice-title">Namespace</div><div className="pv2-notice-body">{display(cleanup?.namespace || "validation")}</div></div><div className="pv2-notice pv2-notice-info"><div className="pv2-notice-title">资源数</div><div className="pv2-notice-body">{display(cleanup?.validation_resource_count || risky.length)}</div></div><div className="pv2-notice pv2-notice-warning"><div className="pv2-notice-title">逾期</div><div className="pv2-notice-body">{display(cleanup?.overdue_count || 0)}</div></div><div className="pv2-notice pv2-notice-warning"><div className="pv2-notice-title">失败</div><div className="pv2-notice-body">{display(cleanup?.failed_count || 0)}</div></div></div><div className="pv2-readable-list">{risky.length ? risky.map((task) => <div className="pv2-readable-item" key={task.task_id}><StatusPill status={task.cleanup_status || (task.cleanup_required ? "cleanup_required" : "unknown")} /> {display(task.title || task.task_id)} <span className="pv2-muted">{display(task.resource_policy_id)}</span></div>) : <div className="pv2-readable-item">暂无逾期或失败的 validation namespace 资源。</div>}</div></section>;
}

export function BusinessProbeFlow({ report, onOpenNode, onOpenEvidence }: { report?: ValidationDiscoveryNightlyReport | null; onOpenNode?: (nodeId: string) => void; onOpenEvidence?: (id?: string) => void }) {
  const nodes = useMemo<Node[]>(() => FLOW.map(([id, label, x]) => {
    const moduleMap = new Map((report?.modules || []).map((item) => [item.module_id, item]));
    const moduleInfo = moduleMap.get(id) || (id === "qe_archive" ? moduleMap.get("qe") : undefined);
    const status = moduleInfo?.status || (moduleInfo ? "healthy" : "unknown");
    return { id, position: { x, y: 90 }, data: { label: `${label}\n${display(status)} / candidate ${display(moduleInfo?.candidate_count || 0)}` }, className: `disc-flow-node disc-flow-${tone(status)}` };
  }), [report?.modules]);
  const edges = useMemo<Edge[]>(() => [["qe", "qe_archive"], ["qe_archive", "strategy_package"], ["strategy_package", "selection"], ["selection", "paper_v2"], ["paper_v2", "data_warehouse"]].map(([source, target]) => ({ id: `${source}-${target}`, source, target })), []);
  const trendData = (report?.modules || []).map((moduleInfo) => ({ module: moduleInfo.module_id, candidates: moduleInfo.candidate_count || 0, p0p1: moduleInfo.p0_p1_count || 0 }));
  return <section className="pv2-card"><div className="pv2-card-head"><div><div className="pv2-eyebrow">Business Probe Flow</div><h2>{"QE -> Archive -> StrategyPackage -> Selection -> Paper v2 -> DW"}</h2></div><button className="pv2-button-ghost" onClick={() => onOpenEvidence?.(report?.evidence_manifest_id)} type="button">打开全链路证据</button></div><div className="disc-flow-wrap"><ReactFlow nodes={nodes} edges={edges} fitView onNodeClick={(_, node) => onOpenNode?.(node.id)} nodesDraggable={false} nodesConnectable={false}><Background /><Controls showInteractive={false} /></ReactFlow></div><div className="disc-chart-box"><ResponsiveContainer width="100%" height={190}><LineChart data={trendData}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="module" /><YAxis /><Tooltip /><Line type="monotone" dataKey="candidates" stroke="#0f766e" name="候选" /><Line type="monotone" dataKey="p0p1" stroke="#b91c1c" name="P0/P1" /></LineChart></ResponsiveContainer></div></section>;
}

export function ToolAdapterPanel({ adapters, results, onRun }: { adapters?: ValidationDiscoveryToolAdapter[]; results?: JsonObject[]; onRun?: (adapter: ValidationDiscoveryToolAdapter) => void }) {
  return <section className="pv2-card"><div className="pv2-card-head"><div><div className="pv2-eyebrow">Tool Adapters</div><h2>确定性发现器与 dry-run</h2></div></div><div className="disc-profile-grid">{(adapters || []).map((adapter) => <article className="disc-profile-card" key={adapter.adapter_id}><div className="disc-profile-head"><strong>{display(adapter.title || adapter.adapter_id)}</strong><StatusPill status={adapter.status} /></div><p className="pv2-muted">{display(adapter.kind)} / {display(adapter.config_path)}</p><div className="pv2-row-actions"><button className="pv2-button-primary" onClick={() => onRun?.(adapter)} type="button">dry-run</button><span className="pv2-chip">writes_production={display(adapter.writes_production)}</span></div></article>)}</div>{(results || []).length ? <details className="disc-evidence-section" open><summary>最近 dry-run 结果</summary>{results?.map((item, index) => <pre key={index}>{JSON.stringify(item, null, 2)}</pre>)}</details> : null}</section>;
}

export function CandidateGroupLinks({ summary, onFilter }: { summary?: ValidationDiscoveryNightlyReport["candidate_summary"]; onFilter?: (status: string) => void }) {
  const byStatus = counts(summary?.by_review_status);
  return <section className="pv2-card"><div className="pv2-card-head"><div><div className="pv2-eyebrow">Candidate Groups</div><h2>候选 Issue 分组</h2></div></div><div className="disc-group-grid">{[["pending_review", "待审核"], ["needs_evidence", "证据不足"], ["promote_requested", "已请求晋级"], ["rejected", "已拒绝"], ["verified", "已验证"]].map(([key, label]) => <button className="disc-group-card" key={key} onClick={() => onFilter?.(key)} type="button"><span>{label}</span><strong>{byStatus[key] || 0}</strong><small>{key}</small></button>)}</div></section>;
}
