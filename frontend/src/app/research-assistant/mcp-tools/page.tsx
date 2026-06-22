"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import { ApiErrorBox, DetailDrawer, EmptyState, StatusPill } from "@/components/research-assistant/AssistantShared";
import {
  LOCAL_DATA_MANAGEMENT_CAPABILITY,
  LOCAL_DATA_MANAGEMENT_PHASES,
  isLocalDataManagementTool,
  localDataRiskLabel,
  localDataToolPhase,
  localDataToolTitle,
  researchAssistantApi,
  type AssistantMcpServer,
  type AssistantMcpTool,
  type AssistantMcpToolEvent,
  type AssistantMcpToolPage,
  type JsonObject,
} from "@/lib/research-assistant/api";

type CatalogServer = {
  serverKey: string;
  displayName: string;
  domain: string;
  aliases: string[];
  summary: string;
  expectedTools: string[];
  risk: string;
};

type PreflightState = {
  loading: boolean;
  result: JsonObject | null;
  error: unknown;
};

const BUSINESS_MCP_CATALOG: CatalogServer[] = [
  {
    serverKey: "aistock-local-data",
    displayName: "Local Data",
    domain: "local_data",
    aliases: ["data health", "sync target", "repair plan"],
    summary: "Read local data health and prepare repair plans; confirmed repair/sync tools remain approval gated.",
    expectedTools: ["local_data_health_overview", "local_data_list_sync_targets", "local_data_apply_repair_confirmed"],
    risk: "read_only / confirmed_action",
  },
  {
    serverKey: "aistock-factor",
    displayName: "Factor Gateway",
    domain: "factor_library / factor_metrics / factor_correlation",
    aliases: ["factor catalog", "IC", "RankIC", "correlation"],
    summary: "Read factor catalog and result summaries; expensive metric/correlation jobs stay plan or preflight first.",
    expectedTools: ["factor_library_list", "factor_metrics_plan", "factor_corr_plan"],
    risk: "read_only / high_cost_compute_confirmed",
  },
  {
    serverKey: "aistock-qe",
    displayName: "QE Gateway",
    domain: "qe_experiment / qe_archive / model_registry",
    aliases: ["QE archive", "leaderboard", "model registry"],
    summary: "Read archived runs, leaderboards and model metadata; new QE runs and registry writes are preflight gated.",
    expectedTools: ["qe_archive_query_run_leaderboard", "model_registry_list", "qe_run_create"],
    risk: "read_only / long_running",
  },
  {
    serverKey: "aistock-validation",
    displayName: "Validation Center",
    domain: "validation",
    aliases: ["validation runs", "plan runner", "G1 gate"],
    summary: "Read validation history directly; starting validation executions and GitHub writes require preflight.",
    expectedTools: ["list_validation_runs", "get_validation_run", "start_validation_execution"],
    risk: "read_only / long_running / external_network",
  },
  {
    serverKey: "aistock-external-research",
    displayName: "External Research",
    domain: "external_research",
    aliases: ["web search", "paper search", "fetch extract"],
    summary: "L2.5 evidence-first retrieval is read-only automatic; saving evidence drafts remains preflight.",
    expectedTools: ["external_research_search_web", "external_research_search_papers", "external_research_save_evidence"],
    risk: "read_only retrieval / draft write preflight",
  },
  {
    serverKey: "aistock-trading-ops",
    displayName: "Trading Ops",
    domain: "strategy_governance / execution_policy",
    aliases: ["strategy package", "execution policy", "minute algo"],
    summary: "Read strategy and execution-policy health; promotion, binding and live-adjacent changes require approval.",
    expectedTools: ["strategy_governance_list_packages", "execution_policy_list_algos", "execution_policy_apply_binding"],
    risk: "read_only / production_adjacent",
  },
];

function normalizeKey(value: unknown): string {
  return String(value || "").trim().toLowerCase();
}

function stringifyValue(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (Array.isArray(value)) return value.length ? value.map((item) => stringifyValue(item)).join(" / ") : "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function mcpStatusLabel(status: unknown): string {
  const value = normalizeKey(status || "unknown");
  if (["ready", "enabled", "approved", "ok", "pass", "passed"].includes(value)) return "ready";
  if (["disabled", "blocked", "failed", "error"].includes(value)) return "blocked";
  if (["missing", "not_registered"].includes(value)) return "not registered";
  if (["not_checked", "not_run"].includes(value)) return value;
  if (["pending", "initializing", "unknown"].includes(value)) return "pending check";
  return String(status || "unknown");
}

function mcpStatusTone(status: unknown): string {
  const value = normalizeKey(status || "unknown");
  if (["ready", "enabled", "approved", "ok", "pass", "passed"].includes(value)) return "ready";
  if (["disabled", "blocked", "failed", "error"].includes(value)) return "error";
  if (["missing", "not_registered"].includes(value)) return "missing";
  return "pending";
}

function McpStatusBadge({ status }: { status: unknown }) {
  return <span className={`ra-mcp-status ra-mcp-status-${mcpStatusTone(status)}`}>{mcpStatusLabel(status)}</span>;
}

function mcpServerDisplay(server: AssistantMcpServer): string {
  return server.display_name_zh || server.display_title || server.title || server.server_key;
}

function mcpServerAliases(server: AssistantMcpServer): string[] {
  return Array.isArray(server.business_aliases_zh) ? server.business_aliases_zh.slice(0, 4) : [];
}

function slashJoin(items: string[]): string | null {
  return items.length ? items.join(" / ") : null;
}

function serverDomain(server: AssistantMcpServer): string {
  const health = server.health_json && typeof server.health_json === "object" ? server.health_json as Record<string, unknown> : {};
  return String(server.domain || server.mcp_module || health.domain || "-");
}

function catalogForServer(server: AssistantMcpServer): CatalogServer | undefined {
  return BUSINESS_MCP_CATALOG.find((item) => item.serverKey === server.server_key || String(item.domain).includes(serverDomain(server)));
}

function mergedServers(servers: AssistantMcpServer[]): AssistantMcpServer[] {
  const seen = new Set<string>();
  const result: AssistantMcpServer[] = [];
  for (const server of servers) {
    seen.add(server.server_key);
    result.push(server);
  }
  for (const catalog of BUSINESS_MCP_CATALOG) {
    if (seen.has(catalog.serverKey)) continue;
    result.push({
      server_id: `catalog-${catalog.serverKey}`,
      server_key: catalog.serverKey,
      title: `${catalog.displayName} MCP`,
      display_title: catalog.displayName,
      display_name_zh: catalog.displayName,
      business_aliases_zh: catalog.aliases,
      summary_zh: catalog.summary,
      status: "not_registered",
      health_json: { domain: catalog.domain, summary_first: true, catalog_hint: true },
    });
  }
  return result;
}

function countForServer(tools: AssistantMcpTool[], serverKey: string): number {
  return tools.filter((tool) => tool.server_key === serverKey).length;
}

function toOptionEntries(counts: Record<string, number> | undefined): string[] {
  return Object.keys(counts || {}).sort();
}

function distributionEntries(counts: Record<string, number> | undefined): Array<[string, number]> {
  return Object.entries(counts || {}).sort(([left], [right]) => left.localeCompare(right));
}

function toolDisplay(tool: AssistantMcpTool): string {
  return isLocalDataManagementTool(tool) ? localDataToolTitle(tool) : tool.title || tool.tool_name;
}

function statusFrom(value: unknown): unknown {
  if (value && typeof value === "object" && !Array.isArray(value)) return (value as Record<string, unknown>).status;
  return undefined;
}

function toolRecommendedProfile(tool: AssistantMcpTool | null): string {
  if (!tool) return "select a tool";
  const tags = Array.isArray(tool.profile_tags) ? tool.profile_tags : [];
  return String(tool.profile || tags[0] || tool.server_key || "gateway");
}

function buildPreflightPayload(tool: AssistantMcpTool | null): JsonObject {
  if (!tool) return {};
  const payload: JsonObject = { limit: 1 };
  if (tool.tool_name === "external_research_save_evidence") payload.evidence_refs = [];
  if (tool.tool_name.includes("github_issue_create")) payload.title = "Phase 5 UI preflight preview";
  return payload;
}

function findDefaultPreflightTool(tools: AssistantMcpTool[]): AssistantMcpTool | null {
  return tools.find((tool) => tool.tool_name === "external_research_save_evidence")
    || tools.find((tool) => tool.requires_approval)
    || tools.find((tool) => tool.tool_name === "start_validation_execution")
    || tools[0]
    || null;
}

function SummaryCards({ toolPage, filteredCount }: { toolPage: AssistantMcpToolPage<AssistantMcpTool> | null; filteredCount: number }) {
  return (
    <div className="ra-mcp-summary-grid" data-testid="ra-mcp-phase5-summary">
      <article className="ra-mcp-stat-card">
        <span>Catalog Source</span>
        <strong data-testid="ra-mcp-catalog-source">{toolPage?.catalog_source || toolPage?.source || "loading"}</strong>
        <p>Unified gateway manifest is the display source; DB rows are only runtime overlay.</p>
      </article>
      <article className="ra-mcp-stat-card">
        <span>Manifest Tools</span>
        <strong data-testid="ra-mcp-manifest-count">{toolPage?.manifest_tool_count ?? "-"}</strong>
        <p>{filteredCount} compact rows shown with include_schema=false.</p>
      </article>
      <article className="ra-mcp-stat-card">
        <span>Backend Health</span>
        <strong data-testid="ra-mcp-backend-health">{mcpStatusLabel(statusFrom(toolPage?.backend_health))}</strong>
        <p>{stringifyValue((toolPage?.backend_health as JsonObject | undefined)?.reason || "explicit status only; no fake smoke pass")}</p>
      </article>
      <article className="ra-mcp-stat-card">
        <span>Recent Smoke</span>
        <strong data-testid="ra-mcp-recent-smoke">{mcpStatusLabel(statusFrom(toolPage?.recent_smoke))}</strong>
        <p>{stringifyValue((toolPage?.recent_smoke as JsonObject | undefined)?.reason || "not_run means no live smoke was claimed")}</p>
      </article>
    </div>
  );
}

function DistributionPanel({ title, counts, testId }: { title: string; counts?: Record<string, number>; testId: string }) {
  const entries = distributionEntries(counts);
  return (
    <article className="ra-mcp-card" data-testid={testId}>
      <div className="ra-mcp-card-head">
        <div>
          <span className="ra-mcp-eyebrow">distribution</span>
          <h3>{title}</h3>
        </div>
        <StatusPill status={entries.length ? "ready" : "unknown"}>{entries.length ? `${entries.length} buckets` : "no counts"}</StatusPill>
      </div>
      <div className="ra-mcp-distribution">
        {entries.length ? entries.map(([key, count]) => (
          <div className="ra-mcp-distribution-row" key={key}>
            <span>{key}</span>
            <strong>{count}</strong>
          </div>
        )) : <p className="ra-muted">No distribution data returned.</p>}
      </div>
    </article>
  );
}

function ServerCard({ server, toolCount }: { server: AssistantMcpServer; toolCount: number }) {
  const catalog = catalogForServer(server);
  const aliases = mcpServerAliases(server);
  const toolNames = (catalog?.expectedTools || []).slice(0, 4);
  return (
    <article className="ra-mcp-card" data-testid={`ra-mcp-server-${server.server_key}`}>
      <div className="ra-mcp-card-head">
        <div>
          <span className="ra-mcp-eyebrow">{catalog?.domain || serverDomain(server)}</span>
          <h3>{mcpServerDisplay(server)}</h3>
        </div>
        <McpStatusBadge status={server.status} />
      </div>
      <p>{server.summary_zh || catalog?.summary || "Registered MCP server; full schema and health payload stay in on-demand details."}</p>
      <div className="ra-chip-row">
        {(aliases.length ? aliases : catalog?.aliases || []).map((alias) => <span className="ra-chip" key={alias}>{alias}</span>)}
      </div>
      {slashJoin(aliases.length ? aliases : catalog?.aliases || []) ? <p>{slashJoin(aliases.length ? aliases : catalog?.aliases || [])}</p> : null}
      <div className="ra-mcp-meta-grid">
        <span>Server</span><strong>{server.server_key}</strong>
        <span>Risk</span><strong>{catalog?.risk || "per tool manifest"}</strong>
        <span>Tools</span><strong>{toolCount || "not in compact page"}</strong>
        <span>Examples</span><strong>{toolNames.length ? toolNames.join(" / ") : "manifest derived"}</strong>
      </div>
      <p className="ra-muted">Default UI stays summary-first; schema and large payloads appear only in detail drawers or artifact refs.</p>
      <DetailDrawer title="Details: server health / catalog" data={{ server, catalog, tool_count: toolCount }} />
    </article>
  );
}

function FilterBar({
  search,
  riskLevel,
  serverKey,
  profile,
  serverOptions,
  riskOptions,
  profileOptions,
  onSearch,
  onRiskLevel,
  onServerKey,
  onProfile,
}: {
  search: string;
  riskLevel: string;
  serverKey: string;
  profile: string;
  serverOptions: string[];
  riskOptions: string[];
  profileOptions: string[];
  onSearch: (value: string) => void;
  onRiskLevel: (value: string) => void;
  onServerKey: (value: string) => void;
  onProfile: (value: string) => void;
}) {
  return (
    <div className="ra-mcp-filter-bar" data-testid="ra-mcp-tool-filter">
      <label>
        <span>Search tools</span>
        <input value={search} onChange={(event) => onSearch(event.target.value)} placeholder="leaderboard, external, validation..." />
      </label>
      <label>
        <span>Server</span>
        <select value={serverKey} onChange={(event) => onServerKey(event.target.value)}>
          <option value="">all servers</option>
          {serverOptions.map((key) => <option key={key} value={key}>{key}</option>)}
        </select>
      </label>
      <label>
        <span>Risk</span>
        <select value={riskLevel} onChange={(event) => onRiskLevel(event.target.value)}>
          <option value="">all risks</option>
          {riskOptions.map((key) => <option key={key} value={key}>{key}</option>)}
        </select>
      </label>
      <label>
        <span>Profile</span>
        <select value={profile} onChange={(event) => onProfile(event.target.value)}>
          <option value="">all profiles</option>
          {profileOptions.map((key) => <option key={key} value={key}>{key}</option>)}
        </select>
      </label>
    </div>
  );
}

function ToolTable({ tools, onSelect, selectedTool }: { tools: AssistantMcpTool[]; onSelect: (tool: AssistantMcpTool) => void; selectedTool: AssistantMcpTool | null }) {
  if (!tools.length) return <EmptyState title="No MCP tools in the current filter" hint="The compact request uses include_schema=false; clear filters or use search terms from the unified manifest." />;
  return (
    <div className="ra-table-wrap" data-testid="ra-mcp-tool-table">
      <table className="ra-table">
        <thead>
          <tr>
            <th>Tool</th>
            <th>Server / profile</th>
            <th>Risk</th>
            <th>Status</th>
            <th>Approval</th>
            <th>Details</th>
          </tr>
        </thead>
        <tbody>
          {tools.map((tool) => {
            const isSelected = selectedTool?.tool_id === tool.tool_id;
            return (
              <tr key={tool.tool_id || `${tool.server_key}-${tool.tool_name}`} data-testid={`ra-mcp-tool-row-${tool.tool_name}`} className={isSelected ? "ra-mcp-row-selected" : undefined}>
                <td>
                  <strong>{toolDisplay(tool)}</strong><br />
                  <span className="ra-muted">{String(tool.description || tool.tool_name)}</span>
                </td>
                <td>
                  <span className="ra-mono">{tool.server_key}</span><br />
                  <span className="ra-muted">profile: {String(tool.profile || "-")} / module: {String(tool.module || tool.mcp_module || "-")}</span>
                </td>
                <td>
                  <StatusPill status={tool.risk_level || "unknown"} />
                  <div className="ra-muted">manifest: {String(tool.manifest_risk_level || "-")}</div>
                </td>
                <td><McpStatusBadge status={tool.status} /></td>
                <td>{tool.requires_approval ? "preflight required" : "auto read-only"}</td>
                <td>
                  <button type="button" className="ra-secondary-button" onClick={() => onSelect(tool)}>Inspect</button>
                  <DetailDrawer title="schema / preflight / confirmations" data={tool} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function PreflightPanel({ tool, state, onRun }: { tool: AssistantMcpTool | null; state: PreflightState; onRun: () => void }) {
  const result = state.result;
  const approvalRequired = Boolean(result?.approval_required || tool?.requires_approval);
  const passed = result?.passed === true;
  const evidenceRefs = Array.isArray(result?.evidence_refs) ? result?.evidence_refs : [];
  const missingConfirmations = Array.isArray(result?.missing_confirmations) ? result?.missing_confirmations : [];
  return (
    <section className="ra-mcp-section" data-testid="ra-mcp-preflight-panel">
      <div className="ra-mcp-section-head">
        <div>
          <span className="ra-mcp-eyebrow">preflight and approval</span>
          <h2>Unified preflight card</h2>
          <p>List and preflight use the same manifest-derived resolver; write and high-risk tools produce approval pending cards only.</p>
        </div>
        <StatusPill status={approvalRequired ? "warning" : passed ? "ready" : "unknown"}>{approvalRequired ? "approval pending" : passed ? "preflight passed" : "not run"}</StatusPill>
      </div>
      <div className="ra-mcp-preflight-grid">
        <article className="ra-mcp-card">
          <h3 data-testid="ra-mcp-selected-tool">{tool ? tool.tool_name : "No tool selected"}</h3>
          <div className="ra-mcp-meta-grid">
            <span>Server</span><strong>{tool?.server_key || "-"}</strong>
            <span>Profile</span><strong data-testid="ra-mcp-profile-recommendation">{toolRecommendedProfile(tool)}</strong>
            <span>Manifest risk</span><strong>{String(tool?.manifest_risk_level || tool?.risk_level || "-")}</strong>
            <span>Approval</span><strong data-testid="ra-mcp-approval-state">{tool?.requires_approval ? "preflight_required" : "auto_read_only"}</strong>
          </div>
          <button type="button" className="ra-primary-button" disabled={!tool || state.loading} onClick={onRun} data-testid="ra-mcp-run-preflight">
            {state.loading ? "Running preflight..." : "Run preflight"}
          </button>
          <p className="ra-muted">The demo payload is minimal and deterministic; backend schema failures are displayed instead of hidden.</p>
        </article>
        <article className="ra-mcp-card" data-testid="ra-mcp-preflight-result">
          <h3>Result</h3>
          {state.error ? <ApiErrorBox error={state.error} title="Preflight failed" /> : null}
          {result ? (
            <>
              <div className="ra-chip-row">
                <span className="ra-chip">status: {passed ? "passed" : approvalRequired ? "approval_required" : "failed"}</span>
                <span className="ra-chip">profile: {stringifyValue(result.profile)}</span>
                <span className="ra-chip">tool_event: {stringifyValue(result.tool_event_id)}</span>
              </div>
              <div className="ra-mcp-meta-grid">
                <span>Canonical</span><strong>{stringifyValue(result.canonical_server_key || result.server_key)}</strong>
                <span>Checks</span><strong>{stringifyValue(result.preflight_checks)}</strong>
                <span>Missing confirmations</span><strong>{stringifyValue(missingConfirmations)}</strong>
                <span>Evidence</span><strong data-testid="ra-mcp-evidence-refs">{stringifyValue(evidenceRefs)}</strong>
              </div>
              <DetailDrawer title="preflight audit payload" data={result} />
            </>
          ) : <p className="ra-muted">Select a tool and run preflight to inspect profile, approval, evidence refs and canonical route.</p>}
        </article>
      </div>
    </section>
  );
}

function AuditPanel({ events }: { events: AssistantMcpToolEvent[] }) {
  return (
    <section className="ra-mcp-section" data-testid="ra-mcp-audit-panel">
      <div className="ra-mcp-section-head">
        <div>
          <span className="ra-mcp-eyebrow">task audit</span>
          <h2>MCP tool event ledger</h2>
          <p>Recent events expose profile/tool/preflight/approval/evidence fields for trace review.</p>
        </div>
        <StatusPill status={events.length ? "ready" : "unknown"}>{events.length ? `${events.length} events` : "no events loaded"}</StatusPill>
      </div>
      {events.length ? (
        <div className="ra-card-list">
          {events.slice(0, 6).map((event) => {
            const response = event.response_json || {};
            return (
              <article className="ra-list-card" key={event.tool_event_id} data-testid="ra-mcp-audit-event">
                <strong>{event.server_key}/{event.tool_name}</strong>
                <p>{event.event_type} - {event.status}</p>
                <div className="ra-chip-row">
                  <span className="ra-chip">profile: {stringifyValue(response.profile)}</span>
                  <span className="ra-chip">approval: {stringifyValue(response.approval_required)}</span>
                  <span className="ra-chip">task: {stringifyValue(event.task_id)}</span>
                  <span className="ra-chip">event: {event.tool_event_id}</span>
                </div>
                <DetailDrawer title="event response / evidence" data={event} />
              </article>
            );
          })}
        </div>
      ) : <EmptyState title="No MCP audit events yet" hint="Preflight writes a tool event with canonical server, profile, approval state and evidence refs." />}
    </section>
  );
}

export default function ResearchAssistantMcpToolsPage() {
  const [servers, setServers] = useState<AssistantMcpServer[]>([]);
  const [tools, setTools] = useState<AssistantMcpTool[]>([]);
  const [toolPage, setToolPage] = useState<AssistantMcpToolPage<AssistantMcpTool> | null>(null);
  const [events, setEvents] = useState<AssistantMcpToolEvent[]>([]);
  const [selectedTool, setSelectedTool] = useState<AssistantMcpTool | null>(null);
  const [search, setSearch] = useState("");
  const [riskLevel, setRiskLevel] = useState("");
  const [serverKey, setServerKey] = useState("");
  const [profile, setProfile] = useState("");
  const [error, setError] = useState<unknown>(null);
  const [preflight, setPreflight] = useState<PreflightState>({ loading: false, result: null, error: null });

  const load = useCallback(async () => {
    setError(null);
    try {
      const [serverPage, page, eventPage] = await Promise.all([
        researchAssistantApi.mcpServers(),
        researchAssistantApi.mcpTools({ limit: 50, include_schema: false }),
        researchAssistantApi.mcpToolEvents({ limit: 20 }),
      ]);
      setServers(serverPage.items);
      setTools(page.items);
      setToolPage(page);
      setEvents(eventPage.items);
      setSelectedTool((current) => current || findDefaultPreflightTool(page.items));
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const localDataTools = useMemo(() => tools.filter(isLocalDataManagementTool), [tools]);
  const visibleServers = useMemo(() => mergedServers(servers), [servers]);
  const riskOptions = useMemo(() => toOptionEntries(toolPage?.risk_distribution), [toolPage]);
  const profileOptions = useMemo(() => toOptionEntries(toolPage?.profile_distribution), [toolPage]);
  const serverOptions = useMemo(() => visibleServers.map((server) => server.server_key).sort(), [visibleServers]);
  const filteredTools = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return tools.filter((tool) => {
      if (serverKey && tool.server_key !== serverKey) return false;
      if (riskLevel && tool.manifest_risk_level !== riskLevel && tool.risk_level !== riskLevel) return false;
      if (profile && tool.profile !== profile) return false;
      if (!needle) return true;
      return [tool.tool_name, tool.title, tool.description, tool.server_key, tool.module, tool.backend_endpoint]
        .some((value) => String(value || "").toLowerCase().includes(needle));
    });
  }, [profile, riskLevel, search, serverKey, tools]);

  const runPreflight = useCallback(async () => {
    if (!selectedTool) return;
    setPreflight({ loading: true, result: null, error: null });
    try {
      const result = await researchAssistantApi.preflightMcpTool({
        server_key: selectedTool.server_key,
        tool_name: selectedTool.tool_name,
        payload_json: buildPreflightPayload(selectedTool),
        summary: `Phase 5 UI preflight for ${selectedTool.tool_name}`,
        idempotency_key: `phase5-ui-${selectedTool.tool_name}`,
      });
      setPreflight({ loading: false, result, error: null });
      const eventPage = await researchAssistantApi.mcpToolEvents({ limit: 20 });
      setEvents(eventPage.items);
    } catch (exc) {
      setPreflight({ loading: false, result: null, error: exc });
    }
  }, [selectedTool]);

  return (
    <main className="ra-mcp-page" data-testid="ra-mcp-tools-page">
      <ApiErrorBox error={error} />
      <section className="ra-mcp-hero">
        <span className="ra-mcp-eyebrow">MCP Gateway Phase 5</span>
        <h2>Research Assistant consumes the unified MCP manifest catalog</h2>
        <p>
          This console reads the gateway-derived catalog, shows canonical profile recommendations, keeps compact
          <span className="ra-mono"> include_schema=false</span> lists by default, and makes approval/preflight evidence visible without claiming live smoke when none was run.
        </p>
        <div className="ra-chip-row">
          <span className="ra-chip">single source: TOOL_MANIFEST</span>
          <span className="ra-chip">profile recommendation</span>
          <span className="ra-chip">preflight required for writes</span>
          <span className="ra-chip">audit evidence visible</span>
        </div>
        <SummaryCards toolPage={toolPage} filteredCount={filteredTools.length} />
      </section>

      <section className="ra-mcp-section">
        <div className="ra-mcp-section-head">
          <div>
            <span className="ra-mcp-eyebrow">catalog health</span>
            <h2>Risk and profile distribution</h2>
          </div>
          <StatusPill status="ready">{toolPage?.server_count ?? visibleServers.length} canonical servers</StatusPill>
        </div>
        <div className="ra-mcp-grid">
          <DistributionPanel title="Risk distribution" counts={toolPage?.risk_distribution} testId="ra-mcp-risk-distribution" />
          <DistributionPanel title="Profile distribution" counts={toolPage?.profile_distribution} testId="ra-mcp-profile-distribution" />
        </div>
      </section>

      <section className="ra-mcp-section">
        <div className="ra-mcp-section-head">
          <div>
            <span className="ra-mcp-eyebrow">business servers</span>
            <h2>Canonical MCP profile map</h2>
          </div>
          <StatusPill status="ready">registered {servers.length} / visible {visibleServers.length}</StatusPill>
        </div>
        <div className="ra-mcp-grid">
          {visibleServers.map((server) => <ServerCard key={server.server_key} server={server} toolCount={countForServer(tools, server.server_key)} />)}
        </div>
      </section>

      <section className="ra-mcp-section">
        <div className="ra-mcp-section-head">
          <div>
            <span className="ra-mcp-eyebrow">tool search</span>
            <h2>MCP Tools</h2>
            <p>Search and filters operate on the compact unified catalog view; schema stays hidden unless opened in details.</p>
          </div>
          <StatusPill status="ready">showing {filteredTools.length} / {tools.length}</StatusPill>
        </div>
        <FilterBar
          search={search}
          riskLevel={riskLevel}
          serverKey={serverKey}
          profile={profile}
          serverOptions={serverOptions}
          riskOptions={riskOptions}
          profileOptions={profileOptions}
          onSearch={setSearch}
          onRiskLevel={setRiskLevel}
          onServerKey={setServerKey}
          onProfile={setProfile}
        />
        <ToolTable tools={filteredTools} selectedTool={selectedTool} onSelect={setSelectedTool} />
      </section>

      <PreflightPanel tool={selectedTool} state={preflight} onRun={runPreflight} />
      <AuditPanel events={events} />

      <section className="ra-mcp-section">
        <div className="ra-mcp-section-head">
          <div>
            <span className="ra-mcp-eyebrow">local data workflow</span>
            <h2>{LOCAL_DATA_MANAGEMENT_CAPABILITY.displayName}</h2>
          </div>
          <StatusPill status={localDataTools.length ? "ready" : "missing"}>{localDataTools.length ? `found ${localDataTools.length} tools` : "waiting for catalog"}</StatusPill>
        </div>
        <div className="ra-mcp-local-card" data-testid="ra-mcp-local-data-capability">
          <p>{LOCAL_DATA_MANAGEMENT_CAPABILITY.summary}</p>
          <div className="ra-mcp-phase-grid">
            {LOCAL_DATA_MANAGEMENT_PHASES.map((phase) => (
              <article className="ra-mcp-phase" key={phase.key}>
                <span className="ra-mcp-eyebrow">{phase.key}</span>
                <h3>{phase.title}</h3>
                <p>{phase.description}</p>
                <span className="ra-chip">{localDataRiskLabel(phase.riskLevel)}</span>
                <span className="ra-chip">{phase.requiresConfirmation ? "confirmation required" : "no confirmation"}</span>
              </article>
            ))}
          </div>
        </div>
        {localDataTools.length ? (
          <div className="ra-card-list" data-testid="ra-mcp-local-data-tool-cards">
            {localDataTools.map((tool) => {
              const phase = localDataToolPhase(tool.tool_name);
              return (
                <article className="ra-list-card" key={tool.tool_id}>
                  <strong>{localDataToolTitle(tool)}</strong>
                  <p>{phase?.description || tool.description || "Local-data management tool; schema, confirmations and trace remain in audit details."}</p>
                  <div className="ra-chip-row">
                    <span className="ra-chip">{phase?.title || "unassigned phase"}</span>
                    <span className="ra-chip">{localDataRiskLabel(tool.risk_level || phase?.riskLevel)}</span>
                    <span className="ra-chip">{tool.requires_approval || phase?.requiresConfirmation ? "confirmation required" : "no confirmation"}</span>
                  </div>
                  <DetailDrawer title="Audit details: tool schema / confirmations" data={tool} />
                </article>
              );
            })}
          </div>
        ) : (
          <EmptyState title="No local-data tools in compact page" hint="The unified catalog still contains local_data tools; adjust search/filter or request more rows if needed." />
        )}
      </section>
    </main>
  );
}
