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

const BUSINESS_MCP_CATALOG: CatalogServer[] = [
  {
    serverKey: "aistock-local-data",
    displayName: "本地数据管理",
    domain: "local_data_management",
    aliases: ["本地数据", "数据同步", "数据修复"],
    summary: "通过统一 MCP Gateway 查询数据健康、生成修复计划，并在确认后执行同步或修复任务。",
    expectedTools: ["local_data_health_overview", "local_data_plan_repair", "local_data_apply_repair_confirmed"],
    risk: "read_only / confirmed_action",
  },
  {
    serverKey: "aistock-factor-library",
    displayName: "因子库",
    domain: "factor_library",
    aliases: ["因子目录", "因子列表", "因子详情"],
    summary: "查询因子元数据、覆盖率、版本和质量标签；列表只返回概要，单因子详情按需展开。",
    expectedTools: ["factor_library_list", "factor_library_get", "factor_library_search"],
    risk: "read_only",
  },
  {
    serverKey: "aistock-factor-metrics",
    displayName: "因子独立指标",
    domain: "factor_metrics",
    aliases: ["IC", "RankIC", "稳定性", "分组收益"],
    summary: "规划或查询因子独立指标计算；提交计算必须先 preflight 和确认，结果默认摘要。",
    expectedTools: ["factor_metrics_plan", "factor_metrics_get_job", "factor_metrics_get_result"],
    risk: "read_only / high_cost_compute_confirmed",
  },
  {
    serverKey: "aistock-factor-correlation",
    displayName: "因子相关性",
    domain: "factor_correlation",
    aliases: ["相关性矩阵", "冗余因子", "替换建议"],
    summary: "规划相关性计算、查看 top pairs 和替换建议；完整矩阵通过 artifact_ref 返回。",
    expectedTools: ["factor_corr_plan", "factor_corr_get_matrix", "factor_corr_suggest_replacements"],
    risk: "read_only / high_cost_compute_confirmed",
  },
  {
    serverKey: "aistock-model-registry",
    displayName: "模型库",
    domain: "model_registry",
    aliases: ["模型版本", "模型 trial", "seed 对比"],
    summary: "查询模型、trial、超参、seed 稳定性和 artifact manifest；模型权重不在列表中内联。",
    expectedTools: ["model_registry_list", "model_registry_get", "model_registry_compare_trials"],
    risk: "read_only / confirmed_registration",
  },
  {
    serverKey: "aistock-strategy-governance",
    displayName: "策略库",
    domain: "strategy_governance",
    aliases: ["策略包", "策略治理", "Paper readiness"],
    summary: "查询策略包健康、Selection/Paper readiness、promotion plan；推广或退役必须确认。",
    expectedTools: ["strategy_governance_list_packages", "strategy_governance_get_health", "strategy_governance_plan_promotion"],
    risk: "read_only / governance_confirmed",
  },
  {
    serverKey: "aistock-execution-policy",
    displayName: "执行策略库",
    domain: "execution_policy",
    aliases: ["minute algo", "TWAP", "VWAP", "POV"],
    summary: "查询执行算法、适用场景和风险限制；实盘或半实盘路径必须审批。",
    expectedTools: ["execution_policy_list_algos", "execution_policy_get", "execution_policy_validate"],
    risk: "read_only / live_gate_approval",
  },
];

function normalizeKey(value: unknown): string {
  return String(value || "").trim().toLowerCase();
}

function mcpStatusLabel(status: unknown): string {
  const value = normalizeKey(status || "unknown");
  if (["ready", "enabled", "approved", "ok"].includes(value)) return "已就绪";
  if (["disabled", "blocked", "failed", "error"].includes(value)) return "不可用";
  if (["missing", "not_registered"].includes(value)) return "未登记";
  if (["pending", "initializing", "unknown"].includes(value)) return "待检查";
  return String(status || "unknown");
}

function mcpStatusTone(status: unknown): string {
  const value = normalizeKey(status || "unknown");
  if (["ready", "enabled", "approved", "ok"].includes(value)) return "ready";
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

function toolsForServer(tools: AssistantMcpTool[], serverKey: string): AssistantMcpTool[] {
  return tools.filter((tool) => tool.server_key === serverKey);
}

function catalogForServer(server: AssistantMcpServer): CatalogServer | undefined {
  return BUSINESS_MCP_CATALOG.find((item) => item.serverKey === server.server_key || item.domain === serverDomain(server));
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

function ServerCard({ server, tools }: { server: AssistantMcpServer; tools: AssistantMcpTool[] }) {
  const catalog = catalogForServer(server);
  const aliases = mcpServerAliases(server);
  const toolNames = tools.length ? tools.map((tool) => tool.tool_name).slice(0, 4) : (catalog?.expectedTools || []).slice(0, 4);
  return (
    <article className="ra-mcp-card" data-testid={`ra-mcp-server-${server.server_key}`}>
      <div className="ra-mcp-card-head">
        <div>
          <span className="ra-mcp-eyebrow">{catalog?.domain || serverDomain(server)}</span>
          <h3>{mcpServerDisplay(server)}</h3>
        </div>
        <McpStatusBadge status={server.status} />
      </div>
      <p>{server.summary_zh || catalog?.summary || "该 MCP server 已登记，能力描述等待后端目录补齐。"}</p>
      <div className="ra-chip-row">
        {(aliases.length ? aliases : catalog?.aliases || []).map((alias) => <span className="ra-chip" key={alias}>{alias}</span>)}
      </div>
      {slashJoin(aliases.length ? aliases : catalog?.aliases || []) ? <p>{slashJoin(aliases.length ? aliases : catalog?.aliases || [])}</p> : null}
      <div className="ra-mcp-meta-grid">
        <span>Server</span><strong>{server.server_key}</strong>
        <span>风险/审批</span><strong>{catalog?.risk || "按工具目录判断"}</strong>
        <span>工具</span><strong>{toolNames.length ? toolNames.join(" / ") : "等待工具目录"}</strong>
      </div>
      <p className="ra-muted">默认列表仅显示概要；schema、health_json 和大 payload 留在按需详情或 artifact_ref 中。</p>
      <DetailDrawer title="按需详情：server health / catalog" data={{ server, catalog, tool_count: tools.length }} />
    </article>
  );
}

function ToolTable({ tools }: { tools: AssistantMcpTool[] }) {
  if (!tools.length) return <EmptyState title="暂无 MCP tool 目录" hint="页面请求使用 limit=50&include_schema=false；schema 只在详情中按需展开。" />;
  return (
    <div className="ra-table-wrap">
      <table className="ra-table">
        <thead>
          <tr>
            <th>工具</th>
            <th>Server</th>
            <th>风险</th>
            <th>状态</th>
            <th>审批</th>
            <th>详情</th>
          </tr>
        </thead>
        <tbody>
          {tools.map((tool) => (
            <tr key={tool.tool_id || `${tool.server_key}-${tool.tool_name}`}>
              <td><strong>{isLocalDataManagementTool(tool) ? localDataToolTitle(tool) : tool.title || tool.tool_name}</strong><br /><span className="ra-muted">{tool.description || tool.tool_name}</span></td>
              <td><span className="ra-mono">{tool.server_key}</span></td>
              <td><StatusPill status={tool.risk_level || "unknown"} /></td>
              <td><McpStatusBadge status={tool.status} /></td>
              <td>{tool.requires_approval ? "需要" : "不需要"}</td>
              <td><DetailDrawer title="schema / preflight / confirmations" data={tool} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function ResearchAssistantMcpToolsPage() {
  const [servers, setServers] = useState<AssistantMcpServer[]>([]);
  const [tools, setTools] = useState<AssistantMcpTool[]>([]);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [serverPage, toolPage] = await Promise.all([
        researchAssistantApi.mcpServers(),
        researchAssistantApi.mcpTools({ limit: 50, include_schema: false }),
      ]);
      setServers(serverPage.items);
      setTools(toolPage.items);
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const localDataTools = useMemo(() => tools.filter(isLocalDataManagementTool), [tools]);
  const visibleServers = useMemo(() => mergedServers(servers), [servers]);

  return (
    <main className="ra-mcp-page" data-testid="ra-mcp-tools-page">
      <ApiErrorBox error={error} />
      <section className="ra-mcp-hero">
        <span className="ra-mcp-eyebrow">MCP Capability Catalog</span>
        <h2>助手会自己选择工具，用户只需要描述任务</h2>
        <p>
          这里展示 Research Assistant 可理解的 MCP 能力地图。列表请求固定为 compact 模式：
          <span className="ra-mono"> limit=50&include_schema=false</span>，大字段、schema、矩阵和原始 payload 只在按需详情或 artifact_ref 中出现。
        </p>
        <div className="ra-chip-row">
          <span className="ra-chip">自然语言路由</span>
          <span className="ra-chip">summary-first</span>
          <span className="ra-chip">确认后才执行写操作</span>
          <span className="ra-chip">统一 MCP Gateway</span>
        </div>
      </section>

      <section className="ra-mcp-section">
        <div className="ra-mcp-section-head">
          <div>
            <span className="ra-mcp-eyebrow">business servers</span>
            <h2>业务 MCP 能力总览</h2>
          </div>
          <StatusPill status="ready">已登记 {servers.length} / 能力地图 {visibleServers.length}</StatusPill>
        </div>
        <div className="ra-mcp-grid">
          {visibleServers.map((server) => <ServerCard key={server.server_key} server={server} tools={toolsForServer(tools, server.server_key)} />)}
        </div>
      </section>

      <section className="ra-mcp-section">
        <div className="ra-mcp-section-head">
          <div>
            <span className="ra-mcp-eyebrow">local data workflow</span>
            <h2>{LOCAL_DATA_MANAGEMENT_CAPABILITY.displayName}</h2>
          </div>
          <StatusPill status={localDataTools.length ? "ready" : "missing"}>{localDataTools.length ? `发现 ${localDataTools.length} 个工具` : "等待目录"}</StatusPill>
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
                <span className="ra-chip">{phase.requiresConfirmation ? "需要确认" : "无需确认"}</span>
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
                  <p>{phase?.description || tool.description || "本地数据管理工具；schema、确认口令和 trace 保留在审计详情中。"}</p>
                  <div className="ra-chip-row">
                    <span className="ra-chip">{phase?.title || "未分配阶段"}</span>
                    <span className="ra-chip">{localDataRiskLabel(tool.risk_level || phase?.riskLevel)}</span>
                    <span className="ra-chip">{tool.requires_approval || phase?.requiresConfirmation ? "需要确认" : "无需确认"}</span>
                  </div>
                  <DetailDrawer title="审计详情：工具 schema / confirmations" data={tool} />
                </article>
              );
            })}
          </div>
        ) : (
          <EmptyState title="尚无本地数据工具" hint="等待后端将 local_data_management 写入 Capability Registry 和 MCP Tool Catalog。" />
        )}
      </section>

      <section className="ra-mcp-section">
        <div className="ra-mcp-section-head">
          <div>
            <span className="ra-mcp-eyebrow">compact tool list</span>
            <h2>MCP Tools</h2>
          </div>
          <StatusPill status="ready">最多显示 50 个概要</StatusPill>
        </div>
        <ToolTable tools={tools} />
      </section>
    </main>
  );
}
