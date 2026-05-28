"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState } from "@/components/research-assistant/AssistantShared";
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

function mcpStatusLabel(status: unknown): string {
  const value = String(status || "unknown").toLowerCase();
  if (["ready", "enabled", "approved", "ok"].includes(value)) return "已就绪";
  if (["disabled", "blocked", "failed", "error"].includes(value)) return "不可用";
  if (["pending", "initializing", "unknown"].includes(value)) return "待检查";
  return String(status || "unknown");
}

function mcpStatusTone(status: unknown): "success" | "danger" | "warning" | "neutral" {
  const value = String(status || "unknown").toLowerCase();
  if (["ready", "enabled", "approved", "ok"].includes(value)) return "success";
  if (["disabled", "blocked", "failed", "error"].includes(value)) return "danger";
  if (["pending", "initializing", "unknown"].includes(value)) return "warning";
  return "neutral";
}

function McpStatusBadge({ status }: { status: unknown }) {
  const tone = mcpStatusTone(status);
  return <span className={`pv2-badge pv2-badge-${tone}`} title={String(status || "unknown")}>{mcpStatusLabel(status)}</span>;
}


function mcpServerDisplay(server: AssistantMcpServer): string {
  return server.display_name_zh || server.display_title || server.title || server.server_key;
}

function mcpServerAliases(server: AssistantMcpServer): string {
  const aliases = Array.isArray(server.business_aliases_zh) ? server.business_aliases_zh : [];
  return aliases.slice(0, 4).join(" / ");
}

export default function ResearchAssistantMcpToolsPage() {
  const [servers, setServers] = useState<AssistantMcpServer[]>([]);
  const [tools, setTools] = useState<AssistantMcpTool[]>([]);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [serverPage, toolPage] = await Promise.all([researchAssistantApi.mcpServers(), researchAssistantApi.mcpTools({ limit: 200 })]);
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
  const localDataServer = servers.find((server) => server.server_key === LOCAL_DATA_MANAGEMENT_CAPABILITY.gatewayModule || server.server_key.includes("gateway"));

  return (
    <main>
      <ApiErrorBox error={error} />
      <SectionCard title="local_data_management 能力" eyebrow="readable capability card">
        <div className="pv2-readable-panel" data-testid="ra-mcp-local-data-capability">
          <div className="pv2-readable-table">
            <div className="pv2-readable-row"><div className="pv2-readable-key">能力名称</div><div className="pv2-readable-value">{LOCAL_DATA_MANAGEMENT_CAPABILITY.displayName}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">Capability key</div><div className="pv2-readable-value pv2-mono">{LOCAL_DATA_MANAGEMENT_CAPABILITY.capabilityKey}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">Gateway module</div><div className="pv2-readable-value pv2-mono">{LOCAL_DATA_MANAGEMENT_CAPABILITY.gatewayModule}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">目录状态</div><div className="pv2-readable-value">{localDataTools.length ? `已发现 ${localDataTools.length} 个本地数据 MCP 工具` : "尚未发现 local_data 工具；不会用假目录冒充可执行能力"}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">服务</div><div className="pv2-readable-value">{localDataServer?.title || localDataServer?.server_key || "等待 MCP server catalog 返回"}</div></div>
          </div>
        </div>
        <div className="pv2-readable-list" style={{ marginTop: 12 }}>
          {LOCAL_DATA_MANAGEMENT_PHASES.map((phase) => (
            <div className="pv2-readable-item" key={phase.key}>
              <strong>{phase.title}</strong>
              <p className="pv2-muted">{phase.description}</p>
              <span className="pv2-chip">{localDataRiskLabel(phase.riskLevel)}</span>
              <span className="pv2-chip">{phase.requiresConfirmation ? "需要确认" : "无需确认"}</span>
              <span className="pv2-chip">{phase.primaryTools.length} 个核心工具</span>
            </div>
          ))}
        </div>
      </SectionCard>
      <SectionCard title="本地数据 MCP 工具卡片" eyebrow="check / plan / confirm / execute / review">
        {localDataTools.length ? (
          <div className="pv2-readable-list" data-testid="ra-mcp-local-data-tool-cards">
            {localDataTools.map((tool) => {
              const phase = localDataToolPhase(tool.tool_name);
              return (
                <div className="pv2-readable-item" key={tool.tool_id}>
                  <strong>{localDataToolTitle(tool)}</strong>
                  <p className="pv2-muted">{phase?.description || tool.description || "本地数据管理工具；schema、确认口令和 trace 保留在审计详情中。"}</p>
                  <span className="pv2-chip">{phase?.title || "未分配阶段"}</span>
                  <span className="pv2-chip">{localDataRiskLabel(tool.risk_level || phase?.riskLevel)}</span>
                  <span className="pv2-chip">{tool.requires_approval || phase?.requiresConfirmation ? "需要确认" : "无需确认"}</span>
                  <DetailDrawer title="审计详情：工具 schema / confirmations" data={tool} />
                </div>
              );
            })}
          </div>
        ) : (
          <EmptyState title="尚无本地数据工具" hint="等待后端将 local_data_management 写入 Capability Registry 和 MCP Tool Catalog。" />
        )}
      </SectionCard>
      <SectionCard title="MCP Servers" eyebrow="health / transport">
        <PaperTable
          rows={servers}
          empty="暂无 MCP server 目录。"
          columns={[
            { key: "server", header: "Server", render: (row) => <><span className="ra-title">{mcpServerDisplay(row)}</span><br /><span className="pv2-muted">{mcpServerAliases(row) || row.title}</span><br /><span className="pv2-muted pv2-mono">{row.server_key}</span></> },
            { key: "status", header: "状态", render: (row) => <McpStatusBadge status={row.status} /> },
            { key: "health", header: "健康详情", render: (row) => <DetailDrawer title="health_json" data={row.health_json || row} /> },
          ]}
        />
        {!servers.length ? <EmptyState title="Server 目录为空" hint="请通过 /catalogs/seed 写入真实目录；页面不使用静态假目录。" /> : null}
      </SectionCard>
      <SectionCard title="MCP Tools" eyebrow="schema / risk / preflight">
        <PaperTable
          rows={tools}
          empty="暂无 MCP tool 目录。"
          columns={[
            { key: "tool", header: "工具", render: (row) => <><span className="ra-title">{isLocalDataManagementTool(row) ? localDataToolTitle(row) : row.title || row.tool_name}</span><br /><span className="pv2-muted pv2-mono">{row.server_key}/{row.tool_name}</span></> },
            { key: "phase", header: "本地数据阶段", render: (row) => localDataToolPhase(row.tool_name)?.title || "-" },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "status", header: "状态", render: (row) => <McpStatusBadge status={row.status} /> },
            { key: "approval", header: "审批", render: (row) => row.requires_approval ? "需要" : "不需要" },
            { key: "schema", header: "审计详情", render: (row) => <DetailDrawer title="input / preflight / confirmations" data={row} /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
