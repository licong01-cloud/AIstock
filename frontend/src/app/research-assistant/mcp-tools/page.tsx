"use client";

import { useCallback, useEffect, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantMcpServer, type AssistantMcpTool } from "@/lib/research-assistant/api";

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

  return (
    <main>
      <ApiErrorBox error={error} />
      <SectionCard title="MCP Servers" eyebrow="health / transport">
        <PaperTable
          rows={servers}
          empty="暂无 MCP server 目录。"
          columns={[
            { key: "server", header: "Server", render: (row) => <><span className="ra-title">{row.title}</span><br /><span className="pv2-muted pv2-mono">{row.server_key}</span></> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
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
            { key: "tool", header: "工具", render: (row) => <><span className="ra-title">{row.title || row.tool_name}</span><br /><span className="pv2-muted pv2-mono">{row.server_key}/{row.tool_name}</span></> },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "approval", header: "审批", render: (row) => row.requires_approval ? "需要" : "不需要" },
            { key: "schema", header: "Schema", render: (row) => <DetailDrawer title="input / preflight / confirmations" data={row} /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
