"use client";

import { useCallback, useEffect, useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, KeyValueGrid } from "@/components/research-assistant/AssistantShared";
import { API_BASE, researchAssistantApi, type AssistantHealth, type AssistantMcpServer, type AssistantMcpTool, type AssistantSkill } from "@/lib/research-assistant/api";

export default function ResearchAssistantSettingsPage() {
  const [health, setHealth] = useState<AssistantHealth | null>(null);
  const [servers, setServers] = useState<AssistantMcpServer[]>([]);
  const [tools, setTools] = useState<AssistantMcpTool[]>([]);
  const [skills, setSkills] = useState<AssistantSkill[]>([]);
  const [seedResult, setSeedResult] = useState<unknown>(null);
  const [error, setError] = useState<unknown>(null);
  const [actionError, setActionError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [nextHealth, serverPage, toolPage, skillPage] = await Promise.all([researchAssistantApi.health(), researchAssistantApi.mcpServers(), researchAssistantApi.mcpTools({ limit: 200 }), researchAssistantApi.skills()]);
      setHealth(nextHealth);
      setServers(serverPage.items);
      setTools(toolPage.items);
      setSkills(skillPage.items);
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  async function seedCatalogs() {
    setActionError(null);
    try {
      const result = await researchAssistantApi.seedCatalogs();
      setSeedResult(result);
      await load();
    } catch (exc) {
      setActionError(exc);
    }
  }

  return (
    <main>
      <ApiErrorBox error={error} />
      <ApiErrorBox error={actionError} title="Catalog seed 失败" />
      <SectionCard title="研究助理设置" eyebrow="real health / catalog">
        <KeyValueGrid rows={[
          ["API Base", `${API_BASE}/research-assistant`],
          ["Service Status", health?.status || "unknown"],
          ["Repository", health?.repository || {}],
          ["Runtime Boundaries", health?.runtime_boundaries || {}],
        ]} />
        <div className="pv2-row-actions" style={{ marginTop: 12 }}>
          <button className="pv2-button-ghost" type="button" onClick={() => void load()}>刷新 health/catalog</button>
          <button className="pv2-button-primary" type="button" onClick={() => void seedCatalogs()}>调用真实 catalog seed API</button>
        </div>
        {seedResult ? <DetailDrawer title="Catalog seed 返回" data={seedResult} /> : null}
      </SectionCard>
      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="MCP Server Catalog" eyebrow="settings source">
          <PaperTable
            rows={servers}
            empty="暂无 MCP server catalog。"
            columns={[
              { key: "server", header: "Server", render: (row) => <><span className="ra-title">{row.title || row.server_key}</span><br /><span className="pv2-muted pv2-mono">{row.server_key}</span></> },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
              { key: "detail", header: "详情", render: (row) => <DetailDrawer title="health_json" data={row.health_json || row} /> },
            ]}
          />
          {!servers.length ? <EmptyState title="Server catalog 为空" /> : null}
        </SectionCard>
        <SectionCard title="Capability Boundary" eyebrow="catalog counts">
          <JsonPanel value={{ server_count: servers.length, tool_count: tools.length, skill_count: skills.length, health }} />
        </SectionCard>
      </div>
    </main>
  );
}
