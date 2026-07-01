"use client";

import { useCallback, useEffect, useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import { ApiErrorBox, DetailDrawer, EmptyState } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantGraphSummary, type JsonObject } from "@/lib/research-assistant/api";
import GraphFlowView from "./GraphFlowView";

export default function ResearchAssistantGraphPage() {
  const [graph, setGraph] = useState<AssistantGraphSummary | null>(null);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      setGraph(await researchAssistantApi.graphSummary("aistock"));
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const entities = (graph?.entities || []) as JsonObject[];
  const relations = (graph?.relations || []) as JsonObject[];
  const paths = (graph?.evolution_paths || []) as JsonObject[];

  return (
    <main>
      <ApiErrorBox error={error} />
      <div className="pv2-grid pv2-grid-3">
        <MetricCard label="实体" value={graph?.entity_count || 0} hint="research_memory_entities" tone="info" />
        <MetricCard label="关系" value={graph?.relation_count || 0} hint="research_memory_relations" tone="info" />
        <MetricCard label="演进路径" value={graph?.evolution_path_count || 0} hint="research_evolution_paths" tone="info" />
      </div>

      <GraphFlowView graph={graph} />

      <SectionCard title="轻量知识图谱" eyebrow="native tables / read-only visualization">
        <JsonPanel value={{ boundary: "本页只读展示 AIstock 原生图谱表；React Flow 拖动只保存本地布局，不改变实体、关系或演进路径。", namespace: graph?.namespace || "aistock" }} />
      </SectionCard>

      <SectionCard title="实体" eyebrow="module / task / paper / experiment">
        <PaperTable
          rows={entities}
          empty="暂无实体。"
          columns={[
            { key: "entity", header: "实体", render: (row) => <><span className="ra-title">{String(row.title || row.entity_key || "-")}</span><br /><span className="pv2-muted">{String(row.entity_type || "-")}</span></> },
            { key: "summary", header: "摘要", render: (row) => String(row.summary || "-") },
            { key: "detail", header: "证据", render: (row) => <DetailDrawer title="entity detail" data={row} /> },
          ]}
        />
        {!entities.length ? <EmptyState title="图谱实体为空" hint="后续研究流会逐步写入模块、实验、论文、因子、演进路径实体。" /> : null}
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="关系" eyebrow="evidence bound">
          <JsonPanel value={relations} />
        </SectionCard>
        <SectionCard title="演进路径" eyebrow="research evolution">
          <JsonPanel value={paths} />
        </SectionCard>
      </div>
    </main>
  );
}
