"use client";

import { useCallback, useEffect, useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantModelProfile, type AssistantRoutingPolicy } from "@/lib/research-assistant/api";

export default function ResearchAssistantModelsPage() {
  const [profiles, setProfiles] = useState<AssistantModelProfile[]>([]);
  const [policies, setPolicies] = useState<AssistantRoutingPolicy[]>([]);
  const [routeResult, setRouteResult] = useState<unknown>(null);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [profilePage, policyPage] = await Promise.all([researchAssistantApi.modelProfiles(), researchAssistantApi.routingPolicies()]);
      setProfiles(profilePage.items);
      setPolicies(policyPage.items);
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  async function routeCheapWorker() {
    setRouteResult(await researchAssistantApi.routeModel({ role: "cheap_worker", risk_level: "low", token_estimate: 1200 }));
  }

  return (
    <main>
      <ApiErrorBox error={error} />
      <SectionCard title="模型配置与路由" eyebrow="primary / cheap worker / temp memory">
        <button className="pv2-button-primary" type="button" onClick={() => void routeCheapWorker()}>测试低价模型路由</button>
        {routeResult ? <DetailDrawer title="路由结果" data={routeResult} /> : null}
        <PaperTable
          rows={profiles}
          empty="暂无模型 profile。"
          columns={[
            { key: "model", header: "模型", render: (row) => <><span className="ra-title">{row.provider} / {row.model_name}</span><br /><span className="pv2-muted pv2-mono">{row.model_profile_id}</span></> },
            { key: "role", header: "角色", render: (row) => row.role || "-" },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "detail", header: "能力/限制", render: (row) => <DetailDrawer title="capabilities / cost / limits" data={row} /> },
          ]}
        />
        {!profiles.length ? <EmptyState title="模型目录为空" /> : null}
      </SectionCard>
      <SectionCard title="路由策略" eyebrow="risk aware routing">
        <PaperTable
          rows={policies}
          empty="暂无路由策略。"
          columns={[
            { key: "policy", header: "策略", render: (row) => <span className="pv2-mono">{row.policy_id}</span> },
            { key: "role", header: "角色", render: (row) => row.role || "-" },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
          ]}
        />
        <JsonPanel value={{ rule: "低价模型只能写临时记忆；主模型审核后才能提升长期记忆。", phase1: "不直接调用外部 LLM，只登记路由契约和 trace 字段。" }} />
      </SectionCard>
    </main>
  );
}
