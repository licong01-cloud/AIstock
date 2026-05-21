"use client";

import { useCallback, useEffect, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, formatDateTime } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantSkill, type AssistantSkillUsageEvent } from "@/lib/research-assistant/api";

export default function ResearchAssistantSkillsPage() {
  const [skills, setSkills] = useState<AssistantSkill[]>([]);
  const [usageEvents, setUsageEvents] = useState<AssistantSkillUsageEvent[]>([]);
  const [error, setError] = useState<unknown>(null);
  const [busySkillKey, setBusySkillKey] = useState<string | null>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [skillPage, usagePage] = await Promise.all([researchAssistantApi.skills(), researchAssistantApi.skillUsageEvents({ limit: 50 })]);
      setSkills(skillPage.items);
      setUsageEvents(usagePage.items);
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  async function setSkillEnabled(skillKey: string, enabled: boolean) {
    setBusySkillKey(skillKey);
    setError(null);
    try {
      if (enabled) await researchAssistantApi.enableSkill(skillKey);
      else await researchAssistantApi.disableSkill(skillKey);
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusySkillKey(null);
    }
  }

  return (
    <main>
      <ApiErrorBox error={error} />
      <SectionCard title="本地 Skill Catalog" eyebrow="local only / no marketplace">
        <PaperTable
          rows={skills}
          empty="暂无 Skill 记录。"
          columns={[
            { key: "skill", header: "Skill", render: (row) => <><span className="ra-title">{row.title || row.skill_key}</span><br /><span className="pv2-muted">{row.description}</span><br /><span className="pv2-mono">{row.skill_key}</span></> },
            { key: "domain", header: "领域", render: (row) => row.domain || "-" },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "permission", header: "权限", render: (row) => row.permission_scope || "-" },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "action", header: "操作", render: (row) => {
              const key = row.skill_key;
              const approved = row.status === "approved" || row.status === "enabled";
              return <button className="pv2-button-ghost" type="button" disabled={!key || busySkillKey === key} onClick={() => void setSkillEnabled(key, !approved)}>{approved ? "停用" : "启用"}</button>;
            } },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="checksum / schema / tags" data={row} /> },
          ]}
        />
        {!skills.length ? <EmptyState title="Skill Catalog 为空" hint="目录应展示 QE 诊断、因子分析和 RDAgent 等本地 Skill 能力。" /> : null}
      </SectionCard>
      <SectionCard title="Skill Usage Trace" eyebrow="real usage events">
        <PaperTable
          rows={usageEvents}
          empty="暂无 Skill usage trace。"
          columns={[
            { key: "skill", header: "Skill", render: (row) => <><span className="ra-title">{row.skill_key || row.skill_id || row.skill_event_id}</span><br /><span className="pv2-muted">{row.status || "-"}</span></> },
            { key: "task", header: "任务", render: (row) => row.task_id || "-" },
            { key: "time", header: "时间", render: (row) => formatDateTime(row.created_at || row.completed_at || row.started_at) },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="skill event payload" data={row} /> },
          ]}
        />
        {!usageEvents.length ? <EmptyState title="Skill Usage Trace 为空" hint="助理通过 Skill 执行分析后，会在这里留下可回放轨迹。" /> : null}
      </SectionCard>
    </main>
  );
}
