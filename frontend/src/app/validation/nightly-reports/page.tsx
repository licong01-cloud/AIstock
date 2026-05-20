"use client";

import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";

import {
  CandidateGroupLinks,
  CleanupRiskPanel,
  EvidenceDrawer,
  ExecutionTimeline,
  LlmReportPanel,
  MetricSummaryCard,
  ModuleResultCard,
  NightlyRunHeader,
  display,
} from "@/components/validation/discovery/ActiveDiscoveryComponents";
import { emptyPage, errorMessage, loadDiscoveryEvidence } from "@/components/validation/discovery/pageUtils";
import {
  type ValidationDiscoveryEvidenceManifest,
  type ValidationDiscoveryLlmProfile,
  type ValidationDiscoveryLlmReport,
  type ValidationDiscoveryNightlyReport,
  type ValidationDiscoveryNightlyReportSummary,
  type ValidationDiscoveryTask,
  validationApi,
} from "@/lib/validation/api";

export default function NightlyReportsPage() {
  const router = useRouter();
  const [reports, setReports] = useState<ValidationDiscoveryNightlyReportSummary[]>([]);
  const [reportId, setReportId] = useState("current");
  const [report, setReport] = useState<ValidationDiscoveryNightlyReport | null>(null);
  const [llmReport, setLlmReport] = useState<ValidationDiscoveryLlmReport | null>(null);
  const [profiles, setProfiles] = useState<ValidationDiscoveryLlmProfile[]>([]);
  const [tasks, setTasks] = useState<ValidationDiscoveryTask[]>([]);
  const [evidence, setEvidence] = useState<ValidationDiscoveryEvidenceManifest | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerTitle, setDrawerTitle] = useState<string | undefined>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [evidenceError, setEvidenceError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [reportPage, detail, llm, profilePage, taskPage] = await Promise.all([
        validationApi.discoveryNightlyReports({ limit: 7 }),
        validationApi.discoveryNightlyReport(reportId),
        validationApi.discoveryNightlyLlmReport(reportId),
        validationApi.discoveryLlmProfiles(),
        validationApi.discoveryTasks({ page_size: 100 }),
      ]);
      setReports(reportPage.items);
      setReport(detail);
      setLlmReport(llm);
      setProfiles(profilePage.items);
      setTasks(taskPage.items);
    } catch (err) {
      setError(errorMessage(err));
      setReports([]);
      setReport(null);
      setLlmReport(null);
      setProfiles([]);
      setTasks(emptyPage<ValidationDiscoveryTask>(100).items);
    } finally {
      setLoading(false);
    }
  }, [reportId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const openEvidence = async (id?: string) => {
    setDrawerTitle(id || "证据包");
    setDrawerOpen(true);
    setEvidence(null);
    setEvidenceError(null);
    try {
      setEvidence(await loadDiscoveryEvidence(id));
    } catch (err) {
      setEvidenceError(errorMessage(err));
    }
  };

  return (
    <>
      {error ? <div className="pv2-error-panel"><div className="pv2-error-kicker">加载失败</div><div className="pv2-error-main">{error}</div></div> : null}
      <NightlyRunHeader reports={reports} selectedReportId={reportId} report={report} loading={loading} onSelectReport={setReportId} onRefresh={refresh} />
      <section className="pv2-grid pv2-grid-4">
        {(report?.summary_cards || []).map((card) => <MetricSummaryCard card={card} key={card.card_id} onClick={() => router.push("/validation/discovery-candidates")} />)}
        {!(report?.summary_cards || []).length && loading ? <div className="pv2-notice pv2-notice-info">正在加载夜间汇报卡片...</div> : null}
      </section>
      <section className="pv2-card">
        <div className="pv2-card-head"><div><div className="pv2-eyebrow">Module Results</div><h2>模块级测试与 Issue 详情</h2><p className="pv2-muted">每个模块独立展示覆盖率、测试、候选、Issue 和失败任务；点击展开查看明细。</p></div></div>
        <div className="disc-profile-grid">
          {(report?.modules || []).map((module) => <ModuleResultCard key={module.module_id} module={module} onOpenEvidence={openEvidence} onFilterCandidates={(moduleId) => router.push(`/validation/discovery-candidates?module=${encodeURIComponent(moduleId)}`)} />)}
        </div>
      </section>
      <ExecutionTimeline nodes={report?.execution_tree} onOpenEvidence={openEvidence} />
      <LlmReportPanel profiles={profiles} report={llmReport} onRunEval={() => router.push("/validation/discovery-llm-profiles")} />
      <CandidateGroupLinks summary={report?.candidate_summary} onFilter={(status) => router.push(`/validation/discovery-candidates?review_status=${encodeURIComponent(status)}`)} />
      <section className="pv2-card">
        <div className="pv2-card-head"><div><div className="pv2-eyebrow">Evidence Bundle</div><h2>证据包入口</h2></div><button className="pv2-button-ghost" onClick={() => openEvidence(report?.evidence_manifest_id)} type="button">打开当前报告证据</button></div>
        <div className="pv2-readable-list"><div className="pv2-readable-item">日志、API/MCP 响应、截图/trace、DB 查询结果、回放命令统一进入 EvidenceDrawer。当前 evidence_manifest_id：<span className="pv2-mono">{display(report?.evidence_manifest_id)}</span></div></div>
      </section>
      <CleanupRiskPanel tasks={tasks} cleanup={report?.cleanup} />
      <EvidenceDrawer open={drawerOpen} title={drawerTitle} manifest={evidence} loading={drawerOpen && !evidence && !evidenceError} error={evidenceError} onClose={() => setDrawerOpen(false)} />
    </>
  );
}
