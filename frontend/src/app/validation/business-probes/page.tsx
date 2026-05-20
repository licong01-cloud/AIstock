"use client";

import { useCallback, useEffect, useState } from "react";

import {
  BusinessProbeFlow,
  CleanupRiskPanel,
  EvidenceDrawer,
  ExecutionTimeline,
  KeyValue,
  ModuleResultCard,
  display,
} from "@/components/validation/discovery/ActiveDiscoveryComponents";
import { emptyPage, errorMessage, loadDiscoveryEvidence } from "@/components/validation/discovery/pageUtils";
import {
  type ValidationDiscoveryEvidenceManifest,
  type ValidationDiscoveryNightlyReport,
  type ValidationDiscoveryTask,
  validationApi,
} from "@/lib/validation/api";

export default function BusinessProbesPage() {
  const [report, setReport] = useState<ValidationDiscoveryNightlyReport | null>(null);
  const [tasks, setTasks] = useState<ValidationDiscoveryTask[]>([]);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
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
      const [detail, taskPage] = await Promise.all([
        validationApi.discoveryNightlyReport("current"),
        validationApi.discoveryTasks({ page_size: 100 }),
      ]);
      setReport(detail);
      setTasks(taskPage.items);
    } catch (err) {
      setError(errorMessage(err));
      setReport(null);
      setTasks(emptyPage<ValidationDiscoveryTask>(100).items);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const openEvidence = async (id?: string) => {
    setDrawerOpen(true);
    setDrawerTitle(id || "业务探针证据");
    setEvidence(null);
    setEvidenceError(null);
    try {
      setEvidence(await loadDiscoveryEvidence(id));
    } catch (err) {
      setEvidenceError(errorMessage(err));
    }
  };

  const selectedModule = report?.modules?.find((module) => module.module_id === selectedNode || (selectedNode === "qe_archive" && module.module_id === "qe"));

  return (
    <>
      <section className="pv2-card">
        <div className="pv2-card-head"><div><div className="pv2-eyebrow">Business Probe</div><h2>业务链路探针</h2><p className="pv2-muted">React Flow 节点颜色来自真实 report/module/task 数据；点击节点查看对应测试、Issue 和证据。</p></div><button className="pv2-button-primary" disabled={loading} onClick={refresh} type="button">{loading ? "刷新中" : "刷新探针"}</button></div>
        {error ? <div className="pv2-notice pv2-notice-warning">{error}</div> : null}
        <KeyValue rows={[["风险等级", "L3 只读、L4 最小写入、L5 生产相邻长流程"], ["安全约束", "L4/L5 不允许无确认启动；所有资源必须绑定 validation_run_id"], ["当前报告", report?.report_id], ["整体状态", report?.run?.status]]} />
      </section>
      <BusinessProbeFlow report={report} onOpenNode={setSelectedNode} onOpenEvidence={openEvidence} />
      {selectedModule ? <section className="pv2-card"><div className="pv2-card-head"><div><div className="pv2-eyebrow">Node Detail</div><h2>{display(selectedModule.display_name || selectedModule.module_id)}</h2></div></div><ModuleResultCard module={selectedModule} onOpenEvidence={openEvidence} /></section> : null}
      <ExecutionTimeline nodes={report?.execution_tree} onOpenEvidence={openEvidence} />
      <CleanupRiskPanel tasks={tasks} cleanup={report?.cleanup} />
      <EvidenceDrawer open={drawerOpen} title={drawerTitle} manifest={evidence} loading={drawerOpen && !evidence && !evidenceError} error={evidenceError} onClose={() => setDrawerOpen(false)} />
    </>
  );
}
