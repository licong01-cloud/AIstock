"use client";

import { useCallback, useEffect, useState } from "react";

import {
  AgentTaskPanel,
  EvidenceDrawer,
  KeyValue,
  display,
} from "@/components/validation/discovery/ActiveDiscoveryComponents";
import { emptyPage, errorMessage, loadDiscoveryEvidence } from "@/components/validation/discovery/pageUtils";
import {
  type ValidationDiscoveryEvidenceManifest,
  type ValidationDiscoveryTask,
  type ValidationPage,
  validationApi,
} from "@/lib/validation/api";

const PAGE_SIZE = 20;

export default function DiscoveryTasksPage() {
  const [tasks, setTasks] = useState<ValidationPage<ValidationDiscoveryTask>>(emptyPage(PAGE_SIZE));
  const [source, setSource] = useState("");
  const [status, setStatus] = useState("");
  const [page, setPage] = useState(1);
  const [form, setForm] = useState({ title: "", module: "validation", detector: "contract_alignment_adapter", risk: "L2", reason: "" });
  const [evidence, setEvidence] = useState<ValidationDiscoveryEvidenceManifest | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerTitle, setDrawerTitle] = useState<string | undefined>();
  const [loading, setLoading] = useState(true);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [evidenceError, setEvidenceError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setTasks(await validationApi.discoveryTasks({ source, status, page, page_size: PAGE_SIZE }));
    } catch (err) {
      setError(errorMessage(err));
      setTasks(emptyPage(PAGE_SIZE));
    } finally {
      setLoading(false);
    }
  }, [page, source, status]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const schedule = async () => {
    const confirm_schedule = ["L4", "L5"].includes(form.risk) ? window.prompt(`请输入 ${form.risk} 确认创建写入型任务`, form.risk) || undefined : undefined;
    try {
      const task = await validationApi.scheduleDiscoveryTask({
        title: form.title || `手工探测 ${form.module}`,
        source: "manual_mcp",
        module: form.module,
        risk_level: form.risk,
        detectors: form.detector.split(",").map((item) => item.trim()).filter(Boolean),
        reason: form.reason || "manual discovery task from UI",
        cleanup_required: ["L4", "L5"].includes(form.risk),
        confirm_schedule,
      });
      setMessage(`任务已创建：${task.task_id}`);
      await refresh();
    } catch (err) {
      setMessage(`创建失败：${errorMessage(err)}`);
    }
  };

  const runTask = async (task: ValidationDiscoveryTask) => {
    const confirm_run = ["L4", "L5"].includes(String(task.risk_level)) ? window.prompt(`请输入 ${task.task_id} 确认运行`, task.task_id) || undefined : undefined;
    try {
      const result = await validationApi.runDiscoveryTask(task.task_id, { dry_run: true, confirm_run });
      setMessage(`dry-run 完成：${display(result.result || result.task_id)}`);
      await refresh();
    } catch (err) {
      setMessage(`运行失败：${errorMessage(err)}`);
    }
  };

  const claimTask = async (task: ValidationDiscoveryTask) => {
    try {
      const result = await validationApi.claimDiscoveryAgentTask(task.task_id, { agent_runtime: "codex", agent_name: "codex-app", workspace: "current isolated worktree" });
      setMessage(`Agent 已 claim：${display(result.task_id)} / ${display(result.status)}`);
      await refresh();
    } catch (err) {
      setMessage(`claim 失败：${errorMessage(err)}`);
    }
  };

  const cancelTask = async (task: ValidationDiscoveryTask) => {
    try {
      await validationApi.cancelDiscoveryTask(task.task_id, "cancelled from active discovery UI");
      setMessage(`任务已取消：${task.task_id}。证据不会删除。`);
      await refresh();
    } catch (err) {
      setMessage(`取消失败：${errorMessage(err)}`);
    }
  };

  const openEvidence = async (id?: string) => {
    setDrawerOpen(true);
    setDrawerTitle(id);
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
      <section className="pv2-card">
        <div className="pv2-card-head"><div><div className="pv2-eyebrow">Manual MCP Task</div><h2>创建专项探测任务</h2><p className="pv2-muted">L4/L5 或写入型任务必须二次确认并绑定 cleanup；本页面默认提交 dry-run。</p></div><button className="pv2-button-primary" onClick={refresh} disabled={loading} type="button">刷新任务</button></div>
        {error ? <div className="pv2-notice pv2-notice-warning">{error}</div> : null}
        {message ? <div className="pv2-notice pv2-notice-info">{message}</div> : null}
        <div className="disc-filter-grid">
          <input className="pv2-input" placeholder="任务标题" value={form.title} onChange={(event) => setForm({ ...form, title: event.target.value })} />
          <input className="pv2-input" placeholder="模块" value={form.module} onChange={(event) => setForm({ ...form, module: event.target.value })} />
          <input className="pv2-input" placeholder="detectors，逗号分隔" value={form.detector} onChange={(event) => setForm({ ...form, detector: event.target.value })} />
          <select className="pv2-select" value={form.risk} onChange={(event) => setForm({ ...form, risk: event.target.value })}><option value="L2">L2 只读</option><option value="L3">L3 真实后端</option><option value="L4">L4 最小写入</option><option value="L5">L5 生产相邻长流程</option></select>
          <button className="pv2-button-primary" onClick={schedule} type="button">创建任务</button>
        </div>
        <KeyValue rows={[["三类任务", "nightly_baseline / change_driven / manual_mcp 均可过滤"], ["Agent 状态", "claim / running / submitted / completed 可追踪"], ["安全约束", "取消不删除证据；重跑生成新 run/result 记录"]]} />
      </section>
      <AgentTaskPanel tasks={tasks} sourceFilter={source} statusFilter={status} onSourceFilter={(value) => { setSource(value); setPage(1); }} onStatusFilter={(value) => { setStatus(value); setPage(1); }} onClaim={claimTask} onRun={runTask} onCancel={cancelTask} onOpenEvidence={openEvidence} />
      <div className="pv2-pagination"><span>第 {page} 页，总数 {tasks.total}</span><button className="pv2-button-ghost" disabled={page <= 1} onClick={() => setPage(page - 1)} type="button">上一页</button><button className="pv2-button-ghost" disabled={!tasks.has_more} onClick={() => setPage(page + 1)} type="button">下一页</button></div>
      <EvidenceDrawer open={drawerOpen} title={drawerTitle} manifest={evidence} loading={drawerOpen && !evidence && !evidenceError} error={evidenceError} onClose={() => setDrawerOpen(false)} />
    </>
  );
}
