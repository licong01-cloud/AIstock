"use client";

import React, { useEffect, useState, useCallback, useRef } from "react";
import { FactorItem, TransformJob, Stats, CodeDetail, TransformConfig } from "./components/types";
import { FactorTable } from "./components/FactorTable";
import { JobsTable } from "./components/JobsTable";
import { CodeModal } from "./components/CodeModal";
import { ProgressPanel } from "./components/ProgressPanel";
import { AgentConfigPanel } from "./components/AgentConfigPanel";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type ProgressEntry = { jobId: string; factorName: string };

export default function FactorTransformationPage() {
  const [factors, setFactors] = useState<FactorItem[]>([]);
  const [jobs, setJobs] = useState<TransformJob[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [loading, setLoading] = useState(true);
  const [jobsLoading, setJobsLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<"factors" | "jobs">("factors");
  const [statusFilter, setStatusFilter] = useState("all");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [searchText, setSearchText] = useState("");
  const [selectedFactors, setSelectedFactors] = useState<Set<string>>(new Set());
  const [availableSources, setAvailableSources] = useState<string[]>([]);
  const [codeModal, setCodeModal] = useState<{
    open: boolean;
    data: CodeDetail | null;
    tab: "original" | "realtime";
  }>({ open: false, data: null, tab: "original" });
  const [progressPanel, setProgressPanel] = useState<ProgressEntry | null>(null);
  const [pageSize, setPageSize] = useState(20);
  const [currentPage, setCurrentPage] = useState(1);
  const [cfg, setCfg] = useState<TransformConfig>({
    max_llm_retries: 3,
    test_start_date: "2023-01-01",
    test_end_date: "2023-12-31",
    llm_model_id: "",
  });
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [toastMsg, setToastMsg] = useState<{ text: string; type: "success" | "error" } | null>(null);
  const statsTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const factorsTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [showAgentConfig, setShowAgentConfig] = useState(false);

  const showToast = (text: string, type: "success" | "error" = "success") => {
    setToastMsg({ text, type });
    setTimeout(() => setToastMsg(null), 3500);
  };

  const loadStats = useCallback(async () => {
    try {
      const r = await fetch(`${API}/quantevolver/factor-transformation/stats`);
      const d = await r.json();
      if (d.ok) setStats(d.stats);
    } catch (_) { /* ignore */ }
  }, []);

  const loadSources = useCallback(async () => {
    try {
      const r = await fetch(`${API}/quantevolver/factor-transformation/sources`);
      const d = await r.json();
      if (d.ok) setAvailableSources(d.sources.map((s: { source: string }) => s.source));
    } catch (_) { /* ignore */ }
  }, []);

  const loadFactors = useCallback(async () => {
    setLoading(true);
    try {
      const p = new URLSearchParams({ limit: "2000", offset: "0" });
      if (sourceFilter !== "all") p.set("factor_source", sourceFilter);
      const r = await fetch(`${API}/quantevolver/factor-transformation/status?${p}`);
      const d = await r.json();
      if (d.ok) setFactors(d.items ?? []);
    } catch (_) {
      showToast("加载因子列表失败", "error");
    } finally {
      setLoading(false);
    }
  }, [sourceFilter]);

  const loadJobs = useCallback(async () => {
    setJobsLoading(true);
    try {
      const r = await fetch(`${API}/quantevolver/factor-transformation/jobs?limit=100`);
      const d = await r.json();
      if (d.ok) setJobs(d.items ?? []);
    } catch (_) {
      showToast("加载任务列表失败", "error");
    } finally {
      setJobsLoading(false);
    }
  }, []);

  useEffect(() => {
    loadStats();
    loadFactors();
    loadSources();
  }, [loadFactors, loadStats, loadSources]);

  useEffect(() => {
    if (activeTab === "jobs") loadJobs();
  }, [activeTab, loadJobs]);

  // 静默刷新因子数据（不触发loading状态）
  const silentLoadFactors = useCallback(async () => {
    try {
      const p = new URLSearchParams({ limit: "2000", offset: "0" });
      if (sourceFilter !== "all") p.set("factor_source", sourceFilter);
      const r = await fetch(`${API}/quantevolver/factor-transformation/status?${p}`);
      const d = await r.json();
      if (d.ok) setFactors(d.items ?? []);
    } catch (_) { /* ignore */ }
  }, [sourceFilter]);

  // 统计数据后台静默刷新（15秒）
  useEffect(() => {
    statsTimerRef.current = setInterval(loadStats, 15000);
    return () => { if (statsTimerRef.current) clearInterval(statsTimerRef.current); };
  }, [loadStats]);

  // 只在有进行中任务时才轮询因子列表（5秒），不触发loading
  useEffect(() => {
    const hasInProgress = factors.some(
      (f) => f.transformation_status &&
        !["SUCCESS", "FAILED", "PENDING"].includes(f.transformation_status as string)
    );
    if (hasInProgress) {
      factorsTimerRef.current = setInterval(silentLoadFactors, 5000);
    } else {
      if (factorsTimerRef.current) { clearInterval(factorsTimerRef.current); factorsTimerRef.current = null; }
    }
    return () => { if (factorsTimerRef.current) clearInterval(factorsTimerRef.current); };
  }, [factors, silentLoadFactors]);

  const doTransform = async (factorName: string, source: string) => {
    setActionLoading(factorName);
    try {
      const r = await fetch(`${API}/quantevolver/factor-transformation/transform`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          factor_name: factorName,
          factor_source: source,
          max_llm_retries: cfg.max_llm_retries,
          test_start_date: cfg.test_start_date,
          test_end_date: cfg.test_end_date,
          llm_model_id: cfg.llm_model_id || null,
        }),
      });
      const d = await r.json();
      if (d.ok) {
        showToast(`因子 ${factorName} 改造任务已提交`);
        if (d.job_id) setProgressPanel({ jobId: d.job_id, factorName });
        setTimeout(() => silentLoadFactors(), 1500);
      } else {
        showToast(d.detail || "提交失败", "error");
      }
    } catch (_) {
      showToast("请求失败", "error");
    } finally {
      setActionLoading(null);
    }
  };

  const doBatch = async () => {
    const names = selectedFactors.size > 0 ? Array.from(selectedFactors) : undefined;
    setActionLoading("batch");
    try {
      const r = await fetch(`${API}/quantevolver/factor-transformation/batch-transform`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          factor_names: names,
          factor_source: sourceFilter !== "all" ? sourceFilter : null,
          max_llm_retries: cfg.max_llm_retries,
          llm_model_id: cfg.llm_model_id || null,
          only_pending: !names,
        }),
      });
      const d = await r.json();
      if (d.ok) {
        showToast(names ? `已提交 ${names.length} 个因子改造任务` : "批量改造任务已提交");
        setSelectedFactors(new Set());
        setTimeout(() => { silentLoadFactors(); loadStats(); }, 2000);
      } else {
        showToast(d.detail || "提交失败", "error");
      }
    } catch (_) {
      showToast("请求失败", "error");
    } finally {
      setActionLoading(null);
    }
  };

  const doReset = async (factorName: string, source: string) => {
    try {
      const r = await fetch(
        `${API}/quantevolver/factor-transformation/factor/${encodeURIComponent(factorName)}/reset?source=${source}`,
        { method: "POST" }
      );
      const d = await r.json();
      if (d.ok) { showToast(`因子 ${factorName} 已重置`); silentLoadFactors(); }
      else showToast(d.detail || "重置失败", "error");
    } catch (_) {
      showToast("请求失败", "error");
    }
  };

  const doViewCode = async (factorName: string, source: string) => {
    try {
      const r = await fetch(
        `${API}/quantevolver/factor-transformation/factor/${encodeURIComponent(factorName)}/code?source=${source}`
      );
      const d = await r.json();
      if (d.ok) setCodeModal({ open: true, data: d.factor, tab: "original" });
      else showToast("获取代码失败", "error");
    } catch (_) {
      showToast("请求失败", "error");
    }
  };

  const filtered = factors.filter((f) => {
    if (statusFilter !== "all") {
      const st = f.transformation_status ?? "PENDING";
      if (statusFilter === "no_code" && f.has_original_code) return false;
      if (statusFilter !== "no_code" && st !== statusFilter) return false;
    }
    if (searchText) {
      const keyword = searchText.toLowerCase();
      const matchName = f.factor_name.toLowerCase().includes(keyword);
      const matchSource = f.source.toLowerCase().includes(keyword);
      if (!matchName && !matchSource) return false;
    }
    return true;
  });

  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const safePage = Math.min(currentPage, totalPages);
  const pagedFactors = filtered.slice((safePage - 1) * pageSize, safePage * pageSize);

  const toggleSel = (name: string) =>
    setSelectedFactors((p) => {
      const n = new Set(p);
      if (n.has(name)) n.delete(name);
      else n.add(name);
      return n;
    });

  const toggleAll = () => {
    if (selectedFactors.size === pagedFactors.length && pagedFactors.length > 0) {
      setSelectedFactors(new Set());
    } else {
      setSelectedFactors(new Set(pagedFactors.map((f) => f.factor_name)));
    }
  };

  const statCards = stats
    ? [
        { label: "因子总数",     value: stats.total,             color: "#7c3aed" },
        { label: "有原始代码",   value: stats.has_original_code, color: "#2563eb" },
        { label: "待处理",       value: stats.pending,           color: "#6b7280" },
        { label: "进行中",       value: stats.in_progress,       color: "#0891b2" },
        { label: "改造成功",     value: stats.success,           color: "#16a34a" },
        { label: "改造失败",     value: stats.failed,            color: "#dc2626" },
        { label: "已有实时代码", value: stats.has_realtime_code, color: "#059669" },
      ]
    : [];

  return (
    <main style={{ padding: 24, minHeight: "100vh", background: "#f9fafb" }}>
      {/* Toast 通知 */}
      {toastMsg && (
        <div style={{
          position: "fixed", top: 16, right: 16, zIndex: 9999,
          padding: "10px 18px", borderRadius: 10,
          background: toastMsg.type === "success" ? "#16a34a" : "#dc2626",
          color: "#fff", fontSize: 14, fontWeight: 600,
          boxShadow: "0 4px 12px rgba(0,0,0,0.2)",
        }}>
          {toastMsg.text}
        </div>
      )}

      {/* 代码查看弹窗 */}
      {codeModal.open && codeModal.data && (
        <CodeModal
          data={codeModal.data}
          tab={codeModal.tab}
          onTabChange={(tab) => setCodeModal((m) => ({ ...m, tab }))}
          onClose={() => setCodeModal({ open: false, data: null, tab: "original" })}
        />
      )}

      {/* Agent提示词配置面板 */}
      {showAgentConfig && (
        <AgentConfigPanel onClose={() => setShowAgentConfig(false)} />
      )}

      {/* 改造进度面板 */}
      {progressPanel && (
        <ProgressPanel
          jobId={progressPanel.jobId}
          factorName={progressPanel.factorName}
          onClose={() => { setProgressPanel(null); silentLoadFactors(); loadStats(); }}
        />
      )}

      {/* 页头 */}
      <section style={{
        background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 50%, #06b6d4 100%)",
        borderRadius: 16, padding: 24, color: "#fff", marginBottom: 24,
      }}>
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between" }}>
          <div>
            <h1 style={{ margin: 0, fontSize: 26, fontWeight: 700 }}>因子代码改造</h1>
            <p style={{ marginTop: 8, opacity: 0.9, fontSize: 14, margin: "8px 0 0" }}>
              将 RDAgent 生成的因子代码转换为可直接从数据库读取实时数据的版本
            </p>
          </div>
          <button
            onClick={() => setShowAgentConfig(true)}
            style={{
              background: "rgba(255,255,255,0.15)", border: "1px solid rgba(255,255,255,0.3)",
              color: "#fff", borderRadius: 8, padding: "8px 16px",
              fontSize: 13, fontWeight: 600, cursor: "pointer", whiteSpace: "nowrap",
            }}
          >
            ⚙️ Agent 提示词配置
          </button>
        </div>
      </section>

      {/* 统计卡片 — QE总览风格 */}
      {statCards.length > 0 && (
        <div style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(130px, 1fr))",
          gap: 12, marginBottom: 20,
        }}>
          {statCards.map((item) => (
            <div key={item.label} style={{
              background: "#fff", borderRadius: 12, padding: "16px 16px 14px",
              boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
              borderLeft: `4px solid ${item.color}`,
            }}>
              <div style={{ fontSize: 26, fontWeight: 700, color: item.color }}>{item.value}</div>
              <div style={{ fontSize: 12, color: "#6b7280", marginTop: 4 }}>{item.label}</div>
            </div>
          ))}
        </div>
      )}

      {/* 改造配置面板 */}
      <div style={{
        background: "#fff", borderRadius: 12, padding: 20,
        boxShadow: "0 1px 3px rgba(0,0,0,0.08)", marginBottom: 16,
      }}>
        <h2 style={{ margin: "0 0 14px", fontSize: 15, fontWeight: 600, color: "#374151" }}>改造配置</h2>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 16, alignItems: "flex-end" }}>
          <div>
            <label style={{ display: "block", fontSize: 12, color: "#6b7280", marginBottom: 4 }}>LLM最大重试次数</label>
            <input type="number" min={0} max={10} value={cfg.max_llm_retries} title="LLM最大重试次数"
              onChange={(e) => setCfg((c) => ({ ...c, max_llm_retries: parseInt(e.target.value) || 0 }))}
              style={{ border: "1px solid #d1d5db", borderRadius: 6, padding: "6px 10px", fontSize: 13, width: 80 }} />
          </div>
          <div>
            <label style={{ display: "block", fontSize: 12, color: "#6b7280", marginBottom: 4 }}>测试开始日期</label>
            <input type="date" value={cfg.test_start_date} title="测试开始日期"
              onChange={(e) => setCfg((c) => ({ ...c, test_start_date: e.target.value }))}
              style={{ border: "1px solid #d1d5db", borderRadius: 6, padding: "6px 10px", fontSize: 13 }} />
          </div>
          <div>
            <label style={{ display: "block", fontSize: 12, color: "#6b7280", marginBottom: 4 }}>测试结束日期</label>
            <input type="date" value={cfg.test_end_date} title="测试结束日期"
              onChange={(e) => setCfg((c) => ({ ...c, test_end_date: e.target.value }))}
              style={{ border: "1px solid #d1d5db", borderRadius: 6, padding: "6px 10px", fontSize: 13 }} />
          </div>
          <div style={{ flex: 1, minWidth: 200 }}>
            <label style={{ display: "block", fontSize: 12, color: "#6b7280", marginBottom: 4 }}>LLM模型ID（留空使用默认）</label>
            <input type="text" placeholder="如: deepseek/deepseek-chat" value={cfg.llm_model_id}
              onChange={(e) => setCfg((c) => ({ ...c, llm_model_id: e.target.value }))}
              style={{ border: "1px solid #d1d5db", borderRadius: 6, padding: "6px 10px", fontSize: 13, width: "100%" }} />
          </div>
          <button onClick={doBatch} disabled={actionLoading === "batch"} style={{
            padding: "8px 18px", fontSize: 13, fontWeight: 600,
            background: actionLoading === "batch" ? "#9ca3af" : "#7c3aed",
            color: "#fff", border: "none", borderRadius: 8, cursor: "pointer", whiteSpace: "nowrap",
          }}>
            {actionLoading === "batch" ? "提交中..." : selectedFactors.size > 0 ? `批量改造 (${selectedFactors.size})` : "批量改造全部待处理"}
          </button>
        </div>
      </div>

      {/* Tab 切换 */}
      <div style={{ display: "flex", gap: 4, marginBottom: 16, background: "#f3f4f6", borderRadius: 10, padding: 4, width: "fit-content" }}>
        {(["factors", "jobs"] as const).map((tab) => (
          <button key={tab} onClick={() => setActiveTab(tab)} style={{
            padding: "8px 20px", fontSize: 13, fontWeight: 500, border: "none",
            borderRadius: 8, cursor: "pointer", transition: "all 0.15s",
            background: activeTab === tab ? "#fff" : "transparent",
            color: activeTab === tab ? "#111827" : "#6b7280",
            boxShadow: activeTab === tab ? "0 1px 3px rgba(0,0,0,0.1)" : "none",
          }}>
            {tab === "factors" ? "因子列表" : "改造任务"}
          </button>
        ))}
      </div>

      {/* 因子列表 Tab */}
      {activeTab === "factors" && (
        <FactorTable
          factors={pagedFactors} loading={loading} total={filtered.length}
          selectedFactors={selectedFactors} statusFilter={statusFilter}
          sourceFilter={sourceFilter} searchText={searchText}
          actionLoading={actionLoading} availableSources={availableSources}
          pageSize={pageSize} currentPage={safePage} totalPages={totalPages}
          onStatusFilterChange={(v) => { setStatusFilter(v); setCurrentPage(1); }}
          onSourceFilterChange={(v) => { setSourceFilter(v); setCurrentPage(1); }}
          onSearchChange={(v) => { setSearchText(v); setCurrentPage(1); }}
          onToggleSelect={toggleSel} onToggleAll={toggleAll}
          onTransform={doTransform} onViewCode={doViewCode} onReset={doReset}
          onRefresh={() => { loadFactors(); loadStats(); }}
          onPageChange={setCurrentPage}
          onPageSizeChange={(s) => { setPageSize(s); setCurrentPage(1); }}
        />
      )}

      {/* 改造任务 Tab */}
      {activeTab === "jobs" && (
        <JobsTable
          jobs={jobs} loading={jobsLoading} onRefresh={loadJobs}
          onViewProgress={(jobId, factorName) => setProgressPanel({ jobId, factorName })}
        />
      )}
    </main>
  );
}
