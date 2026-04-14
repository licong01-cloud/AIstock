"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import TaskTableSection from "./components/TaskTableSection";
import type { SyncCandidateItem } from "./components/TaskRow";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type SyncCandidatesResp = {
  ok: boolean;
  count: number;
  total_count: number;
  items: SyncCandidateItem[];
};

export default function RDAgentTaskSyncPage() {
  // ── 页面级状态 ──
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [items, setItems] = useState<SyncCandidateItem[]>([]);

  const [selected, setSelected] = useState<Record<string, boolean>>({});

  const [currentPage, setCurrentPage] = useState<number>(1);
  const [pageSize, setPageSize] = useState<number>(20);
  const [totalCount, setTotalCount] = useState<number>(0);

  const [syncRunning, setSyncRunning] = useState(false);
  const [syncResult, setSyncResult] = useState<any>(null);

  // 节点选择
  const [selectedNodeId, setSelectedNodeId] = useState<string>("");
  const [availableNodes, setAvailableNodes] = useState<{ node_id: string; display_name: string; status: string }[]>([]);

  // 同步进度 Modal 状态
  const [showSyncModal, setShowSyncModal] = useState(false);
  const [syncLogs, setSyncLogs] = useState<string[]>([]);
  const [syncProgress, setSyncProgress] = useState<{ current: number; total: number; taskId: string } | null>(null);
  const [syncDone, setSyncDone] = useState(false);
  const [syncSummary, setSyncSummary] = useState<{ total: number; success: number; failed: number; results?: any[] } | null>(null);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);

  // 扫描发现状态
  const [scanning, setScanning] = useState(false);
  const [scanResult, setScanResult] = useState<string | null>(null);

  const pollingRef = useRef<NodeJS.Timeout | null>(null);

  const selectedIds = useMemo(
    () => Object.entries(selected).filter(([, v]) => !!v).map(([k]) => k),
    [selected],
  );

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));

  // ref 用于 useCallback 读取最新状态
  const selectedNodeIdRef = useRef(selectedNodeId);
  selectedNodeIdRef.current = selectedNodeId;
  const currentPageRef = useRef(currentPage);
  currentPageRef.current = currentPage;
  const pageSizeRef = useRef(pageSize);
  pageSizeRef.current = pageSize;

  // 加载候选列表（服务端分页）
  const loadCandidates = useCallback(async (page?: number) => {
    setLoading(true);
    setError(null);
    const targetPage = page ?? currentPageRef.current;
    const offset = (targetPage - 1) * pageSizeRef.current;
    try {
      const params = new URLSearchParams({
        limit: String(pageSizeRef.current),
        offset: String(offset),
      });
      if (selectedNodeIdRef.current) params.set("node_id", selectedNodeIdRef.current);
      const res = await fetch(`${API_BASE}/rdagent/tasks/sync-candidates?${params}`);
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `加载同步候选失败: ${res.status}`);
      }
      const data = (await res.json()) as SyncCandidatesResp;
      setItems(data.items || []);
      setTotalCount(data.total_count || 0);
      if (page !== undefined) setCurrentPage(page);

      setSelected((prev) => {
        const next: Record<string, boolean> = {};
        (data.items || []).forEach((it) => {
          const tid = String(it.task_id || "").trim();
          if (!tid) return;
          next[tid] = !!prev[tid];
        });
        return next;
      });
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }, []);

  // 扫描发现：调用 API 从各节点拉取全部 task 写入 DB
  const handleScanDiscover = useCallback(async () => {
    setScanning(true);
    setScanResult(null);
    try {
      const res = await fetch(`${API_BASE}/rdagent/candidate-tasks/refresh`, { method: "POST" });
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `扫描失败: ${res.status}`);
      }
      const data = await res.json();
      const msg = `发现 ${data.total_discovered} 个, 新增 ${data.new_tasks} 个, 更新 ${data.updated_tasks} 个 (查询 ${data.nodes_queried} 节点)`;
      setScanResult(msg);
      loadCandidates(1);
    } catch (e: any) {
      setScanResult(`扫描失败: ${e?.message || "未知错误"}`);
    } finally {
      setScanning(false);
    }
  }, [loadCandidates]);

  useEffect(() => {
    loadCandidates(1);
    // 加载可用节点
    fetch(`${API_BASE}/dispatch/nodes`)
      .then(r => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); })
      .then(nodes => setAvailableNodes(nodes))
      .catch(e => console.warn("加载节点列表失败:", e));
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current);
    };
  }, [loadCandidates]);

  useEffect(() => {
    if (logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [syncLogs]);

  const parseSSE = useCallback((text: string) => {
    const events: { event: string; data: string }[] = [];
    const blocks = text.split("\n\n");
    for (const block of blocks) {
      if (!block.trim()) continue;
      let event = "message";
      const dataLines: string[] = [];
      for (const line of block.split("\n")) {
        if (line.startsWith("event: ")) event = line.slice(7).trim();
        else if (line.startsWith("data: ")) dataLines.push(line.slice(6));
        else if (line.startsWith("data:")) dataLines.push(line.slice(5));
      }
      if (dataLines.length > 0) {
        events.push({ event, data: dataLines.join("\n") });
      }
    }
    return events;
  }, []);

  async function handleSyncSelected() {
    if (selectedIds.length === 0) {
      alert("请先勾选要同步的 task");
      return;
    }
    if (!confirm(`确定同步选中的 ${selectedIds.length} 个 task 资产吗？\n将写入本地 rdagent_assets，并更新数据库 aistock_task_catalog。`)) {
      return;
    }

    setSyncRunning(true);
    setSyncResult(null);
    setSyncLogs([]);
    setSyncProgress(null);
    setSyncDone(false);
    setSyncSummary(null);
    setShowSyncModal(true);

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/sync-stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ task_ids: selectedIds, operator: "ui", node_id: selectedNodeId || null }),
        signal: controller.signal,
      });

      if (!res.ok || !res.body) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || payload?.error || `同步失败: ${res.status}`);
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const parts = buffer.split("\n\n");
        buffer = parts.pop() || "";

        for (const part of parts) {
          const parsed = parseSSE(part + "\n\n");
          for (const { event, data } of parsed) {
            if (event === "ping") continue;
            if (event === "task_begin") {
              try {
                const d = JSON.parse(data);
                setSyncProgress({ current: d.current, total: d.total, taskId: d.task_id });
                setSyncLogs(prev => [...prev, `\n━━ [${d.current}/${d.total}] 开始同步: ${d.task_id} ━━`]);
              } catch {}
            } else if (event === "task_done") {
              try {
                const d = JSON.parse(data);
                const icon = d.ok ? "✓" : "✗";
                setSyncLogs(prev => [...prev, `${icon} [${d.current}/${d.total}] ${d.task_id}: ${d.ok ? "成功" : d.error || "失败"}`]);
              } catch {}
            } else if (event === "done") {
              try {
                const d = JSON.parse(data);
                setSyncSummary(d);
              } catch {}
            } else if (event === "result") {
              try {
                const d = JSON.parse(data);
                setSyncResult(d);
                setSyncSummary(prev => prev || d);
              } catch {}
            } else if (event === "log") {
              setSyncLogs(prev => [...prev, data]);
            }
          }
        }
      }
    } catch (e: any) {
      if (e?.name !== "AbortError") {
        setSyncLogs(prev => [...prev, `[ERROR] ${e?.message || "同步失败"}`]);
      }
    } finally {
      setSyncDone(true);
      setSyncRunning(false);
      abortRef.current = null;
      setSelected({});
      await loadCandidates(currentPage);
    }
  }

  const handleSelectChange = useCallback((taskId: string, checked: boolean) => {
    setSelected((prev) => ({ ...prev, [taskId]: checked }));
  }, []);

  // 翻页处理
  const handlePageChange = useCallback((page: number) => {
    setCurrentPage(page);
    setSelected({});
    loadCandidates(page);
  }, [loadCandidates]);

  // 切换每页数量
  const handlePageSizeChange = useCallback((newSize: number) => {
    setPageSize(newSize);
    setCurrentPage(1);
    setSelected({});
    // 需要用新 pageSize 重新请求
    const offset = 0;
    setLoading(true);
    setError(null);
    const params = new URLSearchParams({ limit: String(newSize), offset: String(offset) });
    if (selectedNodeIdRef.current) params.set("node_id", selectedNodeIdRef.current);
    fetch(`${API_BASE}/rdagent/tasks/sync-candidates?${params}`)
      .then(r => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); })
      .then(data => {
        setItems(data.items || []);
        setTotalCount(data.total_count || 0);
        setSelected({});
      })
      .catch(e => setError(e?.message || "加载失败"))
      .finally(() => setLoading(false));
  }, []);

  return (
    <main style={{ padding: 24 }}>
      <style>{`@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }`}</style>
      <section
        style={{
          background: "linear-gradient(135deg, #0ea5e9 0%, #22c55e 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>RD-Agent Task 资产同步</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          增量同步：先从 RD-Agent 拉取最新 task 摘要（latest/summary），你确认后再同步资产（manifest+assets+factor_entry.py）
        </p>
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          marginBottom: 16,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
          {/* 节点选择 */}
          <label style={{ fontSize: 12 }}>
            节点:
            <select
              value={selectedNodeId}
              onChange={(e) => {
                const v = e.target.value;
                setSelectedNodeId(v);
                selectedNodeIdRef.current = v;
                setCurrentPage(1);
                loadCandidates(1);
              }}
              style={{ marginLeft: 6, padding: 4, fontSize: 12, minWidth: 120 }}
            >
              <option value="">全部节点</option>
              {availableNodes.map(n => (
                <option key={n.node_id} value={n.node_id}>
                  {n.display_name} ({n.status})
                </option>
              ))}
            </select>
          </label>

          <button
            onClick={() => loadCandidates(1)}
            disabled={loading}
            style={{ padding: "6px 10px", fontSize: 12, cursor: "pointer" }}
          >
            {loading ? "加载中..." : "刷新候选"}
          </button>

          <button
            onClick={handleScanDiscover}
            disabled={scanning}
            style={{
              padding: "6px 10px", fontSize: 12, cursor: scanning ? "not-allowed" : "pointer",
              background: scanning ? "#e5e7eb" : "#0369a1", color: scanning ? "#6b7280" : "#fff",
              borderRadius: 8, border: "none",
            }}
          >
            {scanning ? "扫描中..." : "扫描发现"}
          </button>
          {scanResult && (
            <span style={{ fontSize: 11, color: scanResult.startsWith("扫描失败") ? "#dc2626" : "#16a34a" }}>
              {scanResult}
            </span>
          )}

          <button
            onClick={handleSyncSelected}
            disabled={syncRunning || selectedIds.length === 0}
            style={{
              padding: "6px 10px",
              fontSize: 12,
              cursor: selectedIds.length === 0 ? "not-allowed" : "pointer",
              background: selectedIds.length === 0 ? "#e5e7eb" : "#111827",
              color: selectedIds.length === 0 ? "#6b7280" : "#fff",
              borderRadius: 8,
              border: "none",
            }}
          >
            {syncRunning ? `同步中 (${selectedIds.length})...` : `同步选中 (${selectedIds.length})`}
          </button>

          <button
            onClick={() => { setSelected({}); }}
            style={{ padding: "6px 10px", fontSize: 12, cursor: "pointer" }}
          >
            清空勾选
          </button>
        </div>

        {error && (
          <div style={{ marginTop: 10, padding: 10, background: "#fee2e2", borderRadius: 8, fontSize: 12 }}>
            {error}
          </div>
        )}
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          marginBottom: 16,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 12, opacity: 0.8, marginBottom: 8 }}>
          <span>
            共 <strong>{totalCount}</strong> 个候选 Task
            <span style={{ marginLeft: 8 }}>
              | 第 {currentPage} / {totalPages} 页（每页 {pageSize} 条）
            </span>
          </span>
          <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ color: "#6b7280" }}>每页</span>
            <select
              value={pageSize}
              onChange={(e) => handlePageSizeChange(Number(e.target.value))}
              style={{ padding: "4px 8px", fontSize: 12, borderRadius: 4, border: "1px solid #d1d5db" }}
            >
              {[20, 50, 100, 200].map((n) => (
                <option key={n} value={n}>{n} 条</option>
              ))}
            </select>
          </span>
        </div>

        {/* ── 表格区域 ── */}
        <TaskTableSection
          pagedItems={items}
          selected={selected}
          onSelectChange={handleSelectChange}
          onRefreshItems={() => loadCandidates(currentPage)}
        />

        {/* 分页控件 */}
        {totalCount > 0 && (
          <div style={{ display: "flex", justifyContent: "center", alignItems: "center", marginTop: 12, paddingTop: 12, borderTop: "1px solid #e5e7eb" }}>
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <button
                onClick={() => handlePageChange(1)}
                disabled={currentPage <= 1 || loading}
                style={{ padding: "4px 8px", fontSize: 11, cursor: currentPage <= 1 ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage <= 1 ? "#f3f4f6" : "#fff" }}
              >
                首页
              </button>
              <button
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage <= 1 || loading}
                style={{ padding: "4px 10px", fontSize: 11, cursor: currentPage <= 1 ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage <= 1 ? "#f3f4f6" : "#fff" }}
              >
                上一页
              </button>
              <span style={{ fontSize: 12, fontWeight: 500, minWidth: 80, textAlign: "center" }}>
                {currentPage} / {totalPages}
              </span>
              <button
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage >= totalPages || loading}
                style={{ padding: "4px 10px", fontSize: 11, cursor: currentPage >= totalPages ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage >= totalPages ? "#f3f4f6" : "#fff" }}
              >
                下一页
              </button>
              <button
                onClick={() => handlePageChange(totalPages)}
                disabled={currentPage >= totalPages || loading}
                style={{ padding: "4px 8px", fontSize: 11, cursor: currentPage >= totalPages ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage >= totalPages ? "#f3f4f6" : "#fff" }}
              >
                末页
              </button>
            </div>
          </div>
        )}
      </section>

      {syncResult && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 8 }}>同步结果</div>
          <pre style={{ margin: 0, fontSize: 12, whiteSpace: "pre-wrap" }}>{JSON.stringify(syncResult, null, 2)}</pre>
        </section>
      )}

      {/* 同步进度 Modal */}
      {showSyncModal && (
        <div style={{
          position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
          background: 'rgba(0,0,0,0.5)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          zIndex: 1100,
        }}>
          <div style={{
            background: '#fff', borderRadius: 12, padding: 24,
            maxWidth: 800, width: '92%', maxHeight: '85vh',
            display: 'flex', flexDirection: 'column',
            boxShadow: '0 20px 25px -5px rgba(0,0,0,0.15)',
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <h3 style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>同步进度</h3>
              <button
                onClick={() => {
                  if (syncRunning && abortRef.current) {
                    if (confirm("同步正在进行中，确定要中断吗？")) {
                      abortRef.current.abort();
                    }
                    return;
                  }
                  setShowSyncModal(false);
                }}
                style={{
                  padding: '4px 12px', fontSize: 12, border: '1px solid #d1d5db',
                  borderRadius: 6, background: '#fff', cursor: 'pointer',
                }}
              >
                {syncRunning ? "中断" : "关闭"}
              </button>
            </div>

            {syncProgress && (
              <div style={{ marginBottom: 12 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <div style={{
                    width: 8, height: 8, borderRadius: '50%',
                    backgroundColor: syncDone ? (syncSummary && syncSummary.failed > 0 ? '#f59e0b' : '#22c55e') : '#3b82f6',
                    animation: syncDone ? 'none' : 'pulse 1.5s infinite',
                  }} />
                  <span style={{ fontSize: 13, fontWeight: 500 }}>
                    {syncDone ? '同步完成' : `同步中 ${syncProgress.current}/${syncProgress.total}`}
                  </span>
                  <span style={{ fontSize: 12, color: '#6b7280' }}>
                    {syncProgress.taskId}
                  </span>
                </div>
                <div style={{
                  height: 6, borderRadius: 3, background: '#e5e7eb', overflow: 'hidden',
                }}>
                  <div style={{
                    height: '100%', borderRadius: 3,
                    background: syncDone ? '#22c55e' : '#3b82f6',
                    width: `${Math.round(((syncDone ? syncProgress.total : syncProgress.current - 1) / syncProgress.total) * 100)}%`,
                    transition: 'width 0.3s ease',
                  }} />
                </div>
              </div>
            )}

            <div style={{
              flex: 1, minHeight: 200, maxHeight: 'calc(85vh - 220px)',
              backgroundColor: '#0f172a', borderRadius: 8, padding: 14,
              overflowY: 'auto',
              fontFamily: "'Fira Code', Consolas, 'Courier New', monospace",
              fontSize: 12, lineHeight: 1.7, color: '#e2e8f0',
              border: '1px solid #334155',
            }}>
              {syncLogs.map((line, idx) => (
                <div key={idx} style={{
                  color: line.startsWith("[ERROR]") || line.startsWith("✗")
                    ? '#f87171'
                    : line.startsWith("✓")
                      ? '#4ade80'
                      : line.startsWith("━━")
                        ? '#60a5fa'
                        : line.includes("WARNING") || line.includes("WARN")
                          ? '#fbbf24'
                          : '#cbd5e1',
                  whiteSpace: 'pre-wrap',
                  wordBreak: 'break-all',
                }}>
                  {line}
                </div>
              ))}
              <div ref={logsEndRef} />
            </div>

            {syncSummary && (
              <div style={{
                marginTop: 12, padding: 10, background: '#f8fafc',
                borderRadius: 8, border: '1px solid #e2e8f0',
                display: 'flex', gap: 20, fontSize: 13,
              }}>
                <span>共 <strong>{syncSummary.total}</strong> 个 Task</span>
                <span style={{ color: '#16a34a' }}>✓ 成功 <strong>{syncSummary.success}</strong></span>
                {syncSummary.failed > 0 && (
                  <span style={{ color: '#dc2626' }}>✗ 失败 <strong>{syncSummary.failed}</strong></span>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </main>
  );
}
