"use client";

import { useEffect, useMemo, useRef, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type LoopDetail = {
  loop_id: number;
  hypothesis: string | null;
  reason: string | null;
  exp_type: string | null;
  valid_score: number | null;
  test_score: number | null;
  mle_score: any;
  feedback: boolean | null;
  is_sota: boolean;
  annualized_return: number | null;
  max_drawdown: number | null;
  information_ratio: number | null;
};

type SyncCandidateItem = {
  task_id: string;
  latest?: any;
  summary?: any;
  local?: any;
  discovery?: any;
  is_synced?: boolean;
  is_enabled_for_selection?: boolean;
};

type SyncCandidatesResp = {
  ok: boolean;
  count: number;
  items: SyncCandidateItem[];
};

export default function RDAgentTaskSyncPage() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [items, setItems] = useState<SyncCandidateItem[]>([]);

  const [limit, setLimit] = useState<number>(100);
  const [selected, setSelected] = useState<Record<string, boolean>>({});

  // 分页状态
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [pageSize, setPageSize] = useState<number>(100);

  const [syncRunning, setSyncRunning] = useState(false);
  const [syncResult, setSyncResult] = useState<any>(null);

  const [expandedTaskId, setExpandedTaskId] = useState<string | null>(null);
  const [loopsData, setLoopsData] = useState<Record<string, LoopDetail[]>>({});
  const [loopsLoading, setLoopsLoading] = useState<Record<string, boolean>>({});

  // Workspace管理相关状态
  const [workspaceData, setWorkspaceData] = useState<Record<string, any>>({});
  const [workspaceLoading, setWorkspaceLoading] = useState<Record<string, boolean>>({});
  const [showWorkspaceModal, setShowWorkspaceModal] = useState<string | null>(null);
  const [deleteConfirmStep, setDeleteConfirmStep] = useState<number>(0);

  // 刷新相关状态
  const [taskRefreshing, setTaskRefreshing] = useState<Record<string, boolean>>({});
  const [loopRefreshing, setLoopRefreshing] = useState<Record<string, boolean>>({});

  const pollingRef = useRef<NodeJS.Timeout | null>(null);

  const selectedIds = useMemo(
    () => Object.entries(selected).filter(([, v]) => !!v).map(([k]) => k),
    [selected],
  );

  // 分页计算
  const totalPages = Math.max(1, Math.ceil(items.length / pageSize));
  const pagedItems = useMemo(() => {
    const start = (currentPage - 1) * pageSize;
    return items.slice(start, start + pageSize);
  }, [items, currentPage, pageSize]);

  async function loadCandidates() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/sync-candidates?limit=${encodeURIComponent(String(limit))}`);
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `加载同步候选失败: ${res.status}`);
      }
      const data = (await res.json()) as SyncCandidatesResp;
      setItems(data.items || []);
      setCurrentPage(1);

      // 合并选中状态：新数据里不存在的自动剔除
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
  }

  useEffect(() => {
    loadCandidates();
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
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
    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/sync`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ task_ids: selectedIds, operator: "ui" }),
      });
      const payload = await res.json().catch(() => null);
      if (!res.ok) {
        throw new Error(payload?.detail || payload?.error || `同步失败: ${res.status}`);
      }
      setSyncResult(payload);
      await loadCandidates();
    } catch (e: any) {
      alert(e?.message || "同步失败");
    } finally {
      setSyncRunning(false);
    }
  }

  async function toggleEnableForSelection(taskId: string, nextValue: boolean) {
    try {
      const url = nextValue
        ? `${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/enable_for_selection`
        : `${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/disable_for_selection`;
      const res = await fetch(url, { method: "POST" });
      const payload = await res.json().catch(() => null);
      if (!res.ok) throw new Error(payload?.detail || payload?.error || "操作失败");
      await loadCandidates();
    } catch (e: any) {
      alert(e?.message || "操作失败");
    }
  }

  function openTaskDetail(taskId: string) {
    window.open(`/rdagent/tasks/${encodeURIComponent(taskId)}`, "_blank");
  }

  async function toggleLoopDetails(taskId: string) {
    if (expandedTaskId === taskId) {
      // 收起
      setExpandedTaskId(null);
      return;
    }

    // 展开
    setExpandedTaskId(taskId);

    // 如果已经加载过，直接显示
    if (loopsData[taskId]) {
      return;
    }

    // 加载LOOP详情（使用缓存API）
    setLoopsLoading((prev) => ({ ...prev, [taskId]: true }));
    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/candidate-loops`);
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `加载LOOP详情失败: ${res.status}`);
      }
      const data = await res.json();
      if (data.ok && Array.isArray(data.loops)) {
        setLoopsData((prev) => ({ ...prev, [taskId]: data.loops }));
      } else {
        throw new Error(data.error || "加载LOOP详情失败");
      }
    } catch (e: any) {
      alert(e?.message || "加载LOOP详情失败");
      setExpandedTaskId(null);
    } finally {
      setLoopsLoading((prev) => ({ ...prev, [taskId]: false }));
    }
  }

  // 刷新Task数据（清除数据库缓存并重新获取）
  async function refreshTaskData(taskId: string) {
    if (!confirm(`确认刷新Task ${taskId} 的数据？\n\n这将清除数据库中的缓存，并从RD-Agent API重新获取最新数据。`)) {
      return;
    }

    setTaskRefreshing((prev) => ({ ...prev, [taskId]: true }));
    try {
      // 调用后端API清除缓存并刷新
      const res = await fetch(`${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/refresh`, {
        method: 'POST'
      });
      
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `刷新Task失败: ${res.status}`);
      }
      
      const data = await res.json();
      if (data.ok) {
        // 刷新Task后自动加载最新LOOP数据到前端缓存
        try {
          const loopRes = await fetch(`${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/candidate-loops`);
          if (loopRes.ok) {
            const loopData = await loopRes.json();
            if (loopData.ok && Array.isArray(loopData.loops)) {
              setLoopsData((prev) => ({ ...prev, [taskId]: loopData.loops }));
            }
          }
        } catch (_) {
          // LOOP加载失败不影响主流程，清除旧缓存即可
          setLoopsData((prev) => {
            const newData = { ...prev };
            delete newData[taskId];
            return newData;
          });
        }
        
        alert(`✓ Task ${taskId} 数据已刷新\n\n清除了 ${data.deleted_loops || 0} 条旧缓存，已重新获取LOOP数据`);
        
        // 重新加载候选列表
        loadCandidates();
      } else {
        throw new Error(data.error || "刷新Task失败");
      }
    } catch (e: any) {
      alert(`刷新Task失败: ${e?.message || "未知错误"}`);
    } finally {
      setTaskRefreshing((prev) => ({ ...prev, [taskId]: false }));
    }
  }

  // 刷新LOOP数据（强制从API获取最新数据）
  async function refreshLoopData(taskId: string) {
    setLoopRefreshing((prev) => ({ ...prev, [taskId]: true }));
    try {
      // 使用force_refresh=true参数强制从RD-Agent API获取
      const res = await fetch(`${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/candidate-loops?force_refresh=true`);
      
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `刷新LOOP失败: ${res.status}`);
      }
      
      const data = await res.json();
      if (data.ok && Array.isArray(data.loops)) {
        setLoopsData((prev) => ({ ...prev, [taskId]: data.loops }));
        alert(`✓ LOOP数据已刷新\n\n获取了 ${data.loops.length} 个LOOP的最新数据`);
      } else {
        throw new Error(data.error || "刷新LOOP失败");
      }
    } catch (e: any) {
      alert(`刷新LOOP失败: ${e?.message || "未知错误"}`);
    } finally {
      setLoopRefreshing((prev) => ({ ...prev, [taskId]: false }));
    }
  }

  // 加载workspace信息
  async function loadWorkspaceInfo(taskId: string) {
    setWorkspaceLoading((prev) => ({ ...prev, [taskId]: true }));
    try {
      const res = await fetch(`http://127.0.0.1:9000/tasks/${taskId}/workspaces`);
      const data = await res.json();
      if (data.ok) {
        setWorkspaceData((prev) => ({ ...prev, [taskId]: data }));
        setShowWorkspaceModal(taskId);
        setDeleteConfirmStep(0);
      } else {
        alert(`获取workspace信息失败: ${data.error || "未知错误"}`);
      }
    } catch (error: any) {
      alert(`获取workspace信息失败: ${error?.message || "网络错误"}`);
    } finally {
      setWorkspaceLoading((prev) => ({ ...prev, [taskId]: false }));
    }
  }

  // 删除task
  async function deleteTask(taskId: string) {
    try {
      const res = await fetch(`http://127.0.0.1:9000/tasks/${taskId}`, {
        method: 'DELETE'
      });
      const data = await res.json();
      if (data.ok) {
        alert(`成功删除：${data.message}`);
        setShowWorkspaceModal(null);
        setDeleteConfirmStep(0);
        // 刷新任务列表
        loadCandidates();
      } else {
        alert(`删除失败：${data.error || "未知错误"}`);
      }
    } catch (error: any) {
      alert(`删除失败：${error?.message || "网络错误"}`);
    }
  }

  return (
    <main style={{ padding: 24 }}>
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
          <label style={{ fontSize: 12 }}>
            候选数量:
            <input
              value={String(limit)}
              onChange={(e) => setLimit(Number(e.target.value || 20))}
              type="number"
              min={1}
              max={200}
              style={{ marginLeft: 6, padding: 4, fontSize: 12, width: 90 }}
            />
          </label>

          <button
            onClick={() => loadCandidates()}
            disabled={loading}
            style={{ padding: "6px 10px", fontSize: 12, cursor: "pointer" }}
          >
            {loading ? "加载中..." : "刷新候选"}
          </button>

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
            onClick={() => {
              setSelected({});
            }}
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
        <div style={{ fontSize: 12, opacity: 0.8, marginBottom: 8 }}>
          共 {items.length} 个候选（来自 RD-Agent latest + summary，与本地 task_catalog 合并）
          {items.length > pageSize && (
            <span style={{ marginLeft: 8 }}>
              | 第 {currentPage}/{totalPages} 页，每页 {pageSize} 条
            </span>
          )}
        </div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>
                <th style={{ padding: 8, width: 32 }}></th>
                <th style={{ padding: 8, width: 180 }}>task_id</th>
                <th style={{ padding: 8, width: 120 }}>SOTA/Alpha/特征</th>
                <th style={{ padding: 8, width: 70 }}>V2对齐</th>
                <th style={{ padding: 8, width: 90 }}>同步状态</th>
                <th style={{ padding: 8, width: 120 }}>加入Task选股</th>
                <th style={{ padding: 8, minWidth: 250 }}>Task概要</th>
                <th style={{ padding: 8, width: 320 }}>操作</th>
              </tr>
            </thead>
            <tbody>
              {(pagedItems || []).map((it) => {
                const tid = String(it.task_id || "");
                const local = it.local || null;
                // 同步状态：未同步/同步成功/同步失败
                let syncStatus = "未同步";
                if (local?.sync_status) {
                  if (local.sync_status === "success") {
                    syncStatus = "同步成功";
                  } else if (local.sync_status === "failed") {
                    syncStatus = "同步失败";
                  } else {
                    syncStatus = local.sync_status;
                  }
                } else if (it.is_synced) {
                  syncStatus = "同步成功";
                }
                const enabled = !!(local?.is_enabled_for_selection ?? it.is_enabled_for_selection);
                const discovery = it.discovery || null;
                const hasSota = discovery?.has_sota;
                const sotaCount = discovery?.sota_factors_count;
                const checkedAt = discovery?.sota_checked_at_utc;
                const v2 = discovery?.v2_alignment || null;
                const alphaCount = v2?.alpha_factors_count;
                const modelFeatureCount = v2?.model_feature_count;
                const isAligned = v2?.is_aligned;
                const v2HistLen = v2?.hist_len;
                const synced = syncStatus === "同步成功";
                const remoteSummary = (it as { remote?: unknown }).remote;
                const summaryData = it.summary ?? it.latest ?? remoteSummary ?? {};
                const summaryErr = (summaryData as any)?.error;
                
                const isExpanded = expandedTaskId === tid;
                const loops = loopsData[tid] || [];
                const isLoadingLoops = loopsLoading[tid];
                
                // 构建Task概要信息（优先从V2对齐信息构建）
                let taskSummary = "";
                if (summaryErr) {
                  taskSummary = `ERROR: ${String(summaryErr).slice(0, 80)}`;
                } else if (v2) {
                  // 从V2对齐信息构建概要
                  const parts: string[] = [];
                  if (v2HistLen) parts.push(`LOOP: ${v2HistLen}个`);
                  if (typeof sotaCount === "number" && sotaCount > 0) parts.push(`SOTA: ${sotaCount}个`);
                  if (typeof alphaCount === "number") parts.push(`Alpha: ${alphaCount}个`);
                  if (typeof modelFeatureCount === "number") parts.push(`特征: ${modelFeatureCount}`);
                  taskSummary = parts.length > 0 ? parts.join(" | ") : "V2已检查";
                } else if (loops.length > 0) {
                  // 从LOOP数据统计类型和数量
                  const factorCount = loops.filter(l => l.exp_type && l.exp_type.includes('Factor')).length;
                  const modelCount = loops.filter(l => l.exp_type && l.exp_type.includes('Model')).length;
                  const unknownCount = loops.filter(l => l.exp_type === 'unknown').length;
                  
                  const parts: string[] = [];
                  parts.push(`LOOP: ${loops.length}个`);
                  if (factorCount > 0) parts.push(`因子: ${factorCount}个`);
                  if (modelCount > 0) parts.push(`模型: ${modelCount}个`);
                  if (unknownCount > 0) parts.push(`未完成: ${unknownCount}个`);
                  
                  taskSummary = parts.join(" | ");
                } else {
                  const loopCountFromSummary = (summaryData as any)?.loop_count;
                  if (loopCountFromSummary) {
                    taskSummary = `LOOP: ${loopCountFromSummary}个`;
                  } else {
                    taskSummary = "暂无数据";
                  }
                }

                return (
                  <>
                    <tr key={tid} style={{ borderBottom: isExpanded ? "none" : "1px solid #f3f4f6" }}>
                      <td style={{ padding: 8 }}>
                        <input
                          type="checkbox"
                          aria-label={`选择 task ${tid}`}
                          checked={!!selected[tid]}
                          disabled={!!synced}
                          onChange={(e) => setSelected((prev) => ({ ...prev, [tid]: e.target.checked }))}
                        />
                      </td>
                      <td style={{ padding: 8, fontFamily: "monospace" }}>{tid}</td>
                      <td style={{ padding: 8 }}>
                        {hasSota === null || hasSota === undefined ? (
                          <span style={{ color: "#9ca3af" }}>未检查</span>
                        ) : hasSota ? (
                          <div>
                            <div style={{ color: "#10b981", fontWeight: 600 }}>SOTA: {sotaCount ?? 0}</div>
                            {typeof alphaCount === "number" && <div style={{ fontSize: 10, color: "#6b7280" }}>Alpha: {alphaCount}</div>}
                            {typeof modelFeatureCount === "number" && <div style={{ fontSize: 10, color: "#6b7280" }}>特征: {modelFeatureCount}</div>}
                          </div>
                        ) : (
                          <span style={{ color: "#9ca3af" }}>无SOTA</span>
                        )}
                      </td>
                      <td style={{ padding: 8 }}>
                        {isAligned === true ? (
                          <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, background: "#d1fae5", color: "#065f46", fontWeight: 600 }}>ALIGNED</span>
                        ) : isAligned === false ? (
                          <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, background: "#fef3c7", color: "#92400e", fontWeight: 600 }}>MISMATCH</span>
                        ) : (
                          <span style={{ color: "#9ca3af", fontSize: 11 }}>-</span>
                        )}
                      </td>
                      <td style={{ padding: 8 }}>
                        <span style={{
                          padding: "2px 8px",
                          borderRadius: 4,
                          fontSize: 11,
                          background: syncStatus === "同步成功" ? "#d1fae5" : syncStatus === "同步失败" ? "#fee2e2" : "#f3f4f6",
                          color: syncStatus === "同步成功" ? "#065f46" : syncStatus === "同步失败" ? "#991b1b" : "#374151"
                        }}>
                          {syncStatus}
                        </span>
                      </td>
                      <td style={{ padding: 8 }}>
                        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <input
                            type="checkbox"
                            checked={enabled}
                            onChange={(e) => toggleEnableForSelection(tid, e.target.checked)}
                          />
                          <span>{enabled ? "已加入" : "未加入"}</span>
                        </label>
                      </td>
                      <td style={{ padding: 8, minWidth: 300, fontSize: 11 }}>
                        {taskSummary}
                      </td>
                      <td style={{ padding: 8 }}>
                        <div style={{ display: "flex", gap: 4, flexWrap: "nowrap" }}>
                          <button
                            onClick={() => refreshTaskData(tid)}
                            disabled={taskRefreshing[tid]}
                            style={{ 
                              padding: "4px 10px", 
                              fontSize: 11, 
                              cursor: taskRefreshing[tid] ? "wait" : "pointer",
                              background: "#10b981",
                              color: "#fff",
                              border: "none",
                              borderRadius: 4,
                              whiteSpace: "nowrap"
                            }}
                            title="清除数据库缓存并重新获取"
                          >
                            {taskRefreshing[tid] ? "刷新中..." : "刷新Task"}
                          </button>
                          <button
                            onClick={() => toggleLoopDetails(tid)}
                            style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", whiteSpace: "nowrap" }}
                          >
                            {isExpanded ? "收起" : "LOOP详情"}
                          </button>
                          <button
                            onClick={() => loadWorkspaceInfo(tid)}
                            disabled={workspaceLoading[tid]}
                            style={{ padding: "4px 10px", fontSize: 11, cursor: workspaceLoading[tid] ? "wait" : "pointer", whiteSpace: "nowrap" }}
                          >
                            {workspaceLoading[tid] ? "加载中..." : "Workspace"}
                          </button>
                          <button
                            onClick={() => openTaskDetail(tid)}
                            style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", whiteSpace: "nowrap" }}
                          >
                            Task详情
                          </button>
                        </div>
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr key={`${tid}-loops`} style={{ borderBottom: "1px solid #f3f4f6" }}>
                        <td colSpan={8} style={{ padding: 16, background: "#f9fafb" }}>
                          {isLoadingLoops ? (
                            <div style={{ textAlign: "center", color: "#6b7280" }}>加载LOOP详情中...</div>
                          ) : loops.length === 0 ? (
                            <div style={{ textAlign: "center", color: "#6b7280" }}>暂无LOOP数据</div>
                          ) : (
                            <div>
                              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                                <div style={{ fontWeight: 600, fontSize: 13 }}>
                                  LOOP 详情 (共 {loops.length} 个)
                                  {loops.filter(l => l.exp_type === 'unknown').length > 0 && (
                                    <span style={{ marginLeft: 8, fontSize: 11, color: "#dc2626" }}>
                                      含 {loops.filter(l => l.exp_type === 'unknown').length} 个未完成LOOP
                                    </span>
                                  )}
                                </div>
                                <button
                                  onClick={() => refreshLoopData(tid)}
                                  disabled={loopRefreshing[tid]}
                                  style={{ 
                                    padding: "4px 12px", 
                                    fontSize: 11, 
                                    cursor: loopRefreshing[tid] ? "wait" : "pointer",
                                    background: "#3b82f6",
                                    color: "#fff",
                                    border: "none",
                                    borderRadius: 4,
                                    whiteSpace: "nowrap"
                                  }}
                                  title="仅用于正在执行的Task产生新LOOP时，强制从RD-Agent API获取最新LOOP数据"
                                >
                                  {loopRefreshing[tid] ? "刷新中..." : "刷新LOOP数据"}
                                </button>
                              </div>
                              <div style={{ overflowX: "auto" }}>
                                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, background: "#fff" }}>
                                  <thead>
                                    <tr style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb", background: "#f3f4f6" }}>
                                      <th style={{ padding: 6, width: 50 }}>ID</th>
                                      <th style={{ padding: 6, width: 60 }}>类型</th>
                                      <th style={{ padding: 6, maxWidth: 200 }}>假设描述</th>
                                      <th style={{ padding: 6, maxWidth: 150 }} title="通过回测的因子（final_decision=True），括号中显示 通过数/总数">回测因子</th>
                                      <th style={{ padding: 6, width: 70 }}>IC值</th>
                                      <th style={{ padding: 6, width: 80 }}>年化收益</th>
                                      <th style={{ padding: 6, width: 80 }}>最大回撤</th>
                                      <th style={{ padding: 6, width: 70 }}>信息比率</th>
                                      <th style={{ padding: 6, width: 70 }}>Decision</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {loops.map((loop) => {
                                      // 简化类型显示：QlibFactorExperiment -> factor, QlibModelExperiment -> model
                                      let expTypeShort = "-";
                                      if (loop.exp_type) {
                                        if (loop.exp_type.includes("Factor")) {
                                          expTypeShort = "factor";
                                        } else if (loop.exp_type.includes("Model")) {
                                          expTypeShort = "model";
                                        } else if (loop.exp_type === "unknown" || loop.exp_type === "error") {
                                          expTypeShort = loop.exp_type;
                                        }
                                      }
                                      
                                      // 区分Factor和Model的显示逻辑
                                      let factorNames = "-";
                                      let hypothesisSummary = "-";
                                      
                                      if (expTypeShort === "factor") {
                                        // Factor类型：回测因子显示通过筛选的因子（final_decision=True）
                                        const testedCount = loop.tested_count ?? (loop.hypothesis ? loop.hypothesis.split('; ').filter(Boolean).length : 0);
                                        const totalCount = loop.total_count ?? testedCount;
                                        const countLabel = totalCount > testedCount ? ` (${testedCount}/${totalCount})` : testedCount > 0 ? ` (${testedCount})` : "";
                                        factorNames = (loop.hypothesis || "-") + countLabel;
                                        if (loop.reason) {
                                          // 去掉开头的标签如 [资金流因子]
                                          let cleaned = loop.reason.replace(/^\[.*?\]\s*/, '');
                                          // 取前80个字符
                                          hypothesisSummary = cleaned.length > 80 ? cleaned.substring(0, 80) + '...' : cleaned;
                                        }
                                      } else if (expTypeShort === "model") {
                                        // Model类型：回测因子显示hypothesis（模型名称），假设描述也显示hypothesis
                                        const modelName = loop.hypothesis || "-";
                                        factorNames = modelName;
                                        hypothesisSummary = modelName !== "-" ? `模型: ${modelName}` : "-";
                                      }
                                      
                                      return (
                                      <tr key={loop.loop_id} style={{ borderBottom: "1px solid #f3f4f6" }}>
                                        <td style={{ padding: 6, width: 50 }}>{loop.loop_id}</td>
                                        <td style={{ padding: 6, width: 60, fontSize: 10 }}>{expTypeShort}</td>
                                        <td style={{ padding: 6, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontSize: 11 }} title={loop.reason || ""}>
                                          {hypothesisSummary}
                                        </td>
                                        <td style={{ padding: 6, maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontFamily: "monospace", fontSize: 10 }} title={factorNames}>
                                          {factorNames}
                                        </td>
                                        <td style={{ padding: 6, width: 70, color: loop.valid_score && loop.valid_score > 0 ? "#10b981" : "#ef4444" }}>
                                          {loop.valid_score !== null && loop.valid_score !== undefined ? loop.valid_score.toFixed(5) : "-"}
                                        </td>
                                        <td style={{ padding: 6, width: 80, color: loop.annualized_return && loop.annualized_return > 0 ? "#10b981" : "#ef4444" }}>
                                          {loop.annualized_return !== null && loop.annualized_return !== undefined ? (loop.annualized_return * 100).toFixed(2) + "%" : "-"}
                                        </td>
                                        <td style={{ padding: 6, width: 80, color: "#ef4444" }}>
                                          {loop.max_drawdown !== null && loop.max_drawdown !== undefined ? (loop.max_drawdown * 100).toFixed(2) + "%" : "-"}
                                        </td>
                                        <td style={{ padding: 6, width: 70, color: loop.information_ratio && loop.information_ratio > 0 ? "#10b981" : "#6b7280" }}>
                                          {loop.information_ratio !== null && loop.information_ratio !== undefined ? loop.information_ratio.toFixed(3) : "-"}
                                        </td>
                                        <td style={{ padding: 6, width: 70, fontWeight: 600, color: (loop.feedback === true || loop.feedback === "true") ? "#10b981" : "#6b7280" }}>
                                          {(loop.feedback === true || loop.feedback === "true") ? "True" : (loop.feedback === false || loop.feedback === "false") ? "False" : (loop.feedback === null || loop.feedback === undefined) ? "-" : String(loop.feedback)}
                                        </td>
                                      </tr>
                                      );
                                    })}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          )}
                        </td>
                      </tr>
                    )}
                  </>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* 分页控件 */}
        {items.length > 0 && (
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 12, paddingTop: 12, borderTop: "1px solid #e5e7eb" }}>
            <div style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 12 }}>
              <span style={{ color: "#6b7280" }}>每页</span>
              <select
                value={pageSize}
                onChange={(e) => { setPageSize(Number(e.target.value)); setCurrentPage(1); }}
                style={{ padding: "4px 8px", fontSize: 12, borderRadius: 4, border: "1px solid #d1d5db" }}
              >
                {[20, 50, 100, 200].map((n) => (
                  <option key={n} value={n}>{n} 条</option>
                ))}
              </select>
              <span style={{ color: "#6b7280" }}>
                共 {items.length} 条
              </span>
            </div>
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <button
                onClick={() => setCurrentPage(1)}
                disabled={currentPage <= 1}
                style={{ padding: "4px 8px", fontSize: 11, cursor: currentPage <= 1 ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage <= 1 ? "#f3f4f6" : "#fff" }}
              >
                首页
              </button>
              <button
                onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                disabled={currentPage <= 1}
                style={{ padding: "4px 10px", fontSize: 11, cursor: currentPage <= 1 ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage <= 1 ? "#f3f4f6" : "#fff" }}
              >
                上一页
              </button>
              <span style={{ fontSize: 12, fontWeight: 500, minWidth: 80, textAlign: "center" }}>
                {currentPage} / {totalPages}
              </span>
              <button
                onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
                disabled={currentPage >= totalPages}
                style={{ padding: "4px 10px", fontSize: 11, cursor: currentPage >= totalPages ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage >= totalPages ? "#f3f4f6" : "#fff" }}
              >
                下一页
              </button>
              <button
                onClick={() => setCurrentPage(totalPages)}
                disabled={currentPage >= totalPages}
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

      {/* Workspace详情弹窗 */}
      {showWorkspaceModal && (
        <div style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background: 'rgba(0,0,0,0.5)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 1000
        }}>
          <div style={{
            background: '#fff',
            borderRadius: 12,
            padding: 24,
            maxWidth: 700,
            width: '90%',
            maxHeight: '80vh',
            overflow: 'auto',
            boxShadow: '0 20px 25px -5px rgba(0,0,0,0.1)'
          }}>
            {deleteConfirmStep === 0 && (
              <>
                <h3 style={{ margin: '0 0 16px 0', fontSize: 18, fontWeight: 600 }}>Workspace详情</h3>
                <div style={{ marginBottom: 16, padding: 12, background: '#f3f4f6', borderRadius: 8 }}>
                  <div style={{ fontSize: 13, marginBottom: 4 }}><strong>Task ID:</strong> {showWorkspaceModal}</div>
                  <div style={{ fontSize: 13, marginBottom: 4 }}><strong>Task目录:</strong> {workspaceData[showWorkspaceModal]?.task_dir}</div>
                  <div style={{ fontSize: 13 }}><strong>总大小:</strong> {workspaceData[showWorkspaceModal]?.total_size_mb?.toFixed(2) || 0} MB</div>
                </div>
                
                <h4 style={{ margin: '16px 0 8px 0', fontSize: 14, fontWeight: 600 }}>Workspace列表</h4>
                {workspaceData[showWorkspaceModal]?.workspaces?.length > 0 ? (
                  <div style={{ maxHeight: 300, overflow: 'auto', border: '1px solid #e5e7eb', borderRadius: 8 }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                      <thead style={{ background: '#f9fafb', position: 'sticky', top: 0 }}>
                        <tr>
                          <th style={{ padding: 8, textAlign: 'left', borderBottom: '1px solid #e5e7eb' }}>名称</th>
                          <th style={{ padding: 8, textAlign: 'left', borderBottom: '1px solid #e5e7eb' }}>路径</th>
                          <th style={{ padding: 8, textAlign: 'right', borderBottom: '1px solid #e5e7eb' }}>大小(MB)</th>
                        </tr>
                      </thead>
                      <tbody>
                        {workspaceData[showWorkspaceModal]?.workspaces?.map((ws: any, idx: number) => (
                          <tr key={idx} style={{ borderBottom: '1px solid #f3f4f6' }}>
                            <td style={{ padding: 8, fontFamily: 'monospace' }}>{ws.name}</td>
                            <td style={{ padding: 8, fontSize: 11, color: '#6b7280', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={ws.path}>{ws.path}</td>
                            <td style={{ padding: 8, textAlign: 'right', fontFamily: 'monospace' }}>{ws.size_mb?.toFixed(2)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div style={{ padding: 16, textAlign: 'center', color: '#6b7280', background: '#f9fafb', borderRadius: 8 }}>
                    此task没有workspace目录
                  </div>
                )}
                
                <div style={{ marginTop: 20, display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                  <button
                    onClick={() => setDeleteConfirmStep(1)}
                    style={{
                      padding: '8px 16px',
                      fontSize: 13,
                      background: '#ef4444',
                      color: '#fff',
                      border: 'none',
                      borderRadius: 8,
                      cursor: 'pointer',
                      fontWeight: 500
                    }}
                  >
                    删除此Task
                  </button>
                  <button
                    onClick={() => {
                      setShowWorkspaceModal(null);
                      setDeleteConfirmStep(0);
                    }}
                    style={{
                      padding: '8px 16px',
                      fontSize: 13,
                      background: '#f3f4f6',
                      color: '#374151',
                      border: 'none',
                      borderRadius: 8,
                      cursor: 'pointer'
                    }}
                  >
                    关闭
                  </button>
                </div>
              </>
            )}
            
            {deleteConfirmStep === 1 && (
              <>
                <h3 style={{ margin: '0 0 16px 0', fontSize: 18, fontWeight: 600, color: '#dc2626' }}>⚠️ 最后确认</h3>
                <div style={{ marginBottom: 16, padding: 16, background: '#fef2f2', borderRadius: 8, border: '1px solid #fecaca' }}>
                  <p style={{ margin: '0 0 8px 0', fontSize: 14 }}><strong>确定要删除task {showWorkspaceModal} 吗？</strong></p>
                  <p style={{ margin: '0 0 8px 0', fontSize: 13 }}>这将删除所有日志和workspace目录，释放 <strong>{workspaceData[showWorkspaceModal]?.total_size_mb?.toFixed(2) || 0} MB</strong> 空间。</p>
                  <p style={{ margin: 0, fontSize: 13, color: '#dc2626', fontWeight: 600 }}>⚠️ 此操作不可撤销！</p>
                </div>
                
                <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                  <button
                    onClick={() => deleteTask(showWorkspaceModal!)}
                    style={{
                      padding: '8px 16px',
                      fontSize: 13,
                      background: '#dc2626',
                      color: '#fff',
                      border: 'none',
                      borderRadius: 8,
                      cursor: 'pointer',
                      fontWeight: 500
                    }}
                  >
                    确认删除
                  </button>
                  <button
                    onClick={() => setDeleteConfirmStep(0)}
                    style={{
                      padding: '8px 16px',
                      fontSize: 13,
                      background: '#f3f4f6',
                      color: '#374151',
                      border: 'none',
                      borderRadius: 8,
                      cursor: 'pointer'
                    }}
                  >
                    取消
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </main>
  );
}
