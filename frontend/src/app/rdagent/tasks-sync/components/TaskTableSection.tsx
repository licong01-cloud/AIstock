"use client";

import React, { useCallback, useRef, useState, useMemo } from "react";
import TaskRow from "./TaskRow";
import type { LoopDetail, SyncCandidateItem } from "./TaskRow";
import ExpandedLoopPanel from "./ExpandedLoopPanel";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export interface TaskTableSectionProps {
  pagedItems: SyncCandidateItem[];
  selected: Record<string, boolean>;
  onSelectChange: (taskId: string, checked: boolean) => void;
  onRefreshItems: () => void;
}

/**
 * 表格区域独立组件。
 *
 * 性能优化核心：
 * - TaskRow 只接收 item + isSelected + callbacks（全部引用稳定），
 *   展开/加载等高频变化状态 **不传给 TaskRow**，所以 memo 永不失效。
 * - 展开的 Loop 面板作为独立 <ExpandedLoopPanel> 渲染在表格外，
 *   通过 CSS position 定位到正确行下方。这样只有 1 个面板组件重渲染，
 *   不会触发 100 个 TaskRow 的 memo 比较。
 */
export default React.memo(function TaskTableSection({
  pagedItems,
  selected,
  onSelectChange,
  onRefreshItems,
}: TaskTableSectionProps) {
  // ── 表格内部状态 ──
  const [expandedTaskId, setExpandedTaskId] = useState<string | null>(null);
  const [loopsData, setLoopsData] = useState<Record<string, LoopDetail[]>>({});
  const [loopsLoading, setLoopsLoading] = useState<Record<string, boolean>>({});
  const [taskRefreshing, setTaskRefreshing] = useState<Record<string, boolean>>({});
  const [loopRefreshing, setLoopRefreshing] = useState<Record<string, boolean>>({});
  const [loopSyncing, setLoopSyncing] = useState<Record<string, boolean>>({});
  const [syncedLoopIds, setSyncedLoopIds] = useState<Record<string, Set<number>>>({});
  const [workspaceData, setWorkspaceData] = useState<Record<string, any>>({});
  const [workspaceLoading, setWorkspaceLoading] = useState<Record<string, boolean>>({});
  const [workspaceScanning, setWorkspaceScanning] = useState<Record<string, boolean>>({});
  const [showWorkspaceModal, setShowWorkspaceModal] = useState<string | null>(null);
  const [deleteConfirmStep, setDeleteConfirmStep] = useState<number>(0);

  // refs — 避免 useCallback 依赖
  const expandedTaskIdRef = useRef(expandedTaskId);
  expandedTaskIdRef.current = expandedTaskId;
  const loopsDataRef = useRef(loopsData);
  loopsDataRef.current = loopsData;
  const onRefreshItemsRef = useRef(onRefreshItems);
  onRefreshItemsRef.current = onRefreshItems;
  const loopFetchAbortRef = useRef<AbortController | null>(null);

  // ── 回调 ──

  const toggleEnableForSelection = useCallback(async (taskId: string, nextValue: boolean) => {
    try {
      const url = nextValue
        ? `${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/enable_for_selection`
        : `${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/disable_for_selection`;
      const res = await fetch(url, { method: "POST" });
      const payload = await res.json().catch(() => null);
      if (!res.ok) throw new Error(payload?.detail || payload?.error || "操作失败");
      onRefreshItemsRef.current();
    } catch (e: any) {
      alert(e?.message || "操作失败");
    }
  }, []);

  const openTaskDetail = useCallback((taskId: string) => {
    window.open(`/rdagent/tasks/${encodeURIComponent(taskId)}`, "_blank");
  }, []);

  const fetchSyncedLoopIds = useCallback(async (taskId: string) => {
    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/synced-loop-ids`);
      if (res.ok) {
        const data = await res.json();
        if (data.ok && Array.isArray(data.synced_loop_ids)) {
          setSyncedLoopIds((prev) => ({ ...prev, [taskId]: new Set(data.synced_loop_ids) }));
        }
      }
    } catch {
      // 静默
    }
  }, []);

  const toggleLoopDetails = useCallback(async (taskId: string) => {
    // 取消前一个进行中的请求
    if (loopFetchAbortRef.current) {
      loopFetchAbortRef.current.abort();
      loopFetchAbortRef.current = null;
    }

    if (expandedTaskIdRef.current === taskId) {
      setExpandedTaskId(null);
      return;
    }
    setExpandedTaskId(taskId);
    if (loopsDataRef.current[taskId]) {
      fetchSyncedLoopIds(taskId);
      return;
    }

    const controller = new AbortController();
    loopFetchAbortRef.current = controller;

    setLoopsLoading((prev) => ({ ...prev, [taskId]: true }));
    try {
      const res = await fetch(
        `${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/candidate-loops`,
        { signal: controller.signal },
      );
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `加载LOOP详情失败: ${res.status}`);
      }
      const data = await res.json();
      // 响应到达时，如果用户已经切换到别的 task，丢弃结果
      if (expandedTaskIdRef.current !== taskId) return;
      if (data.ok && Array.isArray(data.loops)) {
        setLoopsData((prev) => ({ ...prev, [taskId]: data.loops }));
        setLoopsLoading((prev) => ({ ...prev, [taskId]: false }));
      } else {
        throw new Error(data.error || "加载LOOP详情失败");
      }
      fetchSyncedLoopIds(taskId);
    } catch (e: any) {
      if (e?.name === "AbortError") return;
      alert(e?.message || "加载LOOP详情失败");
      setExpandedTaskId((prev) => prev === taskId ? null : prev);
      setLoopsLoading((prev) => ({ ...prev, [taskId]: false }));
    } finally {
      if (loopFetchAbortRef.current === controller) {
        loopFetchAbortRef.current = null;
      }
    }
  }, [fetchSyncedLoopIds]);

  const refreshTaskData = useCallback(async (taskId: string) => {
    if (!confirm(`确认刷新Task ${taskId} 的数据？\n\n这将清除数据库中的缓存，并从RD-Agent API重新获取最新数据。`)) {
      return;
    }
    setTaskRefreshing((prev) => ({ ...prev, [taskId]: true }));
    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/refresh`, { method: 'POST' });
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `刷新Task失败: ${res.status}`);
      }
      const data = await res.json();
      if (data.ok) {
        try {
          const loopRes = await fetch(`${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/candidate-loops`);
          if (loopRes.ok) {
            const loopData = await loopRes.json();
            if (loopData.ok && Array.isArray(loopData.loops)) {
              setLoopsData((prev) => ({ ...prev, [taskId]: loopData.loops }));
            }
          }
        } catch (_) {
          setLoopsData((prev) => {
            const newData = { ...prev };
            delete newData[taskId];
            return newData;
          });
        }
        alert(`✓ Task ${taskId} 数据已刷新\n\n清除了 ${data.deleted_loops || 0} 条旧LOOP缓存，${data.v2_updated ? 'V2信息已更新，' : ''}重新获取了 ${data.refreshed_loops || 0} 个LOOP`);
        onRefreshItemsRef.current();
      } else {
        throw new Error(data.error || "刷新Task失败");
      }
    } catch (e: any) {
      alert(`刷新Task失败: ${e?.message || "未知错误"}`);
    } finally {
      setTaskRefreshing((prev) => ({ ...prev, [taskId]: false }));
    }
  }, []);

  const refreshLoopData = useCallback(async (taskId: string) => {
    setLoopRefreshing((prev) => ({ ...prev, [taskId]: true }));
    try {
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
  }, []);

  const syncLoopFactors = useCallback(async (taskId: string, loopId: number) => {
    const key = `${taskId}_${loopId}`;
    if (!confirm(`确定将 Loop ${loopId} 的因子同步到因子库吗？\n\n这会将该 Loop 中 final_decision=True 的因子标记为 SOTA 因子并入库。`)) {
      return;
    }
    setLoopSyncing((prev) => ({ ...prev, [key]: true }));
    try {
      const res = await fetch(
        `${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/loops/${loopId}/sync-factors`,
        { method: "POST" }
      );
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "同步失败");
      setSyncedLoopIds((prev) => {
        const existing = prev[taskId] || new Set();
        return { ...prev, [taskId]: new Set([...existing, loopId]) };
      });
      alert(`Loop ${loopId} 因子同步成功: ${data.inserted || 0} 个因子入库`);
      onRefreshItemsRef.current();
    } catch (e: any) {
      alert(`同步失败: ${e?.message || "未知错误"}`);
    } finally {
      setLoopSyncing((prev) => ({ ...prev, [key]: false }));
    }
  }, []);

  // 阶段1：快速加载 log 目录信息，立即弹窗
  const loadWorkspaceInfo = useCallback(async (taskId: string) => {
    setWorkspaceLoading((prev) => ({ ...prev, [taskId]: true }));
    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/workspaces?quick=true`);
      const data = await res.json();
      setWorkspaceData((prev) => ({ ...prev, [taskId]: data }));
      setShowWorkspaceModal(taskId);
      setDeleteConfirmStep(0);
      // log目录存在时，自动启动阶段2：后台扫描 workspace
      if (data.log_dir_exists && data.workspaces_scanned === false) {
        scanWorkspaces(taskId);
      }
    } catch (error: any) {
      setWorkspaceData((prev) => ({ ...prev, [taskId]: {
        ok: false, task_id: taskId, error: error?.message || "网络错误",
        workspaces: [], total_size_mb: 0, log_dir_exists: false,
      } }));
      setShowWorkspaceModal(taskId);
      setDeleteConfirmStep(0);
    } finally {
      setWorkspaceLoading((prev) => ({ ...prev, [taskId]: false }));
    }
  }, []);

  // 阶段2：后台完整 pickle 扫描 workspace（可能耗时 1-3 分钟）
  const scanWorkspaces = useCallback(async (taskId: string) => {
    setWorkspaceScanning((prev) => ({ ...prev, [taskId]: true }));
    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}/workspaces?quick=false`);
      const data = await res.json();
      // 合并 workspace 列表到已有数据（保留 log 目录信息）
      setWorkspaceData((prev) => ({
        ...prev,
        [taskId]: {
          ...(prev[taskId] || {}),
          workspaces: data.workspaces || [],
          total_size_mb: data.total_size_mb ?? prev[taskId]?.total_size_mb ?? 0,
          workspaces_scanned: true,
          scan_error: data.error,
        },
      }));
    } catch (error: any) {
      setWorkspaceData((prev) => ({
        ...prev,
        [taskId]: {
          ...(prev[taskId] || {}),
          workspaces_scanned: true,
          scan_error: error?.message || "扫描失败",
        },
      }));
    } finally {
      setWorkspaceScanning((prev) => ({ ...prev, [taskId]: false }));
    }
  }, []);

  const deleteTask = useCallback(async (taskId: string) => {
    try {
      const res = await fetch(`${API_BASE}/rdagent/tasks/${encodeURIComponent(taskId)}`, { method: 'DELETE' });
      const data = await res.json();
      if (data.ok) {
        alert(`成功删除：${data.message}`);
        setShowWorkspaceModal(null);
        setDeleteConfirmStep(0);
        onRefreshItemsRef.current();
      } else {
        alert(`删除失败：${data.error || "未知错误"}`);
      }
    } catch (error: any) {
      alert(`删除失败：${error?.message || "网络错误"}`);
    }
  }, []);

  // ── 稳定回调包（所有 useCallback 依赖都是 []，引用永不变） ──
  const callbacks = useMemo(() => ({
    onToggleLoopDetails: toggleLoopDetails,
    onToggleEnable: toggleEnableForSelection,
    onRefreshTask: refreshTaskData,
    onLoadWorkspace: loadWorkspaceInfo,
    onOpenTaskDetail: openTaskDetail,
  }), [toggleLoopDetails, toggleEnableForSelection, refreshTaskData, loadWorkspaceInfo, openTaskDetail]);

  // ── 渲染 ──

  // 找到展开行在 pagedItems 中的索引，用于在正确位置插入面板
  const expandedItem = expandedTaskId
    ? pagedItems.find((it) => String(it.task_id || "") === expandedTaskId)
    : null;

  return (
    <>
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, tableLayout: "fixed" }}>
          <colgroup>
            <col style={{ width: 32 }} />
            <col style={{ width: 180 }} />
            <col style={{ width: 130 }} />
            <col style={{ width: 120 }} />
            <col style={{ width: 70 }} />
            <col style={{ width: 90 }} />
            <col style={{ width: 120 }} />
            <col style={{ width: "20%" }} />
            <col style={{ width: 320 }} />
          </colgroup>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>
              <th style={{ padding: 8 }}></th>
              <th style={{ padding: 8 }}>task_id</th>
              <th style={{ padding: 8 }}>节点</th>
              <th style={{ padding: 8 }}>SOTA/Alpha/特征</th>
              <th style={{ padding: 8 }}>V2对齐</th>
              <th style={{ padding: 8 }}>同步状态</th>
              <th style={{ padding: 8 }}>加入Task选股</th>
              <th style={{ padding: 8 }}>Task概要</th>
              <th style={{ padding: 8 }}>操作</th>
            </tr>
          </thead>
          <tbody>
            {(pagedItems || []).map((it) => {
              const tid = String(it.task_id || "");
              const isExp = expandedTaskId === tid;
              return (
                <React.Fragment key={tid}>
                  <TaskRow
                    item={it}
                    isSelected={!!selected[tid]}
                    isExpanded={isExp}
                    isTaskRefreshing={!!taskRefreshing[tid]}
                    isWorkspaceLoading={!!workspaceLoading[tid]}
                    onSelectChange={onSelectChange}
                    callbacks={callbacks}
                  />
                  {isExp && (
                    <ExpandedLoopPanel
                      tid={tid}
                      loops={loopsData[tid]}
                      isLoadingLoops={!!loopsLoading[tid]}
                      isLoopRefreshing={!!loopRefreshing[tid]}
                      loopSyncing={loopSyncing}
                      syncedLoopIdSet={syncedLoopIds[tid]}
                      onRefreshLoopData={refreshLoopData}
                      onSyncLoopFactors={syncLoopFactors}
                    />
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Workspace详情弹窗 */}
      {showWorkspaceModal && (
        <div style={{
          position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
          background: 'rgba(0,0,0,0.5)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          zIndex: 1000
        }}>
          <div style={{
            background: '#fff', borderRadius: 12, padding: 24,
            maxWidth: 700, width: '90%', maxHeight: '80vh',
            overflow: 'auto', boxShadow: '0 20px 25px -5px rgba(0,0,0,0.1)'
          }}>
            {deleteConfirmStep === 0 && (
              <>
                <h3 style={{ margin: '0 0 16px 0', fontSize: 18, fontWeight: 600 }}>Workspace详情</h3>
                {/* 节点不可达或log目录不存在的警告 */}
                {(!workspaceData[showWorkspaceModal]?.ok || workspaceData[showWorkspaceModal]?.log_dir_exists === false) && (
                  <div style={{ marginBottom: 12, padding: 10, background: '#fef3c7', borderRadius: 8, border: '1px solid #fde68a', fontSize: 12, color: '#92400e' }}>
                    {workspaceData[showWorkspaceModal]?.error
                      ? `节点返回错误: ${workspaceData[showWorkspaceModal].error}`
                      : 'Task的log目录在节点上不存在（可能已被手动删除或任务未在节点上创建）。仍可删除以清理DB缓存。'}
                  </div>
                )}
                <div style={{ marginBottom: 16, padding: 12, background: '#f3f4f6', borderRadius: 8 }}>
                  <div style={{ fontSize: 13, marginBottom: 4 }}><strong>Task ID:</strong> {showWorkspaceModal}</div>
                  <div style={{ fontSize: 13, marginBottom: 4 }}><strong>Task目录:</strong> {workspaceData[showWorkspaceModal]?.task_dir || '-'}</div>
                  <div style={{ fontSize: 13, marginBottom: 4 }}>
                    <strong>Log目录:</strong>{' '}
                    {workspaceData[showWorkspaceModal]?.log_dir_exists === true ? (
                      <span style={{ color: '#059669' }}>存在</span>
                    ) : (
                      <span style={{ color: '#dc2626' }}>不存在</span>
                    )}
                  </div>
                  <div style={{ fontSize: 13, marginBottom: 4 }}>
                    <strong>总大小:</strong> {workspaceData[showWorkspaceModal]?.total_size_mb?.toFixed(2) || 0} MB
                    {workspaceData[showWorkspaceModal]?.loop_count != null && (
                      <span style={{ marginLeft: 12, color: '#6b7280' }}>| Loop 数量: {workspaceData[showWorkspaceModal].loop_count}</span>
                    )}
                  </div>
                </div>
                <h4 style={{ margin: '16px 0 8px 0', fontSize: 14, fontWeight: 600 }}>Workspace列表</h4>
                {/* 扫描中状态 */}
                {workspaceScanning[showWorkspaceModal] && (
                  <div style={{ padding: 16, textAlign: 'center', background: '#eff6ff', borderRadius: 8, border: '1px solid #bfdbfe', marginBottom: 8 }}>
                    <div style={{ fontSize: 13, color: '#1d4ed8', fontWeight: 600 }}>
                      <span style={{ animation: 'pulse 1.5s infinite', display: 'inline-block' }}>●</span>
                      {' '}Workspace 扫描中（pickle 反序列化，可能需要 1-3 分钟）...
                    </div>
                  </div>
                )}
                {/* 扫描失败提示 */}
                {!workspaceScanning[showWorkspaceModal] && workspaceData[showWorkspaceModal]?.scan_error && (
                  <div style={{ padding: 10, background: '#fef3c7', borderRadius: 8, fontSize: 12, color: '#92400e', marginBottom: 8 }}>
                    Workspace 扫描失败: {workspaceData[showWorkspaceModal].scan_error}
                  </div>
                )}
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
                    style={{ padding: '8px 16px', fontSize: 13, background: '#ef4444', color: '#fff', border: 'none', borderRadius: 8, cursor: 'pointer', fontWeight: 500 }}
                  >
                    删除此Task
                  </button>
                  <button
                    onClick={() => { setShowWorkspaceModal(null); setDeleteConfirmStep(0); }}
                    style={{ padding: '8px 16px', fontSize: 13, background: '#f3f4f6', color: '#374151', border: 'none', borderRadius: 8, cursor: 'pointer' }}
                  >
                    关闭
                  </button>
                </div>
              </>
            )}
            {deleteConfirmStep === 1 && (
              <>
                <h3 style={{ margin: '0 0 16px 0', fontSize: 18, fontWeight: 600, color: '#dc2626' }}>最后确认</h3>
                <div style={{ marginBottom: 16, padding: 16, background: '#fef2f2', borderRadius: 8, border: '1px solid #fecaca' }}>
                  <p style={{ margin: '0 0 8px 0', fontSize: 14 }}><strong>确定要删除task {showWorkspaceModal} 吗？</strong></p>
                  <p style={{ margin: '0 0 8px 0', fontSize: 13 }}>这将删除所有日志和workspace目录，释放 <strong>{workspaceData[showWorkspaceModal]?.total_size_mb?.toFixed(2) || 0} MB</strong> 空间。</p>
                  <p style={{ margin: 0, fontSize: 13, color: '#dc2626', fontWeight: 600 }}>此操作不可撤销！</p>
                </div>
                <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                  <button
                    onClick={() => deleteTask(showWorkspaceModal!)}
                    style={{ padding: '8px 16px', fontSize: 13, background: '#dc2626', color: '#fff', border: 'none', borderRadius: 8, cursor: 'pointer', fontWeight: 500 }}
                  >
                    确认删除
                  </button>
                  <button
                    onClick={() => setDeleteConfirmStep(0)}
                    style={{ padding: '8px 16px', fontSize: 13, background: '#f3f4f6', color: '#374151', border: 'none', borderRadius: 8, cursor: 'pointer' }}
                  >
                    取消
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </>
  );
});
