"use client";

import { useState, useRef, useEffect, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

/** SSE 断线自动重连最大次数（实验可运行数小时，需要足够多的重连机会） */
const MAX_RECONNECT = 100;
/** 重连间隔（ms） */
const RECONNECT_DELAY = 3000;
const ACTIVE_STATUS_POLL_INTERVAL = 10000;

export interface UseExperimentSSEOptions {
  /** 是否在 SSE 断开后持续轮询最终状态（compose 模式），默认 true */
  pollAfterDisconnect?: boolean;
  /** 最大轮询次数，默认 120（约 6 分钟） */
  maxPollRetries?: number;
  /** 日志环形缓冲大小，默认 500 */
  maxLogLines?: number;
  /** SSE 断开后回调（可用于刷新列表等） */
  onDisconnect?: () => void;
}

export interface UseExperimentSSEReturn {
  /** 当前执行状态 */
  runStatus: string | null;
  /** 日志行数组 */
  runLogs: string[];
  /** 完成后的回测指标 */
  runMetrics: Record<string, any> | null;
  /** 增强诊断指标（IC序列/Loss曲线/收益曲线） */
  enhancedMetrics: Record<string, any> | null;
  /** 日志底部锚点 ref（绑定到 <div ref={logsEndRef} />） */
  logsEndRef: React.RefObject<HTMLDivElement>;
  /** 提交实验并建立 SSE：POST /experiments/{id}/run → SSE */
  startRun: (experimentId: string, nodeId?: string) => Promise<void>;
  /** 仅建立 SSE 日志连接（实验已在执行中） */
  openLogs: (experimentId: string) => void;
  /** 关闭 SSE 连接并清除日志 */
  closeLogs: () => void;
  /** 重置全部状态（新一轮配置前调用） */
  reset: () => void;
}

export function useExperimentSSE(options: UseExperimentSSEOptions = {}): UseExperimentSSEReturn {
  const {
    pollAfterDisconnect = true,
    maxPollRetries = 120,
    maxLogLines = 500,
    onDisconnect,
  } = options;

  const [runStatus, setRunStatus] = useState<string | null>(null);
  const [runLogs, setRunLogs] = useState<string[]>([]);
  const [runMetrics, setRunMetrics] = useState<Record<string, any> | null>(null);
  const [enhancedMetrics, setEnhancedMetrics] = useState<Record<string, any> | null>(null);
  const logsEndRef = useRef<HTMLDivElement>(null!);
  const eventSourceRef = useRef<EventSource | null>(null);
  const statusPollTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const abortRef = useRef(false);
  const reconnectCountRef = useRef(0);
  const onDisconnectRef = useRef(onDisconnect);
  onDisconnectRef.current = onDisconnect;

  // 自动滚动
  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [runLogs]);

  // 卸载时清理
  useEffect(() => {
    return () => {
      abortRef.current = true;
      eventSourceRef.current?.close();
      if (statusPollTimerRef.current) clearInterval(statusPollTimerRef.current);
    };
  }, []);

  const appendLog = useCallback((line: string) => {
    setRunLogs(prev => {
      const next = [...prev, line];
      return next.length > maxLogLines ? next.slice(-maxLogLines) : next;
    });
  }, [maxLogLines]);

  const stopStatusPolling = useCallback(() => {
    if (statusPollTimerRef.current) {
      clearInterval(statusPollTimerRef.current);
      statusPollTimerRef.current = null;
    }
  }, []);

  /** 获取增强指标 */
  const fetchEnhancedMetrics = useCallback(async (experimentId: string) => {
    try {
      if (abortRef.current) return;
      const res = await fetch(`${API}/quantevolver/experiments/${experimentId}/enhanced-metrics`);
      if (abortRef.current) return;
      if (!res.ok) {
        console.error(`[QE] Enhanced metrics failed for experiment ${experimentId}: ${res.status} ${res.statusText}`);
        setEnhancedMetrics({});
        return;
      }
      const data = await res.json();
      if (!abortRef.current) setEnhancedMetrics(data && typeof data === "object" ? data : {});
    } catch (e) {
      console.error(`[QE] Enhanced metrics network error for experiment ${experimentId}:`, e);
      if (!abortRef.current) setEnhancedMetrics({});
    }
  }, []);

  /** 处理实验完成 */
  const handleCompleted = useCallback((data: any, experimentId: string) => {
    stopStatusPolling();
    setRunStatus("completed");
    appendLog("[System] 实验执行完成！");
    if (data.result_metrics) {
      const m = typeof data.result_metrics === "string"
        ? JSON.parse(data.result_metrics)
        : data.result_metrics;
      setRunMetrics(m);
    }
    fetchEnhancedMetrics(experimentId);
  }, [appendLog, fetchEnhancedMetrics, stopStatusPolling]);

  /** SSE 断开后轮询最终状态 */
  const pollFinalStatus = useCallback(async (experimentId: string) => {
    for (let i = 0; i < maxPollRetries; i++) {
      if (abortRef.current) return;
      await new Promise(r => setTimeout(r, 3000));
      if (abortRef.current) return;
      try {
        const res = await fetch(`${API}/quantevolver/experiments/${experimentId}/run-status`);
        if (abortRef.current) return;
        const data = await res.json();
        if (abortRef.current) return;
        if (data.status === "completed") {
          handleCompleted(data, experimentId);
          return;
        } else if (data.status === "failed" || data.status === "timeout") {
          setRunStatus("failed");
          const detail = data.error || data.multi_alpha?.artifact_errors?.join("; ");
          appendLog(`[System] 实验执行${data.status === "timeout" ? "超时" : "失败"}${detail ? `: ${detail}` : ""}`);
          return;
        }
      } catch {
        // 网络错误，继续重试
      }
    }
    if (!abortRef.current) {
      setRunStatus("failed");
      appendLog("[Error] 状态轮询超时，请手动刷新检查");
    }
  }, [maxPollRetries, appendLog, handleCompleted]);

  /** 建立 SSE 连接（支持断线自动重连） */
  const startActiveStatusPolling = useCallback((experimentId: string) => {
    stopStatusPolling();
    const check = async () => {
      if (abortRef.current) return;
      try {
        const res = await fetch(`${API}/quantevolver/experiments/${experimentId}/run-status`);
        if (abortRef.current) return;
        const data = await res.json();
        if (abortRef.current) return;
        if (data.status === "completed") {
          eventSourceRef.current?.close();
          eventSourceRef.current = null;
          handleCompleted(data, experimentId);
          onDisconnectRef.current?.();
        } else if (data.status === "failed" || data.status === "timeout" || data.status === "interrupted") {
          stopStatusPolling();
          eventSourceRef.current?.close();
          eventSourceRef.current = null;
          setRunStatus(data.status === "interrupted" ? "interrupted" : "failed");
          const detail = data.error || data.multi_alpha?.artifact_errors?.join("; ");
          appendLog(`[System] terminal status: ${data.status}${detail ? `: ${detail}` : ""}`);
          onDisconnectRef.current?.();
        }
      } catch (e) {
        console.error(`[QE] active status poll failed for experiment ${experimentId}:`, e);
      }
    };
    statusPollTimerRef.current = setInterval(check, ACTIVE_STATUS_POLL_INTERVAL);
  }, [appendLog, handleCompleted, stopStatusPolling]);

  const connectSSE = useCallback((experimentId: string) => {
    eventSourceRef.current?.close();
    startActiveStatusPolling(experimentId);
    const sse = new EventSource(`${API}/quantevolver/experiments/${experimentId}/logs`);
    eventSourceRef.current = sse;

    sse.onmessage = (event) => {
      // 收到消息说明连接正常，重置重连计数
      reconnectCountRef.current = 0;
      if (event.data) appendLog(event.data);
    };

    sse.onerror = () => {
      sse.close();
      if (eventSourceRef.current === sse) eventSourceRef.current = null;
      if (abortRef.current) return;

      // 先查状态，决定是重连还是结束
      fetch(`${API}/quantevolver/experiments/${experimentId}/run-status`)
        .then(r => r.json())
        .then(data => {
          if (abortRef.current) return;

          if (data.status === "completed") {
            handleCompleted(data, experimentId);
          } else if (data.status === "failed" || data.status === "timeout") {
            setRunStatus("failed");
            const detail = data.error || data.multi_alpha?.artifact_errors?.join("; ");
            appendLog(`[System] 实验执行${data.status === "timeout" ? "超时" : "失败"}${detail ? `: ${detail}` : ""}`);
          } else if (data.status === "running") {
            // 实验仍在运行 → 尝试自动重连 SSE
            reconnectCountRef.current += 1;
            if (reconnectCountRef.current <= MAX_RECONNECT) {
              appendLog(`[System] 日志流断开，${RECONNECT_DELAY / 1000}秒后自动重连 (${reconnectCountRef.current}/${MAX_RECONNECT})...`);
              setTimeout(() => {
                if (!abortRef.current) connectSSE(experimentId);
              }, RECONNECT_DELAY);
              return; // 不触发 onDisconnect
            } else {
              // 重连次数耗尽，回退到轮询
              appendLog("[System] 日志流重连失败，切换到状态轮询模式...");
              if (pollAfterDisconnect) {
                pollFinalStatus(experimentId);
              }
            }
          } else {
            // 未知状态
            if (pollAfterDisconnect) {
              appendLog("[System] 日志流断开，切换到状态轮询模式...");
              pollFinalStatus(experimentId);
            } else {
              appendLog("[System] 日志流断开，实验可能仍在运行");
            }
          }
          onDisconnectRef.current?.();
        })
        .catch(() => {
          if (abortRef.current) return;
          // 查状态也失败 → 尝试重连
          reconnectCountRef.current += 1;
          if (reconnectCountRef.current <= MAX_RECONNECT) {
            appendLog(`[System] 网络异常，${RECONNECT_DELAY / 1000}秒后自动重连 (${reconnectCountRef.current}/${MAX_RECONNECT})...`);
            setTimeout(() => {
              if (!abortRef.current) connectSSE(experimentId);
            }, RECONNECT_DELAY);
          } else {
            appendLog("[Error] 网络异常且重连失败，请手动刷新");
            onDisconnectRef.current?.();
          }
        });
    };
  }, [pollAfterDisconnect, pollFinalStatus, appendLog, handleCompleted, startActiveStatusPolling]);

  /** POST /run → 建立 SSE */
  const startRun = useCallback(async (experimentId: string, nodeId?: string) => {
    abortRef.current = false;
    reconnectCountRef.current = 0;
    setRunStatus("starting");
    setRunLogs(["[System] 正在提交实验到 RDAgent..."]);
    setRunMetrics(null);
    setEnhancedMetrics(null);

    try {
      const params = new URLSearchParams({ engine_mode: "unified" });
      if (nodeId) params.set("node_id", nodeId);
      const runUrl = `${API}/quantevolver/experiments/${experimentId}/run?${params.toString()}`;
      const res = await fetch(runUrl, { method: "POST" });
      const data = await res.json();
      if (!data.ok) {
        setRunStatus("failed");
        appendLog(`[Error] 提交失败: ${data.detail || "未知错误"}`);
        throw new Error(data.detail || "提交失败");
      }
      setRunStatus("running");
      appendLog(`[System] 已提交 (task=${data.qe_task_id}, loop=${data.qe_loop_id})`);
      connectSSE(experimentId);
    } catch (e: any) {
      setRunStatus("failed");
      appendLog(`[Error] 网络错误: ${e?.message || ""}`);
    }
  }, [connectSSE, appendLog]);

  /** 仅建立 SSE（实验已在执行中） */
  const openLogs = useCallback((experimentId: string) => {
    abortRef.current = false;
    reconnectCountRef.current = 0;
    setRunLogs(["[System] 正在连接日志流..."]);
    setRunStatus("running");
    setEnhancedMetrics(null);
    connectSSE(experimentId);
  }, [connectSSE]);

  /** 关闭 SSE */
  const closeLogs = useCallback(() => {
    abortRef.current = true;
    stopStatusPolling();
    eventSourceRef.current?.close();
    eventSourceRef.current = null;
    setRunStatus(null);
    setRunLogs([]);
  }, [stopStatusPolling]);

  /** 全部重置 */
  const reset = useCallback(() => {
    abortRef.current = true;
    stopStatusPolling();
    eventSourceRef.current?.close();
    eventSourceRef.current = null;
    reconnectCountRef.current = 0;
    setRunStatus(null);
    setRunLogs([]);
    setRunMetrics(null);
    setEnhancedMetrics(null);
  }, [stopStatusPolling]);

  return {
    runStatus, runLogs, runMetrics, enhancedMetrics,
    logsEndRef, startRun, openLogs, closeLogs, reset,
  };
}
