"use client";

import React, { useEffect, useState, useCallback } from "react";
import { ArrowLeft, ChevronDown, ChevronRight, Star } from "lucide-react";
import dynamic from "next/dynamic";
import { AllStocksTable } from "../../components/AllStocksTable";
import { FactorAnalysisPanel } from "../../components/FactorAnalysisPanel";
import { StrategyConfigCard } from "../../components/StrategyConfigCard";

const IcSeriesChart = dynamic(() => import("../../components/charts/IcSeriesChart"), { ssr: false });
const ReturnCurveChart = dynamic(() => import("../../components/charts/ReturnCurveChart"), { ssr: false });
const LossCurveChart = dynamic(() => import("../../components/charts/LossCurveChart"), { ssr: false });

const API = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8001/api/v1";

function MetricBadge({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <span style={{ display: "inline-flex", flexDirection: "column", alignItems: "center", padding: "4px 12px", backgroundColor: "#f8fafc", borderRadius: 6, border: "1px solid #e2e8f0", minWidth: 80 }}>
      <span style={{ fontSize: 10, color: "#94a3b8", fontWeight: 600 }}>{label}</span>
      <span style={{ fontSize: 14, fontWeight: 700, color: color ?? "#0f172a", fontFamily: "monospace" }}>{value}</span>
    </span>
  );
}

function MetricCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ textAlign: "center", padding: 16, backgroundColor: "#f8fafc", borderRadius: 8, border: "1px solid #e2e8f0" }}>
      <div style={{ fontSize: 11, color: "#64748b", fontWeight: 600, textTransform: "uppercase" }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700, color: color ?? "#0f172a", fontFamily: "monospace", marginTop: 4 }}>{value}</div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20 }}>
      <h3 style={{ margin: "0 0 16px", fontSize: 13, fontWeight: 700, color: "#334155", textTransform: "uppercase", letterSpacing: "0.05em" }}>{title}</h3>
      {children}
    </div>
  );
}

const ACTION_COLORS: Record<string, string> = {
  initial: "#6366f1",
  factor_adjust: "#f59e0b",
  param_tune: "#3b82f6",
  model_switch: "#8b5cf6",
  factor_model_joint: "#ec4899",
};

interface Loop {
  loop_id: string;
  loop_index: number;
  status: string;
  is_sota: boolean;
  action_type?: string;
  config_json?: any;
  metrics_json?: any;
}

export default function EvolutionDetailPage({ params }: { params: { taskId: string } }) {
  const taskId = params.taskId;

  const [task, setTask] = useState<any>(null);
  const [loops, setLoops] = useState<Loop[]>([]);
  const [expandedLoops, setExpandedLoops] = useState<Set<number>>(new Set());
  const [enhancedCache, setEnhancedCache] = useState<Record<string, any>>({});
  const [loadingLoops, setLoadingLoops] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!taskId) return;
    setLoading(true);
    fetch(`${API}/quantevolver/evolution/tasks/${taskId}`)
      .then(r => {
        if (!r.ok) throw new Error(`任务 API 错误: HTTP ${r.status}`);
        return r.json();
      })
      .then(res => {
        if (res?.status === "success" && res.data) {
          setTask(res.data.task ?? res.data);
          setLoops(res.data.loops ?? []);
          const sotaLoop = (res.data.loops ?? []).find((l: Loop) => l.is_sota);
          if (sotaLoop) {
            setExpandedLoops(new Set([sotaLoop.loop_index]));
            fetchEnhancedDirect(sotaLoop.loop_id);
          }
        } else {
          setError(res?.message ?? "任务不存在");
        }
      })
      .catch(e => setError(String(e?.message ?? "加载失败，请重试")))
      .finally(() => setLoading(false));
  }, [taskId]);

  const [enhancedErrors, setEnhancedErrors] = useState<Record<string, string>>({});

  const fetchEnhancedDirect = useCallback((loopId: string) => {
    setLoadingLoops(prev => new Set(prev).add(loopId));
    fetch(`${API}/quantevolver/evolution/tasks/${taskId}/loops/${loopId}/enhanced-metrics`)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(res => {
        if (res?.status === "success" && res?.data) {
          setEnhancedCache(prev => ({ ...prev, [loopId]: res.data }));
        } else {
          console.warn(`Loop ${loopId} enhanced-metrics 返回异常:`, res?.message);
          setEnhancedErrors(prev => ({ ...prev, [loopId]: res?.message ?? "数据格式异常" }));
        }
      })
      .catch(e => {
        console.error(`Loop ${loopId} enhanced-metrics 加载失败:`, e);
        setEnhancedErrors(prev => ({ ...prev, [loopId]: String(e?.message ?? "加载失败") }));
      })
      .finally(() => setLoadingLoops(prev => { const n = new Set(prev); n.delete(loopId); return n; }));
  }, [taskId]);

  const fetchEnhanced = useCallback((loopId: string) => {
    if (enhancedCache[loopId] || loadingLoops.has(loopId)) return;
    fetchEnhancedDirect(loopId);
  }, [taskId, enhancedCache, loadingLoops, fetchEnhancedDirect]);

  const toggleLoop = (loopIndex: number, loopId: string) => {
    setExpandedLoops(prev => {
      const next = new Set(prev);
      if (next.has(loopIndex)) {
        next.delete(loopIndex);
      } else {
        next.add(loopIndex);
        fetchEnhanced(loopId);
      }
      return next;
    });
  };

  if (loading) {
    return <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", backgroundColor: "#f1f5f9" }}><div style={{ fontSize: 14, color: "#64748b" }}>加载中...</div></div>;
  }
  if (error || !task) {
    return <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", backgroundColor: "#f1f5f9" }}><div style={{ fontSize: 14, color: "#ef4444" }}>{error ?? "任务数据加载失败"}</div></div>;
  }

  const fmtPct = (v: number | null | undefined) => v != null ? (v * 100).toFixed(2) + "%" : "-";
  const fmtNum = (v: number | null | undefined, d = 4) => v != null ? v.toFixed(d) : "-";

  const sotaCount = loops.filter(l => l.is_sota).length;
  const completedCount = loops.filter(l => l.status === "completed").length;

  return (
    <div style={{ minHeight: "100vh", backgroundColor: "#f1f5f9", padding: "24px 32px" }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 24 }}>
        <button onClick={() => window.close()} style={{ display: "flex", alignItems: "center", gap: 6, padding: "6px 12px", borderRadius: 6, border: "1px solid #e2e8f0", backgroundColor: "#fff", cursor: "pointer", fontSize: 13, color: "#475569" }}>
          <ArrowLeft size={14} /> 关闭
        </button>
        <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: "#0f172a" }}>
          {task.task_name ?? task.task_id}
        </h1>
        <span style={{ padding: "2px 10px", borderRadius: 12, fontSize: 11, fontWeight: 600, backgroundColor: task.status === "completed" ? "#eff6ff" : task.status === "running" ? "#f0fdf4" : "#fef2f2", color: task.status === "completed" ? "#3b82f6" : task.status === "running" ? "#22c55e" : "#ef4444" }}>
          {task.status}
        </span>
        <span style={{ fontSize: 12, color: "#64748b" }}>共 {loops.length} Loops | 完成 {completedCount} | SOTA {sotaCount}</span>
      </div>

      {/* Loop 手风琴 */}
      <div style={{ display: "flex", flexDirection: "column", gap: 8, maxWidth: 1200 }}>
        {loops.map(loop => {
          const isExpanded = expandedLoops.has(loop.loop_index);
          const m = loop.metrics_json ?? {};
          const actionType = loop.action_type ?? m.action_type ?? "initial";
          const actionColor = ACTION_COLORS[actionType] ?? "#64748b";
          const em = enhancedCache[loop.loop_id];
          const isLoading = loadingLoops.has(loop.loop_id);
          const td = em?.trade_diagnostics ?? {};

          return (
            <div key={loop.loop_id} style={{ borderRadius: 8, border: `1px solid ${loop.is_sota ? "#f59e0b" : "#e2e8f0"}`, backgroundColor: "#fff", overflow: "hidden", boxShadow: loop.is_sota ? "0 0 0 1px #f59e0b" : undefined }}>
              {/* 收起状态 — 概要行 */}
              <div
                onClick={() => toggleLoop(loop.loop_index, loop.loop_id)}
                style={{ display: "flex", alignItems: "center", gap: 12, padding: "12px 16px", cursor: "pointer", userSelect: "none" }}
              >
                {isExpanded ? <ChevronDown size={16} color="#64748b" /> : <ChevronRight size={16} color="#64748b" />}
                <span style={{ fontWeight: 700, fontSize: 13, color: "#0f172a", minWidth: 60 }}>Loop {loop.loop_index}</span>
                <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 600, backgroundColor: actionColor + "18", color: actionColor }}>{actionType}</span>
                {loop.is_sota && <Star size={14} color="#f59e0b" fill="#f59e0b" />}
                <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 600, backgroundColor: loop.status === "completed" ? "#f0fdf4" : loop.status === "failed" ? "#fef2f2" : "#fffbeb", color: loop.status === "completed" ? "#22c55e" : loop.status === "failed" ? "#ef4444" : "#f59e0b" }}>{loop.status}</span>
                <div style={{ flex: 1 }} />
                <MetricBadge label="IC" value={fmtNum(m.ic)} />
                <MetricBadge label="Sharpe" value={fmtNum(m.information_ratio ?? m.sharpe, 2)} />
                <MetricBadge label="年化" value={fmtPct(m.annualized_return)} color={(m.annualized_return ?? 0) >= 0 ? "#e53935" : "#22a35a"} />
                <MetricBadge label="MDD" value={fmtPct(m.max_drawdown)} color="#22a35a" />
              </div>

              {/* 展开状态 — 完整详情 */}
              {isExpanded && (
                <div style={{ borderTop: "1px solid #e2e8f0", padding: 20, display: "flex", flexDirection: "column", gap: 16 }}>
                  {isLoading && <div style={{ textAlign: "center", padding: 40, color: "#94a3b8" }}>加载增强指标中...</div>}

                  {!isLoading && enhancedErrors[loop.loop_id] && (
                    <div style={{ padding: "10px 16px", backgroundColor: "#fef2f2", border: "1px solid #ef4444", borderRadius: 8, fontSize: 12, color: "#991b1b" }}>
                      增强指标加载失败: {enhancedErrors[loop.loop_id]}
                    </div>
                  )}

                  {!isLoading && (
                    <>
                      {/* 策略与执行配置 */}
                      {loop.config_json && (
                        <StrategyConfigCard source={{ loopConfig: loop.config_json }} />
                      )}

                      {/* 核心指标 */}
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12 }}>
                        <MetricCard label="IC" value={fmtNum(m.ic)} />
                        <MetricCard label="ICIR" value={fmtNum(m.icir)} />
                        <MetricCard label="Rank IC" value={fmtNum(m.rank_ic)} />
                        <MetricCard label="年化收益" value={fmtPct(m.annualized_return)} color={(m.annualized_return ?? 0) >= 0 ? "#e53935" : "#22a35a"} />
                        <MetricCard label="最大回撤" value={fmtPct(m.max_drawdown)} color="#22a35a" />
                      </div>

                      {/* 交易效率 */}
                      {td.avg_turnover != null && (
                        <Section title="交易效率">
                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16 }}>
                            <MetricCard label="平均换手率" value={td.avg_turnover?.toFixed(3) ?? "-"} />
                            <MetricCard label="成本侵蚀(年化)" value={td.cost_drag_annualized != null ? (td.cost_drag_annualized * 100).toFixed(1) + "%" : "-"} />
                            <MetricCard label="日均交易笔数" value={td.daily_trade_count_avg?.toFixed(1) ?? "-"} />
                          </div>
                        </Section>
                      )}

                      {/* 全部持仓股票（支持排序） */}
                      {em?.all_stocks && em.all_stocks.length > 0 ? (
                        <AllStocksTable stocks={em.all_stocks} stockTrades={em?.stock_trades} />
                      ) : (em?.top_stocks || em?.bottom_stocks) ? (
                        <AllStocksTable
                          stocks={[...(em?.top_stocks ?? []), ...(em?.bottom_stocks ?? [])]}
                          stockTrades={em?.stock_trades}
                        />
                      ) : null}

                      {/* 因子贡献度 */}
                      {(em?.factor_analysis?.feature_importance || em?.feature_importance) && (
                        <FactorAnalysisPanel featureImportance={em.factor_analysis?.feature_importance ?? em.feature_importance} />
                      )}

                      {/* IC 诊断 */}
                      {em?.dates && em?.ic_series && (
                        <Section title="IC 诊断">
                          <IcSeriesChart dates={em.dates} ic_series={em.ic_series} rank_ic_series={em.rank_ic_series} ic_rolling_30d_mean={em.ic_rolling_30d_mean} ic_rolling_30d_std={em.ic_rolling_30d_std} ic_positive_ratio={em.ic_positive_ratio} />
                        </Section>
                      )}

                      {/* 收益曲线 */}
                      {em?.dates && (em?.cumulative_excess_no_cost || em?.cumulative_excess_with_cost) && (
                        <Section title="收益曲线">
                          <ReturnCurveChart dates={em.dates} cumulative_excess_no_cost={em.cumulative_excess_no_cost} cumulative_excess_with_cost={em.cumulative_excess_with_cost} cumulative_benchmark={em.cumulative_benchmark} drawdown_series={em.drawdown_series} />
                        </Section>
                      )}

                      {/* 训练过程 */}
                      {em?.train_loss_curve && (
                        <Section title="训练过程">
                          <LossCurveChart train_loss_curve={em.train_loss_curve} val_loss_curve={em.val_loss_curve} best_epoch={em.best_epoch} overfit_ratio={em.overfit_ratio} convergence_ratio={em.convergence_ratio} />
                        </Section>
                      )}

                      {/* Agent 诊断 */}
                      {m.agent_analysis && (
                        <Section title="Agent 诊断">
                          <pre style={{ fontSize: 11, color: "#475569", whiteSpace: "pre-wrap", wordBreak: "break-word", maxHeight: 300, overflow: "auto", backgroundColor: "#f8fafc", padding: 12, borderRadius: 6 }}>
                            {typeof m.agent_analysis === "string" ? m.agent_analysis : JSON.stringify(m.agent_analysis, null, 2)}
                          </pre>
                        </Section>
                      )}
                    </>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
