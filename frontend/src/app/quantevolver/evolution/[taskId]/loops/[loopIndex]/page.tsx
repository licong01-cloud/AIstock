"use client";

import React, { useEffect, useState } from "react";
import { ArrowLeft } from "lucide-react";
import dynamic from "next/dynamic";
import { AllStocksTable } from "../../../../components/AllStocksTable";
import { FactorAnalysisPanel } from "../../../../components/FactorAnalysisPanel";
import { StrategyConfigCard } from "../../../../components/StrategyConfigCard";
import { PaperV2ApiError, strategyPackageApi } from "@/lib/paper-v2/api";
import type { JsonObject } from "@/lib/paper-v2/types";

const IcSeriesChart = dynamic(() => import("../../../../components/charts/IcSeriesChart"), { ssr: false });
const ReturnCurveChart = dynamic(() => import("../../../../components/charts/ReturnCurveChart"), { ssr: false });
const LossCurveChart = dynamic(() => import("../../../../components/charts/LossCurveChart"), { ssr: false });

const API = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8001/api/v1";

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

function apiErrorMessage(error: unknown): string {
  if (error instanceof PaperV2ApiError) return error.message;
  if (error instanceof Error) return error.message;
  return String(error || "unknown error");
}

export default function LoopDetailPage({ params }: { params: { taskId: string; loopIndex: string } }) {
  const { taskId, loopIndex: loopIndexStr } = params;
  const loopIndex = parseInt(loopIndexStr, 10);

  const [loopData, setLoopData] = useState<any>(null);
  const [enhanced, setEnhanced] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [enhancedError, setEnhancedError] = useState<string | null>(null);
  const [candidateBusy, setCandidateBusy] = useState(false);
  const [candidateMessage, setCandidateMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!taskId || isNaN(loopIndex)) return;
    setLoading(true);

    const fetchTask = fetch(`${API}/quantevolver/evolution/tasks/${taskId}`)
      .then(r => r.ok ? r.json() : null)
      .catch(() => null);

    const loopId = `Loop${loopIndex}`;
    const fetchEnhanced = fetch(`${API}/quantevolver/evolution/tasks/${taskId}/loops/${loopId}/enhanced-metrics`)
      .then(r => r.ok ? r.json() : null)
      .catch(() => null);

    Promise.all([fetchTask, fetchEnhanced])
      .then(([taskRes, enhRes]) => {
        const taskData = taskRes?.data || taskRes;
        const loops = taskData?.loops || [];
        const loop = loops.find((l: any) => l.loop_index === loopIndex);
        if (loop) {
          setLoopData(loop);
        } else {
          setError(`Loop ${loopIndex} not found`);
        }

        const raw = (enhRes?.status === "success" && enhRes?.data) ? enhRes.data : enhRes;
        if (raw && typeof raw === "object" && (raw.top_stocks || raw.ic_diagnostics || raw.dates)) {
          const hasData = (v: any) => v != null && (!Array.isArray(v) || v.length > 0);
          const isFlat = hasData(raw.dates) || hasData(raw.ic_series) || hasData(raw.top_stocks) || hasData(raw.train_loss_curve);
          if (isFlat) {
            setEnhanced(raw);
          } else {
            const flat: any = {};
            if (raw.ic_diagnostics) Object.assign(flat, raw.ic_diagnostics);
            if (raw.return_curves) {
              const { dates: rcDates, ...rcRest } = raw.return_curves;
              Object.assign(flat, rcRest);
              flat.return_dates = rcDates;
            }
            if (raw.training_diagnostics) Object.assign(flat, raw.training_diagnostics);
            for (const k of ["top_stocks", "bottom_stocks", "all_stocks", "stock_trades", "trade_diagnostics", "prediction_diagnostics", "factor_analysis", "absolute_returns", "summary"]) {
              if (raw[k]) flat[k] = raw[k];
            }
            setEnhanced(flat);
          }
        } else {
          setEnhancedError("enhanced metrics unavailable");
        }
      })
      .catch(e => setError(String(e?.message ?? "load failed")))
      .finally(() => setLoading(false));
  }, [taskId, loopIndex]);

  if (loading) {
    return <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", backgroundColor: "#f1f5f9" }}><div style={{ fontSize: 14, color: "#64748b" }}>loading...</div></div>;
  }
  if (error || !loopData) {
    return <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", backgroundColor: "#f1f5f9" }}><div style={{ fontSize: 14, color: "#ef4444" }}>{error ?? "data load failed"}</div></div>;
  }

  const metrics = loopData.metrics_json || {};
  const config = (() => {
    if (!loopData.config_json) return {};
    if (typeof loopData.config_json === "string") {
      try { return JSON.parse(loopData.config_json); } catch { return {}; }
    }
    return loopData.config_json;
  })();
  const em = enhanced;
  const summary = em?.summary ?? {};
  const td = em?.trade_diagnostics ?? {};

  const fmtPct = (v: number | null | undefined) => v != null ? (v * 100).toFixed(2) + "%" : "-";
  const fmtNum = (v: number | null | undefined, d = 4) => v != null ? v.toFixed(d) : "-";
  const fmtMoney = (v: number | null | undefined) => {
    if (v == null) return "-";
    const abs = Math.abs(v);
    if (abs >= 1e8) return (v / 1e8).toFixed(4) + "\u4ebf";
    if (abs >= 1e4) return (v / 1e4).toFixed(2) + "\u4e07";
    return v.toFixed(2) + "\u5143";
  };

  const factorList: string[] = config.factor_list || [];
  const loopIdForCandidate = String(loopData.loop_id || loopData.qe_loop_id || loopData.id || `Loop${loopIndex}`);

  const addToCandidatePackages = async () => {
    setCandidateBusy(true);
    setCandidateMessage(null);
    try {
      const candidate = await strategyPackageApi.createCandidateFromQELoop({
        qe_task_id: taskId,
        qe_loop_id: loopIdForCandidate,
        experiment_id: loopData.experiment_id ?? null,
        created_by: "quantevolver_loop_detail",
        display_name: `${taskId} / Loop ${loopIndex}`,
        snapshot_config: {
          source_ui: "quantevolver_loop_detail",
          qe_task_id: taskId,
          qe_loop_id: loopIdForCandidate,
          loop_index: loopIndex,
          loop_data: loopData,
          loop_config: config,
          enhanced_summary: summary,
        } as JsonObject,
        factor_manifest: {
          factor_list: factorList,
          factor_count: factorList.length,
        },
        model_manifest: {
          model_id: config.model_id ?? null,
          model_config: config.model_config ?? null,
          training_config: config.training_config ?? null,
          missing_reproducibility_items: config.seed == null ? ["seed"] : [],
        },
        strategy_manifest: {
          strategy_id: config.strategy_id ?? null,
          action_type: loopData.action_type ?? null,
          daily_strategy_config: config.daily_strategy_config ?? null,
          minute_execution_config: config.minute_execution_config ?? null,
          tail_handling_config: config.tail_handling_config ?? null,
          platform_runtime_boundary: "HMM/ST/PIT/event signals are Paper v2 platform capabilities, not package assets",
        },
        metric_snapshot: {
          ...(metrics || {}),
          enhanced_summary: summary,
        } as JsonObject,
        artifact_refs: {
          enhanced_metrics_available: Boolean(enhanced),
          enhanced_metrics_endpoint: `/quantevolver/evolution/tasks/${taskId}/loops/${loopIdForCandidate}/enhanced-metrics`,
        },
        completeness: {
          candidate_snapshot_created: true,
          strategy_package_manifest_available: Boolean(config.strategy_package_manifest),
          missing_items: config.strategy_package_manifest ? [] : ["strategy_package_manifest"],
        },
        eligibility: {
          candidate_only: true,
          can_enter_selection_or_paper_after_package_validation: true,
          live_approval_reserved: false,
        },
        audit_context: {
          manual_action: true,
          ui_route: `/quantevolver/evolution/${taskId}/loops/${loopIndex}`,
          design_doc: "docs/architecture/paper_v2_qe_candidate_strategy_warehouse_design_20260512.md",
          created_at: new Date().toISOString(),
        },
        manual_action: true,
      });
      setCandidateMessage(`已加入候选策略包: ${candidate.candidate_id}`);
    } catch (e) {
      setCandidateMessage(`加入候选策略包失败: ${apiErrorMessage(e)}`);
    } finally {
      setCandidateBusy(false);
    }
  };

  return (
    <div style={{ minHeight: "100vh", backgroundColor: "#f1f5f9", padding: "24px 32px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 24 }}>
        <a href={`/quantevolver/evolution/${taskId}`}
          style={{ display: "flex", alignItems: "center", gap: 6, padding: "6px 12px", borderRadius: 6, border: "1px solid #e2e8f0", backgroundColor: "#fff", cursor: "pointer", fontSize: 13, color: "#475569", textDecoration: "none" }}>
          <ArrowLeft size={14} /> 返回演进任务
        </a>
        <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: "#0f172a" }}>
          {taskId} / Loop {loopIndex}
        </h1>
        {loopData.is_sota && <span style={{ padding: "2px 10px", borderRadius: 12, fontSize: 11, fontWeight: 600, backgroundColor: "#fef3c7", color: "#d97706" }}>SOTA</span>}
        <span style={{
          padding: "2px 10px", borderRadius: 12, fontSize: 11, fontWeight: 600,
          backgroundColor: loopData.status === "completed" ? "#eff6ff" : loopData.status === "running" ? "#f0fdf4" : "#fef2f2",
          color: loopData.status === "completed" ? "#3b82f6" : loopData.status === "running" ? "#22c55e" : "#ef4444",
        }}>
          {loopData.status}
        </span>
        <button
          disabled={candidateBusy}
          onClick={addToCandidatePackages}
          style={{ marginLeft: "auto", padding: "7px 12px", borderRadius: 8, border: "1px solid #2563eb", background: "#eff6ff", color: "#1d4ed8", cursor: candidateBusy ? "not-allowed" : "pointer", fontSize: 13, fontWeight: 700 }}
        >
          {candidateBusy ? "加入中..." : "加入候选策略包"}
        </button>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 20, maxWidth: 1200 }}>
        {candidateMessage && (
          <div style={{ padding: "10px 16px", backgroundColor: "#f8fafc", border: "1px solid #cbd5e1", borderRadius: 8, fontSize: 12, color: "#334155" }}>
            {candidateMessage}
          </div>
        )}
        {enhancedError && (
          <div style={{ padding: "10px 16px", backgroundColor: "#fffbeb", border: "1px solid #f59e0b", borderRadius: 8, fontSize: 12, color: "#92400e" }}>
            {enhancedError}
          </div>
        )}

        <Section title="Loop 概览">
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 12 }}>
            <MetricCard label="模型" value={config.model_id ?? "-"} />
            <MetricCard label="因子数" value={String(factorList.length)} />
            <MetricCard label="策略" value={config.strategy_id ?? "-"} />
            <MetricCard label="Action" value={loopData.action_type ?? "-"} />
          </div>
        </Section>

        {/* § 策略与执行配置 */}
        <StrategyConfigCard source={{ loopConfig: config }} />

        {(metrics.IC != null || metrics.sharpe != null) && (
          <Section title="超额收益指标 (相对沪深300)">
            <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12 }}>
              <MetricCard label="IC" value={fmtNum(metrics.IC)} />
              <MetricCard label="ICIR" value={fmtNum(metrics.ICIR)} />
              <MetricCard label="Rank IC" value={fmtNum(metrics.Rank_IC)} />
              <MetricCard label="超额年化" value={fmtPct(metrics.annualized_return)} color={(metrics.annualized_return ?? 0) >= 0 ? "#e53935" : "#22a35a"} />
              <MetricCard label="超额最大回撤" value={fmtPct(metrics.max_drawdown)} color="#22a35a" />
              <MetricCard label="Sharpe" value={fmtNum(metrics.sharpe, 2)} />
              <MetricCard label="年化收益(无费)" value={fmtPct(metrics.annualized_return_no_cost)} />
              <MetricCard label="最大回撤(无费)" value={fmtPct(metrics.max_drawdown_no_cost)} />
              <MetricCard label="Sharpe(无费)" value={fmtNum(metrics.sharpe_no_cost, 2)} />
              <MetricCard label="日均超额" value={fmtPct(metrics.daily_return)} />
            </div>
          </Section>
        )}

        {(em?.absolute_returns || em?.factor_analysis?.absolute_returns) && (() => {
          const ar = em.absolute_returns ?? em.factor_analysis?.absolute_returns ?? {};
          return ar.initial_capital != null ? (
            <Section title="绝对收益 (分钟线真实回测)">
              <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12 }}>
                <MetricCard label="初始资金" value={fmtMoney(ar.initial_capital)} />
                <MetricCard label="最终总资产" value={fmtMoney(ar.final_total_value)} color={(ar.total_return ?? 0) >= 0 ? "#e53935" : "#22a35a"} />
                <MetricCard label="年化复利(CAGR)" value={ar.cagr != null ? (ar.cagr * 100).toFixed(2) + "%" : "-"} color={(ar.cagr ?? 0) >= 0 ? "#e53935" : "#22a35a"} />
                <MetricCard label="绝对最大回撤" value={ar.max_drawdown != null ? (ar.max_drawdown * 100).toFixed(2) + "%" : "-"} color="#22a35a" />
                <MetricCard label="绝对夏普" value={ar.sharpe != null ? ar.sharpe.toFixed(2) : "-"} />
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12, marginTop: 12 }}>
                <MetricCard label="总收益率" value={ar.total_return != null ? (ar.total_return * 100).toFixed(2) + "%" : "-"} color={(ar.total_return ?? 0) >= 0 ? "#e53935" : "#22a35a"} />
                <MetricCard label="年化波动率" value={ar.annualized_volatility != null ? (ar.annualized_volatility * 100).toFixed(2) + "%" : "-"} />
                <MetricCard label="资金利用率" value={ar.avg_cash_ratio != null ? ((1 - ar.avg_cash_ratio) * 100).toFixed(1) + "%" : "-"} />
                <MetricCard label="最终现金" value={fmtMoney(ar.final_cash)} />
                <MetricCard label="最终股票市值" value={fmtMoney(ar.final_stock_value)} />
              </div>
              {ar.max_drawdown_date && (
                <div style={{ marginTop: 8, fontSize: 10, color: "#94a3b8" }}>
                  最大回撤日: {ar.max_drawdown_date} · 回测天数: {ar.n_trading_days ?? "-"}天
                </div>
              )}
            </Section>
          ) : null;
        })()}

        {(td.avg_turnover != null || em?.absolute_returns?.avg_cash_ratio != null) && (
          <Section title="交易效率诊断">
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
              <MetricCard label="日均换手率" value={td.avg_turnover?.toFixed(4) ?? "-"} />
              <MetricCard label="总换手率" value={td.total_turnover?.toFixed(2) ?? "-"} />
              <MetricCard label="年化换手率" value={td.annualized_turnover?.toFixed(2) ?? "-"} />
              <MetricCard label="日均交易笔数" value={td.daily_trade_count_avg?.toFixed(1) ?? "-"} />
              <MetricCard label="成本侵蚀(年化)" value={td.cost_drag_annualized != null ? (td.cost_drag_annualized * 100).toFixed(1) + "%" : "-"} />
              <MetricCard label="资金利用率" value={em?.absolute_returns?.avg_cash_ratio != null ? ((1 - em.absolute_returns.avg_cash_ratio) * 100).toFixed(1) + "%" : "-"} />
            </div>
          </Section>
        )}

        {em?.all_stocks && em.all_stocks.length > 0 ? (
          <AllStocksTable stocks={em.all_stocks} stockTrades={em?.stock_trades} />
        ) : (em?.top_stocks || em?.bottom_stocks) ? (
          <AllStocksTable
            stocks={[...(em?.top_stocks ?? []), ...(em?.bottom_stocks ?? [])]}
            stockTrades={em?.stock_trades}
          />
        ) : null}

        {(em?.factor_analysis?.feature_importance || em?.feature_importance) && (
          <FactorAnalysisPanel featureImportance={em.factor_analysis?.feature_importance ?? em.feature_importance} />
        )}

        {em?.dates && em?.ic_series && (
          <Section title="IC 诊断">
            <IcSeriesChart dates={em.dates} ic_series={em.ic_series} rank_ic_series={em.rank_ic_series} ic_rolling_30d_mean={em.ic_rolling_30d_mean} ic_rolling_30d_std={em.ic_rolling_30d_std} ic_positive_ratio={em.ic_positive_ratio} />
          </Section>
        )}

        {(em?.return_dates || em?.dates) && (em?.cumulative_excess_no_cost || em?.cumulative_excess_with_cost) && (
          <Section title="收益曲线">
            <ReturnCurveChart dates={em.return_dates ?? em.dates} cumulative_excess_no_cost={em.cumulative_excess_no_cost} cumulative_excess_with_cost={em.cumulative_excess_with_cost} cumulative_benchmark={em.cumulative_benchmark} drawdown_series={em.drawdown_series} />
          </Section>
        )}

        {em?.train_loss_curve && (
          <Section title="训练过程">
            <LossCurveChart train_loss_curve={em.train_loss_curve} val_loss_curve={em.val_loss_curve} best_epoch={em.best_epoch} overfit_ratio={em.overfit_ratio} convergence_ratio={em.convergence_ratio} />
          </Section>
        )}
      </div>
    </div>
  );
}
