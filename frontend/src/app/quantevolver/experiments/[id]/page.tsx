"use client";

import React, { useEffect, useState } from "react";
import { ArrowLeft } from "lucide-react";
import dynamic from "next/dynamic";
import { AllStocksTable } from "../../components/AllStocksTable";
import { FactorAnalysisPanel } from "../../components/FactorAnalysisPanel";
import { StrategyConfigCard } from "../../components/StrategyConfigCard";

const IcSeriesChart = dynamic(() => import("../../components/charts/IcSeriesChart"), { ssr: false });
const ReturnCurveChart = dynamic(() => import("../../components/charts/ReturnCurveChart"), { ssr: false });
const LossCurveChart = dynamic(() => import("../../components/charts/LossCurveChart"), { ssr: false });

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

export default function ExperimentDetailPage({ params }: { params: { id: string } }) {
  const experimentId = params.id;

  const [experiment, setExperiment] = useState<any>(null);
  const [enhanced, setEnhanced] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [enhancedError, setEnhancedError] = useState<string | null>(null);

  useEffect(() => {
    if (!experimentId) return;
    setLoading(true);

    // 尝试从 DB 获取实验基础信息
    const fetchExperiment = fetch(`${API}/quantevolver/experiments/${experimentId}`)
      .then(r => r.ok ? r.json() : null)
      .catch(() => null);

    // 尝试从 DB 获取 enhanced-metrics
    const fetchEnhancedFromDB = fetch(`${API}/quantevolver/experiments/${experimentId}/enhanced-metrics`)
      .then(r => r.ok ? r.json() : null)
      .catch(() => null);

    // Fallback: 从 RDAgent 直接获取（独立实验 = task_id/Loop1）
    const fetchEnhancedFromRDAgent = fetch(`${API}/quantevolver/evolution/tasks/${experimentId}/loops/${experimentId}_Loop1/enhanced-metrics`)
      .then(r => r.ok ? r.json() : null)
      .catch(() => null);

    Promise.all([fetchExperiment, fetchEnhancedFromDB, fetchEnhancedFromRDAgent])
      .then(([expRes, enhDbRes, enhRdRes]) => {
        // 实验基础信息
        if (expRes?.status === "success" && expRes?.data) {
          setExperiment(expRes.data);
        } else if (expRes?.experiment) {
          setExperiment(expRes.experiment);
        } else {
          // DB 中没有，构造最小实验对象
          setExperiment({ experiment_id: experimentId, experiment_name: experimentId, status: "standalone" });
        }

        // Enhanced metrics: 优先 DB（已展平），fallback RDAgent（嵌套需展平）
        // 后端 /enhanced-metrics 端点返回已展平数据（dates/ic_series/top_stocks 在顶层，无 status/data 包装）
        const hasData = (v: any) => v != null && (!Array.isArray(v) || v.length > 0);
        const isValidFlat = (obj: any) =>
          obj && typeof obj === "object" && !obj.detail &&
          (hasData(obj.dates) || hasData(obj.ic_series) || hasData(obj.top_stocks) || hasData(obj.feature_importance) || hasData(obj.train_loss_curve));

        if (isValidFlat(enhDbRes)) {
          // DB 端点返回的已展平数据，直接使用
          setEnhanced(enhDbRes);
        } else {
          // Fallback: RDAgent 返回嵌套结构，需要展平
          const raw = (enhRdRes?.status === "success" && enhRdRes?.data)
            ? enhRdRes.data
            : enhRdRes;

          if (raw && typeof raw === "object" && (raw.top_stocks || raw.ic_diagnostics)) {
            const flat: any = {};
            // 展平 ic_diagnostics（含 dates/ic_series 等）
            if (raw.ic_diagnostics) {
              Object.assign(flat, raw.ic_diagnostics);
            }
            // return_curves 的 dates 可能不同于 IC 的 dates
            if (raw.return_curves) {
              const { dates: rcDates, ...rcRest } = raw.return_curves;
              Object.assign(flat, rcRest);
              flat.return_dates = rcDates;
            }
            if (raw.training_diagnostics) {
              Object.assign(flat, raw.training_diagnostics);
            }
            // 顶层字段直接复制
            for (const k of ["top_stocks", "bottom_stocks", "all_stocks", "stock_trades", "trade_diagnostics", "prediction_diagnostics", "factor_analysis", "absolute_returns", "summary"]) {
              if (raw[k]) flat[k] = raw[k];
            }
            setEnhanced(flat);
          } else {
            setEnhancedError("增强指标不可用（DB 和 RDAgent 均未返回数据）");
          }
        }
      })
      .catch(e => setError(String(e?.message ?? "加载失败，请重试")))
      .finally(() => setLoading(false));
  }, [experimentId]);

  if (loading) {
    return (
      <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", backgroundColor: "#f1f5f9" }}>
        <div style={{ fontSize: 14, color: "#64748b" }}>加载中...</div>
      </div>
    );
  }

  if (error || !experiment) {
    return (
      <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", backgroundColor: "#f1f5f9" }}>
        <div style={{ fontSize: 14, color: "#ef4444" }}>{error ?? "实验数据加载失败"}</div>
      </div>
    );
  }

  const exp = experiment;
  const em = enhanced;
  const summary = em?.summary ?? {};
  const td = em?.trade_diagnostics ?? {};

  const fmtPct = (v: number | null | undefined) => v != null ? (v * 100).toFixed(2) + "%" : "-";
  const fmtNum = (v: number | null | undefined, d = 4) => v != null ? v.toFixed(d) : "-";

  const fmtMoney = (v: number | null | undefined) => {
    if (v == null) return "-";
    const abs = Math.abs(v);
    if (abs >= 1e8) return (v / 1e8).toFixed(4) + "亿";
    if (abs >= 1e4) return (v / 1e4).toFixed(2) + "万";
    return v.toFixed(2) + "元";
  };

  return (
    <div style={{ minHeight: "100vh", backgroundColor: "#f1f5f9", padding: "24px 32px" }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 24 }}>
        <button
          onClick={() => window.close()}
          style={{ display: "flex", alignItems: "center", gap: 6, padding: "6px 12px", borderRadius: 6, border: "1px solid #e2e8f0", backgroundColor: "#fff", cursor: "pointer", fontSize: 13, color: "#475569" }}
        >
          <ArrowLeft size={14} /> 关闭
        </button>
        <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: "#0f172a" }}>
          {exp.experiment_name ?? exp.experiment_id}
        </h1>
        <span style={{
          padding: "2px 10px", borderRadius: 12, fontSize: 11, fontWeight: 600,
          backgroundColor: exp.status === "completed" ? "#eff6ff" : exp.status === "running" ? "#f0fdf4" : "#fef2f2",
          color: exp.status === "completed" ? "#3b82f6" : exp.status === "running" ? "#22c55e" : "#ef4444",
        }}>
          {exp.status}
        </span>
        {exp.created_at && <span style={{ fontSize: 12, color: "#94a3b8" }}>{new Date(exp.created_at).toLocaleString("zh-CN")}</span>}
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 20, maxWidth: 1200 }}>
        {/* 增强指标加载警告 */}
        {enhancedError && (
          <div style={{ padding: "10px 16px", backgroundColor: "#fffbeb", border: "1px solid #f59e0b", borderRadius: 8, fontSize: 12, color: "#92400e" }}>
            增强指标加载失败: {enhancedError}（Top10/图表等数据不可用）
          </div>
        )}
        {/* § 概览 */}
        <Section title="实验概览">
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 12 }}>
            <MetricCard label="模型" value={exp.model_id ?? "-"} />
            <MetricCard label="因子数" value={exp.factor_names ? String(exp.factor_names.length) : "-"} />
            <MetricCard label="策略" value={exp.strategy_id ?? "-"} />
            <MetricCard label="状态" value={exp.status ?? "-"} />
          </div>
        </Section>

        {/* § 策略与执行配置 */}
        <StrategyConfigCard source={{ experiment: exp }} />

        {/* § 核心指标 (超额收益 — 相对沪深300) */}
        {(exp.ic != null || summary.ic != null) && (
          <Section title="超额收益指标 (相对沪深300)">
            <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12 }}>
              <MetricCard label="IC" value={fmtNum(exp.ic ?? summary.ic)} />
              <MetricCard label="ICIR" value={fmtNum(exp.icir ?? summary.icir)} />
              <MetricCard label="Rank IC" value={fmtNum(exp.rank_ic ?? summary.rank_ic)} />
              <MetricCard label="超额年化" value={fmtPct(exp.annualized_return ?? summary.annualized_return)} color={((exp.annualized_return ?? summary.annualized_return) ?? 0) >= 0 ? "#e53935" : "#22a35a"} />
              <MetricCard label="超额最大回撤" value={fmtPct(exp.max_drawdown ?? summary.max_drawdown)} color="#22a35a" />
              <MetricCard label="信息比率" value={fmtNum(exp.information_ratio ?? summary.information_ratio, 2)} />
              <MetricCard label="年化收益(无费)" value={fmtPct(exp.annualized_return_no_cost ?? summary.annualized_return_no_cost)} />
              <MetricCard label="最大回撤(无费)" value={fmtPct(exp.max_drawdown_no_cost ?? summary.max_drawdown_no_cost)} />
              <MetricCard label="IR(无费)" value={fmtNum(exp.information_ratio_no_cost ?? summary.information_ratio_no_cost, 2)} />
              <MetricCard label="超额收益均值" value={fmtPct(summary.excess_return_with_cost_mean)} />
            </div>
          </Section>
        )}

        {/* § 绝对收益 */}
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

        {/* § 交易效率 */}
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

        {/* § 全部持仓股票（支持排序） */}
        {em?.all_stocks && em.all_stocks.length > 0 ? (
          <AllStocksTable stocks={em.all_stocks} stockTrades={em?.stock_trades} />
        ) : (em?.top_stocks || em?.bottom_stocks) ? (
          // 兼容旧数据：合并 top/bottom 为全量列表
          <AllStocksTable
            stocks={[...(em?.top_stocks ?? []), ...(em?.bottom_stocks ?? [])]}
            stockTrades={em?.stock_trades}
          />
        ) : null}

        {/* § 因子贡献度 */}
        {(em?.factor_analysis?.feature_importance || em?.feature_importance) && (
          <FactorAnalysisPanel featureImportance={em.factor_analysis?.feature_importance ?? em.feature_importance} />
        )}

        {/* § IC 诊断 */}
        {em?.dates && em?.ic_series && (
          <Section title="IC 诊断">
            <IcSeriesChart
              dates={em.dates}
              ic_series={em.ic_series}
              rank_ic_series={em.rank_ic_series}
              ic_rolling_30d_mean={em.ic_rolling_30d_mean}
              ic_rolling_30d_std={em.ic_rolling_30d_std}
              ic_positive_ratio={em.ic_positive_ratio}
            />
          </Section>
        )}

        {/* § 收益曲线 */}
        {(em?.return_dates || em?.dates) && (em?.cumulative_excess_no_cost || em?.cumulative_excess_with_cost) && (
          <Section title="收益曲线">
            <ReturnCurveChart
              dates={em.return_dates ?? em.dates}
              cumulative_excess_no_cost={em.cumulative_excess_no_cost}
              cumulative_excess_with_cost={em.cumulative_excess_with_cost}
              cumulative_benchmark={em.cumulative_benchmark}
              drawdown_series={em.drawdown_series}
            />
          </Section>
        )}

        {/* § 训练过程 */}
        {em?.train_loss_curve && (
          <Section title="训练过程">
            <LossCurveChart
              train_loss_curve={em.train_loss_curve}
              val_loss_curve={em.val_loss_curve}
              best_epoch={em.best_epoch}
              overfit_ratio={em.overfit_ratio}
              convergence_ratio={em.convergence_ratio}
            />
          </Section>
        )}
      </div>
    </div>
  );
}
