"use client";

import React from "react";
import {
  FileCode2, DownloadCloud, TrendingUp, AlertCircle, GitBranch,
} from "lucide-react";
import dynamic from "next/dynamic";
import type { Loop } from "./TopologyPanel";
import LoopMetricsComparison from "./LoopMetricsComparison";
import {
  extractLoopDiagnostics,
  formatBool,
  formatCount,
  formatMoneyCompact,
  formatPercent,
  formatShortText,
} from "./loopDiagnostics";
import { AllStocksTable } from "../../components/AllStocksTable";
import { FactorAnalysisPanel } from "../../components/FactorAnalysisPanel";
import { StrategyConfigCard } from "../../components/StrategyConfigCard";
import EvolutionTrajectory from "../../components/EvolutionTrajectory";
import { PaperV2ApiError, strategyPackageApi } from "@/lib/paper-v2/api";
import type { JsonObject } from "@/lib/paper-v2/types";

const IcSeriesChart = dynamic(() => import("../../components/charts/IcSeriesChart"), { ssr: false });
const LossCurveChart = dynamic(() => import("../../components/charts/LossCurveChart"), { ssr: false });
const ReturnCurveChart = dynamic(() => import("../../components/charts/ReturnCurveChart"), { ssr: false });

function apiErrorMessage(error: unknown): string {
  if (error instanceof PaperV2ApiError) return error.message;
  if (error instanceof Error) return error.message;
  return String(error || "unknown error");
}

function getLoopPathId(loop: Loop): string {
  if (loop.loop_id) {
    const raw = String(loop.loop_id);
    const match = raw.match(/(?:^|_)Loop(\d+)$/i);
    return match ? `Loop${match[1]}` : raw;
  }
  return `Loop${loop.loop_index}`;
}

function getLoopFactorList(loop: Loop): string[] {
  const factors = loop.config_json?.factor_list;
  return Array.isArray(factors) ? factors : [];
}

export function getTaskStatusInfo(status: string): { color: string; bgColor: string; label: string } {
  switch (status) {
    case "running":
      return { color: "#22c55e", bgColor: "#f0fdf4", label: "运行中" };
    case "completed":
      return { color: "#3b82f6", bgColor: "#eff6ff", label: "已完成" };
    case "stopped":
    case "paused":
      return { color: "#f59e0b", bgColor: "#fef3c7", label: "已暂停" };
    case "failed":
      return { color: "#ef4444", bgColor: "#fef2f2", label: "已失败" };
    default:
      return { color: "#94a3b8", bgColor: "#f1f5f9", label: status };
  }
}

interface TaskInfo {
  strategy_id?: string;
  execution_algo?: string;
  unfilled_handler?: string;
  enable_sector_hmm?: boolean;
  task_type?: string;
  source_type?: string;
  evolution_mode?: string;
}

interface LoopDetailPanelProps {
  activeLoopData: Loop | undefined;
  prevLoopData: Loop | undefined;
  rightPanelView: "loop" | "trajectory";
  onSetRightPanelView: (v: "loop" | "trajectory") => void;
  detailTab: string;
  onSetDetailTab: (tab: string) => void;
  enhancedMetrics: any;
  activeTaskId: string | null;
  activeTask?: TaskInfo;
  configDiffLines: string[];
  onSyncAssets: (loopIndex: number) => void;
  onForkFromLoop?: (loopIndex: number) => void;
  taskType?: string;
  // Loop 指标对比表数据（从 TopologyPanel 移到轨迹视图下方）
  loops?: Loop[];
  onLoopSelect?: (index: number) => void;
}

const cardStyle: React.CSSProperties = {
  backgroundColor: "#ffffff",
  borderRadius: "12px",
  boxShadow: "0 4px 12px rgba(0, 0, 0, 0.05)",
  display: "flex",
  flexDirection: "column",
  overflow: "hidden",
  border: "1px solid rgba(255, 255, 255, 0.2)",
  flex: 1,
  minWidth: 0,
};

const headerStyle: React.CSSProperties = {
  padding: "16px 20px",
  borderBottom: "1px solid #f1f5f9",
  backgroundColor: "#f8fafc",
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
};

const DETAIL_TABS = [
  { key: "overview", label: "总览" },
  { key: "ic", label: "IC 诊断" },
  { key: "training", label: "训练过程" },
  { key: "returns", label: "收益曲线" },
  { key: "trade", label: "交易效率" },
  { key: "prediction", label: "预测行为" },
];

const VIEW_OPTIONS = [
  { key: "loop" as const, label: "Loop 详情", Icon: FileCode2 },
  { key: "trajectory" as const, label: "演进轨迹", Icon: TrendingUp },
];

export default React.memo(function LoopDetailPanel({
  activeLoopData,
  prevLoopData,
  rightPanelView,
  onSetRightPanelView,
  detailTab,
  onSetDetailTab,
  enhancedMetrics,
  activeTaskId,
  activeTask,
  configDiffLines,
  onSyncAssets,
  onForkFromLoop,
  taskType,
  loops,
  onLoopSelect,
}: LoopDetailPanelProps) {
  const [candidateBusy, setCandidateBusy] = React.useState(false);
  const [candidateMessage, setCandidateMessage] = React.useState<{ msg: string; ok: boolean } | null>(null);

  const addToCandidatePackages = React.useCallback(async () => {
    if (!activeLoopData || !activeTaskId) {
      setCandidateMessage({ ok: false, msg: "加入候选策略包失败: 缺少任务或 Loop 上下文" });
      return;
    }

    const loopPathId = getLoopPathId(activeLoopData);
    const factorList = getLoopFactorList(activeLoopData);
    setCandidateBusy(true);
    setCandidateMessage(null);
    try {
      const candidate = await strategyPackageApi.createCandidateFromQELoop({
        qe_task_id: activeTaskId,
        qe_loop_id: loopPathId,
        experiment_id: activeLoopData.experiment_id ?? null,
        created_by: "quantevolver_loop_detail_panel",
        display_name: `${activeTaskId} / ${loopPathId}`,
        snapshot_config: {
          source_ui: "quantevolver_loop_detail_panel",
          qe_task_id: activeTaskId,
          qe_loop_id: loopPathId,
          loop_index: activeLoopData.loop_index,
          loop_data: activeLoopData,
          loop_config: activeLoopData.config_json ?? {},
          enhanced_summary: enhancedMetrics?.summary ?? null,
        } as JsonObject,
        factor_manifest: {
          factor_list: factorList,
          factor_count: factorList.length,
        },
        model_manifest: {
          model_id: activeLoopData.config_json?.model_id ?? null,
          model_config: activeLoopData.config_json?.model_config ?? null,
          training_config: activeLoopData.config_json?.training_config ?? null,
          missing_reproducibility_items: activeLoopData.config_json?.seed == null ? ["seed"] : [],
        },
        strategy_manifest: {
          strategy_id: activeLoopData.config_json?.strategy_id ?? activeTask?.strategy_id ?? null,
          action_type: activeLoopData.action_type ?? null,
          daily_strategy_config: activeLoopData.config_json?.daily_strategy_config ?? null,
          minute_execution_config: activeLoopData.config_json?.minute_execution_config ?? null,
          tail_handling_config: activeLoopData.config_json?.tail_handling_config ?? null,
          platform_runtime_boundary: "HMM/ST/PIT/event signals are Paper v2 platform capabilities, not package assets",
        },
        metric_snapshot: {
          ...(activeLoopData.metrics_json || {}),
          enhanced_summary: enhancedMetrics?.summary ?? null,
        } as JsonObject,
        artifact_refs: {
          enhanced_metrics_available: Boolean(enhancedMetrics),
          enhanced_metrics_endpoint: `/quantevolver/evolution/tasks/${activeTaskId}/loops/${loopPathId}/enhanced-metrics`,
        },
        completeness: {
          candidate_snapshot_created: true,
          strategy_package_manifest_available: Boolean(activeLoopData.config_json?.strategy_package_manifest),
          missing_items: activeLoopData.config_json?.strategy_package_manifest ? [] : ["strategy_package_manifest"],
        },
        eligibility: {
          candidate_only: true,
          can_enter_selection_or_paper_after_package_validation: true,
          live_approval_reserved: false,
        },
        audit_context: {
          manual_action: true,
          ui_route: "/quantevolver/evolution",
          source_panel: "LoopDetailPanel",
          design_doc: "docs/architecture/paper_v2_qe_candidate_strategy_warehouse_design_20260512.md",
          created_at: new Date().toISOString(),
        },
        manual_action: true,
      });
      setCandidateMessage({ ok: true, msg: `已加入候选策略包: ${candidate.candidate_id}` });
    } catch (e) {
      setCandidateMessage({ ok: false, msg: `加入候选策略包失败: ${apiErrorMessage(e)}` });
    } finally {
      setCandidateBusy(false);
    }
  }, [activeLoopData, activeTaskId, activeTask?.strategy_id, enhancedMetrics]);

  React.useEffect(() => {
    setCandidateMessage(null);
    setCandidateBusy(false);
  }, [activeLoopData?.loop_id]);

  // 策略演进任务：隐藏训练过程 Tab 和相关内容
  const isStrategyEvo = taskType === "strategy_evo";
  const detailTabsForEvo = DETAIL_TABS.filter(tab => tab.key !== "training" && tab.key !== "prediction");

  if (!activeLoopData && rightPanelView !== "trajectory") {
    return (
      <div style={{ ...cardStyle, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", color: "#94a3b8", backgroundColor: "#f8fafc" }}>
        <div style={{ fontSize: "48px", marginBottom: "16px", opacity: 0.3 }}>→</div>
        <p style={{ fontSize: "16px", fontWeight: 500 }}>请在左侧选择一个任务并在拓扑树中点击具体的 LOOP</p>
      </div>
    );
  }

  return (
    <div style={cardStyle}>
      <div style={headerStyle}>
        <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
          <FileCode2 color="#10b981" size={20} />
          {rightPanelView === "trajectory" ? "演进轨迹总览" : `LOOP ${activeLoopData?.loop_index} 详情看板`}
        </h2>
        <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
          {VIEW_OPTIONS.map((v) => (
            <button key={v.key} onClick={() => onSetRightPanelView(v.key)}
              style={{
                padding: "5px 12px", border: "1px solid", borderRadius: "6px", fontSize: "12px", fontWeight: 600,
                cursor: "pointer", display: "flex", alignItems: "center", gap: "4px",
                backgroundColor: rightPanelView === v.key ? "#3b82f6" : "#fff",
                color: rightPanelView === v.key ? "#fff" : "#64748b",
                borderColor: rightPanelView === v.key ? "#3b82f6" : "#e2e8f0",
              }}>
              <v.Icon size={14} /> {v.label}
            </button>
          ))}
          {rightPanelView === "loop" && activeLoopData && (
            <>
              {activeLoopData.status === "completed" && onForkFromLoop && !isStrategyEvo && (
                <button onClick={() => onForkFromLoop(activeLoopData.loop_index)}
                  style={{
                    padding: "5px 12px", backgroundColor: "#8b5cf6", color: "#fff", border: "none", borderRadius: "6px",
                    fontSize: "12px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "4px",
                    boxShadow: "0 2px 4px rgba(139, 92, 246, 0.2)",
                  }}>
                  <GitBranch size={14} /> 以此为基础演进
                </button>
              )}
              {activeLoopData.status === "completed" && (
                <button data-testid="qe-loop-panel-add-candidate" onClick={addToCandidatePackages}
                  disabled={candidateBusy}
                  style={{
                    padding: "5px 12px", backgroundColor: "#2563eb", color: "#fff", border: "none", borderRadius: "6px",
                    fontSize: "12px", fontWeight: 600, cursor: candidateBusy ? "not-allowed" : "pointer", display: "flex", alignItems: "center", gap: "4px",
                    boxShadow: "0 2px 4px rgba(37, 99, 235, 0.2)",
                  }}>
                  {candidateBusy ? "加入中..." : "加入候选策略包"}
                </button>
              )}
              <button onClick={() => onSyncAssets(activeLoopData.loop_index)}
                style={{
                  padding: "5px 12px", backgroundColor: "#10b981", color: "#fff", border: "none", borderRadius: "6px",
                  fontSize: "12px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "4px",
                  boxShadow: "0 2px 4px rgba(16, 185, 129, 0.2)",
                }}>
                <DownloadCloud size={14} /> 同步资产
              </button>
            </>
          )}
        </div>
      </div>

      {rightPanelView === "trajectory" ? (
        <div style={{ flex: 1, minHeight: 0, minWidth: 0, overflowY: "auto", padding: "24px", backgroundColor: "#fafaf9", display: "flex", flexDirection: "column", gap: "16px" }}>
          <EvolutionTrajectory
            taskId={activeTaskId}
            taskType={activeTask?.task_type || taskType}
            evolutionMode={activeTask?.evolution_mode}
            sourceType={activeTask?.source_type}
          />
          {loops && loops.length > 0 && (
            <LoopMetricsComparison
              loops={loops}
              taskType={taskType}
              evolutionMode={activeTask?.evolution_mode}
              sourceType={activeTask?.source_type}
              onLoopSelect={onLoopSelect}
              selectedLoopIndex={activeLoopData?.loop_index}
            />
          )}
        </div>
      ) : activeLoopData ? (
        <>
          {candidateMessage && (
            <div style={{ margin: "12px 20px 0", padding: "10px 12px", backgroundColor: candidateMessage.ok ? "#f0fdf4" : "#fef2f2", border: `1px solid ${candidateMessage.ok ? "#22c55e" : "#ef4444"}`, borderRadius: 8, fontSize: 12, color: candidateMessage.ok ? "#166534" : "#991b1b" }}>
              {candidateMessage.msg}
            </div>
          )}

          {/* Tab bar */}
          <div style={{
            display: "flex", gap: "4px", padding: "8px 20px", borderBottom: "1px solid #e2e8f0",
            backgroundColor: "#fff", overflowX: "auto",
          }}>
            {(isStrategyEvo ? detailTabsForEvo : DETAIL_TABS).map((tab) => (
              <button key={tab.key} onClick={() => onSetDetailTab(tab.key)}
                style={{
                  padding: "6px 14px", border: "none", borderRadius: "6px", fontSize: "12px",
                  fontWeight: 600, cursor: "pointer", whiteSpace: "nowrap",
                  backgroundColor: detailTab === tab.key ? "#3b82f6" : "transparent",
                  color: detailTab === tab.key ? "#fff" : "#64748b",
                }}>
                {tab.label}
              </button>
            ))}
          </div>

          <div style={{ flex: 1, overflowY: "auto", padding: "24px", display: "flex", flexDirection: "column", gap: "24px", backgroundColor: "#fafaf9" }}>
            {detailTab === "overview" && (
              <>
              <OverviewContent
                activeLoopData={activeLoopData}
                prevLoopData={prevLoopData}
                configDiffLines={configDiffLines}
                activeTask={activeTask}
                enhancedMetrics={enhancedMetrics}
              />
              </>
            )}

            {/* IC 诊断 Tab */}
            {detailTab === "ic" && (
              <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20 }}>
                <h3 style={{ margin: "0 0 16px", fontSize: 13, fontWeight: 700, color: "#3b82f6", textTransform: "uppercase", letterSpacing: "0.05em" }}>IC 时间序列诊断</h3>
                {enhancedMetrics?.ic_diagnostics ? (
                  <IcSeriesChart {...enhancedMetrics.ic_diagnostics} />
                ) : (
                  <div style={{ color: "#94a3b8", textAlign: "center", padding: 40 }}>暂无 IC 诊断数据（需要增强指标支持）</div>
                )}
              </div>
            )}

            {/* 训练过程 Tab */}
            {detailTab === "training" && (
              <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20 }}>
                <h3 style={{ margin: "0 0 16px", fontSize: 13, fontWeight: 700, color: "#8b5cf6", textTransform: "uppercase", letterSpacing: "0.05em" }}>训练过程诊断</h3>
                {enhancedMetrics?.training_diagnostics ? (
                  <LossCurveChart {...enhancedMetrics.training_diagnostics} />
                ) : (
                  <div style={{ color: "#94a3b8", textAlign: "center", padding: 40 }}>暂无训练过程数据</div>
                )}
              </div>
            )}

            {/* 收益曲线 Tab */}
            {detailTab === "returns" && (
              <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20 }}>
                <h3 style={{ margin: "0 0 16px", fontSize: 13, fontWeight: 700, color: "#10b981", textTransform: "uppercase", letterSpacing: "0.05em" }}>收益曲线</h3>
                {enhancedMetrics?.return_curves ? (
                  <ReturnCurveChart {...enhancedMetrics.return_curves} />
                ) : (
                  <div style={{ color: "#94a3b8", textAlign: "center", padding: 40 }}>暂无收益曲线数据</div>
                )}
              </div>
            )}

            {/* 交易效率 Tab */}
            {detailTab === "trade" && (
              <>
                <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20 }}>
                  <h3 style={{ margin: "0 0 16px", fontSize: 13, fontWeight: 700, color: "#f59e0b", textTransform: "uppercase", letterSpacing: "0.05em" }}>交易效率诊断</h3>
                  {enhancedMetrics?.trade_diagnostics ? (
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16 }}>
                      {[
                        { label: "平均换手率", value: enhancedMetrics.trade_diagnostics.avg_turnover?.toFixed(3) ?? "-" },
                        { label: "成本侵蚀(年化)", value: enhancedMetrics.trade_diagnostics.cost_drag_annualized != null ? (enhancedMetrics.trade_diagnostics.cost_drag_annualized * 100).toFixed(1) + "%" : "-" },
                        { label: "日均交易笔数", value: enhancedMetrics.trade_diagnostics.daily_trade_count_avg?.toFixed(1) ?? "-" },
                      ].map((m) => (
                        <div key={m.label} style={{ textAlign: "center", padding: 16, backgroundColor: "#f8fafc", borderRadius: 8, border: "1px solid #e2e8f0" }}>
                          <div style={{ fontSize: 11, color: "#64748b", fontWeight: 600, textTransform: "uppercase" }}>{m.label}</div>
                          <div style={{ fontSize: 22, fontWeight: 700, color: "#0f172a", fontFamily: "monospace", marginTop: 4 }}>{m.value}</div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div style={{ color: "#94a3b8", textAlign: "center", padding: 40 }}>暂无交易效率数据</div>
                  )}
                </div>

                {/* 全部持仓股票（支持排序） */}
                {enhancedMetrics?.all_stocks?.length > 0 ? (
                  <AllStocksTable
                    stocks={enhancedMetrics.all_stocks}
                    stockTrades={enhancedMetrics?.stock_trades}
                  />
                ) : (enhancedMetrics?.top_stocks || enhancedMetrics?.bottom_stocks) ? (
                  <AllStocksTable
                    stocks={[...(enhancedMetrics?.top_stocks ?? []), ...(enhancedMetrics?.bottom_stocks ?? [])]}
                    stockTrades={enhancedMetrics?.stock_trades}
                  />
                ) : (
                  <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20, color: "#94a3b8", textAlign: "center" }}>
                    暂无持仓数据
                  </div>
                )}

                {/* 因子贡献度 */}
                {(enhancedMetrics?.factor_analysis?.feature_importance || enhancedMetrics?.feature_importance) && (
                  <FactorAnalysisPanel
                    featureImportance={enhancedMetrics?.factor_analysis?.feature_importance || enhancedMetrics?.feature_importance}
                  />
                )}
              </>
            )}

            {/* 预测行为 Tab */}
            {detailTab === "prediction" && (
              <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20 }}>
                <h3 style={{ margin: "0 0 16px", fontSize: 13, fontWeight: 700, color: "#ec4899", textTransform: "uppercase", letterSpacing: "0.05em" }}>预测行为诊断</h3>
                {enhancedMetrics?.prediction_diagnostics ? (
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
                    {[
                      { label: "预测标准差", value: enhancedMetrics.prediction_diagnostics.pred_std?.toFixed(4) ?? "-" },
                      { label: "1日自相关", value: enhancedMetrics.prediction_diagnostics.pred_autocorr_1d?.toFixed(3) ?? "-" },
                      { label: "排名换手率", value: enhancedMetrics.prediction_diagnostics.pred_rank_turnover?.toFixed(3) ?? "-" },
                      { label: "Top30稳定性", value: enhancedMetrics.prediction_diagnostics.top30_stability?.toFixed(3) ?? "-" },
                    ].map((m) => (
                      <div key={m.label} style={{ textAlign: "center", padding: 16, backgroundColor: "#f8fafc", borderRadius: 8, border: "1px solid #e2e8f0" }}>
                        <div style={{ fontSize: 11, color: "#64748b", fontWeight: 600, textTransform: "uppercase" }}>{m.label}</div>
                        <div style={{ fontSize: 22, fontWeight: 700, color: "#0f172a", fontFamily: "monospace", marginTop: 4 }}>{m.value}</div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div style={{ color: "#94a3b8", textAlign: "center", padding: 40 }}>暂无预测行为数据</div>
                )}
              </div>
            )}
          </div>
        </>
      ) : null}
    </div>
  );
});

// ── 总览 Tab 子组件 ──

interface OverviewContentProps {
  activeLoopData: Loop;
  prevLoopData: Loop | undefined;
  configDiffLines: string[];
  activeTask?: TaskInfo;
  enhancedMetrics?: any;
}

const OverviewContent = React.memo(function OverviewContent({
  activeLoopData,
  prevLoopData,
  configDiffLines,
  activeTask,
  enhancedMetrics,
}: OverviewContentProps) {
  const diagnostics = extractLoopDiagnostics(activeLoopData, enhancedMetrics);
  const m = diagnostics.metrics || {};
  const ar = diagnostics.absoluteReturns || {};
  const hasAbsoluteReturns = Object.keys(ar).length > 0;
  const positionInfo = diagnostics.position;
  const modelInfo = diagnostics.model;

  const cfg = activeLoopData.config_json;
  const actionType = cfg?.action_type || "initial";
  const actionColors: Record<string, { bg: string; text: string }> = {
    factor_adjust: { bg: "#dbeafe", text: "#1d4ed8" },
    param_tune: { bg: "#fef3c7", text: "#92400e" },
    model_switch: { bg: "#ede9fe", text: "#6d28d9" },
    initial: { bg: "#f1f5f9", text: "#475569" },
  };
  const ac = actionColors[actionType] || actionColors.initial;
  const factors: string[] = cfg?.factor_list || [];
  const useAlpha158 = cfg?.use_alpha158 ?? cfg?.alpha158;
  const modelId = cfg?.model_id || cfg?.model_type;
  const hyperKeys = ["topk", "n_drop", "lr", "batch_size", "d_model", "n_head", "dropout", "n_epochs", "early_stop"];
  const hyperParams = hyperKeys.filter(k => cfg?.[k] !== undefined).map(k => ({ key: k, val: cfg[k] }));

  const metricGroups = [
    {
      label: "信号质量", color: "#3b82f6", items: [
        { label: "IC", source: m, key: "IC", digits: 4 },
        { label: "Rank IC", source: m, key: "Rank_IC", digits: 4 },
        { label: "ICIR", source: m, key: "ICIR", digits: 4 },
      ],
    },
    {
      label: "含成本收益(相对基准)", color: "#8b5cf6", items: [
        { label: "Sharpe", source: m, key: "sharpe", digits: 2 },
        { label: "年化收益", source: m, key: "annualized_return", digits: 2, pct: true },
        { label: "最大回撤", source: m, key: "max_drawdown", digits: 2, pct: true },
      ],
    },
    {
      label: "绝对收益(账户)", color: "#059669", items: hasAbsoluteReturns ? [
        { label: "CAGR", source: ar, key: "cagr", digits: 2, pct: true },
        { label: "总收益", source: ar, key: "total_return", digits: 2, pct: true },
        { label: "绝对 Sharpe", source: ar, key: "sharpe", digits: 2 },
        { label: "绝对最大回撤", source: ar, key: "max_drawdown", digits: 2, pct: true },
        { label: "年化波动率", source: ar, key: "annualized_volatility", digits: 2, pct: true },
        { label: "平均资金利用率", source: ar, key: "avg_cash_ratio", digits: 2, pct: true, invert: true },
        { label: "期末持仓数", source: ar, key: "final_stock_count", digits: 0 },
      ] : [
        { label: "CAGR", source: m, key: "cagr", digits: 3, pct: true },
      ],
    },
  ];

  const modelSummaryItems = [
    { label: "回测模型", value: formatShortText(modelInfo.modelId || modelInfo.modelType, 28), title: modelInfo.modelId || modelInfo.modelType || "" },
    { label: "模型类型", value: modelInfo.modelType || "-" },
    { label: "训练周期", value: modelInfo.labelHorizon || "-" },
    { label: "自定义因子", value: formatCount(modelInfo.customFactorCount), sub: modelInfo.alpha158Enabled === undefined ? "Alpha158: -" : `Alpha158: ${modelInfo.alpha158Enabled ? "ON" : "OFF"}` },
    { label: "HMM", value: formatBool(modelInfo.hmm.enabled), sub: modelInfo.hmm.signalPreset || modelInfo.hmm.version || "-" },
    { label: "HMM 快照", value: formatShortText(modelInfo.hmm.snapshot, 24), title: modelInfo.hmm.snapshot || "" },
  ];

  const holdingSummaryItems = [
    { label: "最小持仓", value: formatCount(positionInfo.minCount) },
    { label: "平均持仓", value: formatCount(positionInfo.avgCount, 1) },
    { label: "最大持仓", value: formatCount(positionInfo.maxCount) },
    { label: "P95 持仓", value: formatCount(positionInfo.p95Count, 1) },
    { label: "期末持仓", value: formatCount(positionInfo.finalStockCount) },
    { label: "结束现金", value: formatMoneyCompact(positionInfo.finalCash) },
    { label: "股票市值", value: formatMoneyCompact(positionInfo.finalStockValue) },
    { label: "结束总权益", value: formatMoneyCompact(positionInfo.finalTotalValue) },
    { label: "结束现金占比", value: formatPercent(positionInfo.finalCashRatio, 2) },
  ];

  const hasHoldingRange = positionInfo.minCount !== undefined || positionInfo.avgCount !== undefined || positionInfo.maxCount !== undefined;

  const renderSummaryCard = (
    title: string,
    color: string,
    items: Array<{ label: string; value: string; sub?: string; title?: string }>,
    note?: string,
  ) => (
    <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)" }}>
      <h3 style={{ margin: "0 0 14px 0", fontSize: "13px", fontWeight: 700, color, textTransform: "uppercase", letterSpacing: "0.05em" }}>{title}</h3>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: "10px" }}>
        {items.map((item) => (
          <div key={item.label} title={item.title} style={{ padding: "10px", backgroundColor: "#f8fafc", border: "1px solid #e2e8f0", borderRadius: "8px", minWidth: 0 }}>
            <div style={{ fontSize: "11px", color: "#64748b", fontWeight: 700, marginBottom: "4px" }}>{item.label}</div>
            <div style={{ fontSize: "16px", color: "#0f172a", fontWeight: 800, fontFamily: "monospace", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.value}</div>
            {item.sub && <div style={{ marginTop: "3px", fontSize: "11px", color: "#64748b", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.sub}</div>}
          </div>
        ))}
      </div>
      {note && <div style={{ marginTop: "10px", fontSize: "12px", color: "#64748b", lineHeight: 1.6 }}>{note}</div>}
    </div>
  );

  return (
    <>
      {/* Agent 诊断报告 */}
      <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)" }}>
        <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#3b82f6", textTransform: "uppercase", letterSpacing: "0.05em", display: "flex", alignItems: "center", gap: "6px" }}>
          <AlertCircle size={16} />
          Agent 结案陈词 & 决策逻辑
        </h3>
        <div style={{ fontSize: "14px", color: "#334155", lineHeight: 1.6, backgroundColor: "#f8fafc", padding: "16px", borderRadius: "6px", border: "1px solid #f1f5f9" }}>
          {activeLoopData.agent_analysis ? (
            <>
              {activeLoopData.agent_analysis.analyst && (
                <p style={{ margin: "0 0 12px 0", whiteSpace: "pre-wrap" }}><strong style={{ color: "#0f172a" }}>诊断 (Analyst):</strong><br/>{typeof activeLoopData.agent_analysis.analyst === "string" ? activeLoopData.agent_analysis.analyst : activeLoopData.agent_analysis.analyst?.report_text || JSON.stringify(activeLoopData.agent_analysis.analyst)}</p>
              )}
              {activeLoopData.agent_analysis.direction && (
                <p style={{ margin: "0 0 12px 0" }}>
                  <strong style={{ color: "#7c3aed" }}>演进方向:</strong>{" "}
                  <span style={{ display: "inline-block", padding: "2px 8px", borderRadius: "4px", fontSize: "12px", fontWeight: 600, backgroundColor: "#ede9fe", color: "#7c3aed" }}>
                    {activeLoopData.agent_analysis.direction.decided_action_type || "\u2014"}
                  </span>
                  {activeLoopData.agent_analysis.direction.evolution_mode && activeLoopData.agent_analysis.direction.evolution_mode !== "auto" && (
                    <span style={{ marginLeft: "8px", fontSize: "12px", color: "#94a3b8" }}>
                      (模式: {activeLoopData.agent_analysis.direction.evolution_mode})
                    </span>
                  )}
                </p>
              )}
              {activeLoopData.agent_analysis.researcher && (
                <p style={{ margin: "0 0 12px 0", whiteSpace: "pre-wrap" }}><strong style={{ color: "#0f172a" }}>决策 (Researcher):</strong><br/>{typeof activeLoopData.agent_analysis.researcher === "string" ? activeLoopData.agent_analysis.researcher : (activeLoopData.agent_analysis.researcher?.action_type ? `方向: ${activeLoopData.agent_analysis.researcher.action_type}` : JSON.stringify(activeLoopData.agent_analysis.researcher, null, 2))}</p>
              )}
              {activeLoopData.agent_analysis.evaluator && (
                <p style={{ margin: "0 0 12px 0", whiteSpace: "pre-wrap" }}><strong style={{ color: "#d97706" }}>SOTA 评估 (Evaluator):</strong><br/>{typeof activeLoopData.agent_analysis.evaluator === "string" ? activeLoopData.agent_analysis.evaluator : (activeLoopData.agent_analysis.evaluator?.is_sota !== undefined ? (activeLoopData.agent_analysis.evaluator.is_sota ? "当前 Loop 被评为 SOTA" : "未达到 SOTA 标准") : JSON.stringify(activeLoopData.agent_analysis.evaluator, null, 2))}</p>
              )}
              {activeLoopData.agent_analysis.reviewer && (
                <p style={{ margin: 0, whiteSpace: "pre-wrap" }}><strong style={{ color: "#0f172a" }}>审查 (Reviewer):</strong><br/>{typeof activeLoopData.agent_analysis.reviewer === "string" ? activeLoopData.agent_analysis.reviewer : (activeLoopData.agent_analysis.reviewer?.approved !== undefined ? (activeLoopData.agent_analysis.reviewer.approved ? "配置审查通过" : "配置审查未通过") : JSON.stringify(activeLoopData.agent_analysis.reviewer, null, 2))}</p>
              )}
            </>
          ) : (
            <p style={{ margin: 0, color: "#94a3b8" }}>暂无 Agent 报告数据...</p>
          )}
        </div>
      </div>

      {/* 策略与执行配置 */}
      <StrategyConfigCard source={{ loopConfig: activeLoopData.config_json, taskConfig: activeTask }} />

      <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "20px" }}>
        {renderSummaryCard(
          "模型 / HMM / 因子",
          "#2563eb",
          modelSummaryItems,
          "字段均从 loop config、metrics_json.enhanced_metrics 或已缓存增强结果读取，不触发重跑或改变实验期行为。",
        )}
        {renderSummaryCard(
          "持仓 / 资金",
          "#059669",
          holdingSummaryItems,
          hasHoldingRange
            ? "目标持仓 50 只允许附近波动；若最大/P95 长期显著高于 60，应优先检查替补、尾盘成交和目标组合漂移。"
            : "历史 Loop 尚未回填 positions 摘要时，最小/平均/最大持仓显示为 “-”；结束现金、股票市值可继续从 absolute_returns 展示。",
        )}
      </div>

      {/* 回测表现指标 */}
      <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)" }}>
        <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em" }}>
          核心指标
          {activeLoopData.is_sota && <span style={{ marginLeft: "8px", fontSize: "11px", color: "#d97706", backgroundColor: "#fef3c7", padding: "2px 8px", borderRadius: "4px" }}>SOTA</span>}
        </h3>
        {metricGroups.map((group) => (
          <div key={group.label} style={{ marginBottom: "16px" }}>
            <div style={{ fontSize: "11px", fontWeight: 700, color: group.color, marginBottom: "8px", textTransform: "uppercase", letterSpacing: "0.05em" }}>{group.label}</div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "12px" }}>
              {group.items.map(({ label, source, key, digits = 4, pct, invert }) => {
                const v = source?.[key];
                let displayVal = "-";
                if (typeof v === "number" && isFinite(v)) {
                  if (key === "final_stock_count") {
                    displayVal = String(Math.round(v));
                  } else if (pct) {
                    const pctVal = invert ? (1 - v) * 100 : v * 100;
                    displayVal = pctVal.toFixed(digits) + "%";
                  } else {
                    displayVal = v.toFixed(digits);
                  }
                }
                return (
                  <div key={key} style={{ textAlign: "center", padding: "12px", backgroundColor: "#f8fafc", borderRadius: "8px", border: "1px solid #e2e8f0" }}>
                    <div style={{ fontSize: "11px", color: "#64748b", fontWeight: 600, textTransform: "uppercase" }}>{label}</div>
                    <div style={{ fontSize: "20px", fontWeight: 700, fontFamily: "monospace", color: "#059669", marginTop: "4px" }}>
                      {displayVal}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "20px" }}>
        {/* 实验配置 */}
        <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)", display: "flex", flexDirection: "column" }}>
          <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em" }}>实验配置 (Config)</h3>
          {cfg ? (
            <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
              {/* Action Type Badge */}
              <div>
                <span style={{ display: "inline-block", padding: "3px 10px", borderRadius: "12px", fontSize: "11px", fontWeight: 700, backgroundColor: ac.bg, color: ac.text }}>
                  {actionType.replace(/_/g, " ").toUpperCase()}
                </span>
              </div>

              {/* Factors */}
              {(factors.length > 0 || useAlpha158 !== undefined) && (
                <div>
                  <div style={{ fontSize: "11px", color: "#64748b", fontWeight: 600, marginBottom: "6px" }}>因子</div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: "4px" }}>
                    {useAlpha158 !== undefined && (
                      <span style={{ display: "inline-block", padding: "2px 8px", borderRadius: "4px", fontSize: "11px", fontFamily: "monospace", backgroundColor: useAlpha158 ? "#dcfce7" : "#fee2e2", color: useAlpha158 ? "#166534" : "#991b1b" }}>
                        Alpha158: {useAlpha158 ? "ON" : "OFF"}
                      </span>
                    )}
                    {factors.map((f: string, i: number) => (
                      <span key={i} style={{ display: "inline-block", padding: "2px 8px", borderRadius: "4px", fontSize: "11px", fontFamily: "monospace", backgroundColor: "#f0f9ff", color: "#0369a1", border: "1px solid #bae6fd" }}>{f}</span>
                    ))}
                  </div>
                  {factors.length > 0 && <div style={{ fontSize: "11px", color: "#94a3b8", marginTop: "4px" }}>共 {factors.length} 个自定义因子{useAlpha158 ? " + Alpha158 基线" : ""}</div>}
                </div>
              )}

              {/* Model */}
              {modelId && (
                <div>
                  <div style={{ fontSize: "11px", color: "#64748b", fontWeight: 600, marginBottom: "6px" }}>模型</div>
                  <span style={{ fontFamily: "monospace", fontSize: "12px", fontWeight: 700, color: "#334155" }}>{modelId}</span>
                  {hyperParams.length > 0 && (
                    <div style={{ display: "flex", flexWrap: "wrap", gap: "6px", marginTop: "6px" }}>
                      {hyperParams.map(hp => (
                        <span key={hp.key} style={{ fontSize: "11px", fontFamily: "monospace", color: "#475569", backgroundColor: "#f8fafc", border: "1px solid #e2e8f0", borderRadius: "4px", padding: "2px 6px" }}>
                          {hp.key}={typeof hp.val === "number" ? hp.val : String(hp.val)}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {/* Collapsible Raw JSON */}
              <details style={{ marginTop: "4px" }}>
                <summary style={{ fontSize: "11px", color: "#94a3b8", cursor: "pointer", userSelect: "none" }}>原始 JSON</summary>
                <pre style={{ margin: "8px 0 0", whiteSpace: "pre-wrap", wordWrap: "break-word", fontSize: "11px", fontFamily: "monospace", color: "#475569", backgroundColor: "#f8fafc", border: "1px solid #e2e8f0", borderRadius: "6px", padding: "10px", maxHeight: "200px", overflowY: "auto" }}>
                  {JSON.stringify(cfg, null, 2)}
                </pre>
              </details>
            </div>
          ) : (
            <div style={{ color: "#94a3b8" }}>No config data available.</div>
          )}
        </div>

        {/* Config Diff */}
        <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)", overflow: "hidden" }}>
          <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em" }}>
            Config Diff (对比上一轮)
          </h3>
          {activeLoopData.loop_index === 0 ? (
            <p style={{ margin: 0, color: "#94a3b8" }}>LOOP 0 为初始配置，无上一轮可对比。</p>
          ) : configDiffLines.length === 0 ? (
            <p style={{ margin: 0, color: "#94a3b8" }}>未检测到配置差异。</p>
          ) : (() => {
            // Parse factor_list diff specially; other keys as text rows
            const prevCfg = prevLoopData?.config_json || {};
            const currCfg = activeLoopData.config_json || {};
            const prevFactors: string[] = prevCfg.factor_list || [];
            const currFactors: string[] = currCfg.factor_list || [];
            const addedFactors = currFactors.filter((f: string) => !prevFactors.includes(f));
            const removedFactors = prevFactors.filter((f: string) => !currFactors.includes(f));
            const keptFactors = currFactors.filter((f: string) => prevFactors.includes(f));
            const hasFactorDiff = addedFactors.length > 0 || removedFactors.length > 0;
            const otherLines = configDiffLines.filter(l => !l.startsWith("factor_list:"));
            return (
              <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
                {hasFactorDiff && (
                  <div>
                    <div style={{ fontSize: "11px", color: "#64748b", fontWeight: 600, marginBottom: "6px" }}>因子变化</div>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: "4px" }}>
                      {removedFactors.map((f: string, i: number) => (
                        <span key={"rm-"+i} style={{ display: "inline-block", padding: "2px 8px", borderRadius: "4px", fontSize: "11px", fontFamily: "monospace", backgroundColor: "#fee2e2", color: "#991b1b", border: "1px solid #fecaca", textDecoration: "line-through" }}>− {f}</span>
                      ))}
                      {addedFactors.map((f: string, i: number) => (
                        <span key={"add-"+i} style={{ display: "inline-block", padding: "2px 8px", borderRadius: "4px", fontSize: "11px", fontFamily: "monospace", backgroundColor: "#dcfce7", color: "#166534", border: "1px solid #bbf7d0" }}>+ {f}</span>
                      ))}
                      {keptFactors.map((f: string, i: number) => (
                        <span key={"kp-"+i} style={{ display: "inline-block", padding: "2px 8px", borderRadius: "4px", fontSize: "11px", fontFamily: "monospace", backgroundColor: "#f0f9ff", color: "#0369a1", border: "1px solid #bae6fd" }}>{f}</span>
                      ))}
                    </div>
                    <div style={{ fontSize: "11px", color: "#94a3b8", marginTop: "4px" }}>
                      {removedFactors.length > 0 && <span style={{ color: "#dc2626" }}>−{removedFactors.length} 删除 </span>}
                      {addedFactors.length > 0 && <span style={{ color: "#16a34a" }}>+{addedFactors.length} 新增 </span>}
                      {keptFactors.length > 0 && <span>{keptFactors.length} 保留</span>}
                    </div>
                  </div>
                )}
                {otherLines.length > 0 && (
                  <div>
                    <div style={{ fontSize: "11px", color: "#64748b", fontWeight: 600, marginBottom: "6px" }}>其他变更</div>
                    <div style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
                      {otherLines.map((line, idx) => (
                        <div key={idx} style={{ fontSize: "12px", fontFamily: "monospace", color: "#334155", backgroundColor: "#fefce8", border: "1px solid #fef08a", borderRadius: "4px", padding: "4px 8px", whiteSpace: "pre-wrap", wordBreak: "break-all" }}>{line}</div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            );
          })()}
        </div>
      </div>
    </>
  );
});

