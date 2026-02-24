"use client";

import { useEffect, useState, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

// 数据来源映射
const DATA_SOURCE_MAP: Record<string, string> = {
  daily_pv: "日线行情",
  daily_basic: "每日基本面",
  moneyflow: "个股资金流向",
  cyq_perf: "筹码分布",
  bak_basic: "股票历史信息",
  multi: "多数据源",
};

// 因子类型映射
const FACTOR_TYPE_MAP: Record<string, string> = {
  CrossSection: "截面因子",
  TimeSeries: "时序因子",
};

type Factor = { factor_name: string; source: string; ic?: number; sharpe?: number; is_sota_factor?: boolean;
  category?: string; grade?: string; description?: string; ann_ret_value?: number; factor_type?: string; data_source?: string };
type Model = { model_id: string; model_name: string; model_type?: string; ic?: number; annualized_return?: number; is_sota?: boolean; display_name?: string };
type Strategy = { strategy_id: string; display_name: string; portfolio_config?: any };

type EvalResult = {
  ok: boolean;
  factor_analysis?: any;
  model_analysis?: any;
  strategy_analysis?: any;
  risks?: { level: string; type: string; message: string }[];
  suggestions?: string[];
  overall_score?: number;
  llm_commentary?: string;
};

type ConfigResult = {
  ok: boolean;
  experiment_id?: string;
  experiment_name?: string;
  experiment_dir?: string;
  wsl_command?: string;
  conf_yaml_preview?: string;
  factor_count?: number;
};

type AIGenerateResult = {
  ok: boolean;
  error?: string;
  user_requirement?: string;
  style_preferences?: any;
  design_rationale?: string;
  analysis_steps?: string[];
  combination?: {
    factor_names: string[];
    factor_count: number;
    factor_details: any[];
    category_summary: Record<string, { count: number; factors: string[]; category_name: string }>;
    model_id: string;
    model_info: any;
    strategy_params: any;
  };
  evaluation?: any;
  all_categories_overview?: Record<string, { category_name: string; total_factors: number; selected_count: number; grade_distribution: Record<string, number> }>;
  metadata_summary?: any;
};

/* ── 页面主入口 ── */
export default function ComposePage() {
  /* ── 页面模式 ── */
  const [mode, setMode] = useState<"ai" | "manual">("ai");

  /* ── AI智能生成 ── */
  const [userRequirement, setUserRequirement] = useState("");
  const [aiResult, setAiResult] = useState<AIGenerateResult | null>(null);
  const [aiLoading, setAiLoading] = useState(false);
  const [expandedCategory, setExpandedCategory] = useState<string | null>(null);
  const [expandedAiFactors, setExpandedAiFactors] = useState<Set<string>>(new Set());
  const [expandedFactorDesc, setExpandedFactorDesc] = useState<Set<string>>(new Set()); // 手动选择表格展开的因子说明

  /* ── 手动选择（保留原有功能） ── */
  const [factors, setFactors] = useState<Factor[]>([]);
  const [models, setModels] = useState<Model[]>([]);
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [selectedFactors, setSelectedFactors] = useState<Set<string>>(new Set());
  const [selectedModel, setSelectedModel] = useState<string>("");
  const [selectedStrategy, setSelectedStrategy] = useState<string>("");
  const [factorSourceTab, setFactorSourceTab] = useState<string>("all");
  const [showAlphaFactors, setShowAlphaFactors] = useState(false);
  const [disableAlphaBaseline, setDisableAlphaBaseline] = useState(false); // 禁用Alpha158基线因子
  const [quickTrain, setQuickTrain] = useState(false); // 快速训练模式：训练时间缩短到20%
  const [factorPage, setFactorPage] = useState(1);
  const factorPageSize = 20;  // 默认每页20条
  
  // 新增筛选状态
  const [factorTypeFilter, setFactorTypeFilter] = useState<string>("");
  const [dataSourceFilter, setDataSourceFilter] = useState<string>("");
  
  // 排序状态：三次点击循环 desc -> asc -> null
  type SortState = null | 'asc' | 'desc';
  const [factorSortKey, setFactorSortKey] = useState<string>("grade");
  const [factorSortDir, setFactorSortDir] = useState<SortState>("asc");

  /* ── 数据划分 ── */
  const [dataSplit, setDataSplit] = useState({
    train_start: "2018-08-01", train_end: "2022-12-31",
    valid_start: "2023-01-01", valid_end: "2024-06-30",
    test_start: "2024-07-01", test_end: "2025-12-01",
  });
  const [topk, setTopk] = useState(50);
  const [nDrop, setNDrop] = useState(5);

  /* ── 结果 ── */
  const [evalResult, setEvalResult] = useState<EvalResult | null>(null);
  const [configResult, setConfigResult] = useState<ConfigResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState<string | null>(null);

  /* ── 加载手动模式数据 ── */
  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      // 使用因子库API获取因子（复用rdagent/catalogs/factors）
      const [fRes, mRes, sRes, cRes] = await Promise.all([
        fetch(`${API}/rdagent/catalogs/factors?limit=500&exclude_source=qlib_alpha158,alpha158,alpha360`).then(r => r.json()),
        fetch(`${API}/quantevolver/models?limit=100`).then(r => r.json()),
        fetch(`${API}/quantevolver/strategies?limit=50`).then(r => r.json()),
        fetch(`${API}/quantevolver/factor-analyst/classifications?limit=500&active_only=false`).then(r => r.json()).catch(() => ({ items: [] })),
      ]);
      const classMap: Record<string, any> = {};
      (cRes.items || []).forEach((c: any) => { classMap[c.factor_name] = c; });
      
      // 因子库API返回的因子字段：name, source, factor_type, data_source, ic, sharpe 等
      const enrichedFactors = (fRes.items || []).map((f: any) => {
        const cls = classMap[f.name];
        return { 
          factor_name: f.name,
          source: f.source,
          ic: f.ic,
          sharpe: f.sharpe,
          is_sota_factor: f.is_sota_factor,
          expression: f.expression,
          factor_type: f.factor_type,
          data_source: f.data_source,
          category: cls?.category, 
          grade: cls?.grade, 
          description: f.description_cn || cls?.description, 
          ann_ret_value: cls?.ann_ret_value,
          best_metrics: f.best_metrics,
        };
      });
      setFactors(enrichedFactors);
      // 注入LGBModel内置默认选项（QLib内置，无需数据库记录）
      const dbModels = mRes.items || [];
      const hasLGB = dbModels.some((m: Model) => m.model_name === "LGBModel" || m.model_type === "LGB");
      const allModels = hasLGB ? dbModels : [
        {
          model_id: "__builtin_lgbmodel__",
          model_name: "LGBModel",
          model_type: "LGB",
          display_name: "LGBModel (QLib内置)",
          ic: null,
          annualized_return: null,
          is_sota: false,
        },
        ...dbModels,
      ];
      setModels(allModels);
      setStrategies(sRes.items || []);
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  /* ── AI智能生成 ── */
  async function aiGenerate() {
    if (!userRequirement.trim()) { alert("请输入投资需求描述"); return; }
    setAiLoading(true);
    setAiResult(null);
    setConfigResult(null);
    try {
      const res = await fetch(`${API}/quantevolver/portfolio/generate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_requirement: userRequirement.trim(),
          use_llm: true,
          max_factors: 30,
        }),
      });
      const data: AIGenerateResult = await res.json();
      setAiResult(data);
      if (data.ok && data.combination) {
        setSelectedFactors(new Set(data.combination.factor_names));
        setSelectedModel(data.combination.model_id || "");
        if (data.combination.strategy_params) {
          setTopk(data.combination.strategy_params.topk || 50);
          setNDrop(data.combination.strategy_params.n_drop || 5);
        }
      }
    } catch (e: any) {
      alert("智能生成失败: " + (e?.message || ""));
    }
    setAiLoading(false);
  }

  /* ── 手动模式辅助函数 ── */
  const GRADE_ORDER: Record<string, number> = { S: 0, A: 1, B: 2, C: 3, D: 4 };

  // 三次点击排序处理函数
  function handleFactorSort(field: string) {
    if (factorSortKey !== field) {
      setFactorSortKey(field);
      setFactorSortDir("desc");
    } else if (factorSortDir === "desc") {
      setFactorSortDir("asc");
    } else if (factorSortDir === "asc") {
      setFactorSortKey("");
      setFactorSortDir(null);
    } else {
      setFactorSortDir("desc");
    }
    setFactorPage(1);
  }

  // 获取排序指示器
  function getSortIndicator(field: string): string {
    if (factorSortKey !== field) return "";
    if (factorSortDir === "desc") return " ▼";
    if (factorSortDir === "asc") return " ▲";
    return "";
  }

  const allFilteredFactors = factors.filter(f => {
    // 来源筛选
    if (factorSourceTab === "sota") return f.source === "rdagent_task_sync";
    if (factorSourceTab === "alpha158") return f.source === "alpha158";
    if (factorSourceTab === "alpha360") return f.source === "alpha360";
    // "all" 模式下，alpha因子需要showAlphaFactors开关
    if (!showAlphaFactors && (f.source === "alpha158" || f.source === "alpha360")) return false;
    // 因子类型筛选
    if (factorTypeFilter && f.factor_type !== factorTypeFilter) return false;
    // 数据来源筛选
    if (dataSourceFilter && f.data_source !== dataSourceFilter) return false;
    return true;
  }).sort((a, b) => {
    if (!factorSortKey || !factorSortDir) return 0;
    const dir = factorSortDir === "asc" ? 1 : -1;
    if (factorSortKey === "grade") {
      return dir * ((GRADE_ORDER[a.grade || "D"] ?? 5) - (GRADE_ORDER[b.grade || "D"] ?? 5));
    }
    if (factorSortKey === "ic") {
      return dir * ((b.ic ?? -999) - (a.ic ?? -999));
    }
    if (factorSortKey === "sharpe") {
      return dir * ((b.sharpe ?? -999) - (a.sharpe ?? -999));
    }
    if (factorSortKey === "category") {
      return dir * ((a.category || "ZZZ").localeCompare(b.category || "ZZZ"));
    }
    if (factorSortKey === "name") {
      return dir * a.factor_name.localeCompare(b.factor_name);
    }
    return 0;
  });

  const totalFactorPages = Math.max(1, Math.ceil(allFilteredFactors.length / factorPageSize));
  const pagedFactors = allFilteredFactors.slice((factorPage - 1) * factorPageSize, factorPage * factorPageSize);

  function toggleFactor(name: string) {
    setSelectedFactors(prev => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name); else next.add(name);
      return next;
    });
  }

  /* ── AI评估组合 ── */
  async function evaluateCombination() {
    if (selectedFactors.size === 0) { alert("请先选择因子"); return; }
    setActionLoading("evaluate");
    try {
      const res = await fetch(`${API}/quantevolver/portfolio/evaluate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          factor_names: Array.from(selectedFactors),
          model_id: selectedModel || undefined,
          strategy_id: selectedStrategy || undefined,
          custom_params: { topk, n_drop: nDrop, disable_alpha158: disableAlphaBaseline, quick_train: quickTrain },
        }),
      });
      setEvalResult(await res.json());
    } catch (e: any) { alert("评估失败: " + (e?.message || "")); }
    setActionLoading(null);
  }

  /* ── 生成QLib配置 ── */
  async function generateConfig() {
    if (selectedFactors.size === 0) { alert("请先选择因子"); return; }
    setActionLoading("generate");
    try {
      const res = await fetch(`${API}/quantevolver/config/generate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          factor_names: Array.from(selectedFactors),
          model_id: selectedModel || undefined,
          strategy_id: selectedStrategy || undefined,
          data_split: dataSplit,
          custom_params: { topk, n_drop: nDrop, disable_alpha158: disableAlphaBaseline, quick_train: quickTrain },
        }),
      });
      setConfigResult(await res.json());
    } catch (e: any) { alert("生成失败: " + (e?.message || "")); }
    setActionLoading(null);
  }

  /* ── 渲染 ── */
  return (
    <main className="p-6 max-w-[1400px] mx-auto bg-gray-50 min-h-screen">
      {/* 顶部Banner */}
      <section className="rounded-2xl p-5 text-white mb-5" style={{ background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 50%, #06b6d4 100%)" }}>
        <h1 className="text-2xl font-bold m-0">组合配置</h1>
        <p className="mt-2 opacity-90 text-sm">输入投资需求 → AI分析因子库 → 智能推荐组合 → 生成QLib配置 → WSL执行回测</p>
      </section>

      {/* 模式切换 */}
      <div className="flex gap-0 mb-5">
        <button onClick={() => setMode("ai")}
          className={`px-5 py-2 text-sm font-semibold border-none cursor-pointer rounded-l-lg ${mode === "ai" ? "bg-purple-600 text-white" : "bg-gray-100 text-gray-700"}`}>
          AI智能生成
        </button>
        <button onClick={() => setMode("manual")}
          className={`px-5 py-2 text-sm font-semibold border-none cursor-pointer rounded-r-lg ${mode === "manual" ? "bg-purple-600 text-white" : "bg-gray-100 text-gray-700"}`}>
          手动配置
        </button>
      </div>

      {/* ════════════════════════════════════════════════════════════ */}
      {/* AI智能生成模式 */}
      {/* ════════════════════════════════════════════════════════════ */}
      {mode === "ai" && (
        <div className="grid gap-5" style={{ gridTemplateColumns: "1fr 400px" }}>
          {/* 左侧 */}
          <div>
            {/* 需求输入 */}
            <section className="bg-white rounded-xl p-4 shadow-sm mb-4">
              <h2 className="text-base font-semibold mb-3">投资需求描述</h2>
              <textarea
                value={userRequirement}
                onChange={e => setUserRequirement(e.target.value)}
                placeholder="请描述您的投资需求，例如：&#10;• 稳健低回撤策略，注重风险控制&#10;• 重点关注资金流和筹码因子，捕捉主力动向&#10;• 精选少量高夏普因子，追求风险调整后收益&#10;• 价值和动量因子结合，多样化配置&#10;• 激进高收益策略，大量因子全面覆盖"
                className="w-full border border-gray-300 rounded-lg p-3 text-sm resize-none focus:outline-none focus:ring-2 focus:ring-purple-400"
                rows={5}
              />
              <div className="flex gap-3 mt-3">
                <button
                  onClick={aiGenerate}
                  disabled={aiLoading}
                  className="px-6 py-2.5 bg-purple-600 text-white font-semibold rounded-lg border-none cursor-pointer text-sm hover:bg-purple-700 disabled:opacity-50"
                >
                  {aiLoading ? "分析中..." : "AI智能生成组合"}
                </button>
                <div className="flex gap-2 flex-wrap">
                  {["稳健低回撤", "资金流+筹码", "精选高夏普", "全面覆盖"].map(tag => (
                    <button key={tag} onClick={() => setUserRequirement(
                      tag === "稳健低回撤" ? "稳健低回撤策略，注重风险控制，偏好质量和价值因子" :
                      tag === "资金流+筹码" ? "重点关注资金流和筹码因子，捕捉主力动向和筹码变化" :
                      tag === "精选高夏普" ? "精选少量高夏普因子，追求风险调整后收益" :
                      "全面覆盖各类因子，大量因子多样化配置"
                    )}
                      className="px-3 py-1 text-xs border border-gray-300 rounded-full bg-white cursor-pointer hover:bg-purple-50 hover:border-purple-300"
                    >{tag}</button>
                  ))}
                </div>
              </div>
            </section>

            {/* AI分析结果 */}
            {aiResult && aiResult.ok && (
              <>
                {/* 分析思路 */}
                <section className="bg-white rounded-xl p-4 shadow-sm mb-4">
                  <h2 className="text-base font-semibold mb-3">分析思路</h2>
                  <div className="space-y-1.5">
                    {(aiResult.analysis_steps || []).map((step, i) => (
                      <div key={i} className="flex items-start gap-2 text-sm text-gray-700">
                        <span className="flex-shrink-0 w-5 h-5 rounded-full bg-purple-100 text-purple-700 text-xs flex items-center justify-center font-bold">{i + 1}</span>
                        <span>{step.replace(/^\d+\.\s*/, "")}</span>
                      </div>
                    ))}
                  </div>
                  {aiResult.design_rationale && (
                    <div className="mt-3 p-3 bg-purple-50 rounded-lg text-sm text-purple-800">
                      <strong>设计理由：</strong>{aiResult.design_rationale}
                    </div>
                  )}
                </section>

                {/* 因子库全量概览 */}
                {aiResult.all_categories_overview && (
                  <section className="bg-white rounded-xl p-4 shadow-sm mb-4">
                    <h2 className="text-base font-semibold mb-3">因子库全量概览</h2>
                    <div className="text-xs text-gray-500 mb-2">
                      共 {aiResult.metadata_summary?.total_factors_available || 0} 个已评级因子，
                      {Object.keys(aiResult.all_categories_overview).length} 个类别
                    </div>
                    <div className="grid grid-cols-2 gap-2">
                      {Object.entries(aiResult.all_categories_overview).sort((a, b) => b[1].total_factors - a[1].total_factors).map(([cat, info]) => (
                        <div key={cat} className={`p-2.5 rounded-lg border text-xs ${info.selected_count > 0 ? "border-purple-300 bg-purple-50" : "border-gray-200 bg-gray-50"}`}>
                          <div className="flex justify-between items-center">
                            <span className="font-semibold text-gray-800">{info.category_name}</span>
                            <span className="text-gray-500">{info.total_factors}个</span>
                          </div>
                          <div className="flex gap-1 mt-1 flex-wrap">
                            {Object.entries(info.grade_distribution).sort().map(([g, cnt]) => (
                              <span key={g} className="px-1.5 py-0.5 rounded text-[10px] font-bold text-white"
                                style={{ background: g === "S" ? "#7c3aed" : g === "A" ? "#2563eb" : g === "B" ? "#10b981" : g === "C" ? "#f59e0b" : "#6b7280" }}>
                                {g}:{cnt}
                              </span>
                            ))}
                          </div>
                          {info.selected_count > 0 && (
                            <div className="mt-1 text-purple-700 font-semibold">已选 {info.selected_count} 个</div>
                          )}
                        </div>
                      ))}
                    </div>
                  </section>
                )}

                {/* 推荐因子详情（表格格式） */}
                {aiResult.combination && (
                  <section className="bg-white rounded-xl p-4 shadow-sm mb-4">
                    <h2 className="text-base font-semibold mb-3">
                      推荐因子组合
                      <span className="ml-2 text-sm font-normal text-gray-500">
                        {aiResult.combination.factor_count} 个因子，{Object.keys(aiResult.combination.category_summary).length} 个类别
                      </span>
                    </h2>
                    <div className="overflow-x-auto">
                      <table className="w-full text-xs" style={{ borderCollapse: "collapse" }}>
                        <thead>
                          <tr className="text-left border-b-2 border-gray-200">
                            <th className="px-2 py-1.5 font-semibold text-[11px]" style={{ maxWidth: 180 }}>因子名称</th>
                            <th className="px-2 py-1.5 font-semibold text-[11px]">维度</th>
                            <th className="px-2 py-1.5 font-semibold text-[11px]">类别</th>
                            <th className="px-2 py-1.5 font-semibold text-[11px]">评级</th>
                            <th className="px-2 py-1.5 font-semibold text-[11px]">IC</th>
                            <th className="px-2 py-1.5 font-semibold text-[11px]">年化</th>
                            <th className="px-2 py-1.5 font-semibold text-[11px]">说明</th>
                            <th className="px-2 py-1.5 font-semibold text-[11px]">推荐理由</th>
                          </tr>
                        </thead>
                        <tbody>
                          {(aiResult.combination.factor_details || []).map((f: any) => {
                            const isExpanded = expandedAiFactors.has(f.factor_name);
                            return (
                              <tr key={f.factor_name} className="border-b border-gray-100">
                                <td className="px-2 py-1.5" style={{ maxWidth: 180 }}>
                                  <span className="font-mono font-semibold text-[11px]" style={{ wordBreak: "break-all" }}>{f.factor_name}</span>
                                </td>
                                <td className="px-2 py-1.5">
                                  {f.factor_dimension ? (
                                    <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${f.factor_dimension === "cross_sectional" ? "bg-blue-100 text-blue-700" : "bg-green-100 text-green-700"}`}>
                                      {f.factor_dimension === "cross_sectional" ? "截面" : "时序"}
                                    </span>
                                  ) : <span className="text-gray-300">-</span>}
                                </td>
                                <td className="px-2 py-1.5">
                                  {f.category && (
                                    <span className="px-1.5 py-0.5 rounded text-[9px] font-semibold bg-gray-100 text-gray-600">{f.category}</span>
                                  )}
                                </td>
                                <td className="px-2 py-1.5">
                                  {f.grade && (
                                    <span className="px-1.5 py-0.5 rounded text-[10px] font-bold text-white"
                                      style={{ background: f.grade === "S" ? "#7c3aed" : f.grade === "A" ? "#2563eb" : f.grade === "B" ? "#10b981" : f.grade === "C" ? "#f59e0b" : "#6b7280" }}>
                                      {f.grade}
                                    </span>
                                  )}
                                </td>
                                <td className="px-2 py-1.5 text-[11px]">{f.ic_value != null ? Number(f.ic_value).toFixed(4) : "-"}</td>
                                <td className="px-2 py-1.5 text-[11px]">{f.ann_ret_value != null ? (Number(f.ann_ret_value) * 100).toFixed(1) + "%" : "-"}</td>
                                <td className="px-2 py-1.5">
                                  {f.description ? (
                                    <>
                                      <span
                                        onClick={() => setExpandedAiFactors(prev => {
                                          const next = new Set(prev);
                                          if (next.has(f.factor_name)) next.delete(f.factor_name); else next.add(f.factor_name);
                                          return next;
                                        })}
                                        className="text-purple-600 cursor-pointer text-[10px]"
                                        style={{ borderBottom: "1px dashed #7c3aed", userSelect: "none" }}
                                      >
                                        {isExpanded ? "收起" : "展开"}
                                      </span>
                                      {isExpanded && (
                                        <div className="mt-1 p-2 bg-purple-50 rounded text-[11px] text-gray-700 leading-relaxed border-l-2 border-purple-400">
                                          {f.description}
                                        </div>
                                      )}
                                    </>
                                  ) : <span className="text-gray-300 text-[10px]">-</span>}
                                </td>
                                <td className="px-2 py-1.5 text-[11px] text-purple-700">{f.selection_reason || "-"}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </section>
                )}

                {/* 模型信息 */}
                {aiResult.combination?.model_info && (
                  <section className="bg-white rounded-xl p-4 shadow-sm mb-4">
                    <h2 className="text-base font-semibold mb-3">推荐模型</h2>
                    <div className="p-3 bg-blue-50 rounded-lg border border-blue-200">
                      <div className="flex items-center gap-2 text-sm font-semibold text-gray-800">
                        {aiResult.combination.model_info.display_name || aiResult.combination.model_info.model_name}
                        {aiResult.combination.model_info.is_sota && <span className="text-xs text-amber-500">SOTA</span>}
                      </div>
                      <div className="mt-1 text-xs text-gray-600 flex gap-4">
                        <span>类型: {aiResult.combination.model_info.model_type}</span>
                        {aiResult.combination.model_info.ic != null && <span>IC: {Number(aiResult.combination.model_info.ic).toFixed(4)}</span>}
                        {aiResult.combination.model_info.annualized_return != null && <span>年化: {(Number(aiResult.combination.model_info.annualized_return) * 100).toFixed(1)}%</span>}
                      </div>
                      {aiResult.combination.model_info.description && (
                        <div className="mt-2 text-xs text-gray-600">{aiResult.combination.model_info.description}</div>
                      )}
                    </div>
                  </section>
                )}
              </>
            )}

            {/* AI生成失败 */}
            {aiResult && !aiResult.ok && (
              <section className="bg-white rounded-xl p-4 shadow-sm mb-4">
                <div className="text-red-600 text-sm">生成失败：{aiResult.error}</div>
              </section>
            )}
          </div>

          {/* 右侧：操作和评估 */}
          <div>
            {/* 操作 */}
            <section className="bg-white rounded-xl p-4 shadow-sm">
              <h2 className="text-base font-semibold mb-3">操作</h2>
              <div className="flex flex-col gap-2">
                <button onClick={evaluateCombination} disabled={actionLoading === "evaluate" || selectedFactors.size === 0}
                  className="w-full py-2.5 bg-purple-600 text-white font-semibold rounded-lg border-none cursor-pointer text-sm disabled:opacity-50">
                  {actionLoading === "evaluate" ? "评估中..." : "AI评估组合"}
                </button>
                <button onClick={generateConfig} disabled={actionLoading === "generate" || selectedFactors.size === 0}
                  className="w-full py-2.5 bg-blue-600 text-white font-semibold rounded-lg border-none cursor-pointer text-sm disabled:opacity-50">
                  {actionLoading === "generate" ? "生成中..." : "生成QLib配置"}
                </button>
              </div>
              <div className="mt-3 pt-3 border-t border-gray-100 text-xs text-gray-600 space-y-1">
                <div><strong>因子：</strong>{selectedFactors.size} 个</div>
                <div><strong>模型：</strong>{selectedModel || "未选择"}</div>
                <div><strong>topk/n_drop：</strong>{topk}/{nDrop}</div>
                <div><strong>Alpha基线：</strong>{disableAlphaBaseline ? "禁用" : "启用"}</div>
              </div>
            </section>

            {/* 数据划分 */}
            <section className="bg-white rounded-xl p-4 shadow-sm mt-4">
              <h2 className="text-base font-semibold mb-3">数据划分</h2>
              <div className="space-y-2">
                {[
                  { label: "训练集", sk: "train_start", ek: "train_end" },
                  { label: "验证集", sk: "valid_start", ek: "valid_end" },
                  { label: "回测集", sk: "test_start", ek: "test_end" },
                ].map(seg => (
                  <div key={seg.label} className="flex items-center gap-2 text-xs">
                    <span className="w-12 font-semibold">{seg.label}</span>
                    <input type="date" value={(dataSplit as any)[seg.sk]}
                      onChange={e => setDataSplit(prev => ({ ...prev, [seg.sk]: e.target.value }))}
                      className="px-1.5 py-1 border border-gray-300 rounded text-xs w-28" />
                    <span className="text-gray-400">~</span>
                    <input type="date" value={(dataSplit as any)[seg.ek]}
                      onChange={e => setDataSplit(prev => ({ ...prev, [seg.ek]: e.target.value }))}
                      className="px-1.5 py-1 border border-gray-300 rounded text-xs w-28" />
                  </div>
                ))}
              </div>
            </section>

            {/* 评估结果 */}
            {(evalResult || aiResult?.evaluation) && (
              <section className="bg-white rounded-xl p-4 shadow-sm mt-4">
                <h2 className="text-base font-semibold mb-3">组合评估</h2>
                {(() => {
                  const ev = evalResult || aiResult?.evaluation;
                  if (!ev) return null;
                  const score = ev.overall_score || 0;
                  return (
                    <>
                      <div className="text-center mb-3">
                        <div className="inline-flex items-center justify-center w-20 h-20 rounded-full text-2xl font-bold"
                          style={{
                            background: `conic-gradient(${scoreColor(score)} ${score * 3.6}deg, #f3f4f6 0deg)`,
                            color: scoreColor(score),
                          }}>
                          {score.toFixed(0)}
                        </div>
                        <div className="text-xs text-gray-500 mt-1">综合评分</div>
                      </div>
                      {/* LLM综合分析评论 */}
                      {ev.llm_commentary && (
                        <div className="mb-3 p-3 bg-purple-50 rounded-lg border border-purple-200">
                          <div className="text-xs font-semibold mb-1.5 text-purple-800">AI综合分析</div>
                          <div className="text-xs text-gray-700 leading-relaxed whitespace-pre-wrap">{ev.llm_commentary}</div>
                        </div>
                      )}
                      {ev.risks?.length > 0 && (
                        <div className="mb-2">
                          <div className="text-xs font-semibold mb-1">风险提示</div>
                          {ev.risks.map((r: any, i: number) => (
                            <div key={i} className={`px-2 py-1 rounded text-xs mb-1 ${r.level === "critical" ? "bg-red-100 text-red-800" : r.level === "warning" ? "bg-amber-100 text-amber-800" : r.type === "llm_insight" ? "bg-purple-100 text-purple-800" : "bg-gray-100 text-gray-700"}`}>
                              {r.message}
                            </div>
                          ))}
                        </div>
                      )}
                      {ev.suggestions?.length > 0 && (
                        <div>
                          <div className="text-xs font-semibold mb-1">优化建议</div>
                          {ev.suggestions.map((s: string, i: number) => (
                            <div key={i} className="px-2 py-1 rounded text-xs mb-1 bg-green-50 text-green-800">{s}</div>
                          ))}
                        </div>
                      )}
                    </>
                  );
                })()}
              </section>
            )}

            {/* 生成结果 */}
            {configResult?.ok && (
              <section className="bg-white rounded-xl p-4 shadow-sm mt-4">
                <h2 className="text-base font-semibold mb-3">配置已生成</h2>
                <div className="text-xs space-y-1 mb-2">
                  <div><strong>实验ID：</strong>{configResult.experiment_id}</div>
                  <div><strong>因子数：</strong>{configResult.factor_count}</div>
                  <div><strong>目录：</strong><span className="font-mono text-[11px]">{configResult.experiment_dir}</span></div>
                </div>
                {configResult.wsl_command && (
                  <>
                    <div className="text-xs font-semibold mb-1">WSL执行命令</div>
                    <pre className="bg-slate-800 text-slate-200 p-3 rounded-lg text-[11px] overflow-auto max-h-48 whitespace-pre-wrap">
                      {configResult.wsl_command}
                    </pre>
                    <button onClick={() => { navigator.clipboard.writeText(configResult.wsl_command || ""); alert("已复制到剪贴板"); }}
                      className="mt-2 px-3 py-1 text-xs border border-gray-300 rounded bg-white cursor-pointer hover:bg-gray-50">
                      复制命令
                    </button>
                  </>
                )}
              </section>
            )}
          </div>
        </div>
      )}

      {/* ════════════════════════════════════════════════════════════ */}
      {/* 手动配置模式 */}
      {/* ════════════════════════════════════════════════════════════ */}
      {mode === "manual" && (
        <div className="space-y-5">
          {/* 因子选择 */}
          <section className="bg-white rounded-xl p-4 shadow-sm">
            <div className="flex justify-between items-center mb-3">
              <h2 className="text-base font-semibold m-0">因子选择</h2>
              <span className="text-xs text-gray-500">已选 {selectedFactors.size} 个 · 共 {allFilteredFactors.length} 个</span>
            </div>
            {/* 筛选控件 */}
            <div className="flex flex-wrap gap-2 mb-3 items-center">
              {/* 来源筛选 */}
              <div className="flex gap-0">
                {[
                  { key: "all", label: "全部" },
                  { key: "sota", label: "RDAgent SOTA" },
                  { key: "alpha158", label: "Alpha158" },
                  { key: "alpha360", label: "Alpha360" },
                ].map((t, idx, arr) => (
                  <button key={t.key} onClick={() => { setFactorSourceTab(t.key); setFactorPage(1); }}
                    className={`px-3 py-1 text-xs border-none cursor-pointer ${factorSourceTab === t.key ? "bg-purple-600 text-white" : "bg-gray-100 text-gray-700"} ${idx === 0 ? "rounded-l-md" : idx === arr.length - 1 ? "rounded-r-md" : ""}`}>
                    {t.label}
                  </button>
                ))}
              </div>
              {/* 因子类型筛选 */}
              <select value={factorTypeFilter} onChange={e => { setFactorTypeFilter(e.target.value); setFactorPage(1); }}
                className="px-2 py-1 text-xs border border-gray-300 rounded">
                <option value="">全部类型</option>
                <option value="CrossSection">截面因子</option>
                <option value="TimeSeries">时序因子</option>
              </select>
              {/* 数据来源筛选 */}
              <select value={dataSourceFilter} onChange={e => { setDataSourceFilter(e.target.value); setFactorPage(1); }}
                className="px-2 py-1 text-xs border border-gray-300 rounded">
                <option value="">全部来源</option>
                <option value="daily_pv">日线行情</option>
                <option value="daily_basic">每日基本面</option>
                <option value="moneyflow">个股资金流向</option>
                <option value="cyq_perf">筹码分布</option>
                <option value="bak_basic">股票历史信息</option>
                <option value="multi">多数据源</option>
              </select>
              {/* Alpha因子开关 */}
              {factorSourceTab === "all" && (
                <label className="flex items-center gap-1 text-xs cursor-pointer">
                  <input type="checkbox" checked={showAlphaFactors} onChange={e => { setShowAlphaFactors(e.target.checked); setFactorPage(1); }} />
                  显示Alpha因子
                </label>
              )}
              {/* 操作按钮 */}
              <div className="flex gap-1 ml-auto">
                <button onClick={() => { const next = new Set(selectedFactors); allFilteredFactors.forEach(f => next.add(f.factor_name)); setSelectedFactors(next); }}
                  className="px-2 py-1 text-xs border border-gray-300 rounded bg-white cursor-pointer hover:bg-gray-50">全选当前页</button>
                <button onClick={() => setSelectedFactors(new Set())}
                  className="px-2 py-1 text-xs border border-gray-300 rounded bg-white cursor-pointer hover:bg-gray-50">清空</button>
              </div>
            </div>
            {/* 因子表格 - 与因子库页面风格完全一致 */}
            <div className="overflow-x-auto">
              {loading ? (
                <div className="p-5 text-center text-gray-400 text-sm">加载中...</div>
              ) : pagedFactors.length === 0 ? (
                <div className="p-5 text-center text-gray-400 text-sm">暂无因子</div>
              ) : (
                <table className="w-full border-collapse" style={{ background: "#fff" }}>
                  <thead>
                    <tr style={{ background: "#f9fafb" }}>
                      <th className="px-2 py-2 font-semibold text-xs w-8 border-b-2 border-gray-200 border-r border-gray-100">
                        <input type="checkbox" 
                          checked={pagedFactors.every(f => selectedFactors.has(f.factor_name))}
                          ref={el => { if (el) el.indeterminate = pagedFactors.some(f => selectedFactors.has(f.factor_name)) && !pagedFactors.every(f => selectedFactors.has(f.factor_name)); }}
                          onChange={e => {
                            const next = new Set(selectedFactors);
                            if (e.target.checked) {
                              pagedFactors.forEach(f => next.add(f.factor_name));
                            } else {
                              pagedFactors.forEach(f => next.delete(f.factor_name));
                            }
                            setSelectedFactors(next);
                          }}
                        />
                      </th>
                      <th onClick={() => handleFactorSort("name")} className="px-2 py-2 font-semibold text-xs cursor-pointer border-b-2 border-gray-200 border-r border-gray-100 hover:bg-gray-100">
                        因子名 {getSortIndicator("name")}
                      </th>
                      <th onClick={() => handleFactorSort("grade")} className="px-2 py-2 font-semibold text-xs cursor-pointer border-b-2 border-gray-200 border-r border-gray-100 hover:bg-gray-100">
                        评级 {getSortIndicator("grade")}
                      </th>
                      <th onClick={() => handleFactorSort("factor_type")} className="px-2 py-2 font-semibold text-xs cursor-pointer border-b-2 border-gray-200 border-r border-gray-100 hover:bg-gray-100">
                        因子类型 {getSortIndicator("factor_type")}
                      </th>
                      <th onClick={() => handleFactorSort("data_source")} className="px-2 py-2 font-semibold text-xs cursor-pointer border-b-2 border-gray-200 border-r border-gray-100 hover:bg-gray-100">
                        数据来源 {getSortIndicator("data_source")}
                      </th>
                      <th onClick={() => handleFactorSort("ic")} className="px-2 py-2 font-semibold text-xs cursor-pointer border-b-2 border-gray-200 border-r border-gray-100 hover:bg-gray-100">
                        IC {getSortIndicator("ic")}
                      </th>
                      <th onClick={() => handleFactorSort("sharpe")} className="px-2 py-2 font-semibold text-xs cursor-pointer border-b-2 border-gray-200 border-r border-gray-100 hover:bg-gray-100">
                        Sharpe {getSortIndicator("sharpe")}
                      </th>
                      <th className="px-2 py-2 font-semibold text-xs border-b-2 border-gray-200">
                        说明
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {pagedFactors.map(f => (
                      <tr key={`${f.factor_name}-${f.source}`}
                        onClick={() => toggleFactor(f.factor_name)}
                        className={`cursor-pointer border-b border-gray-200 ${selectedFactors.has(f.factor_name) ? "bg-purple-50" : "hover:bg-gray-50"}`}>
                        <td className="px-2 py-1.5 border-r border-gray-100">
                          <input type="checkbox" checked={selectedFactors.has(f.factor_name)} onChange={() => toggleFactor(f.factor_name)} onClick={e => e.stopPropagation()} />
                        </td>
                        <td className="px-2 py-1.5 font-mono font-semibold text-xs border-r border-gray-100">{f.factor_name}</td>
                        <td className="px-2 py-1.5 border-r border-gray-100">
                          {f.grade && (
                            <span className="px-2 py-1 rounded text-xs font-bold text-white"
                              style={{ background: f.grade === "S" ? "#7c3aed" : f.grade === "A" ? "#2563eb" : f.grade === "B" ? "#10b981" : f.grade === "C" ? "#f59e0b" : "#6b7280" }}>
                              {f.grade}
                            </span>
                          )}
                        </td>
                        <td className="px-2 py-1.5 border-r border-gray-100">
                          <span className="px-1.5 py-0.5 rounded text-[10px] font-medium"
                            style={{ background: f.factor_type === "CrossSection" ? "#dbeafe" : f.factor_type === "TimeSeries" ? "#fef3c7" : "#f3f4f6" }}>
                            {f.factor_type ? FACTOR_TYPE_MAP[f.factor_type] || f.factor_type : "-"}
                          </span>
                        </td>
                        <td className="px-2 py-1.5 border-r border-gray-100">
                          <span className="px-1.5 py-0.5 rounded text-[10px] font-medium"
                            style={{ background: f.data_source ? "#e0f2fe" : "#f3f4f6" }}>
                            {f.data_source ? DATA_SOURCE_MAP[f.data_source] || f.data_source : "-"}
                          </span>
                        </td>
                        <td className="px-2 py-1.5 text-xs border-r border-gray-100">{f.ic != null ? f.ic.toFixed(4) : "-"}</td>
                        <td className="px-2 py-1.5 text-xs border-r border-gray-100">{f.sharpe != null ? f.sharpe.toFixed(2) : "-"}</td>
                        <td className="px-2 py-1.5 text-xs">
                          {f.description ? (
                            <>
                              <span
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setExpandedFactorDesc(prev => {
                                    const next = new Set(prev);
                                    if (next.has(f.factor_name)) next.delete(f.factor_name); else next.add(f.factor_name);
                                    return next;
                                  });
                                }}
                                className="text-purple-600 cursor-pointer text-[11px] select-none"
                                style={{ borderBottom: "1px dashed #7c3aed" }}
                              >
                                {expandedFactorDesc.has(f.factor_name) ? "收起" : "展开"}
                              </span>
                              {expandedFactorDesc.has(f.factor_name) && (
                                <div className="mt-1 p-2 bg-purple-50 rounded text-[11px] text-gray-700 leading-relaxed border-l-2 border-purple-400">
                                  {f.description}
                                </div>
                              )}
                            </>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
            {/* 分页控件 */}
            {totalFactorPages > 1 && (
              <div className="flex items-center justify-center gap-2 mt-3 pt-3 border-t border-gray-100">
                <button disabled={factorPage <= 1} onClick={() => setFactorPage(1)}
                  className="px-2 py-0.5 text-[11px] border border-gray-300 rounded bg-white cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed">首页</button>
                <button disabled={factorPage <= 1} onClick={() => setFactorPage(p => p - 1)}
                  className="px-2 py-0.5 text-[11px] border border-gray-300 rounded bg-white cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed">上一页</button>
                <span className="text-[11px] text-gray-500">{factorPage}/{totalFactorPages}</span>
                <button disabled={factorPage >= totalFactorPages} onClick={() => setFactorPage(p => p + 1)}
                  className="px-2 py-0.5 text-[11px] border border-gray-300 rounded bg-white cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed">下一页</button>
                <button disabled={factorPage >= totalFactorPages} onClick={() => setFactorPage(totalFactorPages)}
                  className="px-2 py-0.5 text-[11px] border border-gray-300 rounded bg-white cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed">末页</button>
              </div>
            )}
          </section>

          {/* 模型选择 - 表格形式，与模型库页面一致 */}
          <section className="bg-white rounded-xl p-4 shadow-sm">
            <div className="flex justify-between items-center mb-3">
              <h2 className="text-base font-semibold m-0">模型选择</h2>
              <span className="text-xs text-gray-500">已选: {selectedModel || "未选择"}</span>
            </div>
            {models.length === 0 ? (
              <div className="text-xs text-gray-400 text-center py-4">暂无模型数据</div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full border-collapse" style={{ background: "#fff" }}>
                  <thead>
                    <tr style={{ background: "#f9fafb" }}>
                      <th className="px-2 py-2 font-semibold text-xs w-8 border-b-2 border-gray-200 border-r border-gray-100">选择</th>
                      <th className="px-2 py-2 font-semibold text-xs border-b-2 border-gray-200 border-r border-gray-100">模型名</th>
                      <th className="px-2 py-2 font-semibold text-xs border-b-2 border-gray-200 border-r border-gray-100">类型</th>
                      <th className="px-2 py-2 font-semibold text-xs border-b-2 border-gray-200 border-r border-gray-100">IC</th>
                      <th className="px-2 py-2 font-semibold text-xs border-b-2 border-gray-200 border-r border-gray-100">年化收益</th>
                      <th className="px-2 py-2 font-semibold text-xs border-b-2 border-gray-200">标签</th>
                    </tr>
                  </thead>
                  <tbody>
                    {models.map(m => (
                      <tr key={m.model_id}
                        onClick={() => setSelectedModel(m.model_id)}
                        className={`cursor-pointer border-b border-gray-200 ${selectedModel === m.model_id ? "bg-purple-50" : "hover:bg-gray-50"}`}>
                        <td className="px-2 py-1.5 border-r border-gray-100">
                          <input type="radio" name="model" checked={selectedModel === m.model_id} onChange={() => setSelectedModel(m.model_id)} onClick={e => e.stopPropagation()} />
                        </td>
                        <td className="px-2 py-1.5 font-mono font-semibold text-xs border-r border-gray-100">{m.display_name || m.model_name}</td>
                        <td className="px-2 py-1.5 text-xs text-gray-600 border-r border-gray-100">{m.model_type || "-"}</td>
                        <td className="px-2 py-1.5 text-xs border-r border-gray-100">{m.ic != null ? m.ic.toFixed(4) : "-"}</td>
                        <td className="px-2 py-1.5 text-xs border-r border-gray-100">{m.annualized_return != null ? `${(m.annualized_return * 100).toFixed(1)}%` : "-"}</td>
                        <td className="px-2 py-1.5">
                          {m.is_sota && <span className="px-1.5 py-0.5 rounded text-[10px] font-bold bg-amber-100 text-amber-700">SOTA</span>}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          {/* 策略选择 + 参数 */}
          <section className="bg-white rounded-xl p-4 shadow-sm">
            <h2 className="text-base font-semibold mb-3">策略选择</h2>
            <div className="flex flex-col gap-1.5">
              {strategies.length === 0 ? (
                <div className="text-xs text-gray-400">暂无策略数据</div>
              ) : strategies.map(s => (
                <label key={s.strategy_id}
                  className={`flex gap-2 items-center p-2 text-xs cursor-pointer rounded-md ${selectedStrategy === s.strategy_id ? "bg-green-50 border border-green-300" : "bg-gray-50 border border-transparent"}`}>
                  <input type="radio" name="strategy" checked={selectedStrategy === s.strategy_id} onChange={() => setSelectedStrategy(s.strategy_id)} />
                  <span className="font-semibold">{s.display_name}</span>
                </label>
              ))}
            </div>
            <div className="mt-3 pt-3 border-t border-gray-100">
              <div className="text-xs font-semibold mb-2">策略参数</div>
              <div className="flex gap-4">
                <label className="text-xs">topk:
                  <input type="number" value={topk} onChange={e => setTopk(Number(e.target.value))}
                    className="w-14 ml-1 px-1.5 py-0.5 text-xs border border-gray-300 rounded" />
                </label>
                <label className="text-xs">n_drop:
                  <input type="number" value={nDrop} onChange={e => setNDrop(Number(e.target.value))}
                    className="w-14 ml-1 px-1.5 py-0.5 text-xs border border-gray-300 rounded" />
                </label>
              </div>
            </div>
            <div className="mt-3 pt-3 border-t border-gray-100">
              <div className="text-xs font-semibold mb-2">基线因子配置</div>
              <label className="flex items-center gap-2 text-xs cursor-pointer">
                <input type="checkbox" checked={disableAlphaBaseline} onChange={e => setDisableAlphaBaseline(e.target.checked)} />
                <span>禁用Alpha158基线因子</span>
                <span className="text-gray-400 text-[10px]">(勾选后不注入Alpha158DL)</span>
              </label>
            </div>
            <div className="mt-3 pt-3 border-t border-gray-100">
              <div className="text-xs font-semibold mb-2">训练模式配置</div>
              <label className="flex items-center gap-2 text-xs cursor-pointer">
                <input type="checkbox" checked={quickTrain} onChange={e => setQuickTrain(e.target.checked)} />
                <span>快速训练模式</span>
                <span className="text-gray-400 text-[10px]">(勾选后训练时间缩短到20%，用于快速验证)</span>
              </label>
              {quickTrain && (
                <div className="mt-1 text-[10px] text-amber-600">
                  ⚠️ 快速模式会减少训练轮次和早停次数，可能影响模型精度
                </div>
              )}
            </div>
          </section>

          {/* 数据划分 */}
          <section className="bg-white rounded-xl p-4 shadow-sm">
            <h2 className="text-base font-semibold mb-3">数据划分</h2>
            <div className="grid grid-cols-3 gap-3">
              {[
                { label: "训练集", sk: "train_start", ek: "train_end" },
                { label: "验证集", sk: "valid_start", ek: "valid_end" },
                { label: "回测集", sk: "test_start", ek: "test_end" },
              ].map(seg => (
                <div key={seg.label}>
                  <div className="text-xs font-semibold mb-1">{seg.label}</div>
                  <input type="date" value={(dataSplit as any)[seg.sk]}
                    onChange={e => setDataSplit(prev => ({ ...prev, [seg.sk]: e.target.value }))}
                    className="px-1 py-0.5 text-[11px] border border-gray-300 rounded w-[110px]" />
                  <span className="text-[11px] text-gray-400"> ~ </span>
                  <input type="date" value={(dataSplit as any)[seg.ek]}
                    onChange={e => setDataSplit(prev => ({ ...prev, [seg.ek]: e.target.value }))}
                    className="px-1 py-0.5 text-[11px] border border-gray-300 rounded w-[110px]" />
                </div>
              ))}
            </div>
          </section>

          {/* AI评估和生成QLib配置 - 移到底部 */}
          <section className="bg-white rounded-xl p-4 shadow-sm">
            <h2 className="text-base font-semibold mb-3">操作</h2>
            <div className="flex flex-col gap-2">
              <button onClick={evaluateCombination} disabled={actionLoading === "evaluate" || selectedFactors.size === 0}
                className="w-full py-2.5 bg-purple-600 text-white font-semibold rounded-lg border-none cursor-pointer text-sm disabled:opacity-50">
                {actionLoading === "evaluate" ? "评估中..." : "AI评估组合"}
              </button>
              <button onClick={generateConfig} disabled={actionLoading === "generate" || selectedFactors.size === 0}
                className="w-full py-2.5 bg-blue-600 text-white font-semibold rounded-lg border-none cursor-pointer text-sm disabled:opacity-50">
                {actionLoading === "generate" ? "生成中..." : "生成QLib配置"}
              </button>
            </div>
            <div className="mt-3 pt-3 border-t border-gray-100 text-xs text-gray-600 space-y-1">
              <div><strong>因子：</strong>{selectedFactors.size} 个</div>
              <div><strong>模型：</strong>{selectedModel || "未选择"}</div>
              <div><strong>策略：</strong>{selectedStrategy || "未选择"}</div>
              <div><strong>Alpha基线：</strong>{disableAlphaBaseline ? "禁用" : "启用"}</div>
            </div>
          </section>

          {/* 评估结果 */}
          {evalResult && (
            <section className="bg-white rounded-xl p-4 shadow-sm">
              <h2 className="text-base font-semibold mb-3">评估结果</h2>
              <div className="text-center mb-3">
                <div className="inline-flex items-center justify-center w-20 h-20 rounded-full text-2xl font-bold"
                  style={{
                    background: `conic-gradient(${scoreColor(evalResult.overall_score || 0)} ${(evalResult.overall_score || 0) * 3.6}deg, #f3f4f6 0deg)`,
                    color: scoreColor(evalResult.overall_score || 0),
                  }}>
                  {evalResult.overall_score?.toFixed(0)}
                </div>
                <div className="text-xs text-gray-500 mt-1">综合评分</div>
              </div>
              {/* LLM综合分析评论 */}
              {evalResult.llm_commentary && (
                <div className="mb-3 p-3 bg-purple-50 rounded-lg border border-purple-200">
                  <div className="text-xs font-semibold mb-1.5 text-purple-800">AI综合分析</div>
                  <div className="text-xs text-gray-700 leading-relaxed whitespace-pre-wrap">{evalResult.llm_commentary}</div>
                </div>
              )}
              {evalResult.risks?.map((r, i) => (
                <div key={i} className={`px-2 py-1 rounded text-xs mb-1 ${r.level === "critical" ? "bg-red-100 text-red-800" : r.level === "warning" ? "bg-amber-100 text-amber-800" : r.type === "llm_insight" ? "bg-purple-100 text-purple-800" : "bg-gray-100 text-gray-700"}`}>
                  {r.message}
                </div>
              ))}
              {evalResult.suggestions?.map((s, i) => (
                <div key={i} className="px-2 py-1 rounded text-xs mb-1 bg-green-50 text-green-800">{s}</div>
              ))}
            </section>
          )}

          {/* 生成结果 */}
          {configResult?.ok && (
            <section className="bg-white rounded-xl p-4 shadow-sm">
              <h2 className="text-base font-semibold mb-3">配置已生成</h2>
              <div className="text-xs space-y-1 mb-2">
                <div><strong>实验ID：</strong>{configResult.experiment_id}</div>
                <div><strong>因子数：</strong>{configResult.factor_count}</div>
                <div><strong>目录：</strong><span className="font-mono text-[11px]">{configResult.experiment_dir}</span></div>
              </div>
              {configResult.wsl_command && (
                <>
                  <div className="text-xs font-semibold mb-1">WSL执行命令</div>
                  <pre className="bg-slate-800 text-slate-200 p-3 rounded-lg text-[11px] overflow-auto max-h-48 whitespace-pre-wrap">
                    {configResult.wsl_command}
                  </pre>
                  <button onClick={() => { navigator.clipboard.writeText(configResult.wsl_command || ""); alert("已复制到剪贴板"); }}
                    className="mt-2 px-3 py-1 text-xs border border-gray-300 rounded bg-white cursor-pointer hover:bg-gray-50">
                    复制命令
                  </button>
                </>
              )}
            </section>
          )}
        </div>
      )}
    </main>
  );
}

function scoreColor(score: number): string {
  if (score >= 80) return "#10b981";
  if (score >= 60) return "#f59e0b";
  return "#ef4444";
}
