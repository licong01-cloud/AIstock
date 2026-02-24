"use client";

import { useEffect, useState, useCallback, useMemo } from "react";
import FactorList from "../components/FactorList";
import ModelList from "../components/ModelList";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

const DATA_SOURCE_MAP: Record<string, string> = {
  daily_pv: "日线行情", daily_basic: "每日基本面", moneyflow: "个股资金流向", cyq_perf: "筹码分布", bak_basic: "股票历史信息", multi: "多数据源",
};
const FACTOR_TYPE_MAP: Record<string, string> = {
  CrossSection: "截面因子", TimeSeries: "时序因子",
};
const GRADE_ORDER: Record<string, number> = { S: 0, A: 1, B: 2, C: 3, D: 4 };

type Factor = { factor_name: string; source: string; ic?: number; sharpe?: number; is_sota_factor?: boolean;
  category?: string; grade?: string; description?: string; ann_ret_value?: number; factor_type?: string; data_source?: string };
type Model = { model_id: string; model_name: string; model_type?: string; ic?: number; annualized_return?: number; is_sota?: boolean; display_name?: string; description?: string };
type Strategy = { strategy_id: string; display_name: string; portfolio_config?: any };

type EvalResult = {
  ok: boolean;
  risks?: { level: string; type: string; message: string }[];
  suggestions?: string[];
  overall_score?: number;
  llm_commentary?: string;
};

type ConfigResult = {
  ok: boolean;
  experiment_id?: string;
  experiment_dir?: string;
  wsl_command?: string;
  factor_count?: number;
};

export default function ComposePage() {
  const [currentStep, setCurrentStep] = useState(1);
  const STEPS = ["因子选择", "模型选择", "策略选择", "组合配置与评估", "生成执行与下发"];

  /* ── AI智能生成 ── */
  const [userRequirement, setUserRequirement] = useState("");
  const [aiLoading, setAiLoading] = useState(false);
  const [aiResult, setAiResult] = useState<any>(null);

  /* ── 数据集 ── */
  const [factors, setFactors] = useState<Factor[]>([]);
  const [models, setModels] = useState<Model[]>([]);
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [loading, setLoading] = useState(true);

  /* ── 选择状态 ── */
  const [selectedFactors, setSelectedFactors] = useState<Set<string>>(new Set());
  const [selectedModel, setSelectedModel] = useState<string>("");
  const [selectedStrategy, setSelectedStrategy] = useState<string>("");

  /* ── 因子列表控制 ── */
  const [factorPage, setFactorPage] = useState(1);
  const factorPageSize = 20;
  const [factorTypeFilter, setFactorTypeFilter] = useState<string>("");
  const [dataSourceFilter, setDataSourceFilter] = useState<string>("");
  const [factorSourceTab, setFactorSourceTab] = useState<string>("all");
  const [showAlphaFactors, setShowAlphaFactors] = useState(false);
  const [factorSortKey, setFactorSortKey] = useState<string>("grade");
  const [factorSortDir, setFactorSortDir] = useState<"asc"|"desc"|null>("asc");

  /* ── 配置参数 ── */
  const [topk, setTopk] = useState(50);
  const [nDrop, setNDrop] = useState(5);
  const [disableAlphaBaseline, setDisableAlphaBaseline] = useState(false);
  const [quickTrain, setQuickTrain] = useState(false);
  const [dataSplit, setDataSplit] = useState({
    train_start: "2018-08-01", train_end: "2022-12-31",
    valid_start: "2023-01-01", valid_end: "2024-06-30",
    test_start: "2024-07-01", test_end: "2025-12-01",
  });
  const [dispatchMode, setDispatchMode] = useState<"independent" | "evolution">("independent");
  const [evolutionLoops, setEvolutionLoops] = useState(5);
  const [evolutionObjective, setEvolutionObjective] = useState("");

  /* ── 结果状态 ── */
  const [evalResult, setEvalResult] = useState<EvalResult | null>(null);
  const [configResult, setConfigResult] = useState<ConfigResult | null>(null);
  const [actionLoading, setActionLoading] = useState<string | null>(null);

  /* ── 加载数据 ── */
  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [fRes, mRes, sRes, cRes] = await Promise.all([
        fetch(`${API}/rdagent/catalogs/factors?limit=1000&exclude_source=qlib_alpha158,alpha158,alpha360`).then(r => r.json()),
        fetch(`${API}/quantevolver/models?limit=100`).then(r => r.json()),
        fetch(`${API}/quantevolver/strategies?limit=50`).then(r => r.json()),
        fetch(`${API}/quantevolver/factor-analyst/classifications?limit=1000&active_only=false`).then(r => r.json()).catch(() => ({ items: [] })),
      ]);
      const classMap: Record<string, any> = {};
      (cRes.items || []).forEach((c: any) => { classMap[c.factor_name] = c; });
      
      const enrichedFactors = (fRes.items || []).map((f: any) => {
        const cls = classMap[f.name];
        return { 
          factor_name: f.name, source: f.source, ic: f.performance_metrics?.ic ?? f.ic, sharpe: f.performance_metrics?.information_ratio ?? f.sharpe, is_sota_factor: f.is_sota_factor,
          factor_type: f.factor_type, data_source: f.data_source, category: cls?.category, grade: cls?.grade, 
          description: f.description_cn || cls?.description, ann_ret_value: f.performance_metrics?.annualized_return ?? f.annualized_return ?? cls?.ann_ret_value,
        };
      });
      setFactors(enrichedFactors);

      const dbModels = mRes.items || [];
      const hasLGB = dbModels.some((m: Model) => m.model_name === "LGBModel" || m.model_type === "LGB");
      const allModels = hasLGB ? dbModels : [
        { model_id: "__builtin_lgbmodel__", model_name: "LGBModel", model_type: "LGB", display_name: "LGBModel (QLib内置)", is_sota: false },
        ...dbModels,
      ];
      setModels(allModels);
      setStrategies(sRes.items || []);
    } catch (e) { console.error("加载数据失败", e); }
    setLoading(false);
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  /* ── AI 智能生成 ── */
  async function aiGenerate() {
    if (!userRequirement.trim()) { alert("请输入实验组合设计目标"); return; }
    setAiLoading(true); setAiResult(null); setConfigResult(null); setEvalResult(null);
    try {
      const res = await fetch(`${API}/quantevolver/experiment/smart-select`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_requirement: userRequirement.trim(), use_llm: true, max_factors: 30 }),
      });
      const data = await res.json();
      setAiResult(data);
      if (data.ok && data.combination) {
        setSelectedFactors(new Set(data.combination.factor_names));
        setSelectedModel(data.combination.model_id || "");
        if (data.combination.strategy_params) {
          setTopk(data.combination.strategy_params.topk || 50);
          setNDrop(data.combination.strategy_params.n_drop || 5);
        }
        setCurrentStep(4);
      } else {
        alert("智能生成失败: " + (data.error || "未知错误"));
      }
    } catch (e: any) {
      alert("智能生成失败: " + (e?.message || ""));
    }
    setAiLoading(false);
  }

  /* ── 因子筛选排序逻辑 ── */
  function handleFactorSort(field: string) {
    if (factorSortKey !== field) { setFactorSortKey(field); setFactorSortDir("desc"); }
    else if (factorSortDir === "desc") { setFactorSortDir("asc"); }
    else if (factorSortDir === "asc") { setFactorSortKey(""); setFactorSortDir(null); }
    else { setFactorSortDir("desc"); }
    setFactorPage(1);
  }

  const allFilteredFactors = useMemo(() => {
    return factors.filter(f => {
      if (factorSourceTab === "sota") return f.source === "rdagent_task_sync";
      if (factorSourceTab === "alpha158") return f.source === "alpha158";
      if (factorSourceTab === "alpha360") return f.source === "alpha360";
      if (!showAlphaFactors && (f.source === "alpha158" || f.source === "alpha360")) return false;
      if (factorTypeFilter && f.factor_type !== factorTypeFilter) return false;
      if (dataSourceFilter && f.data_source !== dataSourceFilter) return false;
      return true;
    }).sort((a, b) => {
      if (!factorSortKey || !factorSortDir) return 0;
      const dir = factorSortDir === "asc" ? 1 : -1;
      if (factorSortKey === "grade") return dir * ((GRADE_ORDER[a.grade || "D"] ?? 5) - (GRADE_ORDER[b.grade || "D"] ?? 5));
      if (factorSortKey === "ic") return dir * ((b.ic ?? -999) - (a.ic ?? -999));
      if (factorSortKey === "sharpe") return dir * ((b.sharpe ?? -999) - (a.sharpe ?? -999));
      if (factorSortKey === "name") return dir * a.factor_name.localeCompare(b.factor_name);
      return 0;
    });
  }, [factors, factorSourceTab, showAlphaFactors, factorTypeFilter, dataSourceFilter, factorSortKey, factorSortDir]);

  const totalFactorPages = Math.max(1, Math.ceil(allFilteredFactors.length / factorPageSize));
  const pagedFactors = allFilteredFactors.slice((factorPage - 1) * factorPageSize, factorPage * factorPageSize);

  /* ── 操作动作 ── */
  async function evaluateCombination() {
    if (selectedFactors.size === 0) { alert("请先选择因子"); return; }
    setActionLoading("evaluate");
    try {
      const res = await fetch(`${API}/quantevolver/experiment/evaluate-portfolio`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          factor_names: Array.from(selectedFactors), model_id: selectedModel || undefined, strategy_id: selectedStrategy || undefined,
          custom_params: { topk, n_drop: nDrop, disable_alpha158: disableAlphaBaseline, quick_train: quickTrain },
        }),
      });
      setEvalResult(await res.json());
    } catch (e: any) { alert("评估失败: " + (e?.message || "")); }
    setActionLoading(null);
  }

  async function generateConfig() {
    if (selectedFactors.size === 0) { alert("请先选择因子"); return; }
    setActionLoading("generate");
    try {
      const res = await fetch(`${API}/quantevolver/config/generate`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          factor_names: Array.from(selectedFactors), model_id: selectedModel || undefined, strategy_id: selectedStrategy || undefined,
          data_split: dataSplit, custom_params: { topk, n_drop: nDrop, disable_alpha158: disableAlphaBaseline, quick_train: quickTrain },
          dispatch_mode: dispatchMode,
          evolution_params: dispatchMode === "evolution" ? { loops: evolutionLoops, objective: evolutionObjective } : undefined
        }),
      });
      setConfigResult(await res.json());
    } catch (e: any) { alert("生成失败: " + (e?.message || "")); }
    setActionLoading(null);
  }

  const getSortIndicator = (field: string) => factorSortKey === field ? (factorSortDir === "desc" ? " ▼" : factorSortDir === "asc" ? " ▲" : "") : "";
  const toggleFactor = (name: string) => setSelectedFactors(prev => { const n = new Set(prev); n.has(name) ? n.delete(name) : n.add(name); return n; });

  return (
    <main className="p-6 max-w-[1400px] mx-auto bg-slate-50 min-h-screen font-sans text-slate-800">
      {/* 顶部标题区 */}
      <header className="mb-6">
         <h1 className="text-3xl font-bold text-slate-900">QE实验设计</h1>
         <p className="text-slate-500 mt-2 text-sm">双轨驱动：支持AI智能生成配置与人工分步流程式选择，为您构建优质的因子组合与模型演进任务。</p>
      </header>
      
      {/* AI 智能实验设计区 */}
      <section className="bg-white rounded-2xl p-6 shadow-sm mb-8 border border-slate-200">
        <h2 className="text-lg font-bold mb-4 text-purple-700 flex items-center gap-2">
          <span>✨ AI 智能实验设计</span>
        </h2>
        <textarea
          value={userRequirement}
          onChange={e => setUserRequirement(e.target.value)}
          placeholder="请输入您的实验组合设计目标，例如：构建一个偏向于动量反转的日频量化组合，配合高频微观结构因子..."
          className="w-full border border-slate-300 rounded-xl p-4 text-sm resize-none focus:outline-none focus:ring-2 focus:ring-purple-400 focus:border-transparent transition-shadow"
          rows={3}
        />
        <div className="flex gap-3 mt-4 items-center">
          <button
            onClick={aiGenerate}
            disabled={aiLoading}
            className="px-6 py-2.5 bg-purple-600 text-white font-semibold rounded-lg shadow hover:bg-purple-700 disabled:opacity-50 transition-colors"
          >
            {aiLoading ? "正在深度解析并生成..." : "智能生成配置"}
          </button>
          <div className="flex gap-2">
             {["稳健低回撤", "资金流选股", "高夏普动量"].map(tag => (
               <button key={tag} onClick={() => setUserRequirement(tag + "组合，要求全面考虑风险控制")}
                 className="px-3 py-1.5 text-xs text-slate-600 border border-slate-200 rounded-full hover:bg-slate-100 transition-colors">
                 {tag}
               </button>
             ))}
          </div>
        </div>
      </section>

      {/* Stepper 导航区 */}
      <section className="mb-8 px-4">
        <div className="flex items-center justify-between relative">
          <div className="absolute left-0 top-1/2 -translate-y-1/2 w-full h-[2px] bg-slate-200 z-0"></div>
          {STEPS.map((step, idx) => {
            const stepNum = idx + 1;
            const isActive = currentStep === stepNum;
            const isPassed = currentStep > stepNum;
            return (
              <div key={stepNum} className="relative z-10 flex flex-col items-center cursor-pointer group" onClick={() => setCurrentStep(stepNum)}>
                <div className={`w-10 h-10 rounded-full flex items-center justify-center font-bold text-sm shadow-sm transition-colors duration-200
                  ${isActive ? 'bg-purple-600 text-white ring-4 ring-purple-100' : isPassed ? 'bg-purple-100 text-purple-600' : 'bg-white text-slate-400 border-2 border-slate-200'}`}>
                  {isPassed ? "✓" : stepNum}
                </div>
                <div className={`mt-2 text-sm font-medium ${isActive ? 'text-purple-700' : isPassed ? 'text-slate-700' : 'text-slate-400'}`}>
                  {step}
                </div>
              </div>
            );
          })}
        </div>
      </section>

      {/* 下部：分步卡片区 */}
      <section className="bg-white rounded-2xl shadow-sm border border-slate-200 min-h-[500px] overflow-hidden">
        
        {/* Step 1: 因子选择 */}
        {currentStep === 1 && (
          <div className="p-6">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-xl font-bold text-slate-800">1. 因子选择 (Factor Selection)</h2>
              <span className="bg-purple-100 text-purple-700 px-3 py-1 rounded-full text-sm font-semibold">已选 {selectedFactors.size} 项</span>
            </div>

            <div className="border border-slate-200 rounded-lg overflow-hidden">
              <FactorList
                mode="selection"
                selectedFactors={selectedFactors}
                onFactorSelect={(selected) => setSelectedFactors(selected)}
              />
            </div>

            <div className="flex justify-end mt-6">
              <button onClick={() => setCurrentStep(2)} className="px-8 py-2.5 bg-slate-900 text-white font-medium rounded-lg hover:bg-slate-800 transition-colors">下一步：选择模型</button>
            </div>
          </div>
        )}

        {/* Step 2: 模型选择 */}
        {currentStep === 2 && (
          <div className="p-6">
            <h2 className="text-xl font-bold text-slate-800 mb-4">2. 模型选择 (Model Selection)</h2>
            
            <div className="border border-slate-200 rounded-lg overflow-hidden mb-6">
              <ModelList
                mode="selection"
                selectedModel={selectedModel}
                onSelectModel={(modelId) => setSelectedModel(modelId)}
              />
            </div>

            <div className="flex justify-between mt-6">
              <button onClick={() => setCurrentStep(1)} className="px-6 py-2.5 bg-white border border-slate-300 text-slate-700 font-medium rounded-lg hover:bg-slate-50 transition-colors">上一步</button>
              <button onClick={() => setCurrentStep(3)} className="px-8 py-2.5 bg-slate-900 text-white font-medium rounded-lg hover:bg-slate-800 transition-colors">下一步：选择策略</button>
            </div>
          </div>
        )}

        {/* Step 3: 策略选择 */}
        {currentStep === 3 && (
          <div className="p-6">
            <h2 className="text-xl font-bold text-slate-800 mb-4">3. 策略选择 (Strategy Selection)</h2>
            
            <div className="mb-6">
              <h3 className="text-sm font-bold text-slate-500 uppercase tracking-wider mb-3">日频策略 (Daily Frequency)</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {strategies.map(s => (
                  <div key={s.strategy_id} onClick={() => setSelectedStrategy(s.strategy_id)}
                    className={`border-2 rounded-xl p-4 cursor-pointer transition-all ${selectedStrategy === s.strategy_id ? "border-purple-600 bg-purple-50/30 shadow-md" : "border-slate-200 hover:border-purple-300 hover:bg-slate-50"}`}>
                    <div className="flex items-center gap-3">
                      <div className="w-5 h-5 rounded-full border border-slate-300 flex items-center justify-center bg-white flex-shrink-0">
                        {selectedStrategy === s.strategy_id && <div className="w-3 h-3 bg-purple-600 rounded-full" />}
                      </div>
                      <div className="font-bold text-slate-800">{s.display_name}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="mb-8">
              <h3 className="text-sm font-bold text-slate-500 uppercase tracking-wider mb-3">日内高频策略 (Intraday High-Frequency)</h3>
              <div className="border-2 border-slate-200 rounded-xl p-4 bg-slate-50 opacity-60 cursor-not-allowed flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-5 h-5 rounded-full border border-slate-300 flex items-center justify-center bg-white flex-shrink-0"></div>
                  <div className="font-bold text-slate-800">Intraday T0 Reversal (日内T0反转)</div>
                </div>
                <span className="text-xs font-semibold bg-slate-200 text-slate-600 px-2 py-1 rounded">尚未就绪 (回测数据缺失)</span>
              </div>
            </div>

            <div className="bg-slate-50 rounded-xl p-5 border border-slate-200 mb-6">
              <h3 className="text-sm font-bold text-slate-800 mb-4">策略核心参数调整</h3>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
                <div>
                  <label className="block text-xs font-semibold text-slate-600 mb-1.5">Top K (持仓数量)</label>
                  <input type="number" value={topk} onChange={e => setTopk(Number(e.target.value))} className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-purple-400 outline-none" />
                </div>
                <div>
                  <label className="block text-xs font-semibold text-slate-600 mb-1.5">N Drop (每日替换)</label>
                  <input type="number" value={nDrop} onChange={e => setNDrop(Number(e.target.value))} className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-purple-400 outline-none" />
                </div>
              </div>
            </div>

            <div className="flex justify-between mt-6">
              <button onClick={() => setCurrentStep(2)} className="px-6 py-2.5 bg-white border border-slate-300 text-slate-700 font-medium rounded-lg hover:bg-slate-50 transition-colors">上一步</button>
              <button onClick={() => setCurrentStep(4)} className="px-8 py-2.5 bg-slate-900 text-white font-medium rounded-lg hover:bg-slate-800 transition-colors">下一步：组合配置与评估</button>
            </div>
          </div>
        )}

        {/* Step 4: 组合配置与AI评估 */}
        {currentStep === 4 && (
          <div className="p-6">
            <h2 className="text-xl font-bold text-slate-800 mb-6">4. 组合配置预览与 AI 评估</h2>
            
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
              {/* 左侧预览 */}
              <div className="bg-slate-50 rounded-xl p-5 border border-slate-200">
                <h3 className="text-sm font-bold text-slate-800 mb-4">当前配置清单</h3>
                <div className="space-y-4 text-sm">
                  <div className="flex justify-between items-baseline border-b border-slate-200 pb-2">
                    <span className="text-slate-500">已选因子数量</span>
                    <span className="font-bold text-purple-600 text-lg">{selectedFactors.size} 个</span>
                  </div>
                  <div className="flex justify-between items-baseline border-b border-slate-200 pb-2">
                    <span className="text-slate-500">选定模型</span>
                    <span className="font-semibold text-slate-800 text-right max-w-[200px] truncate" title={selectedModel}>{selectedModel || "未选择"}</span>
                  </div>
                  <div className="flex justify-between items-baseline border-b border-slate-200 pb-2">
                    <span className="text-slate-500">选定策略</span>
                    <span className="font-semibold text-slate-800">{selectedStrategy || "未选择"} (TopK={topk}, n_drop={nDrop})</span>
                  </div>
                </div>

                <div className="mt-6 pt-4 border-t border-slate-200">
                  <button onClick={evaluateCombination} disabled={actionLoading === "evaluate"}
                    className="w-full py-3 bg-indigo-600 text-white font-bold rounded-lg hover:bg-indigo-700 disabled:opacity-50 transition-colors flex items-center justify-center gap-2">
                    {actionLoading === "evaluate" ? "正在调用 LLM 进行深度评估..." : "AI 评估此组合的合理性"}
                  </button>
                </div>
              </div>

              {/* 右侧评估结果 */}
              <div className="bg-white rounded-xl p-5 border border-indigo-100 shadow-inner min-h-[250px]">
                {evalResult ? (
                  <div>
                    <div className="flex items-center gap-4 mb-4">
                      <div className="w-16 h-16 rounded-full flex items-center justify-center text-xl font-bold bg-indigo-50 text-indigo-700 border-4 border-indigo-200">
                        {evalResult.overall_score?.toFixed(0)}
                      </div>
                      <div>
                        <h3 className="text-lg font-bold text-slate-800">综合评估得分</h3>
                        <p className="text-xs text-slate-500">基于多维度因子互补性和策略匹配度</p>
                      </div>
                    </div>
                    {evalResult.llm_commentary && (
                      <div className="text-sm text-slate-700 leading-relaxed mb-4 whitespace-pre-wrap p-3 bg-slate-50 rounded-lg">
                        {evalResult.llm_commentary}
                      </div>
                    )}
                    {evalResult.risks && evalResult.risks.length > 0 && (
                      <div className="mb-3">
                        <h4 className="text-xs font-bold text-red-600 uppercase mb-2">识别到的风险</h4>
                        {evalResult.risks.map((r, i) => <div key={i} className="text-xs bg-red-50 text-red-700 p-2 rounded mb-1 border border-red-100">{r.message}</div>)}
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-400">
                    <div className="text-4xl mb-2">🤖</div>
                    <p className="text-sm">点击左侧按钮，使用 AI 分析您的投资组合</p>
                  </div>
                )}
              </div>
            </div>

            <div className="flex justify-between mt-6 pt-6 border-t border-slate-200">
              <button onClick={() => setCurrentStep(3)} className="px-6 py-2.5 bg-white border border-slate-300 text-slate-700 font-medium rounded-lg hover:bg-slate-50 transition-colors">上一步</button>
              <button onClick={() => setCurrentStep(5)} className="px-8 py-2.5 bg-purple-600 text-white font-medium rounded-lg hover:bg-purple-700 shadow-md transition-colors">确认配置并进入下一步</button>
            </div>
          </div>
        )}

        {/* Step 5: 任务下发设置 */}
        {currentStep === 5 && (
          <div className="p-6">
            <h2 className="text-xl font-bold text-slate-800 mb-6">5. 任务下发设置 (Task Dispatching)</h2>

            <div className="bg-slate-50 rounded-xl p-5 border border-slate-200 mb-6">
              <h3 className="text-sm font-bold text-slate-800 mb-4">时间区间与数据基线</h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-4">
                {[ { label: "训练集", sk: "train_start", ek: "train_end" }, { label: "验证集", sk: "valid_start", ek: "valid_end" }, { label: "测试集(回测)", sk: "test_start", ek: "test_end" } ].map(seg => (
                  <div key={seg.label}>
                    <label className="block text-xs font-semibold text-slate-600 mb-1.5">{seg.label}</label>
                    <div className="flex items-center gap-2">
                      <input type="date" value={(dataSplit as any)[seg.sk]} onChange={e => setDataSplit(p => ({ ...p, [seg.sk]: e.target.value }))} className="px-2 py-1.5 text-xs border border-slate-300 rounded bg-white w-full" />
                      <span className="text-slate-400">-</span>
                      <input type="date" value={(dataSplit as any)[seg.ek]} onChange={e => setDataSplit(p => ({ ...p, [seg.ek]: e.target.value }))} className="px-2 py-1.5 text-xs border border-slate-300 rounded bg-white w-full" />
                    </div>
                  </div>
                ))}
              </div>
              <div className="flex gap-6 mt-4 pt-4 border-t border-slate-200">
                <label className="flex items-center gap-2 text-sm text-slate-700 cursor-pointer">
                  <input type="checkbox" className="rounded text-purple-600 w-4 h-4" checked={disableAlphaBaseline} onChange={e => setDisableAlphaBaseline(e.target.checked)} />
                  禁用 Alpha158 基线因子
                </label>
                <label className="flex items-center gap-2 text-sm text-slate-700 cursor-pointer">
                  <input type="checkbox" className="rounded text-purple-600 w-4 h-4" checked={quickTrain} onChange={e => setQuickTrain(e.target.checked)} />
                  启用快速训练模式 <span className="text-xs text-amber-600 ml-1">(训练时间缩短至20%)</span>
                </label>
              </div>
            </div>

            <div className="mb-8">
              <h3 className="text-sm font-bold text-slate-800 mb-4">选择任务分流模式</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div onClick={() => setDispatchMode("independent")}
                  className={`p-5 rounded-xl border-2 cursor-pointer transition-all ${dispatchMode === "independent" ? "border-blue-600 bg-blue-50" : "border-slate-200 hover:border-blue-300"}`}>
                  <div className="flex items-center justify-between mb-2">
                    <h4 className="font-bold text-blue-800 text-lg">执行独立任务 (单次回测)</h4>
                    <div className="w-5 h-5 rounded-full border border-blue-300 flex items-center justify-center bg-white">
                      {dispatchMode === "independent" && <div className="w-3 h-3 bg-blue-600 rounded-full" />}
                    </div>
                  </div>
                  <p className="text-sm text-slate-600">单次生成 QLib 配置并发送到 WSL 环境执行，不会触发自动循环演进。适合验证当前组合的效果。</p>
                </div>
                
                <div onClick={() => setDispatchMode("evolution")}
                  className={`p-5 rounded-xl border-2 cursor-pointer transition-all ${dispatchMode === "evolution" ? "border-purple-600 bg-purple-50" : "border-slate-200 hover:border-purple-300"}`}>
                  <div className="flex items-center justify-between mb-2">
                    <h4 className="font-bold text-purple-800 text-lg">启动 QE 自动演进 (持续迭代)</h4>
                    <div className="w-5 h-5 rounded-full border border-purple-300 flex items-center justify-center bg-white">
                      {dispatchMode === "evolution" && <div className="w-3 h-3 bg-purple-600 rounded-full" />}
                    </div>
                  </div>
                  <p className="text-sm text-slate-600 mb-3">以此配置作为 Task 0 (初始环境)，交给 AIstock 调度大脑进行自动化、循环的因子与模型挖掘。</p>
                  
                  {dispatchMode === "evolution" && (
                    <div className="space-y-3 mt-4 pt-4 border-t border-purple-200" onClick={e => e.stopPropagation()}>
                      <div>
                        <label className="block text-xs font-semibold text-purple-800 mb-1">演进总体目标描述</label>
                        <input type="text" value={evolutionObjective} onChange={e => setEvolutionObjective(e.target.value)} placeholder="例如：挖掘低相关性的新动量因子，提升多头收益" className="w-full px-3 py-1.5 border border-purple-200 rounded text-sm focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500" />
                      </div>
                      <div>
                        <label className="block text-xs font-semibold text-purple-800 mb-1">预设循环迭代次数 (Loops)</label>
                        <input type="number" value={evolutionLoops} onChange={e => setEvolutionLoops(Number(e.target.value))} className="w-24 px-3 py-1.5 border border-purple-200 rounded text-sm focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500" />
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>

            {configResult?.ok && (
              <div className="bg-emerald-50 border border-emerald-200 rounded-xl p-5 mb-6">
                <h3 className="font-bold text-emerald-800 mb-2">任务已成功生成！</h3>
                <p className="text-sm text-emerald-700 mb-1"><strong>Experiment ID:</strong> {configResult.experiment_id}</p>
                <p className="text-sm text-emerald-700 mb-3"><strong>工作目录:</strong> {configResult.experiment_dir}</p>
                <div className="bg-slate-900 rounded-lg p-3 overflow-x-auto">
                  <pre className="text-xs text-green-400 font-mono m-0">{configResult.wsl_command}</pre>
                </div>
                <div className="mt-3 flex gap-3">
                  <button onClick={() => { navigator.clipboard.writeText(configResult.wsl_command || ""); alert("已复制命令"); }} className="px-4 py-1.5 bg-white text-emerald-700 font-medium text-sm border border-emerald-300 rounded hover:bg-emerald-100">复制终端命令</button>
                  {dispatchMode === "evolution" && <button onClick={() => window.open("/quantevolver/evolution", "_blank")} className="px-4 py-1.5 bg-emerald-600 text-white font-medium text-sm rounded hover:bg-emerald-700">前往演进监控大屏</button>}
                </div>
              </div>
            )}

            <div className="flex justify-between mt-6 pt-6 border-t border-slate-200">
              <button onClick={() => setCurrentStep(4)} className="px-6 py-2.5 bg-white border border-slate-300 text-slate-700 font-medium rounded-lg hover:bg-slate-50 transition-colors">上一步</button>
              <button onClick={generateConfig} disabled={actionLoading === "generate" || selectedFactors.size === 0}
                className="px-10 py-3 bg-slate-900 text-white font-bold text-lg rounded-xl hover:bg-black shadow-lg hover:shadow-xl transition-all flex items-center gap-2">
                {actionLoading === "generate" ? "正在执行生成中..." : dispatchMode === "independent" ? "执行独立任务 (Generate)" : "启动 QE 自动演进 (Start Evolution)"}
              </button>
            </div>
          </div>
        )}
      </section>
    </main>
  );
}
