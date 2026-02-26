"use client";

import React, { useEffect, useState, useCallback, useMemo } from "react";
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
type Strategy = { strategy_id: string; display_name: string; strategy_type?: string; portfolio_config?: any; description?: string; market?: string; catalog_source?: string; };

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
  error?: string;
};

/* ━━ 类型标签颜色 ━━ */
const TYPE_COLORS: Record<string, { bg: string; text: string; label: string }> = {
  daily: { bg: "#dbeafe", text: "#2563eb", label: "日频" },
  intraday: { bg: "#fef3c7", text: "#d97706", label: "日内" },
};

export default function ComposePage() {
  const [currentStep, setCurrentStep] = useState(1);
  const STEPS = ["因子选择", "模型选择", "策略选择", "组合配置与评估", "生成执行与下发"];

  /* ━━ 公共样式常量（与 evolution 页面统一） ━━ */
  const cardStyle: React.CSSProperties = {
    backgroundColor: "#ffffff",
    borderRadius: "12px",
    boxShadow: "0 4px 12px rgba(0, 0, 0, 0.05)",
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
    border: "1px solid rgba(255, 255, 255, 0.2)",
  };

  const headerStyle: React.CSSProperties = {
    padding: "16px 20px",
    borderBottom: "1px solid #f1f5f9",
    backgroundColor: "#f8fafc",
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
  };

  const btnPrimary: React.CSSProperties = {
    padding: "8px 20px",
    backgroundColor: "#2563eb",
    color: "#fff",
    border: "none",
    borderRadius: "6px",
    fontSize: "13px",
    fontWeight: 600,
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    gap: "6px",
    boxShadow: "0 2px 4px rgba(37, 99, 235, 0.2)",
    transition: "background-color 0.2s",
  };

  const btnSecondary: React.CSSProperties = {
    padding: "8px 20px",
    backgroundColor: "#f1f5f9",
    color: "#475569",
    border: "1px solid #e2e8f0",
    borderRadius: "6px",
    fontSize: "13px",
    fontWeight: 600,
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    gap: "6px",
    transition: "background-color 0.2s",
  };

  const inputStyle: React.CSSProperties = {
    width: "100%",
    padding: "8px 12px",
    borderRadius: "6px",
    border: "1px solid #cbd5e1",
    fontSize: "14px",
    boxSizing: "border-box",
    outline: "none",
  };

  const labelStyle: React.CSSProperties = {
    display: "block",
    fontSize: "13px",
    fontWeight: 600,
    color: "#475569",
    marginBottom: "6px",
  };

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
        const keys = data.combination.factor_names.map((name: string) => {
          const f = factors.find((f: any) => f.factor_name === name);
          return f ? `${f.factor_name}||${f.source}` : name;
        });
        setSelectedFactors(new Set(keys));
        setSelectedModel(data.combination.model_id || "");
        setSelectedStrategy(data.combination.strategy_id || "TopkDropoutStrategy");
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
      const data = await res.json();
      if (!res.ok) { alert("评估失败: " + (data?.detail || res.statusText)); setActionLoading(null); return; }
      setEvalResult(data);
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
    <div style={{
      padding: "24px",
      boxSizing: "border-box",
      fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif",
      minHeight: "calc(100vh - 48px)",
      maxWidth: "1400px",
      margin: "0 auto",
    }}>
      {/* 顶部标题区 */}
      <div style={{ marginBottom: "24px" }}>
        <h1 style={{ margin: 0, fontSize: "24px", fontWeight: 700, color: "#ffffff", textShadow: "0 1px 3px rgba(0,0,0,0.15)" }}>QE实验设计</h1>
        <p style={{ margin: "8px 0 0", fontSize: "14px", color: "rgba(255,255,255,0.75)" }}>双轨驱动：支持AI智能生成配置与人工分步流程式选择，为您构建优质的因子组合与模型演进任务。</p>
      </div>
      
      {/* AI 智能实验设计区 */}
      <div style={{ ...cardStyle, marginBottom: "24px" }}>
        <div style={headerStyle}>
          <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#7c3aed", display: "flex", alignItems: "center", gap: "8px" }}>
            ✨ AI 智能实验设计
          </h2>
        </div>
        <div style={{ padding: "20px" }}>
          <textarea
            value={userRequirement}
            onChange={e => setUserRequirement(e.target.value)}
            placeholder="请输入您的实验组合设计目标，例如：构建一个偏向于动量反转的日频量化组合，配合高频微观结构因子..."
            rows={3}
            style={{ ...inputStyle, resize: "none", padding: "12px 16px", fontSize: "14px", lineHeight: 1.6, borderRadius: "8px" }}
          />
          <div style={{ display: "flex", gap: "12px", marginTop: "16px", alignItems: "center", flexWrap: "wrap" }}>
            <button
              onClick={aiGenerate}
              disabled={aiLoading}
              style={{ ...btnPrimary, backgroundColor: "#7c3aed", padding: "10px 24px", fontSize: "14px", boxShadow: "0 2px 4px rgba(124, 58, 237, 0.25)", opacity: aiLoading ? 0.5 : 1 }}
            >
              {aiLoading ? "正在深度解析并生成..." : "智能生成配置"}
            </button>
            <div style={{ display: "flex", gap: "8px" }}>
              {["稳健低回撤", "资金流选股", "高夏普动量"].map(tag => (
                <button key={tag} onClick={() => setUserRequirement(tag + "组合，要求全面考虑风险控制")}
                  style={{ padding: "4px 12px", fontSize: "12px", color: "#64748b", border: "1px solid #e2e8f0", borderRadius: "16px", backgroundColor: "#fff", cursor: "pointer", transition: "all 0.2s" }}>
                  {tag}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* 流程导航 - 横向按钮样式 */}
      <div style={{ ...cardStyle, marginBottom: "24px" }}>
        <div style={{ display: "flex", width: "100%" }}>
          {STEPS.map((step, idx) => {
            const stepNum = idx + 1;
            const isActive = currentStep === stepNum;
            const isPassed = currentStep > stepNum;

            return (
              <button
                key={stepNum}
                onClick={() => setCurrentStep(stepNum)}
                style={{
                  flex: 1,
                  padding: "16px 8px",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  gap: "10px",
                  border: "none",
                  borderRight: idx < STEPS.length - 1 ? "1px solid #f1f5f9" : "none",
                  backgroundColor: isActive ? "#eff6ff" : isPassed ? "#f8fafc" : "#ffffff",
                  cursor: "pointer",
                  transition: "all 0.2s ease",
                  borderBottom: isActive ? "3px solid #2563eb" : "3px solid transparent",
                }}
              >
                <div style={{
                  width: "32px",
                  height: "32px",
                  borderRadius: "50%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontWeight: 700,
                  fontSize: "13px",
                  flexShrink: 0,
                  backgroundColor: isActive ? "#2563eb" : isPassed ? "#dbeafe" : "#f1f5f9",
                  color: isActive ? "#ffffff" : isPassed ? "#2563eb" : "#94a3b8",
                  boxShadow: isActive ? "0 2px 4px rgba(37, 99, 235, 0.3)" : "none",
                  transition: "all 0.2s ease",
                }}>
                  {isPassed ? "✓" : stepNum}
                </div>
                <span style={{
                  fontWeight: 600,
                  fontSize: "14px",
                  whiteSpace: "nowrap",
                  color: isActive ? "#1e293b" : isPassed ? "#475569" : "#94a3b8",
                }}>
                  {step}
                </span>
              </button>
            );
          })}
        </div>
      </div>

      {/* 下部：分步卡片区 */}
      <div style={{ ...cardStyle, minHeight: "500px" }}>

        {/* Step 1: 因子选择 */}
        {currentStep === 1 && (
          <div style={{ padding: "24px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
              <h2 style={{ margin: 0, fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>1. 因子选择 (Factor Selection)</h2>
              <span style={{ backgroundColor: "#dbeafe", color: "#2563eb", padding: "4px 12px", borderRadius: "12px", fontSize: "13px", fontWeight: 600 }}>已选 {selectedFactors.size} 项</span>
            </div>

            <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden" }}>
              <FactorList
                mode="selection"
                selectedFactors={selectedFactors}
                onFactorSelect={(selected) => setSelectedFactors(selected)}
              />
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", marginTop: "24px" }}>
              <button onClick={() => setCurrentStep(2)} style={{ ...btnPrimary, padding: "10px 28px", fontSize: "14px" }}>下一步：选择模型</button>
            </div>
          </div>
        )}

        {/* Step 2: 模型选择 */}
        {currentStep === 2 && (
          <div style={{ padding: "24px" }}>
            <h2 style={{ margin: "0 0 16px", fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>2. 模型选择 (Model Selection)</h2>

            <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden", marginBottom: "24px" }}>
              <ModelList
                mode="selection"
                selectedModel={selectedModel}
                onSelectModel={(modelId) => setSelectedModel(modelId)}
              />
            </div>

            <div style={{ display: "flex", justifyContent: "space-between", marginTop: "24px" }}>
              <button onClick={() => setCurrentStep(1)} style={btnSecondary}>上一步</button>
              <button onClick={() => setCurrentStep(3)} style={{ ...btnPrimary, padding: "10px 28px", fontSize: "14px" }}>下一步：选择策略</button>
            </div>
          </div>
        )}

        {/* Step 3: 策略选择 */}
        {currentStep === 3 && (
          <div style={{ padding: "24px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
              <h2 style={{ margin: 0, fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>选择交易策略与核心参数</h2>
              <span style={{ fontSize: "13px", color: "#64748b" }}>
                已选择策略：<strong style={{ color: "#2563eb" }}>{selectedStrategy || "未选择"}</strong>
              </span>
            </div>

            {/* 策略列表 */}
            <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden", marginBottom: "24px" }}>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                  <thead>
                    <tr style={{ background: "#f9fafb", textAlign: "left" }}>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 60, textAlign: "center" }}>选择</th>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12 }}>策略名称</th>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12 }}>描述</th>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 70 }}>类型</th>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 70 }}>市场</th>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 80 }}>来源</th>
                    </tr>
                  </thead>
                  <tbody>
                    {strategies.length > 0 ? strategies.map(s => {
                      const tc = TYPE_COLORS[s.strategy_type || "daily"] || TYPE_COLORS.daily;
                      const isSelected = selectedStrategy === s.strategy_id;
                      return (
                        <tr key={s.strategy_id}
                          onClick={() => setSelectedStrategy(s.strategy_id)}
                          style={{ 
                            borderTop: "1px solid #f3f4f6", 
                            cursor: "pointer",
                            background: isSelected ? "#f5f3ff" : "transparent"
                          }}
                          onMouseEnter={e => { if(!isSelected) e.currentTarget.style.background = "#f9fafb" }}
                          onMouseLeave={e => { if(!isSelected) e.currentTarget.style.background = "transparent" }}
                        >
                          <td style={{ padding: "10px 16px", textAlign: "center" }}>
                            <input 
                              type="radio" 
                              name="strategy-selection"
                              checked={isSelected}
                              onChange={() => setSelectedStrategy(s.strategy_id)}
                              style={{ cursor: "pointer", width: 16, height: 16, accentColor: "#7c3aed" }}
                            />
                          </td>
                          <td style={{ padding: "10px 16px" }}>
                            <div style={{ fontWeight: 600, color: "#111827" }}>{s.display_name}</div>
                            <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2 }}>{s.strategy_id}</div>
                          </td>
                          <td style={{ padding: "10px 16px", color: "#4b5563", fontSize: 12, maxWidth: 320, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={s.description}>
                            {s.description || "-"}
                          </td>
                          <td style={{ padding: "10px 16px" }}>
                            <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600, background: tc.bg, color: tc.text }}>
                              {tc.label}
                            </span>
                          </td>
                          <td style={{ padding: "10px 16px", fontSize: 12, color: "#6b7280" }}>{s.market || "-"}</td>
                          <td style={{ padding: "10px 16px", fontSize: 12, color: "#6b7280" }}>{s.catalog_source || "-"}</td>
                        </tr>
                      );
                    }) : (
                      <tr>
                        <td colSpan={6} style={{ padding: "32px", textAlign: "center", color: "#9ca3af", fontSize: 14 }}>
                          尚未加载到策略数据
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div style={{ backgroundColor: "#f8fafc", borderRadius: "8px", padding: "20px", border: "1px solid #e2e8f0", marginBottom: "24px" }}>
              <h3 style={{ margin: "0 0 16px", fontSize: "13px", fontWeight: 700, color: "#1e293b" }}>策略核心参数调整</h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "20px", maxWidth: "400px" }}>
                <div>
                  <label style={labelStyle}>Top K (持仓数量)</label>
                  <input type="number" value={topk} onChange={e => setTopk(Number(e.target.value))} style={inputStyle} />
                </div>
                <div>
                  <label style={labelStyle}>N Drop (每日替换)</label>
                  <input type="number" value={nDrop} onChange={e => setNDrop(Number(e.target.value))} style={inputStyle} />
                </div>
              </div>
            </div>

            <div style={{ display: "flex", justifyContent: "space-between", marginTop: "24px" }}>
              <button onClick={() => setCurrentStep(2)} style={btnSecondary}>上一步</button>
              <button onClick={() => setCurrentStep(4)} style={{ ...btnPrimary, padding: "10px 28px", fontSize: "14px" }}>下一步：组合配置与评估</button>
            </div>
          </div>
        )}

        {/* Step 4: 组合配置与AI评估 */}
        {currentStep === 4 && (
          <div style={{ padding: "24px" }}>
            <h2 style={{ margin: "0 0 20px", fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>4. 组合配置预览与 AI 评估</h2>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "20px", marginBottom: "24px" }}>
              {/* 左侧预览 */}
              <div style={{ backgroundColor: "#f8fafc", borderRadius: "8px", padding: "20px", border: "1px solid #e2e8f0" }}>
                <h3 style={{ margin: "0 0 16px", fontSize: "13px", fontWeight: 700, color: "#1e293b", textTransform: "uppercase", letterSpacing: "0.05em" }}>当前配置清单</h3>
                <div style={{ fontSize: "14px", display: "flex", flexDirection: "column", gap: "16px" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                    <span style={{ color: "#64748b", fontWeight: 500 }}>已选因子数量</span>
                    <span style={{ fontSize: "20px", fontWeight: 700, fontFamily: "monospace", color: "#059669" }}>{selectedFactors.size} 个</span>
                  </div>
                  {selectedFactors.size > 0 && (
                    <div style={{ display: "flex", flexWrap: "wrap", gap: "6px", paddingBottom: "12px", borderBottom: "1px solid #f1f5f9", maxHeight: "120px", overflowY: "auto" }}>
                      {Array.from(selectedFactors).map(k => {
                        const name = k.split("||")[0];
                        return (
                          <span key={k} style={{ padding: "2px 8px", backgroundColor: "#dbeafe", color: "#2563eb", fontSize: "11px", borderRadius: "4px", fontFamily: "monospace", border: "1px solid #bfdbfe" }}>
                            {name}
                          </span>
                        );
                      })}
                    </div>
                  )}
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                    <span style={{ color: "#64748b", fontWeight: 500 }}>选定模型</span>
                    <span style={{ fontWeight: 600, color: "#1e293b", textAlign: "right", maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={selectedModel}>
                      {(() => {
                        if (!selectedModel) return "未选择";
                        const m = models.find(m => m.model_id === selectedModel);
                        return m ? `${m.display_name || m.model_name} (${m.model_type || "未知类型"})` : selectedModel;
                      })()}
                    </span>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                    <span style={{ color: "#64748b", fontWeight: 500 }}>选定策略</span>
                    <span style={{ fontWeight: 600, color: "#1e293b" }}>
                      {(() => {
                        if (!selectedStrategy) return "未选择";
                        const s = strategies.find(s => s.strategy_id === selectedStrategy);
                        return `${s?.display_name || selectedStrategy} (TopK=${topk}, n_drop=${nDrop})`;
                      })()}
                    </span>
                  </div>
                </div>

                <div style={{ marginTop: "20px", paddingTop: "16px", borderTop: "1px solid #e2e8f0" }}>
                  <button onClick={evaluateCombination} disabled={actionLoading === "evaluate"}
                    style={{ ...btnPrimary, width: "100%", justifyContent: "center", padding: "10px 20px", fontSize: "14px", backgroundColor: "#3b82f6", opacity: actionLoading === "evaluate" ? 0.5 : 1 }}>
                    {actionLoading === "evaluate" ? "正在调用 LLM 进行深度评估..." : "AI 评估此组合的合理性"}
                  </button>
                </div>
              </div>

              {/* 右侧评估结果 */}
              <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", padding: "20px", border: "1px solid #e2e8f0", boxShadow: "0 1px 3px rgba(0,0,0,0.05)", minHeight: "250px", display: "flex", flexDirection: "column" }}>
                {evalResult ? (
                  <div>
                    <div style={{ display: "flex", alignItems: "center", gap: "16px", marginBottom: "16px" }}>
                      <div style={{ width: "56px", height: "56px", borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center", fontSize: "20px", fontWeight: 700, backgroundColor: "#dbeafe", color: "#1d4ed8", border: "3px solid #93c5fd" }}>
                        {evalResult.overall_score?.toFixed(0)}
                      </div>
                      <div>
                        <h3 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b" }}>综合评估得分</h3>
                        <p style={{ margin: "4px 0 0", fontSize: "12px", color: "#64748b" }}>基于多维度因子互补性和策略匹配度</p>
                      </div>
                    </div>
                    {evalResult.llm_commentary && (
                      <div style={{ fontSize: "14px", color: "#334155", lineHeight: 1.6, marginBottom: "16px", whiteSpace: "pre-wrap", padding: "12px", backgroundColor: "#f8fafc", borderRadius: "6px", border: "1px solid #f1f5f9" }}>
                        {evalResult.llm_commentary}
                      </div>
                    )}
                    {evalResult.risks && evalResult.risks.length > 0 && (
                      <div style={{ marginBottom: "12px" }}>
                        <h4 style={{ margin: "0 0 8px", fontSize: "12px", fontWeight: 700, color: "#dc2626", textTransform: "uppercase", letterSpacing: "0.05em" }}>识别到的风险</h4>
                        {evalResult.risks.map((r, i) => (
                          <div key={i} style={{ fontSize: "12px", backgroundColor: "#fef2f2", color: "#b91c1c", padding: "8px 12px", borderRadius: "6px", marginBottom: "4px", border: "1px solid #fecaca" }}>{r.message}</div>
                        ))}
                      </div>
                    )}
                  </div>
                ) : (
                  <div style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", color: "#94a3b8" }}>
                    <div style={{ fontSize: "36px", marginBottom: "8px" }}>🤖</div>
                    <p style={{ margin: 0, fontSize: "14px" }}>点击左侧按钮，使用 AI 分析您的投资组合</p>
                  </div>
                )}
              </div>
            </div>

            <div style={{ display: "flex", justifyContent: "space-between", marginTop: "24px", paddingTop: "20px", borderTop: "1px solid #f1f5f9" }}>
              <button onClick={() => setCurrentStep(3)} style={btnSecondary}>上一步</button>
              <button onClick={() => setCurrentStep(5)} style={{ ...btnPrimary, padding: "10px 28px", fontSize: "14px" }}>确认配置并进入下一步</button>
            </div>
          </div>
        )}

        {/* Step 5: 任务下发设置 */}
        {currentStep === 5 && (
          <div style={{ padding: "24px" }}>
            <h2 style={{ margin: "0 0 20px", fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>5. 任务下发设置 (Task Dispatching)</h2>

            <div style={{ backgroundColor: "#f8fafc", borderRadius: "8px", padding: "20px", border: "1px solid #e2e8f0", marginBottom: "24px" }}>
              <h3 style={{ margin: "0 0 16px", fontSize: "13px", fontWeight: 700, color: "#1e293b", textTransform: "uppercase", letterSpacing: "0.05em" }}>时间区间与数据基线</h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "20px", marginBottom: "16px" }}>
                {[ { label: "训练集", sk: "train_start", ek: "train_end" }, { label: "验证集", sk: "valid_start", ek: "valid_end" }, { label: "测试集(回测)", sk: "test_start", ek: "test_end" } ].map(seg => (
                  <div key={seg.label}>
                    <label style={labelStyle}>{seg.label}</label>
                    <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                      <input type="date" value={(dataSplit as any)[seg.sk]} onChange={e => setDataSplit(p => ({ ...p, [seg.sk]: e.target.value }))} style={{ ...inputStyle, padding: "6px 8px", fontSize: "12px" }} />
                      <span style={{ color: "#94a3b8" }}>-</span>
                      <input type="date" value={(dataSplit as any)[seg.ek]} onChange={e => setDataSplit(p => ({ ...p, [seg.ek]: e.target.value }))} style={{ ...inputStyle, padding: "6px 8px", fontSize: "12px" }} />
                    </div>
                  </div>
                ))}
              </div>
              <div style={{ display: "flex", gap: "24px", marginTop: "16px", paddingTop: "16px", borderTop: "1px solid #e2e8f0" }}>
                <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "14px", color: "#475569", cursor: "pointer" }}>
                  <input type="checkbox" checked={disableAlphaBaseline} onChange={e => setDisableAlphaBaseline(e.target.checked)} style={{ width: "16px", height: "16px", accentColor: "#2563eb" }} />
                  禁用 Alpha158 基线因子
                </label>
                <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "14px", color: "#475569", cursor: "pointer" }}>
                  <input type="checkbox" checked={quickTrain} onChange={e => setQuickTrain(e.target.checked)} style={{ width: "16px", height: "16px", accentColor: "#2563eb" }} />
                  启用快速训练模式 <span style={{ fontSize: "12px", color: "#d97706", marginLeft: "4px" }}>(训练时间缩短至20%)</span>
                </label>
              </div>
            </div>

            <div style={{ marginBottom: "32px" }}>
              <h3 style={{ margin: "0 0 16px", fontSize: "13px", fontWeight: 700, color: "#1e293b", textTransform: "uppercase", letterSpacing: "0.05em" }}>选择任务分流模式</h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px" }}>
                <div onClick={() => setDispatchMode("independent")}
                  style={{
                    padding: "20px", borderRadius: "8px", cursor: "pointer", transition: "all 0.2s",
                    border: dispatchMode === "independent" ? "2px solid #3b82f6" : "2px solid #e2e8f0",
                    backgroundColor: dispatchMode === "independent" ? "#eff6ff" : "#ffffff",
                  }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
                    <h4 style={{ margin: 0, fontWeight: 700, color: "#1d4ed8", fontSize: "16px" }}>执行独立任务 (单次回测)</h4>
                    <div style={{ width: "20px", height: "20px", borderRadius: "50%", border: "2px solid #93c5fd", display: "flex", alignItems: "center", justifyContent: "center", backgroundColor: "#fff" }}>
                      {dispatchMode === "independent" && <div style={{ width: "10px", height: "10px", backgroundColor: "#3b82f6", borderRadius: "50%" }} />}
                    </div>
                  </div>
                  <p style={{ margin: 0, fontSize: "13px", color: "#475569", lineHeight: 1.5 }}>单次生成 QLib 配置并发送到 WSL 环境执行，不会触发自动循环演进。适合验证当前组合的效果。</p>
                </div>

                <div onClick={() => setDispatchMode("evolution")}
                  style={{
                    padding: "20px", borderRadius: "8px", cursor: "pointer", transition: "all 0.2s",
                    border: dispatchMode === "evolution" ? "2px solid #7c3aed" : "2px solid #e2e8f0",
                    backgroundColor: dispatchMode === "evolution" ? "#f5f3ff" : "#ffffff",
                  }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
                    <h4 style={{ margin: 0, fontWeight: 700, color: "#6d28d9", fontSize: "16px" }}>启动 QE 自动演进 (持续迭代)</h4>
                    <div style={{ width: "20px", height: "20px", borderRadius: "50%", border: "2px solid #c4b5fd", display: "flex", alignItems: "center", justifyContent: "center", backgroundColor: "#fff" }}>
                      {dispatchMode === "evolution" && <div style={{ width: "10px", height: "10px", backgroundColor: "#7c3aed", borderRadius: "50%" }} />}
                    </div>
                  </div>
                  <p style={{ margin: "0 0 12px", fontSize: "13px", color: "#475569", lineHeight: 1.5 }}>以此配置作为 Task 0 (初始环境)，交给 AIstock 调度大脑进行自动化、循环的因子与模型挖掘。</p>

                  {dispatchMode === "evolution" && (
                    <div style={{ display: "flex", flexDirection: "column", gap: "12px", marginTop: "16px", paddingTop: "16px", borderTop: "1px solid #ddd6fe" }} onClick={e => e.stopPropagation()}>
                      <div>
                        <label style={{ ...labelStyle, color: "#6d28d9" }}>演进总体目标描述</label>
                        <input type="text" value={evolutionObjective} onChange={e => setEvolutionObjective(e.target.value)} placeholder="例如：挖掘低相关性的新动量因子，提升多头收益" style={{ ...inputStyle, borderColor: "#c4b5fd", fontSize: "13px" }} />
                      </div>
                      <div>
                        <label style={{ ...labelStyle, color: "#6d28d9" }}>预设循环迭代次数 (Loops)</label>
                        <input type="number" value={evolutionLoops} onChange={e => setEvolutionLoops(Number(e.target.value))} style={{ ...inputStyle, width: "96px", borderColor: "#c4b5fd", fontSize: "13px" }} />
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>

            {configResult?.ok && (
              <div style={{ backgroundColor: "#dcfce7", border: "1px solid #86efac", borderRadius: "8px", padding: "20px", marginBottom: "24px" }}>
                <h3 style={{ margin: "0 0 8px", fontWeight: 700, color: "#166534", fontSize: "15px" }}>任务已成功生成！</h3>
                <p style={{ margin: "0 0 4px", fontSize: "13px", color: "#15803d" }}><strong>Experiment ID:</strong> {configResult.experiment_id}</p>
                <p style={{ margin: "0 0 12px", fontSize: "13px", color: "#15803d" }}><strong>工作目录:</strong> {configResult.experiment_dir}</p>
                <div style={{ backgroundColor: "#0f172a", borderRadius: "6px", padding: "12px", overflowX: "auto" }}>
                  <pre style={{ margin: 0, fontSize: "12px", color: "#4ade80", fontFamily: "'Fira Code', Consolas, monospace" }}>{configResult.wsl_command}</pre>
                </div>
                <div style={{ marginTop: "12px", display: "flex", gap: "12px" }}>
                  <button onClick={() => { navigator.clipboard.writeText(configResult.wsl_command || ""); alert("已复制命令"); }}
                    style={{ ...btnSecondary, fontSize: "13px", borderColor: "#86efac", color: "#166534" }}>复制终端命令</button>
                  {dispatchMode === "evolution" && (
                    <button onClick={() => window.open("/quantevolver/evolution", "_blank")}
                      style={{ ...btnPrimary, backgroundColor: "#10b981", fontSize: "13px", boxShadow: "0 2px 4px rgba(16, 185, 129, 0.2)" }}>前往演进监控大屏</button>
                  )}
                </div>
              </div>
            )}

            <div style={{ display: "flex", justifyContent: "space-between", marginTop: "24px", paddingTop: "20px", borderTop: "1px solid #f1f5f9" }}>
              <button onClick={() => setCurrentStep(4)} style={btnSecondary}>上一步</button>
              <button onClick={generateConfig} disabled={actionLoading === "generate" || selectedFactors.size === 0}
                style={{
                  ...btnPrimary,
                  padding: "12px 32px",
                  fontSize: "15px",
                  fontWeight: 700,
                  backgroundColor: "#1e293b",
                  boxShadow: "0 4px 12px rgba(0, 0, 0, 0.15)",
                  opacity: (actionLoading === "generate" || selectedFactors.size === 0) ? 0.5 : 1,
                }}>
                {actionLoading === "generate" ? "正在执行生成中..." : dispatchMode === "independent" ? "执行独立任务 (Generate)" : "启动 QE 自动演进 (Start Evolution)"}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
