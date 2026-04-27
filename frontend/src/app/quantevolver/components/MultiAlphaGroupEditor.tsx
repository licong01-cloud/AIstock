"use client";

import React, { useState, useCallback, useMemo } from "react";

const MULTI_ALPHA_DISTRIBUTED_ENABLED =
  process.env.NEXT_PUBLIC_MULTI_ALPHA_DISTRIBUTED_ENABLED === "1";

// ── Types ──────────────────────────────────────────────────────────

export interface AlphaGroupConfig {
  group_name: string;
  factor_names: string[];
  model_id: string;
  dataset_type: string;
  compute_resource: string;
  preferred_node_id?: string;
  holding_period_hint?: string;
  model_source_experiment_id?: string;
  model_source_group_name?: string;
  reuse_mode?: string;
}

export interface MetaModelConfig {
  method: string;
  cv_strategy: string;
  lookback_days: number;
}

export interface MultiAlphaConfig {
  alpha_groups: AlphaGroupConfig[];
  meta_model: MetaModelConfig;
  execution_mode: string;
  auto_selected: boolean;
}

interface MultiAlphaGroupEditorProps {
  config: MultiAlphaConfig | null;
  onConfigChange: (config: MultiAlphaConfig) => void;
  apiBase: string;
}

interface ClassifiedFactor {
  factor_name: string;
  grade: string;
  ic_value: number | null;
  category: string;
  data_source_group: string;
  holding_period_class: string | null;
  icir_annualized: number | null;
}

interface CompatibleModel {
  model_id: string;
  model_type: string;
  dataset_type: string;
  compute_resource: string;
  description: string;
}

interface CorrelationPair {
  factor_a: string;
  factor_b: string;
  correlation: number;
}

type ReusableGroup = {
  experiment_id: string;
  group_name: string;
  model_id: string;
  group_ic: number | null;
  completed_at: string | null;
};

// ── Component ──────────────────────────────────────────────────────

export default function MultiAlphaGroupEditor({
  config,
  onConfigChange,
  apiBase,
}: MultiAlphaGroupEditorProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reusableSources, setReusableSources] = useState<Record<string, ReusableGroup[]>>({});

  // A2: auto-select 参数
  const [autoParams, setAutoParams] = useState({
    min_grade: "B",
    min_ic: "0.02",
    max_factors_per_group: "15",
    max_intra_corr: "0.7",
    execution_mode: "serial",
  });

  // A1: 展开状态
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());

  // A1: 候选因子列表缓存
  const [candidateFactors, setCandidateFactors] = useState<Record<string, ClassifiedFactor[]>>({});
  const [loadingCandidates, setLoadingCandidates] = useState<string | null>(null);
  const [showAddFactor, setShowAddFactor] = useState<string | null>(null);

  // A3: 相关性检查
  const [correlationResults, setCorrelationResults] = useState<Record<string, CorrelationPair[]>>({});
  const [checkingCorrelation, setCheckingCorrelation] = useState<string | null>(null);

  // A4: 新增组
  const [showNewGroup, setShowNewGroup] = useState(false);
  const [newGroupDsg, setNewGroupDsg] = useState("price_volume");

  // A4: 兼容模型缓存
  const [compatibleModels, setCompatibleModels] = useState<CompatibleModel[] | null>(null);

  // ── Auto-select (A2: 带参数) ────────────────────────────────────

  const handleAutoSelect = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      if (autoParams.execution_mode === "distributed" && !MULTI_ALPHA_DISTRIBUTED_ENABLED) {
        throw new Error("分布式 Multi-Alpha 尚未启用；请使用 WSL 单节点串行模式完成阶段 1 验证。");
      }
      const params = new URLSearchParams({
        min_grade: autoParams.min_grade,
        min_ic: autoParams.min_ic,
        max_factors_per_group: autoParams.max_factors_per_group,
        max_intra_corr: autoParams.max_intra_corr,
        execution_mode: autoParams.execution_mode,
      });
      const resp = await fetch(
        `${apiBase}/quantevolver/multi-alpha/auto-select?${params}`,
        { method: "POST" }
      );
      const data = await resp.json();
      if (data.ok) {
        onConfigChange(data.multi_alpha_config);
        setExpandedGroups(new Set());
      } else {
        setError(data.detail || "自动分组失败");
      }
    } catch (e: any) {
      setError(e.message || "请求失败");
    } finally {
      setLoading(false);
    }
  }, [apiBase, autoParams, onConfigChange]);

  // ── Group expand/collapse (A1) ──────────────────────────────────

  const toggleGroup = useCallback((groupName: string) => {
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(groupName)) {
        next.delete(groupName);
      } else {
        next.add(groupName);
      }
      return next;
    });
  }, []);

  // ── Remove group ────────────────────────────────────────────────

  const removeGroup = useCallback(
    (groupName: string) => {
      if (!config) return;
      const newGroups = config.alpha_groups.filter((g) => g.group_name !== groupName);
      if (newGroups.length >= 2) {
        onConfigChange({ ...config, alpha_groups: newGroups });
      }
    },
    [config, onConfigChange]
  );

  // ── Remove factor from group (A1) ──────────────────────────────

  const removeFactor = useCallback(
    (groupName: string, factorName: string) => {
      if (!config) return;
      const newGroups = config.alpha_groups.map((g) =>
        g.group_name === groupName
          ? { ...g, factor_names: g.factor_names.filter((f) => f !== factorName) }
          : g
      );
      onConfigChange({ ...config, alpha_groups: newGroups });
    },
    [config, onConfigChange]
  );

  // ── Add factor to group (A1) ───────────────────────────────────

  const addFactor = useCallback(
    (groupName: string, factorName: string) => {
      if (!config) return;
      const newGroups = config.alpha_groups.map((g) =>
        g.group_name === groupName && !g.factor_names.includes(factorName)
          ? { ...g, factor_names: [...g.factor_names, factorName] }
          : g
      );
      onConfigChange({ ...config, alpha_groups: newGroups });
    },
    [config, onConfigChange]
  );

  // ── Fetch candidate factors (A1) ───────────────────────────────

  const fetchCandidateFactors = useCallback(
    async (groupName: string, dsg: string, existingFactors: string[]) => {
      setLoadingCandidates(groupName);
      try {
        const params = new URLSearchParams({
          data_source_group: dsg,
          min_grade: "C",
          exclude_names: existingFactors.join(","),
          limit: "50",
        });
        const resp = await fetch(
          `${apiBase}/quantevolver/multi-alpha/classified-factors?${params}`
        );
        const data = await resp.json();
        if (data.ok) {
          setCandidateFactors((prev) => ({ ...prev, [groupName]: data.factors }));
        }
      } catch (e) {
        console.error("加载候选因子失败:", e);
      } finally {
        setLoadingCandidates(null);
      }
    },
    [apiBase]
  );

  // ── Check intra-group correlation (A3) ─────────────────────────

  const checkGroupCorrelation = useCallback(
    async (groupName: string, factorNames: string[]) => {
      if (factorNames.length < 2) return;
      setCheckingCorrelation(groupName);
      try {
        const resp = await fetch(
          `${apiBase}/quantevolver/evolution/correlations/subset`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ factor_names: factorNames }),
          }
        );
        const data = await resp.json();
        if (data.ok) {
          const pairs: CorrelationPair[] = (data.correlations || []).filter(
            (c: any) => Math.abs(c.correlation) > 0.5
          );
          setCorrelationResults((prev) => ({ ...prev, [groupName]: pairs }));
        }
      } catch (e) {
        console.error("相关性检查失败:", e);
      } finally {
        setCheckingCorrelation(null);
      }
    },
    [apiBase]
  );

  // ── Change group model (A4) ────────────────────────────────────

  const fetchCompatibleModels = useCallback(async () => {
    if (compatibleModels) return;
    try {
      const resp = await fetch(`${apiBase}/quantevolver/multi-alpha/compatible-models`);
      const data = await resp.json();
      if (data.ok) setCompatibleModels(data.models);
    } catch (e) {
      console.error("加载模型列表失败:", e);
    }
  }, [apiBase, compatibleModels]);

  const updateGroupModel = useCallback(
    (groupName: string, modelId: string) => {
      if (!config) return;
      const model = compatibleModels?.find((m) => m.model_id === modelId);
      const newGroups = config.alpha_groups.map((g) =>
        g.group_name === groupName
          ? {
              ...g,
              model_id: modelId,
              dataset_type: model?.dataset_type || g.dataset_type,
              compute_resource: model?.compute_resource || g.compute_resource,
            }
          : g
      );
      onConfigChange({ ...config, alpha_groups: newGroups });
    },
    [config, onConfigChange, compatibleModels]
  );

  // ── Update meta model ──────────────────────────────────────────

  const updateMetaMethod = useCallback(
    (method: string) => {
      if (!config) return;
      onConfigChange({ ...config, meta_model: { ...config.meta_model, method } });
    },
    [config, onConfigChange]
  );

  const updateExecutionMode = useCallback(
    (execution_mode: string) => {
      if (!config) return;
      onConfigChange({ ...config, execution_mode });
    },
    [config, onConfigChange]
  );

  // ── Reuse source management ────────────────────────────────────

  const fetchReusableSources = useCallback(
    async (groupName: string) => {
      if (reusableSources[groupName]) return;
      try {
        const resp = await fetch(
          `${apiBase}/quantevolver/multi-alpha/reusable-groups?group_name=${encodeURIComponent(groupName)}&limit=10`
        );
        const data = await resp.json();
        if (data.ok) {
          setReusableSources((prev) => ({ ...prev, [groupName]: data.items || [] }));
        }
      } catch (e) {
        console.error(`加载可复用源 (${groupName}) 失败:`, e);
      }
    },
    [apiBase, reusableSources]
  );

  const updateGroupReuse = useCallback(
    (groupName: string, reuseMode: string, sourceExpId?: string) => {
      if (!config) return;
      const isReusing = reuseMode !== "retrain";
      const newGroups = config.alpha_groups.map((g) =>
        g.group_name === groupName
          ? {
              ...g,
              reuse_mode: reuseMode,
              model_source_experiment_id: isReusing ? sourceExpId || undefined : undefined,
              model_source_group_name: isReusing ? groupName : undefined,
            }
          : g
      );
      onConfigChange({ ...config, alpha_groups: newGroups });
    },
    [config, onConfigChange]
  );

  // ── Add new group (A4) ─────────────────────────────────────────

  const addNewGroup = useCallback(
    (dsg: string) => {
      if (!config) return;
      const existing = config.alpha_groups.map((g) => g.group_name);
      // Generate unique name
      let name = dsg;
      let idx = 2;
      while (existing.includes(name)) {
        name = `${dsg}_${idx}`;
        idx++;
      }
      // Default model based on dsg
      const gpuGroups = ["price_volume"];
      const isGpu = gpuGroups.includes(dsg);
      const newGroup: AlphaGroupConfig = {
        group_name: name,
        factor_names: [],
        model_id: isGpu ? "__seed_ALSTM_default_v1__" : "__seed_LGBModel_conservative_v1__",
        dataset_type: isGpu ? "TSDatasetH" : "DatasetH",
        compute_resource: isGpu ? "gpu" : "cpu",
      };
      onConfigChange({
        ...config,
        alpha_groups: [...config.alpha_groups, newGroup],
      });
      setExpandedGroups((prev) => new Set(prev).add(name));
      setShowNewGroup(false);
    },
    [config, onConfigChange]
  );

  // ── Infer data_source_group from group_name ────────────────────

  const inferDsg = (groupName: string): string => {
    const knownDsgs = ["price_volume", "money_flow", "fundamental", "valuation", "chip", "sector", "liquidity", "cross_dataset"];
    for (const d of knownDsgs) {
      if (groupName.startsWith(d)) return d;
    }
    return "price_volume";
  };

  // ── Render ─────────────────────────────────────────────────────

  if (!config) {
    return (
      <div className="space-y-4">
        {/* A2: 自动分组参数面板 */}
        <div className="p-3 bg-gray-50 dark:bg-gray-800/50 rounded-lg border border-gray-200 dark:border-gray-700">
          <div className="text-xs font-medium text-gray-500 mb-2">自动分组参数</div>
          <div className="grid grid-cols-5 gap-2">
            <div>
              <label className="text-xs text-gray-500">评级 &ge;</label>
              <select
                className="w-full mt-0.5 text-xs p-1.5 border rounded dark:bg-gray-700 dark:border-gray-600"
                value={autoParams.min_grade}
                onChange={(e) => setAutoParams((p) => ({ ...p, min_grade: e.target.value }))}
              >
                {["S", "A", "B", "C", "D"].map((g) => (
                  <option key={g} value={g}>{g}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="text-xs text-gray-500">IC &ge;</label>
              <input
                type="number"
                step="0.005"
                className="w-full mt-0.5 text-xs p-1.5 border rounded dark:bg-gray-700 dark:border-gray-600"
                value={autoParams.min_ic}
                onChange={(e) => setAutoParams((p) => ({ ...p, min_ic: e.target.value }))}
              />
            </div>
            <div>
              <label className="text-xs text-gray-500">每组上限</label>
              <input
                type="number"
                className="w-full mt-0.5 text-xs p-1.5 border rounded dark:bg-gray-700 dark:border-gray-600"
                value={autoParams.max_factors_per_group}
                onChange={(e) => setAutoParams((p) => ({ ...p, max_factors_per_group: e.target.value }))}
              />
            </div>
            <div>
              <label className="text-xs text-gray-500">去重阈值</label>
              <input
                type="number"
                step="0.05"
                className="w-full mt-0.5 text-xs p-1.5 border rounded dark:bg-gray-700 dark:border-gray-600"
                value={autoParams.max_intra_corr}
                onChange={(e) => setAutoParams((p) => ({ ...p, max_intra_corr: e.target.value }))}
              />
            </div>
            <div className="flex items-end">
              <button
                className="w-full px-3 py-1.5 bg-purple-600 text-white rounded text-xs font-medium hover:bg-purple-700 disabled:opacity-50"
                onClick={handleAutoSelect}
                disabled={loading}
              >
                {loading ? "分析中..." : "一键生成分组"}
              </button>
            </div>
          </div>
        </div>
        {error && (
          <div className="text-sm text-red-500 bg-red-50 p-2 rounded">{error}</div>
        )}
      </div>
    );
  }

  const totalFactors = config.alpha_groups.reduce((s, g) => s + g.factor_names.length, 0);

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="text-sm text-gray-600 dark:text-gray-400">
          <span className="font-medium text-purple-600">{config.alpha_groups.length}</span> 组 /{" "}
          <span className="font-medium">{totalFactors}</span> 因子
          {config.auto_selected && (
            <span className="ml-2 text-xs bg-purple-100 text-purple-700 px-2 py-0.5 rounded">自动推荐</span>
          )}
        </div>
        <button
          className="px-3 py-1 text-xs bg-gray-100 hover:bg-gray-200 dark:bg-gray-700 dark:hover:bg-gray-600 rounded text-gray-600 dark:text-gray-300"
          onClick={handleAutoSelect}
          disabled={loading}
        >
          {loading ? "重新分析..." : "重新生成"}
        </button>
      </div>

      {/* A2: 参数面板(折叠式) */}
      <details className="text-xs">
        <summary className="cursor-pointer text-gray-500 hover:text-gray-700">调整自动分组参数</summary>
        <div className="mt-2 grid grid-cols-5 gap-2 p-2 bg-gray-50 dark:bg-gray-800/50 rounded">
          <div>
            <label className="text-gray-500">评级 &ge;</label>
            <select className="w-full mt-0.5 p-1 border rounded dark:bg-gray-700 dark:border-gray-600"
              value={autoParams.min_grade}
              onChange={(e) => setAutoParams((p) => ({ ...p, min_grade: e.target.value }))}
            >
              {["S", "A", "B", "C", "D"].map((g) => <option key={g} value={g}>{g}</option>)}
            </select>
          </div>
          <div>
            <label className="text-gray-500">IC &ge;</label>
            <input type="number" step="0.005" className="w-full mt-0.5 p-1 border rounded dark:bg-gray-700 dark:border-gray-600"
              value={autoParams.min_ic} onChange={(e) => setAutoParams((p) => ({ ...p, min_ic: e.target.value }))} />
          </div>
          <div>
            <label className="text-gray-500">每组上限</label>
            <input type="number" className="w-full mt-0.5 p-1 border rounded dark:bg-gray-700 dark:border-gray-600"
              value={autoParams.max_factors_per_group} onChange={(e) => setAutoParams((p) => ({ ...p, max_factors_per_group: e.target.value }))} />
          </div>
          <div>
            <label className="text-gray-500">去重阈值</label>
            <input type="number" step="0.05" className="w-full mt-0.5 p-1 border rounded dark:bg-gray-700 dark:border-gray-600"
              value={autoParams.max_intra_corr} onChange={(e) => setAutoParams((p) => ({ ...p, max_intra_corr: e.target.value }))} />
          </div>
          <div className="flex items-end">
            <button className="w-full px-2 py-1 bg-purple-600 text-white rounded hover:bg-purple-700 disabled:opacity-50"
              onClick={handleAutoSelect} disabled={loading}>{loading ? "..." : "重新生成"}</button>
          </div>
        </div>
      </details>

      {/* Groups (A1: 可展开编辑) */}
      <div className="space-y-3">
        {config.alpha_groups.map((group) => {
          const isExpanded = expandedGroups.has(group.group_name);
          const dsg = inferDsg(group.group_name);
          const corrPairs = correlationResults[group.group_name] || [];
          const highCorr = corrPairs.filter((c) => Math.abs(c.correlation) > 0.7);

          return (
            <div
              key={group.group_name}
              className="border border-gray-200 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-800"
            >
              {/* Group header — click to expand */}
              <div
                className="p-3 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50 flex items-center justify-between"
                onClick={() => toggleGroup(group.group_name)}
              >
                <div className="flex items-center gap-2">
                  <span className="text-xs text-gray-400">{isExpanded ? "▼" : "▶"}</span>
                  <span className="font-medium text-sm">{group.group_name}</span>
                  <span className="text-xs text-gray-500">{group.factor_names.length} 因子</span>
                  {group.compute_resource === "gpu" && (
                    <span className="text-xs bg-green-100 text-green-700 px-1.5 py-0.5 rounded">GPU</span>
                  )}
                  {group.compute_resource === "cpu" && (
                    <span className="text-xs bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded">CPU</span>
                  )}
                  <span className="text-xs text-gray-400">
                    {group.model_id.replace(/__seed_|__/g, "").replace(/_v\d+$/, "")}
                  </span>
                </div>
                <button
                  className="text-xs text-red-500 hover:text-red-700"
                  onClick={(e) => { e.stopPropagation(); removeGroup(group.group_name); }}
                  disabled={config.alpha_groups.length <= 2}
                >
                  删除
                </button>
              </div>

              {/* Expanded content (A1) */}
              {isExpanded && (
                <div className="px-3 pb-3 border-t border-gray-100 dark:border-gray-700">
                  {/* Model selector (A4) */}
                  <div className="flex items-center gap-2 mt-2 mb-2">
                    <span className="text-xs text-gray-500">模型:</span>
                    <select
                      className="text-xs p-1 border rounded dark:bg-gray-700 dark:border-gray-600 flex-1"
                      value={group.model_id}
                      onFocus={fetchCompatibleModels}
                      onChange={(e) => updateGroupModel(group.group_name, e.target.value)}
                    >
                      <option value={group.model_id}>
                        {group.model_id.replace(/__seed_|__/g, "")} ({group.dataset_type})
                      </option>
                      {(compatibleModels || [])
                        .filter((m) => m.model_id !== group.model_id)
                        .map((m) => (
                          <option key={m.model_id} value={m.model_id}>
                            {m.description || m.model_id} ({m.dataset_type})
                          </option>
                        ))}
                    </select>
                  </div>

                  {/* Reuse selector */}
                  <div className="flex items-center gap-2 mb-2">
                    <span className="text-xs text-gray-500">复用:</span>
                    <select
                      className="text-xs p-1 border rounded dark:bg-gray-700 dark:border-gray-600"
                      value={group.reuse_mode || "retrain"}
                      onFocus={() => fetchReusableSources(group.group_name)}
                      onChange={(e) => {
                        const mode = e.target.value;
                        updateGroupReuse(group.group_name, mode, mode === "retrain" ? undefined : group.model_source_experiment_id);
                      }}
                    >
                      <option value="retrain">全新训练</option>
                      <option value="reuse_prediction">复用预测</option>
                      <option value="reuse_model">复用模型</option>
                    </select>
                    {group.reuse_mode && group.reuse_mode !== "retrain" && (
                      <select
                        className="text-xs p-1 border rounded dark:bg-gray-700 dark:border-gray-600 flex-1"
                        value={group.model_source_experiment_id || ""}
                        onFocus={() => fetchReusableSources(group.group_name)}
                        onChange={(e) => updateGroupReuse(group.group_name, group.reuse_mode || "reuse_prediction", e.target.value)}
                      >
                        <option value="">-- 选择源 --</option>
                        {(reusableSources[group.group_name] || []).map((src) => (
                          <option key={src.experiment_id} value={src.experiment_id}>
                            {src.experiment_id} | IC:{src.group_ic != null ? src.group_ic.toFixed(4) : "-"}
                          </option>
                        ))}
                      </select>
                    )}
                  </div>

                  {/* Factor list */}
                  <div className="max-h-48 overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="text-gray-400 border-b dark:border-gray-700">
                          <th className="text-left py-1 font-normal">因子名</th>
                          <th className="text-right py-1 font-normal w-16">操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        {group.factor_names.map((fn) => (
                          <tr key={fn} className="border-b border-gray-50 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30">
                            <td className="py-0.5 text-gray-700 dark:text-gray-300 font-mono">{fn}</td>
                            <td className="py-0.5 text-right">
                              <button
                                className="text-red-400 hover:text-red-600"
                                onClick={() => removeFactor(group.group_name, fn)}
                              >
                                移除
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  {/* Actions row */}
                  <div className="flex items-center gap-2 mt-2">
                    {/* Add factor button (A1) */}
                    <button
                      className="text-xs px-2 py-1 bg-purple-50 text-purple-600 rounded hover:bg-purple-100 dark:bg-purple-900/30 dark:text-purple-400"
                      onClick={() => {
                        if (showAddFactor === group.group_name) {
                          setShowAddFactor(null);
                        } else {
                          setShowAddFactor(group.group_name);
                          fetchCandidateFactors(group.group_name, dsg, group.factor_names);
                        }
                      }}
                    >
                      {showAddFactor === group.group_name ? "收起" : "+ 添加因子"}
                    </button>

                    {/* Correlation check (A3) */}
                    <button
                      className="text-xs px-2 py-1 bg-blue-50 text-blue-600 rounded hover:bg-blue-100 dark:bg-blue-900/30 dark:text-blue-400 disabled:opacity-50"
                      onClick={() => checkGroupCorrelation(group.group_name, group.factor_names)}
                      disabled={checkingCorrelation === group.group_name || group.factor_names.length < 2}
                    >
                      {checkingCorrelation === group.group_name ? "检查中..." : "组内相关性检查"}
                    </button>
                  </div>

                  {/* Candidate factors panel (A1) */}
                  {showAddFactor === group.group_name && (
                    <div className="mt-2 p-2 bg-gray-50 dark:bg-gray-900/50 rounded border border-gray-200 dark:border-gray-700 max-h-40 overflow-y-auto">
                      {loadingCandidates === group.group_name ? (
                        <div className="text-xs text-gray-500 text-center py-2">加载候选因子...</div>
                      ) : (candidateFactors[group.group_name] || []).length === 0 ? (
                        <div className="text-xs text-gray-500 text-center py-2">无可用候选因子</div>
                      ) : (
                        <table className="w-full text-xs">
                          <thead>
                            <tr className="text-gray-400">
                              <th className="text-left py-0.5 font-normal">因子</th>
                              <th className="text-center py-0.5 font-normal w-10">评级</th>
                              <th className="text-right py-0.5 font-normal w-16">IC</th>
                              <th className="text-right py-0.5 font-normal w-12">操作</th>
                            </tr>
                          </thead>
                          <tbody>
                            {(candidateFactors[group.group_name] || []).map((cf) => (
                              <tr key={cf.factor_name} className="hover:bg-white dark:hover:bg-gray-800">
                                <td className="py-0.5 font-mono text-gray-600 dark:text-gray-400">{cf.factor_name}</td>
                                <td className="py-0.5 text-center">
                                  <span className={`px-1 rounded ${
                                    cf.grade === "S" ? "bg-yellow-100 text-yellow-700" :
                                    cf.grade === "A" ? "bg-green-100 text-green-700" :
                                    cf.grade === "B" ? "bg-blue-100 text-blue-700" :
                                    "bg-gray-100 text-gray-600"
                                  }`}>
                                    {cf.grade}
                                  </span>
                                </td>
                                <td className="py-0.5 text-right text-gray-500">
                                  {cf.ic_value != null ? Math.abs(cf.ic_value).toFixed(4) : "-"}
                                </td>
                                <td className="py-0.5 text-right">
                                  <button
                                    className="text-green-600 hover:text-green-800"
                                    onClick={() => addFactor(group.group_name, cf.factor_name)}
                                  >
                                    +加入
                                  </button>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      )}
                    </div>
                  )}

                  {/* Correlation results (A3) */}
                  {highCorr.length > 0 && (
                    <div className="mt-2 p-2 bg-red-50 dark:bg-red-900/20 rounded text-xs">
                      <div className="font-medium text-red-600 mb-1">高相关对 (&gt;0.7):</div>
                      {highCorr.map((c, i) => (
                        <div key={i} className="flex items-center justify-between py-0.5">
                          <span className="text-red-500 font-mono">
                            {c.factor_a} &harr; {c.factor_b}: {c.correlation.toFixed(3)}
                          </span>
                          <div className="flex gap-1">
                            <button
                              className="text-red-400 hover:text-red-600"
                              onClick={() => removeFactor(group.group_name, c.factor_a)}
                            >
                              移除{c.factor_a.slice(0, 8)}
                            </button>
                            <span className="text-gray-300">|</span>
                            <button
                              className="text-red-400 hover:text-red-600"
                              onClick={() => removeFactor(group.group_name, c.factor_b)}
                            >
                              移除{c.factor_b.slice(0, 8)}
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                  {correlationResults[group.group_name] && highCorr.length === 0 && (
                    <div className="mt-2 text-xs text-green-600">组内无高相关因子对</div>
                  )}
                </div>
              )}

              {/* Collapsed preview */}
              {!isExpanded && (
                <div className="px-3 pb-2 text-xs text-gray-400 dark:text-gray-500 truncate">
                  {group.factor_names.slice(0, 5).join(", ")}
                  {group.factor_names.length > 5 && ` +${group.factor_names.length - 5} more`}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* A4: 新增组 */}
      <div>
        {showNewGroup ? (
          <div className="p-3 border border-dashed border-purple-300 rounded-lg bg-purple-50/50 dark:bg-purple-900/10">
            <div className="flex items-center gap-2 text-xs">
              <span className="text-gray-600">数据源组:</span>
              <select
                className="p-1 border rounded dark:bg-gray-700 dark:border-gray-600"
                value={newGroupDsg}
                onChange={(e) => setNewGroupDsg(e.target.value)}
              >
                {["price_volume", "money_flow", "fundamental", "valuation", "chip", "sector", "liquidity", "cross_dataset"].map((d) => (
                  <option key={d} value={d}>{d}</option>
                ))}
              </select>
              <button
                className="px-2 py-1 bg-purple-600 text-white rounded hover:bg-purple-700"
                onClick={() => addNewGroup(newGroupDsg)}
              >
                创建
              </button>
              <button
                className="px-2 py-1 text-gray-500 hover:text-gray-700"
                onClick={() => setShowNewGroup(false)}
              >
                取消
              </button>
            </div>
          </div>
        ) : (
          <button
            className="w-full py-2 border border-dashed border-gray-300 dark:border-gray-600 rounded-lg text-xs text-gray-500 hover:border-purple-400 hover:text-purple-600"
            onClick={() => setShowNewGroup(true)}
          >
            + 新增组
          </button>
        )}
      </div>

      {/* Meta-Model Config */}
      <div className="p-3 bg-gray-50 dark:bg-gray-800/50 rounded-lg border border-gray-200 dark:border-gray-700">
        <div className="text-xs font-medium text-gray-500 mb-2">Meta-Model 配置</div>
        <div className="grid grid-cols-3 gap-3">
          <div>
            <label className="text-xs text-gray-500">合成方法</label>
            <select
              className="w-full mt-0.5 text-xs p-1.5 border rounded dark:bg-gray-700 dark:border-gray-600"
              value={config.meta_model.method}
              onChange={(e) => updateMetaMethod(e.target.value)}
            >
              <option value="ic_weighted">IC 加权</option>
              <option value="ols">OLS Ridge</option>
              <option value="stacking">LightGBM Stacking</option>
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500">CV 策略</label>
            <select
              className="w-full mt-0.5 text-xs p-1.5 border rounded dark:bg-gray-700 dark:border-gray-600"
              value={config.meta_model.cv_strategy}
              onChange={(e) =>
                onConfigChange({ ...config, meta_model: { ...config.meta_model, cv_strategy: e.target.value } })
              }
            >
              <option value="train_valid_test">Train/Valid/Test (回测)</option>
              <option value="walk_forward">Walk-Forward (实盘)</option>
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500">执行模式</label>
            <select
              className="w-full mt-0.5 text-xs p-1.5 border rounded dark:bg-gray-700 dark:border-gray-600"
              value={config.execution_mode}
              onChange={(e) => updateExecutionMode(e.target.value)}
            >
              <option value="serial">串行</option>
              <option value="local_parallel">本机并行</option>
              <option value="distributed" disabled={!MULTI_ALPHA_DISTRIBUTED_ENABLED}>
                分布式{MULTI_ALPHA_DISTRIBUTED_ENABLED ? "" : "（阶段 3 启用）"}
              </option>
            </select>
            {!MULTI_ALPHA_DISTRIBUTED_ENABLED && (
              <p className="mt-1 text-[11px] text-amber-600">
                当前阶段仅允许 WSL 单节点验证；分布式训练将在阶段 3 完成后开放。
              </p>
            )}
          </div>
        </div>
      </div>

      {error && (
        <div className="text-sm text-red-500 bg-red-50 p-2 rounded">{error}</div>
      )}
    </div>
  );
}
