"use client";

import { useEffect, useState, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Prompt = {
  id: number;
  agent_type: string;
  prompt_key: string;
  display_name: string;
  description?: string;
  system_prompt: string;
  user_prompt_template: string;
  is_active: boolean;
  version: number;
  created_at?: string;
  updated_at?: string;
};

type LLMModel = { id: string; name: string; source: string };

const AGENT_LABELS: Record<string, string> = {
  factor_analyst: "因子分析师",
  portfolio_architect: "组合架构师",
  model_analyst: "模型分析师",
  experiment_analyst: "实验结果分析师",
  factor_repairer: "因子代码修复Agent",
  factor_analyzer: "因子特征分析Agent",
  evolution_analyst: "演进诊断分析师(Analyst)",
  evolution_evaluator: "SOTA评估员(Evaluator)",
  evolution_researcher: "演进策略研究员(Researcher)",
  evolution_reviewer: "配置审查员(Reviewer)",
  factor_classifier: "因子分类Agent",
  factor_describer: "因子描述Agent",
  strategy_analyzer: "策略分析师",
};

// 分组配置：用于在模型配置区域分组展示
const AGENT_GROUPS: { label: string; color: string; types: string[] }[] = [
  {
    label: "QE自动演进(Auto-Evolution)",
    color: "#ec4899", // Pink
    types: ["evolution_analyst", "evolution_evaluator", "evolution_researcher", "evolution_reviewer"],
  },
  {
    label: "QE选股分析",
    color: "#7c3aed",
    types: ["portfolio_architect", "model_analyst", "experiment_analyst", "strategy_analyzer"],
  },
  {
    label: "因子库管理",
    color: "#10b981", // Emerald
    types: ["factor_classifier", "factor_describer"],
  },
  {
    label: "因子代码改写",
    color: "#0891b2",
    types: ["factor_repairer", "factor_analyzer"],
  },
];

const AGENT_TYPES = [
  "evolution_analyst", "evolution_evaluator", "evolution_researcher", "evolution_reviewer",
  "portfolio_architect", "model_analyst", "experiment_analyst", "strategy_analyzer",
  "factor_repairer", "factor_analyzer",
  "factor_classifier", "factor_describer",
];

export default function PromptsPage() {
  const [prompts, setPrompts] = useState<Prompt[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [agentFilter, setAgentFilter] = useState("");
  const [editingPrompt, setEditingPrompt] = useState<Prompt | null>(null);
  const [saving, setSaving] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [newPrompt, setNewPrompt] = useState({
    agent_type: "evolution_analyst",
    prompt_key: "",
    display_name: "",
    description: "",
    system_prompt: "",
    user_prompt_template: "",
  });

  const [llmModels, setLlmModels] = useState<LLMModel[]>([]);
  const [agentModelConfigs, setAgentModelConfigs] = useState<Record<string, string>>({});
  const [modelSaving, setModelSaving] = useState<string | null>(null);

  const loadLlmModels = useCallback(async () => {
    try {
      const [mRes, cRes] = await Promise.all([
        fetch(`${API}/quantevolver/llm-models`).then(r => r.json()),
        fetch(`${API}/quantevolver/agent-model-config`).then(r => r.json()),
      ]);
      
      setLlmModels(mRes.models || []);
      
      // cRes.agents is an array of objects: { agent_type, model_id, ... }
      const configsMap: Record<string, string> = {};
      if (cRes.agents && Array.isArray(cRes.agents)) {
        cRes.agents.forEach((agent: any) => {
          if (agent.agent_type && agent.model_id) {
            configsMap[agent.agent_type] = agent.model_id;
          }
        });
      }
      setAgentModelConfigs(configsMap);
    } catch { /* ignore */ }
  }, []);

  async function saveAgentModel(agentType: string, modelId: string) {
    setModelSaving(agentType);
    try {
      const res = await fetch(`${API}/quantevolver/agent-model-config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ agent_type: agentType, model_id: modelId }),
      });
      const data = await res.json();
      if (data.ok) {
        setAgentModelConfigs(prev => ({ ...prev, [agentType]: modelId }));
      }
    } catch { /* ignore */ }
    setModelSaving(null);
  }

  const loadPrompts = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({ active_only: "false" });
      if (agentFilter) params.set("agent_type", agentFilter);
      const res = await fetch(`${API}/quantevolver/prompts?${params}`);
      const data = await res.json();
      setPrompts(data.items || []);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    }
    setLoading(false);
  }, [agentFilter]);

  useEffect(() => { loadPrompts(); loadLlmModels(); }, [loadPrompts, loadLlmModels]);

  async function savePrompt() {
    if (!editingPrompt) return;
    setSaving(true);
    try {
      const res = await fetch(
        `${API}/quantevolver/prompts/${editingPrompt.agent_type}/${editingPrompt.prompt_key}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            system_prompt: editingPrompt.system_prompt,
            user_prompt_template: editingPrompt.user_prompt_template,
            display_name: editingPrompt.display_name,
            description: editingPrompt.description,
            is_active: editingPrompt.is_active,
          }),
        }
      );
      const data = await res.json();
      if (data.ok) {
        alert(`保存成功！版本: v${data.version}`);
        setEditingPrompt(null);
        loadPrompts();
      } else {
        alert("保存失败: " + (data.error || data.detail || "未知错误"));
      }
    } catch (e: any) {
      alert("保存失败: " + (e?.message || ""));
    }
    setSaving(false);
  }

  async function createPrompt() {
    if (!newPrompt.prompt_key || !newPrompt.display_name) {
      alert("请填写提示词键名和显示名称");
      return;
    }
    setSaving(true);
    try {
      const res = await fetch(`${API}/quantevolver/prompts`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(newPrompt),
      });
      const data = await res.json();
      if (data.ok) {
        alert("创建成功！");
        setShowCreate(false);
        setNewPrompt({
          agent_type: "evolution_analyst", prompt_key: "", display_name: "",
          description: "", system_prompt: "", user_prompt_template: "",
        });
        loadPrompts();
      } else {
        alert("创建失败: " + (data.error || data.detail || "未知错误"));
      }
    } catch (e: any) {
      alert("创建失败: " + (e?.message || ""));
    }
    setSaving(false);
  }

  async function deletePrompt(id: number, name: string) {
    if (!confirm(`确定要删除提示词 "${name}" 吗？`)) return;
    try {
      const res = await fetch(`${API}/quantevolver/prompts/${id}`, { method: "DELETE" });
      const data = await res.json();
      if (data.ok) {
        loadPrompts();
      } else {
        alert("删除失败: " + (data.error || data.detail || ""));
      }
    } catch (e: any) {
      alert("删除失败: " + (e?.message || ""));
    }
  }

  // 编辑弹窗
  if (editingPrompt) {
    return (
      <main style={{ padding: 24 }}>
        <section
          style={{
            background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 100%)",
            borderRadius: 16, padding: 20, color: "#fff", marginBottom: 16,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <div>
              <h1 style={{ margin: 0, fontSize: 24 }}>编辑提示词</h1>
              <p style={{ marginTop: 4, opacity: 0.9, fontSize: 13 }}>
                {AGENT_LABELS[editingPrompt.agent_type] || editingPrompt.agent_type} / {editingPrompt.prompt_key}
                <span style={{ marginLeft: 8, opacity: 0.7 }}>v{editingPrompt.version}</span>
              </p>
            </div>
            <button
              onClick={() => setEditingPrompt(null)}
              style={{ padding: "6px 16px", fontSize: 13, borderRadius: 6, border: "1px solid rgba(255,255,255,0.3)", background: "transparent", color: "#fff", cursor: "pointer" }}
            >
              返回列表
            </button>
          </div>
        </section>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          {/* 基本信息 */}
          <section style={cardStyle}>
            <h3 style={{ margin: "0 0 12px", fontSize: 14, fontWeight: 600 }}>基本信息</h3>
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              <label style={{ fontSize: 12 }}>
                <span style={{ fontWeight: 600 }}>显示名称</span>
                <input
                  value={editingPrompt.display_name}
                  onChange={e => setEditingPrompt({ ...editingPrompt, display_name: e.target.value })}
                  style={inputStyle}
                />
              </label>
              <label style={{ fontSize: 12 }}>
                <span style={{ fontWeight: 600 }}>描述</span>
                <textarea
                  value={editingPrompt.description || ""}
                  onChange={e => setEditingPrompt({ ...editingPrompt, description: e.target.value })}
                  rows={3}
                  style={{ ...inputStyle, resize: "vertical" }}
                />
              </label>
              <label style={{ fontSize: 12, display: "flex", alignItems: "center", gap: 6 }}>
                <input
                  type="checkbox"
                  checked={editingPrompt.is_active}
                  onChange={e => setEditingPrompt({ ...editingPrompt, is_active: e.target.checked })}
                />
                <span style={{ fontWeight: 600 }}>激活状态</span>
              </label>
            </div>
          </section>

          {/* 提示信息 */}
          <section style={cardStyle}>
            <h3 style={{ margin: "0 0 12px", fontSize: 14, fontWeight: 600 }}>使用说明</h3>
            <div style={{ fontSize: 12, color: "#6b7280", lineHeight: 1.8 }}>
              <p><strong>系统提示词</strong>：定义Agent的角色、能力和输出格式要求。</p>
              <p><strong>用户提示词模板</strong>：包含占位符（如 {"{factor_name}"}），运行时会被实际数据替换。</p>
              <p style={{ marginTop: 8, padding: 8, background: "#f3f4f6", borderRadius: 6 }}>
                <strong>因子分析师</strong>可用变量：factor_name, factor_source, expression, code_preview, code_text
              </p>
              <p style={{ padding: 8, background: "#f3f4f6", borderRadius: 6 }}>
                <strong>组合架构师</strong>可用变量：factor_count, factor_names, category_distribution, avg_ic, avg_grade, model_id, model_type, strategy_id, strategy_params, train_period, valid_period, test_period
              </p>
              <p style={{ padding: 8, background: "#f3f4f6", borderRadius: 6 }}>
                <strong>实验结果分析师</strong>可用变量：experiment_id, experiment_name, evolution_goal, factor_names, model_id, strategy_id, strategy_params, IC, ICIR, Rank_IC, Rank_ICIR, annualized_return, information_ratio, max_drawdown, sharpe_ratio, daily_win_rate, weekly_win_rate, stock_win_rate, total_trades, profit_loss_ratio, avg_turnover, SOTA_comparison, trace_summary, hypothesis
              </p>
            </div>
          </section>
        </div>

        {/* 系统提示词 */}
        <section style={{ ...cardStyle, marginTop: 16 }}>
          <h3 style={{ margin: "0 0 8px", fontSize: 14, fontWeight: 600 }}>
            系统提示词 (System Prompt)
            <span style={{ fontSize: 11, color: "#9ca3af", fontWeight: 400, marginLeft: 8 }}>
              {editingPrompt.system_prompt.length} 字符
            </span>
          </h3>
          <textarea
            value={editingPrompt.system_prompt}
            onChange={e => setEditingPrompt({ ...editingPrompt, system_prompt: e.target.value })}
            rows={16}
            style={{ ...textareaStyle, fontFamily: "monospace" }}
          />
        </section>

        {/* 用户提示词模板 */}
        <section style={{ ...cardStyle, marginTop: 16 }}>
          <h3 style={{ margin: "0 0 8px", fontSize: 14, fontWeight: 600 }}>
            用户提示词模板 (User Prompt Template)
            <span style={{ fontSize: 11, color: "#9ca3af", fontWeight: 400, marginLeft: 8 }}>
              {editingPrompt.user_prompt_template.length} 字符
            </span>
          </h3>
          <textarea
            value={editingPrompt.user_prompt_template}
            onChange={e => setEditingPrompt({ ...editingPrompt, user_prompt_template: e.target.value })}
            rows={10}
            style={{ ...textareaStyle, fontFamily: "monospace" }}
          />
        </section>

        {/* 保存按钮 */}
        <div style={{ marginTop: 16, display: "flex", gap: 12 }}>
          <button
            onClick={savePrompt}
            disabled={saving}
            style={{
              padding: "10px 24px", fontSize: 14, fontWeight: 600,
              background: "#7c3aed", color: "#fff", border: "none", borderRadius: 8,
              cursor: saving ? "not-allowed" : "pointer", opacity: saving ? 0.6 : 1,
            }}
          >
            {saving ? "保存中..." : "保存修改"}
          </button>
          <button
            onClick={() => setEditingPrompt(null)}
            style={{
              padding: "10px 24px", fontSize: 14, fontWeight: 600,
              background: "#f3f4f6", color: "#374151", border: "none", borderRadius: 8, cursor: "pointer",
            }}
          >
            取消
          </button>
        </div>
      </main>
    );
  }

  // 列表视图
  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #7c3aed 0%, #f59e0b 100%)",
          borderRadius: 16, padding: 20, color: "#fff", marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>提示词管理</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          配置因子分析师和组合架构师的LLM提示词，自定义Agent行为
        </p>
      </section>

      {/* 筛选栏 */}
      <section style={{ ...cardStyle, marginBottom: 16 }}>
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <select
            value={agentFilter}
            onChange={e => setAgentFilter(e.target.value)}
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db" }}
          >
            <option value="">全部Agent</option>
            <optgroup label="QE自动演进">
              <option value="evolution_analyst">演进诊断分析师(Analyst)</option>
              <option value="evolution_evaluator">SOTA评估官(Evaluator)</option>
              <option value="evolution_researcher">演进策略研究员(Researcher)</option>
              <option value="evolution_reviewer">配置审查员(Reviewer)</option>
            </optgroup>
            <optgroup label="QE选股分析">
              <option value="portfolio_architect">组合架构师</option>
              <option value="model_analyst">模型分析员</option>
              <option value="experiment_analyst">实验结果分析员</option>
              <option value="strategy_analyzer">策略分析师</option>
            </optgroup>
            <optgroup label="因子库管理">
              <option value="factor_classifier">因子分类Agent</option>
              <option value="factor_describer">因子描述Agent</option>
            </optgroup>
            <optgroup label="因子代码改写">
              <option value="factor_repairer">因子代码修复Agent</option>
              <option value="factor_analyzer">因子改造审核Agent</option>
            </optgroup>
          </select>

          <button
            onClick={loadPrompts}
            disabled={loading}
            style={{ padding: "6px 12px", fontSize: 12, cursor: "pointer", borderRadius: 6, border: "1px solid #d1d5db", background: "#fff" }}
          >
            {loading ? "加载中..." : "刷新"}
          </button>

          <button
            onClick={() => setShowCreate(true)}
            style={{
              padding: "6px 14px", fontSize: 12, cursor: "pointer",
              borderRadius: 6, border: "none", background: "#7c3aed", color: "#fff", fontWeight: 600,
            }}
          >
            新建提示词
          </button>

          <span style={{ fontSize: 12, color: "#9ca3af" }}>共 {prompts.length} 条</span>
        </div>
        {error && <div style={{ marginTop: 8, padding: 8, background: "#fee2e2", borderRadius: 6, fontSize: 12 }}>{error}</div>}
      </section>

      {/* LLM模型配置 - 分组展示 */}
      <section style={{ ...cardStyle, marginBottom: 16 }}>
        <h3 style={{ margin: "0 0 12px", fontSize: 14, fontWeight: 600 }}>
          Agent LLM模型配置
          <span style={{ fontSize: 11, color: "#9ca3af", fontWeight: 400, marginLeft: 8 }}>
            为每个Agent指定使用的LLM模型（来源于RDAgent .env配置）
          </span>
        </h3>
        {AGENT_GROUPS.map(group => (
          <div key={group.label} style={{ marginBottom: 16 }}>
            <div style={{
              fontSize: 11, fontWeight: 700, color: group.color,
              marginBottom: 8, paddingBottom: 4,
              borderBottom: `2px solid ${group.color}22`,
              display: "flex", alignItems: "center", gap: 6,
            }}>
              <span style={{
                display: "inline-block", width: 8, height: 8,
                borderRadius: "50%", background: group.color,
              }} />
              {group.label}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 10 }}>
              {group.types.map(at => (
                <div key={at} style={{
                  padding: 12, borderRadius: 8,
                  border: `1px solid ${group.color}33`,
                  background: `${group.color}08`,
                }}>
                  <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6, color: "#374151" }}>
                    {AGENT_LABELS[at] || at}
                  </div>
                  <select
                    title={`${AGENT_LABELS[at] || at} LLM模型选择`}
                    value={agentModelConfigs[at] || ""}
                    onChange={e => saveAgentModel(at, e.target.value)}
                    disabled={modelSaving === at}
                    style={{
                      width: "100%", padding: "6px 8px", fontSize: 11, borderRadius: 6,
                      border: "1px solid #d1d5db", background: "#fff",
                    }}
                  >
                    <option value="">默认模型</option>
                    {llmModels.map(m => (
                      <option key={m.id} value={m.id}>{m.name}</option>
                    ))}
                  </select>
                  {agentModelConfigs[at] && (
                    <div style={{ fontSize: 10, color: group.color, marginTop: 4 }}>
                      当前: {agentModelConfigs[at]}
                    </div>
                  )}
                  {modelSaving === at && (
                    <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 4 }}>保存中...</div>
                  )}
                </div>
              ))}
            </div>
          </div>
        ))}
      </section>

      {/* 创建表单 */}
      {showCreate && (
        <section style={{ ...cardStyle, marginBottom: 16, border: "2px solid #7c3aed" }}>
          <h3 style={{ margin: "0 0 12px", fontSize: 14, fontWeight: 600 }}>新建提示词</h3>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, marginBottom: 10 }}>
            <label style={{ fontSize: 12 }}>
              <span style={{ fontWeight: 600 }}>Agent类型</span>
              <select
                value={newPrompt.agent_type}
                onChange={e => setNewPrompt({ ...newPrompt, agent_type: e.target.value })}
                style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db" }}
              >
                <option value="">全部Agent</option>
                <optgroup label="QE自动演进">
                  <option value="evolution_analyst">演进诊断分析师(Analyst)</option>
                  <option value="evolution_evaluator">SOTA评估官(Evaluator)</option>
                  <option value="evolution_researcher">演进策略研究员(Researcher)</option>
                  <option value="evolution_reviewer">配置审查员(Reviewer)</option>
                </optgroup>
                <optgroup label="QE选股分析">
                  <option value="portfolio_architect">组合架构师</option>
                  <option value="model_analyst">模型分析员</option>
                  <option value="experiment_analyst">实验结果分析员</option>
                  <option value="strategy_analyzer">策略分析师</option>
                </optgroup>
                <optgroup label="因子库管理">
                  <option value="factor_classifier">因子分类Agent</option>
                  <option value="factor_describer">因子描述Agent</option>
                </optgroup>
                <optgroup label="因子代码改写">
                  <option value="factor_repairer">因子代码修复Agent</option>
                  <option value="factor_analyzer">因子改造审核Agent</option>
                </optgroup>
              </select>
              <span style={{ fontWeight: 600 }}>提示词键名</span>
              <input
                placeholder="如: analyze_factor_v2"
                value={newPrompt.prompt_key}
                onChange={e => setNewPrompt({ ...newPrompt, prompt_key: e.target.value })}
                style={inputStyle}
              />
            </label>
            <label style={{ fontSize: 12 }}>
              <span style={{ fontWeight: 600 }}>显示名称</span>
              <input
                placeholder="如: 因子分析提示词V2"
                value={newPrompt.display_name}
                onChange={e => setNewPrompt({ ...newPrompt, display_name: e.target.value })}
                style={inputStyle}
              />
            </label>
          </div>
          <label style={{ fontSize: 12, display: "block", marginBottom: 10 }}>
            <span style={{ fontWeight: 600 }}>描述</span>
            <input
              placeholder="提示词用途描述"
              value={newPrompt.description}
              onChange={e => setNewPrompt({ ...newPrompt, description: e.target.value })}
              style={inputStyle}
            />
          </label>
          <label style={{ fontSize: 12, display: "block", marginBottom: 10 }}>
            <span style={{ fontWeight: 600 }}>系统提示词</span>
            <textarea
              value={newPrompt.system_prompt}
              onChange={e => setNewPrompt({ ...newPrompt, system_prompt: e.target.value })}
              rows={6}
              style={{ ...textareaStyle, fontFamily: "monospace" }}
            />
          </label>
          <label style={{ fontSize: 12, display: "block", marginBottom: 10 }}>
            <span style={{ fontWeight: 600 }}>用户提示词模板</span>
            <textarea
              value={newPrompt.user_prompt_template}
              onChange={e => setNewPrompt({ ...newPrompt, user_prompt_template: e.target.value })}
              rows={4}
              style={{ ...textareaStyle, fontFamily: "monospace" }}
            />
          </label>
          <div style={{ display: "flex", gap: 8 }}>
            <button
              onClick={createPrompt}
              disabled={saving}
              style={{
                padding: "6px 16px", fontSize: 12, fontWeight: 600,
                background: "#7c3aed", color: "#fff", border: "none", borderRadius: 6, cursor: "pointer",
              }}
            >
              {saving ? "创建中..." : "创建"}
            </button>
            <button
              onClick={() => setShowCreate(false)}
              style={{ padding: "6px 16px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", background: "#fff", cursor: "pointer" }}
            >
              取消
            </button>
          </div>
        </section>
      )}

      {/* 提示词卡片列表（分组展示） */}
      {AGENT_GROUPS.map(group => {
        // 过滤出属于当前分组的 prompt
        const groupPrompts = prompts.filter(p => group.types.includes(p.agent_type));
        
        if (groupPrompts.length === 0) return null;

        return (
          <div key={`group-${group.label}`} style={{ marginBottom: 24 }}>
            <div style={{
              fontSize: 14, fontWeight: 700, color: group.color,
              marginBottom: 12, paddingBottom: 6,
              borderBottom: `2px solid ${group.color}33`,
              display: "flex", alignItems: "center", gap: 8,
            }}>
              <span style={{
                display: "inline-block", width: 10, height: 10,
                borderRadius: "50%", background: group.color,
              }} />
              {group.label}
              <span style={{ fontSize: 12, fontWeight: 400, color: "#9ca3af" }}>
                ({groupPrompts.length})
              </span>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              {groupPrompts.map(p => (
                <section
                  key={p.id}
                  style={{
                    ...cardStyle,
                    border: p.is_active ? `1px solid ${group.color}66` : "1px solid #e5e7eb",
                    opacity: p.is_active ? 1 : 0.7,
                  }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 8 }}>
                    <div>
                      <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700 }}>{p.display_name}</h3>
                      <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2 }}>
                        <span style={{
                          padding: "1px 6px", borderRadius: 4, fontSize: 10, fontWeight: 600,
                          background: `${group.color}15`,
                          color: group.color,
                        }}>
                          {AGENT_LABELS[p.agent_type] || p.agent_type}
                        </span>
                        <span style={{ marginLeft: 6 }}>{p.prompt_key}</span>
                        <span style={{ marginLeft: 6 }}>v{p.version}</span>
                        {!p.is_active && <span style={{ marginLeft: 6, color: "#ef4444" }}>已停用</span>}
                      </div>
                    </div>
                    <div style={{ display: "flex", gap: 4 }}>
                      <button
                        onClick={() => setEditingPrompt({ ...p })}
                        style={{
                          padding: "4px 10px", fontSize: 11, fontWeight: 600,
                          background: group.color, color: "#fff", border: "none", borderRadius: 4, cursor: "pointer",
                        }}
                      >
                        编辑
                      </button>
                      <button
                        onClick={() => deletePrompt(p.id, p.display_name)}
                        style={{
                          padding: "4px 10px", fontSize: 11,
                          background: "#fee2e2", color: "#991b1b", border: "none", borderRadius: 4, cursor: "pointer",
                        }}
                      >
                        删除
                      </button>
                    </div>
                  </div>

                  {p.description && (
                    <p style={{ fontSize: 12, color: "#6b7280", margin: "0 0 8px" }}>{p.description}</p>
                  )}

                  <div style={{ fontSize: 11, color: "#9ca3af" }}>
                    <div>系统提示词: {p.system_prompt.length} 字符</div>
                    <div>用户模板: {p.user_prompt_template.length} 字符</div>
                    {p.updated_at && <div>更新: {new Date(p.updated_at).toLocaleString("zh-CN")}</div>}
                  </div>

                  {/* 预览 */}
                  <details style={{ marginTop: 8 }}>
                    <summary style={{ fontSize: 11, cursor: "pointer", color: group.color }}>预览系统提示词</summary>
                    <pre style={{
                      marginTop: 4, padding: 8, background: "#f9fafb", borderRadius: 6,
                      fontSize: 11, whiteSpace: "pre-wrap", maxHeight: 200, overflow: "auto",
                      border: "1px solid #e5e7eb",
                    }}>
                      {p.system_prompt || "(空)"}
                    </pre>
                  </details>
                </section>
              ))}
            </div>
          </div>
        );
      })}

      {!loading && prompts.length === 0 && (
        <div style={{ textAlign: "center", padding: 40, color: "#9ca3af" }}>暂无提示词配置</div>
      )}
    </main>
  );
}

const cardStyle: React.CSSProperties = {
  background: "#fff", borderRadius: 12, padding: 16,
  boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
};

const inputStyle: React.CSSProperties = {
  display: "block", width: "100%", padding: "6px 10px", fontSize: 12,
  borderRadius: 6, border: "1px solid #d1d5db", marginTop: 4,
  boxSizing: "border-box",
};

const textareaStyle: React.CSSProperties = {
  display: "block", width: "100%", padding: "8px 10px", fontSize: 12,
  borderRadius: 6, border: "1px solid #d1d5db", marginTop: 4,
  boxSizing: "border-box", resize: "vertical", lineHeight: 1.6,
};
