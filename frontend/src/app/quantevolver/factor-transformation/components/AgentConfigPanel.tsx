"use client";

import React, { useEffect, useState, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type AgentConfig = {
  agent_type: string;
  display_name: string | null;
  description: string | null;
  model_id: string;
  system_prompt: string | null;
  updated_at: string | null;
};

type Props = {
  onClose: () => void;
};

export function AgentConfigPanel({ onClose }: Props) {
  const [agents, setAgents] = useState<AgentConfig[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState<string | null>(null);
  const [editState, setEditState] = useState<Record<string, AgentConfig>>({});
  const [toast, setToast] = useState<{ text: string; ok: boolean } | null>(null);

  const showToast = (text: string, ok = true) => {
    setToast({ text, ok });
    setTimeout(() => setToast(null), 3000);
  };

  const loadAgents = useCallback(async () => {
    setLoading(true);
    try {
      const r = await fetch(`${API}/quantevolver/agent-model-config`);
      const d = await r.json();
      if (d.ok && d.agents) {
        setAgents(d.agents);
        const init: Record<string, AgentConfig> = {};
        for (const a of d.agents) init[a.agent_type] = { ...a };
        setEditState(init);
      }
    } catch (_) {
      showToast("加载配置失败", false);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadAgents(); }, [loadAgents]);

  const handleSave = async (agentType: string) => {
    const cfg = editState[agentType];
    if (!cfg) return;
    setSaving(agentType);
    try {
      const r = await fetch(`${API}/quantevolver/agent-model-config/prompt`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          agent_type: agentType,
          model_id: cfg.model_id,
          system_prompt: cfg.system_prompt,
          display_name: cfg.display_name,
          description: cfg.description,
        }),
      });
      const d = await r.json();
      if (d.ok) {
        showToast(`${cfg.display_name || agentType} 配置已保存`);
        loadAgents();
      } else {
        showToast(d.detail || "保存失败", false);
      }
    } catch (_) {
      showToast("请求失败", false);
    } finally {
      setSaving(null);
    }
  };

  const updateField = (agentType: string, field: keyof AgentConfig, value: string) => {
    setEditState((prev) => ({
      ...prev,
      [agentType]: { ...prev[agentType], [field]: value },
    }));
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      style={{ backgroundColor: "rgba(0,0,0,0.65)" }}
      onClick={onClose}
    >
      <div
        className="bg-white rounded-2xl shadow-2xl flex flex-col"
        style={{ width: "min(94vw, 860px)", maxHeight: "90vh" }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* 标题栏 */}
        <div
          style={{
            background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 100%)",
            borderRadius: "16px 16px 0 0",
            padding: "16px 24px",
            color: "#fff",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            flexShrink: 0,
          }}
        >
          <div>
            <div style={{ fontWeight: 700, fontSize: 16 }}>Agent 提示词配置</div>
            <div style={{ fontSize: 12, opacity: 0.85, marginTop: 2 }}>
              为每个改造 Agent 配置专用提示词和独立模型
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              background: "rgba(255,255,255,0.15)", border: "none", color: "#fff",
              borderRadius: 8, padding: "4px 14px", cursor: "pointer", fontSize: 14, fontWeight: 600,
            }}
          >
            关闭
          </button>
        </div>

        {/* Toast */}
        {toast && (
          <div style={{
            position: "fixed", top: 16, right: 16, zIndex: 9999,
            padding: "10px 18px", borderRadius: 10,
            background: toast.ok ? "#16a34a" : "#dc2626",
            color: "#fff", fontSize: 14, fontWeight: 600,
            boxShadow: "0 4px 12px rgba(0,0,0,0.2)",
          }}>
            {toast.text}
          </div>
        )}

        {/* 内容区 */}
        <div style={{ flex: 1, overflowY: "auto", padding: 20, display: "flex", flexDirection: "column", gap: 16 }}>
          {loading && (
            <div style={{ textAlign: "center", color: "#9ca3af", padding: 40 }}>加载中...</div>
          )}

          {!loading && agents.length === 0 && (
            <div style={{
              background: "#fef3c7", border: "1px solid #fcd34d", borderRadius: 10,
              padding: 16, color: "#92400e", fontSize: 13,
            }}>
              未找到 Agent 配置。请先运行迁移脚本：
              <code style={{ display: "block", marginTop: 8, fontFamily: "monospace", background: "#fff7ed", padding: "6px 10px", borderRadius: 6 }}>
                python debug_tools/migrate_agent_prompts.py
              </code>
            </div>
          )}

          {!loading && agents.map((agent) => {
            const edit = editState[agent.agent_type] || agent;
            const isDirty = JSON.stringify(edit) !== JSON.stringify(agent);
            return (
              <div
                key={agent.agent_type}
                style={{
                  background: "#fff", borderRadius: 12,
                  border: isDirty ? "1.5px solid #7c3aed" : "1px solid #e5e7eb",
                  padding: 18,
                  boxShadow: "0 1px 3px rgba(0,0,0,0.06)",
                }}
              >
                {/* Agent 标题 */}
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 14 }}>
                  <div>
                    <div style={{ fontWeight: 700, fontSize: 15, color: "#111827" }}>
                      {edit.display_name || agent.agent_type}
                    </div>
                    <div style={{ fontSize: 12, color: "#6b7280", marginTop: 2 }}>
                      {edit.description || ""}
                    </div>
                    <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2, fontFamily: "monospace" }}>
                      agent_type: {agent.agent_type}
                    </div>
                  </div>
                  <button
                    onClick={() => handleSave(agent.agent_type)}
                    disabled={saving === agent.agent_type || !isDirty}
                    style={{
                      padding: "7px 16px", fontSize: 13, fontWeight: 600, border: "none",
                      borderRadius: 8, cursor: isDirty ? "pointer" : "default",
                      background: isDirty ? "#7c3aed" : "#e5e7eb",
                      color: isDirty ? "#fff" : "#9ca3af",
                      transition: "all 0.15s",
                    }}
                  >
                    {saving === agent.agent_type ? "保存中..." : isDirty ? "保存修改" : "已保存"}
                  </button>
                </div>

                {/* 模型选择 */}
                <div style={{ marginBottom: 12 }}>
                  <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 4 }}>
                    LLM 模型 ID
                  </label>
                  <input
                    type="text"
                    value={edit.model_id || ""}
                    onChange={(e) => updateField(agent.agent_type, "model_id", e.target.value)}
                    placeholder="如: deepseek/deepseek-chat, gpt-4o, claude-3-5-sonnet"
                    style={{
                      width: "100%", border: "1px solid #d1d5db", borderRadius: 6,
                      padding: "7px 10px", fontSize: 13, boxSizing: "border-box",
                      outline: "none",
                    }}
                  />
                  <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 3 }}>
                    支持 litellm 格式，如 deepseek/deepseek-chat、openai/gpt-4o、anthropic/claude-3-5-sonnet-20241022
                  </div>
                </div>

                {/* 提示词编辑 */}
                <div>
                  <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 4 }}>
                    System Prompt（提示词）
                  </label>
                  <textarea
                    value={edit.system_prompt || ""}
                    onChange={(e) => updateField(agent.agent_type, "system_prompt", e.target.value)}
                    rows={10}
                    placeholder="输入 System Prompt..."
                    style={{
                      width: "100%", border: "1px solid #d1d5db", borderRadius: 6,
                      padding: "8px 10px", fontSize: 12, fontFamily: "monospace",
                      boxSizing: "border-box", resize: "vertical", outline: "none",
                      lineHeight: 1.6, color: "#1f2937",
                    }}
                  />
                  <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 3 }}>
                    字符数: {(edit.system_prompt || "").length}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
