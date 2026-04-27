"use client";

import React, { useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Node = {
  node_id: string;
  display_name: string;
  status: string;
  capabilities: string[];
};

const TASK_TYPES = [
  { value: "fin_factor", label: "因子研发", short: "F" },
  { value: "fin_model", label: "模型研发", short: "M" },
  { value: "fin_quant", label: "联合演进", short: "Q" },
  { value: "fin_factor_report", label: "研报因子", short: "R" },
  { value: "qe_evolution", label: "QE 自动演进", short: "QE" },
];

export default function TaskCreatePanel({
  nodes,
  onCreated,
}: {
  nodes: Node[];
  onCreated: () => void;
}) {
  const [taskName, setTaskName] = useState("");
  const [taskType, setTaskType] = useState("fin_factor");
  const [nodeId, setNodeId] = useState("");
  const [evolvingN, setEvolvingN] = useState(10);
  const [allDuration, setAllDuration] = useState("");
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [actionSelection, setActionSelection] = useState("llm");
  const [costeerMaxLoop, setCosteerMaxLoop] = useState(5);
  const [multiProcN, setMultiProcN] = useState(1);
  const [appTpl, setAppTpl] = useState("../app_tpl/all/v4/rdagent");
  const [customEnvText, setCustomEnvText] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");

  // QE 演进参数
  const [qeSourceTaskId, setQeSourceTaskId] = useState("");
  const [qeSourceType, setQeSourceType] = useState("qe_experiment");
  const [qeTargetDesc, setQeTargetDesc] = useState("");
  const [qeMaxLoops, setQeMaxLoops] = useState(10);
  const [qeEvolutionMode, setQeEvolutionMode] = useState("auto");

  const isQE = taskType === "qe_evolution";

  // 默认选第一个在线节点
  React.useEffect(() => {
    if (!nodeId && nodes.length > 0) {
      const online = nodes.find(n => n.status === "online");
      setNodeId(online?.node_id || nodes[0].node_id);
    }
  }, [nodes, nodeId]);

  const handleSubmit = async () => {
    if (!taskName.trim()) { setError("请输入任务名称"); return; }
    if (!nodeId) { setError("请选择目标节点"); return; }
    if (isQE && !qeSourceTaskId.trim()) { setError("QE 任务必须指定来源实验 ID"); return; }
    setSubmitting(true);
    setError("");
    try {
      const body: Record<string, any> = {
        task_name: taskName.trim(),
        task_type: taskType,
        node_id: nodeId,
      };

      if (isQE) {
        body.qe_config = {
          source_task_id: qeSourceTaskId.trim(),
          source_type: qeSourceType,
          target_desc: qeTargetDesc.trim(),
          max_loops: qeMaxLoops,
          evolution_mode: qeEvolutionMode,
        };
      } else {
        body.evolving_n = evolvingN;
        body.action_selection = actionSelection;
        body.costeer_max_loop = costeerMaxLoop;
        body.multi_proc_n = multiProcN;
        body.app_tpl = appTpl.trim();
        if (allDuration) body.all_duration = allDuration;
        if (customEnvText.trim()) {
          try {
            const parsedEnv = JSON.parse(customEnvText);
            if (!parsedEnv || Array.isArray(parsedEnv) || typeof parsedEnv !== "object") {
              throw new Error("custom_env must be a JSON object");
            }
            body.custom_env = parsedEnv;
          } catch (envErr: any) {
            throw new Error(`Custom Env JSON invalid: ${envErr?.message || envErr}`);
          }
        }
      }

      const res = await fetch(`${API_BASE}/dispatch/tasks`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail || `HTTP ${res.status}`);
      }
      setTaskName("");
      onCreated();
    } catch (e: any) {
      setError(e.message || "创建失败");
    }
    setSubmitting(false);
  };

  const selectStyle = {
    padding: "8px 12px", fontSize: 13, borderRadius: 6,
    border: "1px solid #d1d5db", outline: "none", background: "#fff",
  };
  const inputStyle = { ...selectStyle, width: "100%" };

  return (
    <div style={{
      background: "#fff", borderRadius: 12, padding: 20, marginBottom: 20,
      border: "1px solid #e5e7eb",
    }}>
      <h3 style={{ fontSize: 15, fontWeight: 600, margin: "0 0 12px", color: "#1f2937" }}>
        创建任务
      </h3>

      <div style={{ display: "flex", gap: 12, marginBottom: 12, flexWrap: "wrap" }}>
        {/* 任务名称 */}
        <div style={{ flex: 2, minWidth: 200 }}>
          <input
            style={inputStyle}
            placeholder="任务名称"
            value={taskName}
            onChange={e => setTaskName(e.target.value)}
          />
        </div>

        {/* 任务类型 */}
        <div style={{ flex: 1, minWidth: 140 }}>
          <select
            style={{ ...selectStyle, width: "100%" }}
            value={taskType}
            onChange={e => setTaskType(e.target.value)}
          >
            {TASK_TYPES.map(t => (
              <option key={t.value} value={t.value}>{t.label}</option>
            ))}
          </select>
        </div>

        {/* 目标节点 */}
        <div style={{ flex: 1, minWidth: 140 }}>
          <select
            style={{ ...selectStyle, width: "100%" }}
            value={nodeId}
            onChange={e => setNodeId(e.target.value)}
          >
            <option value="">选择节点</option>
            {nodes.map(n => (
              <option key={n.node_id} value={n.node_id}>
                {n.display_name} ({n.status})
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* RDAgent 基本参数 */}
      {!isQE && (
        <div style={{ display: "flex", gap: 12, marginBottom: 12, alignItems: "center" }}>
          <div>
            <label style={{ fontSize: 11, color: "#6b7280" }}>循环数</label>
            <input
              style={{ ...selectStyle, width: 80 }}
              type="number"
              min={1}
              max={100}
              value={evolvingN}
              onChange={e => setEvolvingN(parseInt(e.target.value) || 10)}
            />
          </div>
          <div>
            <label style={{ fontSize: 11, color: "#6b7280" }}>时间限制</label>
            <input
              style={{ ...selectStyle, width: 100 }}
              placeholder="HH:MM:SS"
              value={allDuration}
              onChange={e => setAllDuration(e.target.value)}
            />
          </div>
          <button
            onClick={() => setShowAdvanced(!showAdvanced)}
            style={{
              padding: "6px 12px", fontSize: 12, borderRadius: 6,
              border: "1px solid #d1d5db", background: "#f9fafb", cursor: "pointer",
              color: "#6b7280", marginTop: 14,
            }}
          >
            {showAdvanced ? "收起" : "高级配置"}
          </button>
        </div>
      )}

      {/* RDAgent 高级配置 */}
      {!isQE && showAdvanced && (
        <div style={{
          display: "flex", gap: 12, marginBottom: 12, padding: 12,
          background: "#f9fafb", borderRadius: 8, flexWrap: "wrap",
        }}>
          <div>
            <label style={{ fontSize: 11, color: "#6b7280" }}>策略选择</label>
            <select
              style={selectStyle}
              value={actionSelection}
              onChange={e => setActionSelection(e.target.value)}
            >
              <option value="llm">LLM</option>
              <option value="bandit">Bandit</option>
              <option value="random">Random</option>
            </select>
          </div>
          <div>
            <label style={{ fontSize: 11, color: "#6b7280" }}>CoSTEER MaxLoop</label>
            <input
              style={{ ...selectStyle, width: 80 }}
              type="number"
              min={1}
              max={20}
              value={costeerMaxLoop}
              onChange={e => setCosteerMaxLoop(parseInt(e.target.value) || 5)}
            />
          </div>
          <div>
            <label style={{ fontSize: 11, color: "#6b7280" }}>Multi Proc</label>
            <input
              style={{ ...selectStyle, width: 80 }}
              type="number"
              min={1}
              max={16}
              value={multiProcN}
              onChange={e => setMultiProcN(parseInt(e.target.value) || 1)}
            />
          </div>
          <div style={{ flex: "1 1 360px", minWidth: 280 }}>
            <label style={{ fontSize: 11, color: "#6b7280", display: "block" }}>RD-Agent app_tpl</label>
            <div style={{ display: "flex", gap: 8 }}>
              <input
                style={{ ...inputStyle, flex: 1 }}
                value={appTpl}
                onChange={e => setAppTpl(e.target.value)}
                placeholder="../app_tpl/all/v4/rdagent"
              />
              <button
                type="button"
                onClick={() => setAppTpl("../app_tpl/all/v25/rdagent")}
                style={{
                  padding: "8px 10px", fontSize: 12, borderRadius: 6,
                  border: "1px solid #d1d5db", background: "#fff", cursor: "pointer",
                }}
              >
                V25
              </button>
            </div>
            <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 4 }}>
              V25 requires the target node to have app_tpl/all/v25 deployed.
            </div>
          </div>
          <div style={{ flex: "1 1 100%", minWidth: 280 }}>
            <label style={{ fontSize: 11, color: "#6b7280", display: "block" }}>
              Custom Env JSON
            </label>
            <textarea
              style={{ ...inputStyle, minHeight: 54, fontFamily: "monospace", resize: "vertical" }}
              value={customEnvText}
              onChange={e => setCustomEnvText(e.target.value)}
              placeholder='{"V25_DEVICE":"cuda"}'
            />
          </div>
        </div>
      )}

      {/* QE 演进配置 */}
      {isQE && (
        <div style={{
          display: "flex", flexDirection: "column", gap: 10, marginBottom: 12, padding: 12,
          background: "#faf5ff", borderRadius: 8, border: "1px solid #e9d5ff",
        }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: "#6d28d9", marginBottom: 2 }}>
            QE 演进配置
          </div>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            <div style={{ flex: 2, minWidth: 200 }}>
              <label style={{ fontSize: 11, color: "#6b7280", display: "block", marginBottom: 2 }}>
                来源实验 ID *
              </label>
              <input
                style={inputStyle}
                placeholder="base_experiment_id (必填)"
                value={qeSourceTaskId}
                onChange={e => setQeSourceTaskId(e.target.value)}
              />
            </div>
            <div style={{ flex: 1, minWidth: 140 }}>
              <label style={{ fontSize: 11, color: "#6b7280", display: "block", marginBottom: 2 }}>
                来源类型
              </label>
              <select
                style={{ ...selectStyle, width: "100%" }}
                value={qeSourceType}
                onChange={e => setQeSourceType(e.target.value)}
              >
                <option value="qe_experiment">QE 实验</option>
                <option value="rdagent_task_sota">RDAgent SOTA</option>
              </select>
            </div>
            <div style={{ flex: 1, minWidth: 80 }}>
              <label style={{ fontSize: 11, color: "#6b7280", display: "block", marginBottom: 2 }}>
                最大循环数
              </label>
              <input
                style={{ ...selectStyle, width: 80 }}
                type="number"
                min={1}
                max={100}
                value={qeMaxLoops}
                onChange={e => setQeMaxLoops(parseInt(e.target.value) || 10)}
              />
            </div>
          </div>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            <div style={{ flex: 1, minWidth: 140 }}>
              <label style={{ fontSize: 11, color: "#6b7280", display: "block", marginBottom: 2 }}>
                演进模式
              </label>
              <select
                style={{ ...selectStyle, width: "100%" }}
                value={qeEvolutionMode}
                onChange={e => setQeEvolutionMode(e.target.value)}
              >
                <option value="auto">自动</option>
                <option value="factor_only">仅因子</option>
                <option value="model_only">仅模型</option>
                <option value="joint">联合演进</option>
              </select>
            </div>
            <div style={{ flex: 2, minWidth: 200 }}>
              <label style={{ fontSize: 11, color: "#6b7280", display: "block", marginBottom: 2 }}>
                目标描述 (可选)
              </label>
              <input
                style={inputStyle}
                placeholder="演进目标描述"
                value={qeTargetDesc}
                onChange={e => setQeTargetDesc(e.target.value)}
              />
            </div>
          </div>
        </div>
      )}

      {error && (
        <div style={{ color: "#ef4444", fontSize: 13, marginBottom: 8 }}>{error}</div>
      )}

      <button
        onClick={handleSubmit}
        disabled={submitting}
        style={{
          padding: "8px 24px", fontSize: 14, fontWeight: 600, borderRadius: 8,
          border: "none", background: "#6366f1", color: "#fff", cursor: "pointer",
          opacity: submitting ? 0.6 : 1,
        }}
      >
        {submitting ? "创建中..." : "创建任务"}
      </button>
    </div>
  );
}
