"""Apply QE config summary panel + clone button to evolution page.tsx"""
import sys

FILE = r"F:\Dev\AIstock\frontend\src\app\quantevolver\evolution\page.tsx"
with open(FILE, "r", encoding="utf-8") as f:
    content = f.read()

checks = []

# === 1. Extend Task interface ===
old_task = """  task_type?: string;  // 'evolution' | 'strategy_evo'
  node_id?: string;
  created_at: string;
  updated_at: string;
}"""

new_task = """  task_type?: string;  // 'evolution' | 'strategy_evo'
  node_id?: string;
  created_at: string;
  updated_at: string;
  // Extended fields from SELECT *
  evolution_guidance?: string;
  evolution_mode?: string;
  strategy_id?: string;
  strategy_params?: Record<string, any>;
  execution_algo?: string;
  execution_algo_params?: Record<string, any>;
  unfilled_handler?: string;
  unfilled_handler_params?: Record<string, any>;
  label_type?: string;
  stock_pool?: string;
  factor_blacklist?: string[];
}"""

if old_task in content:
    content = content.replace(old_task, new_task, 1)
    checks.append("[OK] Task interface extended")
else:
    checks.append("[SKIP] Task interface already extended or not found")

# === 2. Add Copy to imports ===
old_import = '  Square, RotateCcw, Pause, XCircle, RefreshCw, Trash2'
new_import = '  Square, RotateCcw, Pause, XCircle, RefreshCw, Trash2, Copy'
if old_import in content and 'Copy' not in content.split('from "lucide-react"')[0]:
    content = content.replace(old_import, new_import, 1)
    checks.append("[OK] Copy import added")
else:
    checks.append("[SKIP] Copy import already exists or pattern not found")

# === 3. Add cloneFromTask + factorsExpanded states ===
old_state = "  const [strategyEvoLoops, setStrategyEvoLoops] = useState<any[]>([]);"
new_state = """  const [cloneFromTask, setCloneFromTask] = useState<Task | null>(null);
  const [factorsExpanded, setFactorsExpanded] = useState(false);
  const [strategyEvoLoops, setStrategyEvoLoops] = useState<any[]>([]);"""
if "cloneFromTask" not in content:
    content = content.replace(old_state, new_state, 1)
    checks.append("[OK] cloneFromTask + factorsExpanded states added")
else:
    checks.append("[SKIP] States already exist")

# === 4. Add useEffect for cloneFromTask ===
old_marker = """  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 获取任务详情和 Loops"""

new_marker = """  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 从 cloneFromTask 预填充新建任务弹窗
  useEffect(() => {
    if (showCreateTask && cloneFromTask) {
      const sp: Record<string, any> = cloneFromTask.strategy_params || {};
      setNewTask({
        task_name: cloneFromTask.task_name + "_副本",
        target_desc: cloneFromTask.target_desc || "",
        max_loops: cloneFromTask.max_loops,
        base_experiment_id: cloneFromTask.base_experiment_id || "",
        source_type: "qe_experiment",
        source_task_id: "",
        include_alpha_baseline: false,
        evolution_guidance: (cloneFromTask as any).evolution_guidance || "",
        evolution_mode: (cloneFromTask as any).evolution_mode || "auto",
        fork_from_task_id: "",
        fork_from_loop_index: -1,
        inherit_history: false,
        strategy_id: cloneFromTask.strategy_id || "",
        strategy_params: cloneFromTask.strategy_params || {},
        execution_algo: (cloneFromTask as any).execution_algo || "",
        execution_algo_params: (cloneFromTask as any).execution_algo_params || {},
        unfilled_handler: (cloneFromTask as any).unfilled_handler || "",
        enable_sector_hmm: !!sp.enable_sector_hmm,
        hmm_model_version_id: sp.hmm_model_version_id || "",
        hmm_signal_preset: sp.hmm_signal_preset || "preset_A",
        node_id: cloneFromTask.node_id || undefined,
        label_type: (cloneFromTask as any).label_type || "close",
      });
      if (cloneFromTask.node_id) setSelectedNodeId(cloneFromTask.node_id);
      if ((cloneFromTask as any).stock_pool) {
        setBlacklistEnabled(true);
        setStockPoolPath((cloneFromTask as any).stock_pool);
      }
      setCloneFromTask(null);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showCreateTask, cloneFromTask]);

  // 获取任务详情和 Loops"""

if "cloneFromTask 预填充" not in content:
    content = content.replace(old_marker, new_marker, 1)
    checks.append("[OK] cloneFromTask useEffect added")
else:
    checks.append("[SKIP] useEffect already exists")

# === 5. Add config summary IIFE in task map ===
old_tr = """                        </tr>
                      );
                    })}
                  </tbody>"""

new_tr = """                        </tr>
                        {/* 配置摘要面板 - 仅 active 任务展开 */}
                        {isActive && (() => {
                          const sp: Record<string, any> = (task as any).strategy_params || {};
                          const hmmOn = !!sp.enable_sector_hmm;
                          const sName = task.strategy_id
                            ? (strategyCatalog.find((s: any) => s.strategy_id === task.strategy_id)?.display_name || task.strategy_id)
                            : "默认";
                          const eName = (task as any).execution_algo || "收盘价成交";
                          const firstLoop = loops.find((l: Loop) => l.task_id === task.task_id);
                          const cfg: any = firstLoop?.config_json || {};
                          const fList: string[] = cfg.factor_list || [];
                          const mId: string = cfg.model_id || "";
                          const lblMap: Record<string, string> = { close: "收盘价", open: "开盘价", vwap: "VWAP" };
                          const modeMap: Record<string, string> = { auto: "自动", factor_only: "仅因子", model_only: "仅模型", joint: "联合" };
                          const CB = (l: string, v: string, c: string) => (
                            <span style={{ display: "inline-flex", alignItems: "center", gap: "4px", fontSize: "11px" }}>
                              <span style={{ color: "#94a3b8", fontWeight: 500 }}>{l}:</span>
                              <span style={{ fontSize: "10px", fontWeight: 700, padding: "1px 7px", borderRadius: "10px", backgroundColor: c + "18", color: c }}>{v}</span>
                            </span>
                          );
                          return (
                          <tr>
                            <td colSpan={6} style={{ padding: "0 16px 10px 16px", backgroundColor: "#eff6ff", borderBottom: "2px solid #bfdbfe" }}>
                              <div style={{ display: "flex", alignItems: "flex-start", gap: "12px", padding: "8px 12px", backgroundColor: "#f8faff", borderRadius: "8px", border: "1px solid #e0e7ff" }}>
                                <div style={{ flex: 1, minWidth: 0 }}>
                                  <div style={{ display: "flex", flexWrap: "wrap", gap: "8px", alignItems: "center", marginBottom: "6px" }}>
                                    <CB l="策略" v={sName} c="#3b82f6" />
                                    <CB l="执行" v={eName} c="#8b5cf6" />
                                    {mId && <CB l="模型" v={mId} c="#059669" />}
                                    <CB l="标签" v={lblMap[(task as any).label_type || "close"] || (task as any).label_type || "收盘价"} c="#d97706" />
                                    <CB l="演进" v={modeMap[(task as any).evolution_mode || "auto"] || (task as any).evolution_mode || "自动"} c="#6366f1" />
                                    <CB l="HMM" v={hmmOn ? `启用 (${sp.hmm_signal_preset || "preset_A"})` : "未启用"} c={hmmOn ? "#16a34a" : "#94a3b8"} />
                                    <CB l="行业黑名单" v={(task as any).stock_pool ? "已启用" : "未启用"} c={(task as any).stock_pool ? "#16a34a" : "#94a3b8"} />
                                    <CB l="节点" v={task.node_id ? (nodes.find((n: any) => n.node_id === task.node_id)?.display_name || task.node_id) : "本地默认"} c="#64748b" />
                                  </div>
                                  <div style={{ marginBottom: "4px" }}>
                                    <span style={{ fontSize: "11px", color: "#64748b", fontWeight: 600, marginRight: "6px" }}>因子 ({fList.length}):</span>
                                    {fList.length === 0 && <span style={{ fontSize: "11px", color: "#94a3b8" }}>加载中...</span>}
                                    {fList.length > 0 && (
                                      <>
                                        {fList.slice(0, factorsExpanded ? undefined : 8).map((f: string, i: number) => (
                                          <span key={i} style={{ display: "inline-block", fontSize: "10px", padding: "1px 6px", borderRadius: "4px", backgroundColor: "#e0f2fe", color: "#0369a1", marginRight: "3px", marginBottom: "2px" }}>{f}</span>
                                        ))}
                                        {fList.length > 8 && (
                                          <button onClick={() => setFactorsExpanded(!factorsExpanded)}
                                            style={{ fontSize: "10px", color: "#3b82f6", background: "none", border: "none", cursor: "pointer", fontWeight: 600 }}>
                                            {factorsExpanded ? "收起" : `+${fList.length - 8} 更多`}
                                          </button>
                                        )}
                                      </>
                                    )}
                                  </div>
                                  {(task.target_desc || (task as any).evolution_guidance) && (
                                    <div style={{ display: "flex", gap: "16px", fontSize: "11px", color: "#475569" }}>
                                      {task.target_desc && (
                                        <div style={{ flex: 1, minWidth: 0 }}>
                                          <span style={{ fontWeight: 600, color: "#64748b" }}>目标: </span>
                                          <span title={task.target_desc}>{task.target_desc.length > 80 ? task.target_desc.slice(0, 80) + "..." : task.target_desc}</span>
                                        </div>
                                      )}
                                      {(task as any).evolution_guidance && (
                                        <div style={{ flex: 1, minWidth: 0 }}>
                                          <span style={{ fontWeight: 600, color: "#64748b" }}>指引: </span>
                                          <span title={(task as any).evolution_guidance}>{(task as any).evolution_guidance.length > 80 ? (task as any).evolution_guidance.slice(0, 80) + "..." : (task as any).evolution_guidance}</span>
                                        </div>
                                      )}
                                    </div>
                                  )}
                                  {(task as any).factor_blacklist && (task as any).factor_blacklist.length > 0 && (
                                    <div style={{ marginTop: "4px" }}>
                                      <span style={{ fontSize: "11px", color: "#ef4444", fontWeight: 600, marginRight: "6px" }}>因子黑名单:</span>
                                      {(task as any).factor_blacklist.map((f: string, i: number) => (
                                        <span key={i} style={{ display: "inline-block", fontSize: "10px", padding: "1px 6px", borderRadius: "4px", backgroundColor: "#fef2f2", color: "#dc2626", marginRight: "3px", textDecoration: "line-through" }}>{f}</span>
                                      ))}
                                    </div>
                                  )}
                                </div>
                                <button
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    setCloneFromTask(task);
                                    setShowCreateTask(true);
                                    fetchSourceExperiments();
                                    fetchSourceTasks();
                                    fetchNodes();
                                  }}
                                  title="基于此配置新建演进任务"
                                  style={{
                                    flexShrink: 0, display: "flex", alignItems: "center", gap: "4px",
                                    padding: "6px 12px", borderRadius: "6px", border: "1px solid #3b82f6",
                                    backgroundColor: "#eff6ff", color: "#2563eb", fontSize: "11px",
                                    fontWeight: 600, cursor: "pointer", whiteSpace: "nowrap",
                                  }}
                                >
                                  <Copy size={12} /> 基于此配置新建
                                </button>
                              </div>
                            </td>
                          </tr>
                          );
                        })()}
                      </tr>
                      );
                    })}
                  </tbody>"""

if "配置摘要面板" not in content:
    content = content.replace(old_tr, new_tr, 1)
    checks.append("[OK] Config summary IIFE + clone button added")
else:
    checks.append("[SKIP] Config summary already exists")

with open(FILE, "w", encoding="utf-8") as f:
    f.write(content)

for c in checks:
    print(c)
print(f"\nTotal lines: {content.count(chr(10)) + 1}")
