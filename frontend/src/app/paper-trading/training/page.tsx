"use client";

import { useState, useEffect, useRef, useCallback } from "react";

const API = "http://127.0.0.1:8001/api/v1";

interface TrainingJob {
  job_id: string;
  status: string;
  signal_source?: string;
  signal_source_id?: string;
  best_epoch?: number;
  best_valid_loss?: number;
  valid_ic?: number;
  valid_icir?: number;
  started_at?: string;
  completed_at?: string;
  error_message?: string;
}

interface SourceOption {
  id: string;
  label: string;
}

export default function TrainingPage() {
  // 配置
  const [signalSource, setSignalSource] = useState("rdagent_task");
  const [sourceId, setSourceId] = useState("");
  const [manualInput, setManualInput] = useState(false);
  const [loopId, setLoopId] = useState("");
  const [trainStart, setTrainStart] = useState("2021-01-01");
  const [trainEnd, setTrainEnd] = useState("2025-06-30");
  const [validStart, setValidStart] = useState("2025-07-01");
  const [validEnd, setValidEnd] = useState("2026-03-10");
  const [nEpochs, setNEpochs] = useState(20);
  const [batchSize, setBatchSize] = useState(16384);
  const [earlyStop, setEarlyStop] = useState(5);
  const [lr, setLr] = useState(0.001);

  // 来源列表
  const [rdagentTasks, setRdagentTasks] = useState<SourceOption[]>([]);
  const [qeExperiments, setQeExperiments] = useState<SourceOption[]>([]);
  const [qeEvolutions, setQeEvolutions] = useState<SourceOption[]>([]);
  const [loadingOptions, setLoadingOptions] = useState(false);

  // 状态
  const [status, setStatus] = useState<TrainingJob | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [starting, setStarting] = useState(false);
  const [history, setHistory] = useState<TrainingJob[]>([]);

  const logEndRef = useRef<HTMLDivElement>(null);
  const esRef = useRef<EventSource | null>(null);

  // 加载来源列表
  useEffect(() => {
    setLoadingOptions(true);
    Promise.all([
      fetch(`${API}/rdagent/tasks/local`)
        .then((r) => r.json())
        .then((d) => {
          const items: any[] = d.items || d || [];
          setRdagentTasks(items.map((t: any) => ({ id: t.task_id || t.task_run_id || "", label: t.task_id || t.task_run_id || "unknown" })));
        })
        .catch(() => {}),
      fetch(`${API}/quantevolver/experiments?limit=200`)
        .then((r) => r.json())
        .then((d) => {
          const items: any[] = d.items || d || [];
          setQeExperiments(items.map((e: any) => ({ id: e.experiment_id || "", label: e.experiment_name ? `${e.experiment_id} — ${e.experiment_name}` : e.experiment_id || "unknown" })));
        })
        .catch(() => {}),
      fetch(`${API}/quantevolver/evolution/tasks`)
        .then((r) => r.json())
        .then((d) => {
          const items: any[] = d.data || d.items || d || [];
          setQeEvolutions(items.map((t: any) => ({ id: t.task_id || "", label: t.task_name ? `${t.task_id} — ${t.task_name}` : t.task_id || "unknown" })));
        })
        .catch(() => {}),
    ]).finally(() => setLoadingOptions(false));
  }, []);

  useEffect(() => {
    setSourceId("");
    setLoopId("");
    setManualInput(false);
  }, [signalSource]);

  const currentOptions = signalSource === "rdagent_task" ? rdagentTasks : signalSource === "qe_experiment" ? qeExperiments : qeEvolutions;

  // 自动滚动日志
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs]);

  // 加载历史
  useEffect(() => {
    fetch(`${API}/paper-trading/training/history`)
      .then((r) => r.json())
      .then(setHistory)
      .catch(() => {});
  }, [status]);

  // 检查当前状态
  useEffect(() => {
    fetch(`${API}/paper-trading/training/status`)
      .then((r) => r.json())
      .then((d) => {
        if (d.status && d.status !== "idle") {
          setStatus(d);
          if (d.job_id && d.status === "running") {
            connectSSE(d.job_id);
          }
        }
      })
      .catch(() => {});
    return () => esRef.current?.close();
  }, []);

  const connectSSE = useCallback((jobId: string) => {
    esRef.current?.close();
    const es = new EventSource(`${API}/paper-trading/training/${jobId}/logs`);
    esRef.current = es;

    es.addEventListener("log", (e) => {
      setLogs((prev) => [...prev, (e as MessageEvent).data]);
    });
    es.addEventListener("done", () => {
      es.close();
      refreshStatus();
    });
    es.addEventListener("error", () => {
      es.close();
      refreshStatus();
    });
    es.onerror = () => {
      es.close();
    };
  }, []);

  const refreshStatus = () => {
    fetch(`${API}/paper-trading/training/status`)
      .then((r) => r.json())
      .then(setStatus)
      .catch(() => {});
  };

  const startTraining = async () => {
    setStarting(true);
    setLogs([]);
    setStatus(null);
    try {
      const body: any = {
        signal_source: signalSource,
        signal_source_id: sourceId,
        train_start: trainStart,
        train_end: trainEnd,
        valid_start: validStart,
        valid_end: validEnd,
        n_epochs: nEpochs,
        batch_size: batchSize,
        early_stop: earlyStop,
        lr: lr,
      };
      if (signalSource === "rdagent_task" && loopId) {
        body.signal_loop_id = parseInt(loopId);
      }
      const resp = await fetch(`${API}/paper-trading/training/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!resp.ok) throw new Error(await resp.text());
      const data = await resp.json();
      setStatus({ job_id: data.job_id, status: "running" });
      connectSSE(data.job_id);
    } catch (e: any) {
      alert("启动失败: " + (e.message || e));
    } finally {
      setStarting(false);
    }
  };

  const cancelTraining = async () => {
    if (!status?.job_id) return;
    try {
      await fetch(`${API}/paper-trading/training/${status.job_id}/cancel`, { method: "POST" });
      esRef.current?.close();
      refreshStatus();
    } catch (e: any) {
      alert("取消失败: " + (e.message || e));
    }
  };

  const isRunning = status?.status === "running";
  const sourceLabel = signalSource === "rdagent_task" ? "Task Run ID" : signalSource === "qe_experiment" ? "Experiment ID" : "Evolution Task ID";

  return (
    <div>
      {/* 配置区 */}
      <div style={cardStyle}>
        <h3 style={h3Style}>选择组合</h3>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16 }}>
          <div>
            <label style={labelStyle}>信号来源</label>
            <select value={signalSource} onChange={(e) => setSignalSource(e.target.value)} style={inputStyle}>
              <option value="rdagent_task">RDAgent Task</option>
              <option value="qe_experiment">QE 单次实验</option>
              <option value="qe_evolution">QE 演进 SOTA</option>
            </select>
          </div>
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <label style={{ ...labelStyle, marginBottom: 0 }}>{sourceLabel}</label>
              <button onClick={() => { setManualInput(!manualInput); setSourceId(""); }} style={toggleBtnStyle}>
                {manualInput ? "切换下拉" : "手工输入"}
              </button>
            </div>
            <div style={{ marginTop: 4 }}>
              {manualInput ? (
                <input value={sourceId} onChange={(e) => setSourceId(e.target.value)} style={inputStyle} placeholder="手工输入 ID" />
              ) : (
                <select value={sourceId} onChange={(e) => setSourceId(e.target.value)} style={inputStyle} disabled={loadingOptions}>
                  <option value="">{loadingOptions ? "加载中..." : currentOptions.length === 0 ? "暂无可用任务" : "-- 请选择 --"}</option>
                  {currentOptions.map((opt) => (<option key={opt.id} value={opt.id}>{opt.label}</option>))}
                </select>
              )}
            </div>
          </div>
          {signalSource === "rdagent_task" && (
            <div>
              <label style={labelStyle}>Loop ID</label>
              <input value={loopId} onChange={(e) => setLoopId(e.target.value)} type="number" style={inputStyle} placeholder="留空=最新SOTA" />
            </div>
          )}
        </div>

        <h3 style={{ ...h3Style, marginTop: 20 }}>重训练配置</h3>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 16 }}>
          <div>
            <label style={labelStyle}>训练起始</label>
            <input value={trainStart} onChange={(e) => setTrainStart(e.target.value)} type="date" style={inputStyle} />
          </div>
          <div>
            <label style={labelStyle}>训练截止</label>
            <input value={trainEnd} onChange={(e) => setTrainEnd(e.target.value)} type="date" style={inputStyle} />
          </div>
          <div>
            <label style={labelStyle}>验证起始</label>
            <input value={validStart} onChange={(e) => setValidStart(e.target.value)} type="date" style={inputStyle} />
          </div>
          <div>
            <label style={labelStyle}>验证截止 (T-2)</label>
            <input value={validEnd} onChange={(e) => setValidEnd(e.target.value)} type="date" style={inputStyle} />
          </div>
        </div>

        <h3 style={{ ...h3Style, marginTop: 20 }}>高级参数</h3>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 16 }}>
          <div>
            <label style={labelStyle}>Epochs</label>
            <input value={nEpochs} onChange={(e) => setNEpochs(Number(e.target.value))} type="number" style={inputStyle} />
          </div>
          <div>
            <label style={labelStyle}>Batch Size</label>
            <input value={batchSize} onChange={(e) => setBatchSize(Number(e.target.value))} type="number" style={inputStyle} />
          </div>
          <div>
            <label style={labelStyle}>Early Stop</label>
            <input value={earlyStop} onChange={(e) => setEarlyStop(Number(e.target.value))} type="number" style={inputStyle} />
          </div>
          <div>
            <label style={labelStyle}>Learning Rate</label>
            <input value={lr} onChange={(e) => setLr(Number(e.target.value))} type="number" step="0.0001" style={inputStyle} />
          </div>
        </div>

        <div style={{ marginTop: 20, display: "flex", gap: 12 }}>
          <button
            onClick={startTraining}
            disabled={starting || isRunning || !sourceId}
            style={{ ...btnStyle, opacity: starting || isRunning || !sourceId ? 0.5 : 1 }}
          >
            {starting ? "启动中..." : isRunning ? "训练中..." : "开始训练"}
          </button>
          {isRunning && (
            <button onClick={cancelTraining} style={btnDangerStyle}>取消训练</button>
          )}
        </div>
      </div>

      {/* 训练状态 */}
      {status && status.status !== "idle" && (
        <div style={cardStyle}>
          <h3 style={{ margin: "0 0 12px", fontSize: 15, fontWeight: 600 }}>
            训练进度 — {status.job_id}
            <span style={{
              marginLeft: 12, fontSize: 12, padding: "2px 8px", borderRadius: 4,
              background: status.status === "running" ? "#dbeafe" : status.status === "completed" ? "#dcfce7" : "#fef2f2",
              color: status.status === "running" ? "#1d4ed8" : status.status === "completed" ? "#16a34a" : "#dc2626",
            }}>
              {status.status}
            </span>
          </h3>
          {status.best_epoch != null && (
            <div style={{ fontSize: 13, color: "#374151", marginBottom: 8 }}>
              Best Epoch: <b>{status.best_epoch}</b> | Valid Loss: <b>{status.best_valid_loss?.toFixed(4)}</b>
              {status.valid_ic != null && <> | IC: <b>{status.valid_ic.toFixed(4)}</b></>}
            </div>
          )}
        </div>
      )}

      {/* 实时日志 */}
      {logs.length > 0 && (
        <div style={cardStyle}>
          <h3 style={{ margin: "0 0 8px", fontSize: 15, fontWeight: 600 }}>实时日志</h3>
          <div style={{
            background: "#111827", color: "#d1d5db", fontFamily: "monospace",
            fontSize: 12, padding: 12, borderRadius: 6, maxHeight: 400,
            overflowY: "auto", lineHeight: 1.6,
          }}>
            {logs.map((l, i) => (
              <div key={i}>{l}</div>
            ))}
            <div ref={logEndRef} />
          </div>
        </div>
      )}

      {/* 训练历史 */}
      {history.length > 0 && (
        <div style={cardStyle}>
          <h3 style={{ margin: "0 0 12px", fontSize: 15, fontWeight: 600 }}>训练历史</h3>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ background: "#f9fafb" }}>
                <th style={thStyle}>Job ID</th>
                <th style={thStyle}>来源</th>
                <th style={thStyle}>状态</th>
                <th style={thStyle}>Best Epoch</th>
                <th style={thStyle}>Valid Loss</th>
                <th style={thStyle}>开始时间</th>
              </tr>
            </thead>
            <tbody>
              {history.map((j) => (
                <tr key={j.job_id}>
                  <td style={tdStyle}><code>{j.job_id}</code></td>
                  <td style={tdStyle}>{j.signal_source} / {j.signal_source_id}</td>
                  <td style={tdStyle}>
                    <span style={{
                      fontSize: 11, padding: "1px 6px", borderRadius: 3,
                      background: j.status === "completed" ? "#dcfce7" : j.status === "running" ? "#dbeafe" : "#fef2f2",
                    }}>
                      {j.status}
                    </span>
                  </td>
                  <td style={tdStyle}>{j.best_epoch ?? "-"}</td>
                  <td style={tdStyle}>{j.best_valid_loss?.toFixed(4) ?? "-"}</td>
                  <td style={tdStyle}>{j.started_at ? new Date(j.started_at).toLocaleString() : "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

const cardStyle: React.CSSProperties = { background: "#fff", borderRadius: 12, padding: 20, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" };
const h3Style: React.CSSProperties = { margin: "0 0 12px", fontSize: 15, fontWeight: 600 };
const labelStyle: React.CSSProperties = { display: "block", fontSize: 12, fontWeight: 500, color: "#6b7280", marginBottom: 4 };
const inputStyle: React.CSSProperties = { width: "100%", padding: "6px 10px", border: "1px solid #d1d5db", borderRadius: 6, fontSize: 13, outline: "none" };
const btnStyle: React.CSSProperties = { padding: "8px 20px", background: "linear-gradient(135deg, #2563eb, #1d4ed8)", color: "#fff", border: "none", borderRadius: 6, fontSize: 13, fontWeight: 600, cursor: "pointer" };
const btnDangerStyle: React.CSSProperties = { padding: "8px 20px", background: "#dc2626", color: "#fff", border: "none", borderRadius: 6, fontSize: 13, fontWeight: 600, cursor: "pointer" };
const toggleBtnStyle: React.CSSProperties = { fontSize: 11, color: "#2563eb", background: "none", border: "none", cursor: "pointer", padding: 0 };
const thStyle: React.CSSProperties = { padding: "8px 12px", textAlign: "left", fontSize: 12, fontWeight: 600, borderBottom: "1px solid #e5e7eb" };
const tdStyle: React.CSSProperties = { padding: "8px 12px", borderBottom: "1px solid #f3f4f6" };
