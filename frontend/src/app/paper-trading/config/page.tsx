"use client";

import { useState, useEffect, useCallback } from "react";

const API = "http://127.0.0.1:8001/api/v1";

interface Portfolio {
  id: number;
  portfolio_name: string;
  signal_source: string;
  signal_source_id: string;
  signal_loop_id?: number;
  model_source: string;
  initial_capital: number;
  max_positions: number;
  max_position_pct?: number;
  max_turnover_pct?: number;
  benchmark?: string;
  auto_run?: boolean;
  execute_time?: string;
  enable_factor_attribution?: boolean;
  enable_live_ic?: boolean;
  fee_config?: any;
  enable_intraday?: boolean;
  intraday_exec_mode?: string;
  intraday_strategy?: string;
  intraday_config?: any;
  intraday_freq?: string;
  status: string;
  start_date?: string;
  created_at: string;
}

interface FeeFields {
  commission_rate: string;
  stamp_tax_rate: string;
  transfer_fee_rate: string;
  slippage: string;
  min_commission: string;
}

const DEFAULT_FEES: FeeFields = {
  commission_rate: "0.0003",
  stamp_tax_rate: "0.0005",
  transfer_fee_rate: "0.00002",
  slippage: "0.001",
  min_commission: "5",
};

const FEE_LABELS: Record<keyof FeeFields, string> = {
  commission_rate: "佣金费率",
  stamp_tax_rate: "印花税率",
  transfer_fee_rate: "过户费率",
  slippage: "滑点",
  min_commission: "最低佣金(元)",
};

const FEE_HINTS: Record<keyof FeeFields, string> = {
  commission_rate: "万三 = 0.0003",
  stamp_tax_rate: "万五 = 0.0005",
  transfer_fee_rate: "万0.2 = 0.00002",
  slippage: "千一 = 0.001",
  min_commission: "一般 5 元",
};

const FEE_MAX: Record<keyof FeeFields, number> = {
  commission_rate: 0.01,
  stamp_tax_rate: 0.01,
  transfer_fee_rate: 0.001,
  slippage: 0.05,
  min_commission: 100,
};

interface SourceOption {
  id: string;
  label: string;
}

interface ExecAlgo {
  algo_code: string;
  algo_name: string;
  category: string;
  description: string;
  param_schema: any;
  is_enabled: boolean;
}

const FREQ_OPTIONS = [
  { value: "1m", label: "1 分钟" },
  { value: "5m", label: "5 分钟" },
  { value: "15m", label: "15 分钟" },
  { value: "30m", label: "30 分钟" },
];

const EMPTY_FORM = {
  portfolio_name: "",
  signal_source: "rdagent_task",
  signal_source_id: "",
  signal_loop_id: "",
  model_source: "original",
  initial_capital: 1000000,
  max_positions: 20,
  max_position_pct: 0.1,
  max_turnover_pct: 0.3,
  benchmark: "000300.SH",
  auto_run: true,
  execute_time: "17:30",
  enable_factor_attribution: true,
  enable_live_ic: true,
  enable_intraday: false,
  intraday_exec_mode: "replay",
  intraday_strategy: "CLOSE_PRICE",
  intraday_config: {} as Record<string, any>,
  intraday_freq: "5m",
  start_date: "",
};

/* ---------- 手续费校验 ---------- */
function validateFee(key: keyof FeeFields, value: string): string | null {
  if (value.trim() === "") return "不能为空";
  const n = Number(value);
  if (isNaN(n)) return "必须为数字";
  if (n < 0) return "不能为负数";
  if (n > FEE_MAX[key]) return `不能超过 ${FEE_MAX[key]}`;
  return null;
}

function feeFieldsToConfig(fields: FeeFields): { default_fees: Record<string, number>; custom_fees: Record<string, never> } {
  return {
    default_fees: {
      commission_rate: Number(fields.commission_rate),
      stamp_tax_rate: Number(fields.stamp_tax_rate),
      transfer_fee_rate: Number(fields.transfer_fee_rate),
      slippage: Number(fields.slippage),
      min_commission: Number(fields.min_commission),
    },
    custom_fees: {},
  };
}

function configToFeeFields(config: any): FeeFields {
  const df = config?.default_fees || {};
  return {
    commission_rate: String(df.commission_rate ?? DEFAULT_FEES.commission_rate),
    stamp_tax_rate: String(df.stamp_tax_rate ?? DEFAULT_FEES.stamp_tax_rate),
    transfer_fee_rate: String(df.transfer_fee_rate ?? DEFAULT_FEES.transfer_fee_rate),
    slippage: String(df.slippage ?? DEFAULT_FEES.slippage),
    min_commission: String(df.min_commission ?? DEFAULT_FEES.min_commission),
  };
}

function validateAllFees(fields: FeeFields): Record<string, string> {
  const errors: Record<string, string> = {};
  for (const k of Object.keys(fields) as (keyof FeeFields)[]) {
    const err = validateFee(k, fields[k]);
    if (err) errors[k] = err;
  }
  return errors;
}

/* ---------- 手续费编辑组件 ---------- */
function FeeEditor({ fees, onChange, errors }: {
  fees: FeeFields;
  onChange: (fees: FeeFields) => void;
  errors: Record<string, string>;
}) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10 }}>
      {(Object.keys(FEE_LABELS) as (keyof FeeFields)[]).map((k) => (
        <div key={k}>
          <label style={labelStyle}>{FEE_LABELS[k]}</label>
          <input
            value={fees[k]}
            onChange={(e) => onChange({ ...fees, [k]: e.target.value })}
            style={{
              ...inputStyle,
              borderColor: errors[k] ? "#ef4444" : "#d1d5db",
            }}
            placeholder={FEE_HINTS[k]}
          />
          {errors[k] ? (
            <div style={{ fontSize: 11, color: "#ef4444", marginTop: 2 }}>{errors[k]}</div>
          ) : (
            <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 2 }}>{FEE_HINTS[k]}</div>
          )}
        </div>
      ))}
    </div>
  );
}

/* ---------- 执行时间校验 ---------- */
function validateExecuteTime(v: string): string | null {
  if (!/^\d{1,2}:\d{2}$/.test(v)) return "格式 HH:MM";
  const [h, m] = v.split(":").map(Number);
  if (h < 0 || h > 23 || m < 0 || m > 59) return "无效时间";
  return null;
}

export default function ConfigPage() {
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [showCreate, setShowCreate] = useState(false);
  const [loading, setLoading] = useState(false);

  // 编辑状态
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editForm, setEditForm] = useState<Record<string, any>>({});
  const [editFees, setEditFees] = useState<FeeFields>({ ...DEFAULT_FEES });
  const [saving, setSaving] = useState(false);

  // 新建表单
  const [form, setForm] = useState({ ...EMPTY_FORM });
  const [fees, setFees] = useState<FeeFields>({ ...DEFAULT_FEES });
  const [manualInput, setManualInput] = useState(false);

  // 来源列表
  const [rdagentTasks, setRdagentTasks] = useState<SourceOption[]>([]);
  const [qeExperiments, setQeExperiments] = useState<SourceOption[]>([]);
  const [qeEvolutions, setQeEvolutions] = useState<SourceOption[]>([]);
  const [loadingOptions, setLoadingOptions] = useState(false);

  // 执行算法列表
  const [execAlgos, setExecAlgos] = useState<ExecAlgo[]>([]);

  // caught_up 配置
  const [caughtUpConfig, setCaughtUpConfig] = useState<Record<number, { execute_time: string; enable_intraday: boolean; intraday_exec_mode: string }>>({});

  const loadPortfolios = useCallback(async () => {
    try {
      const resp = await fetch(`${API}/paper-trading/portfolios`);
      const data = await resp.json();
      setPortfolios(data);
    } catch (e) {
      console.error(e);
    }
  }, []);

  useEffect(() => {
    loadPortfolios();
  }, [loadPortfolios]);

  // 加载来源列表
  useEffect(() => {
    setLoadingOptions(true);
    Promise.all([
      fetch(`${API}/rdagent/tasks/local-with-metrics`)
        .then((r) => r.json())
        .then((d) => {
          const items: any[] = d.items || d || [];
          setRdagentTasks(
            items.map((t: any) => {
              const metrics = t.best_ann_return != null
                ? ` - 年化: ${(t.best_ann_return * 100).toFixed(1)}%, 回撤: ${(t.best_max_drawdown * 100).toFixed(1)}%, IC: ${(t.best_ic || 0).toFixed(3)}`
                : "";
              return {
                id: t.task_id || "",
                label: `${t.task_id}${metrics}`,
              };
            })
          );
        })
        .catch(() => {}),
      fetch(`${API}/quantevolver/experiments?limit=200`)
        .then((r) => r.json())
        .then((d) => {
          const items: any[] = d.items || d || [];
          setQeExperiments(
            items.map((e: any) => ({
              id: e.experiment_id || "",
              label: e.experiment_name
                ? `${e.experiment_id} — ${e.experiment_name}`
                : e.experiment_id || "unknown",
            }))
          );
        })
        .catch(() => {}),
      fetch(`${API}/quantevolver/evolution/tasks`)
        .then((r) => r.json())
        .then((d) => {
          const items: any[] = d.data || d.items || d || [];
          setQeEvolutions(
            items.map((t: any) => ({
              id: t.task_id || "",
              label: t.task_name
                ? `${t.task_id} — ${t.task_name}`
                : t.task_id || "unknown",
            }))
          );
        })
        .catch(() => {}),
      fetch(`${API}/paper-trading/execution-algorithms`)
        .then((r) => r.json())
        .then((d) => setExecAlgos(Array.isArray(d) ? d : []))
        .catch(() => {}),
    ]).finally(() => setLoadingOptions(false));
  }, []);

  // 切换信号来源时重置
  const handleSignalSourceChange = (newSource: string) => {
    setForm({ ...form, signal_source: newSource, signal_source_id: "", signal_loop_id: "" });
    setManualInput(false);
  };

  const currentOptions =
    form.signal_source === "rdagent_task"
      ? rdagentTasks
      : form.signal_source === "qe_experiment"
        ? qeExperiments
        : qeEvolutions;

  const feeErrors = validateAllFees(fees);
  const editFeeErrors = validateAllFees(editFees);
  const hasFeeErrors = Object.keys(feeErrors).length > 0;
  const hasEditFeeErrors = Object.keys(editFeeErrors).length > 0;
  const executeTimeError = validateExecuteTime(form.execute_time);
  const editExecuteTimeError = validateExecuteTime(editForm.execute_time || "17:30");

  const createPortfolio = async () => {
    // 验证必填字段
    if (!form.portfolio_name) {
      alert("请填写模拟盘名称");
      document.querySelector<HTMLInputElement>('input[placeholder*="RDAgent"]')?.focus();
      return;
    }
    if (!form.signal_source_id) {
      alert("请选择信号来源ID");
      return;
    }
    if (hasFeeErrors) {
      alert("手续费参数有误，请检查");
      return;
    }
    // 追赶模式（历史 start_date）不需要验证 execute_time
    const isCatchupMode = form.start_date && new Date(form.start_date) < new Date(new Date().toISOString().split('T')[0]);
    if (!isCatchupMode && executeTimeError) {
      alert("执行时间格式有误");
      return;
    }
    setLoading(true);
    try {
      const body: any = {
        ...form,
        fee_config: feeFieldsToConfig(fees),
        signal_loop_id: form.signal_loop_id ? parseInt(form.signal_loop_id) : undefined,
        start_date: form.start_date || undefined,
      };
      // 追赶模式：不发送执行时间和盘中执行配置（追赶完成后再配置）
      if (!isCatchupMode) {
        body.execute_time = form.execute_time;
        body.enable_intraday = form.enable_intraday;
        body.intraday_exec_mode = form.enable_intraday ? form.intraday_exec_mode : undefined;
        body.intraday_strategy = form.enable_intraday ? form.intraday_strategy : "CLOSE_PRICE";
        body.intraday_config = form.enable_intraday ? form.intraday_config : undefined;
        body.intraday_freq = form.enable_intraday ? form.intraday_freq : undefined;
      }
      const resp = await fetch(`${API}/paper-trading/portfolios`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!resp.ok) throw new Error(await resp.text());
      setShowCreate(false);
      setForm({ ...EMPTY_FORM });
      setFees({ ...DEFAULT_FEES });
      loadPortfolios();
    } catch (e: any) {
      alert("创建失败: " + (e.message || e));
    } finally {
      setLoading(false);
    }
  };

  const startEdit = async (id: number) => {
    try {
      const resp = await fetch(`${API}/paper-trading/portfolios/${id}`);
      if (!resp.ok) throw new Error(await resp.text());
      const data = await resp.json();
      setEditingId(id);
      // execute_time 从 DB 可能返回 "17:30:00"，截取 HH:MM
      const rawTime = data.execute_time || "17:30";
      const execTime = typeof rawTime === "string" ? rawTime.slice(0, 5) : "17:30";
      setEditForm({
        portfolio_name: data.portfolio_name || "",
        max_positions: data.max_positions || 20,
        max_position_pct: data.max_position_pct || 0.1,
        max_turnover_pct: data.max_turnover_pct || 0.3,
        benchmark: data.benchmark || "000300.SH",
        auto_run: data.auto_run ?? true,
        execute_time: execTime,
        enable_factor_attribution: data.enable_factor_attribution ?? true,
        enable_live_ic: data.enable_live_ic ?? true,
        enable_intraday: data.enable_intraday ?? false,
        intraday_exec_mode: data.intraday_exec_mode || "replay",
        intraday_strategy: data.intraday_strategy || "CLOSE_PRICE",
        intraday_config: data.intraday_config || {},
        intraday_freq: data.intraday_freq || "5m",
        start_date: data.start_date || "",
      });
      const fc = typeof data.fee_config === "string" ? JSON.parse(data.fee_config) : data.fee_config;
      setEditFees(configToFeeFields(fc));
    } catch (e: any) {
      alert("加载失败: " + (e.message || e));
    }
  };

  const saveEdit = async () => {
    if (editingId == null) return;
    if (hasEditFeeErrors) { alert("手续费参数有误，请检查"); return; }
    if (editExecuteTimeError) { alert("执行时间格式有误"); return; }
    setSaving(true);
    try {
      const body = {
        ...editForm,
        fee_config: feeFieldsToConfig(editFees),
        enable_intraday: editForm.enable_intraday,
        intraday_exec_mode: editForm.enable_intraday ? editForm.intraday_exec_mode : undefined,
        intraday_strategy: editForm.enable_intraday ? editForm.intraday_strategy : "CLOSE_PRICE",
        intraday_config: editForm.enable_intraday ? editForm.intraday_config : undefined,
        intraday_freq: editForm.enable_intraday ? editForm.intraday_freq : undefined,
        start_date: editForm.start_date || undefined,
      };
      const resp = await fetch(`${API}/paper-trading/portfolios/${editingId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!resp.ok) throw new Error(await resp.text());
      setEditingId(null);
      loadPortfolios();
    } catch (e: any) {
      alert("保存失败: " + (e.message || e));
    } finally {
      setSaving(false);
    }
  };

  const controlPortfolio = async (id: number, action: string) => {
    try {
      const resp = await fetch(`${API}/paper-trading/portfolios/${id}/${action}`, { method: "POST" });
      if (!resp.ok) throw new Error(await resp.text());
      loadPortfolios();
    } catch (e: any) {
      alert(`操作失败: ${e.message}`);
    }
  };

  const deletePortfolio = async (id: number) => {
    if (!confirm("确认删除？")) return;
    try {
      const resp = await fetch(`${API}/paper-trading/portfolios/${id}`, { method: "DELETE" });
      if (!resp.ok) throw new Error(await resp.text());
      loadPortfolios();
    } catch (e: any) {
      alert(`删除失败: ${e.message}`);
    }
  };

  // 追赶进度
  const [catchupProgress, setCatchupProgress] = useState<Record<number, {
    total_days: number; completed_days: number; current_date: string; pct: number; error?: string; error_time?: string;
  }>>({});

  const startCatchup = async (id: number) => {
    try {
      const resp = await fetch(`${API}/paper-trading/portfolios/${id}/start-catchup`, { method: "POST" });
      if (!resp.ok) throw new Error(await resp.text());
      loadPortfolios();
    } catch (e: any) { alert(`启动追赶失败: ${e.message}`); }
  };

  const goLive = async (id: number) => {
    try {
      // 先保存 caught_up 配置
      const config = caughtUpConfig[id];
      if (config) {
        const updateResp = await fetch(`${API}/paper-trading/portfolios/${id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            execute_time: config.execute_time,
            enable_intraday: config.enable_intraday,
            intraday_exec_mode: config.intraday_exec_mode,
          }),
        });
        if (!updateResp.ok) throw new Error("保存配置失败: " + await updateResp.text());
      }
      // 再上线
      const resp = await fetch(`${API}/paper-trading/portfolios/${id}/go-live`, { method: "POST" });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(err.detail || "操作失败");
      }
      setCaughtUpConfig(prev => { const n = { ...prev }; delete n[id]; return n; });
      loadPortfolios();
    } catch (e: any) { alert(e.message); }
  };

  const pollCatchupProgress = useCallback(async (id: number) => {
    try {
      const resp = await fetch(`${API}/paper-trading/portfolios/${id}/catchup-progress`);
      if (!resp.ok) return;
      const data = await resp.json();
      if (data.status === "catching_up") {
        setCatchupProgress(prev => ({ ...prev, [id]: data }));
      } else {
        setCatchupProgress(prev => { const n = { ...prev }; delete n[id]; return n; });
        if (data.status === "caught_up") loadPortfolios();
      }
    } catch {}
  }, [loadPortfolios]);

  useEffect(() => {
    const catchingUp = portfolios.filter(p => p.status === "catching_up");
    if (catchingUp.length === 0) return;
    catchingUp.forEach(p => pollCatchupProgress(p.id));
    const timer = setInterval(() => {
      catchingUp.forEach(p => pollCatchupProgress(p.id));
    }, 5000);
    return () => clearInterval(timer);
  }, [portfolios, pollCatchupProgress]);

  const statusColors: Record<string, { bg: string; color: string; label: string }> = {
    created: { bg: "#f3f4f6", color: "#6b7280", label: "未启动" },
    catching_up: { bg: "#ecfdf5", color: "#059669", label: "追赶中" },
    caught_up: { bg: "#f5f3ff", color: "#7c3aed", label: "待上线" },
    running: { bg: "#dbeafe", color: "#1d4ed8", label: "运行中" },
    paused: { bg: "#fef3c7", color: "#b45309", label: "已暂停" },
    stopped: { bg: "#fef2f2", color: "#dc2626", label: "已停止" },
  };

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <span style={{ fontSize: 13, color: "#6b7280" }}>共 {portfolios.length} 个模拟盘</span>
        <button onClick={() => setShowCreate(!showCreate)} style={btnStyle}>
          {showCreate ? "取消" : "+ 新建模拟盘"}
        </button>
      </div>

      {/* 新建表单 */}
      {showCreate && (() => {
        const isCatchupMode = form.start_date && new Date(form.start_date) < new Date(new Date().toISOString().split('T')[0]);
        return (
        <div style={cardStyle}>
          <h3 style={{ margin: "0 0 16px", fontSize: 15, fontWeight: 600 }}>新建模拟盘</h3>
          {isCatchupMode && (
            <div style={{ padding: 12, background: "#ecfdf5", borderRadius: 8, marginBottom: 16, fontSize: 13, color: "#059669" }}>
              追赶模式：将从 {form.start_date} 开始回放历史交易，追赶完成后再配置执行时间和执行模式
            </div>
          )}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 16 }}>
            <div>
              <label style={labelStyle}>模拟盘名称</label>
              <input value={form.portfolio_name} onChange={(e) => setForm({ ...form, portfolio_name: e.target.value })} style={inputStyle} placeholder="如 RDAgent-Task5-Loop3" />
            </div>
            <div>
              <label style={labelStyle}>信号来源</label>
              <select value={form.signal_source} onChange={(e) => handleSignalSourceChange(e.target.value)} style={inputStyle}>
                <option value="rdagent_task">RDAgent Task</option>
                <option value="qe_experiment">QE 单次实验</option>
                <option value="qe_evolution">QE 演进</option>
              </select>
            </div>
            <div>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <label style={{ ...labelStyle, marginBottom: 0 }}>
                  {form.signal_source === "rdagent_task" ? "Task Run ID" : form.signal_source === "qe_experiment" ? "Experiment ID" : "Evolution Task ID"}
                </label>
                <button
                  onClick={() => { setManualInput(!manualInput); setForm({ ...form, signal_source_id: "" }); }}
                  style={{ fontSize: 11, color: "#2563eb", background: "none", border: "none", cursor: "pointer", padding: 0 }}
                >
                  {manualInput ? "切换下拉" : "手工输入"}
                </button>
              </div>
              <div style={{ marginTop: 4 }}>
                {manualInput ? (
                  <input
                    value={form.signal_source_id}
                    onChange={(e) => setForm({ ...form, signal_source_id: e.target.value })}
                    placeholder={form.signal_source === "rdagent_task" ? "task_xxx" : "exp_xxx"}
                    style={inputStyle}
                  />
                ) : (
                  <select
                    value={form.signal_source_id}
                    onChange={(e) => setForm({ ...form, signal_source_id: e.target.value })}
                    style={inputStyle}
                    disabled={loadingOptions}
                  >
                    <option value="">
                      {loadingOptions ? "加载中..." : currentOptions.length === 0 ? "暂无可用任务" : "-- 请选择 --"}
                    </option>
                    {currentOptions.map((opt) => (
                      <option key={opt.id} value={opt.id}>{opt.label}</option>
                    ))}
                  </select>
                )}
              </div>
            </div>
            <div>
              <label style={labelStyle}>启动日期</label>
              <input
                type="date"
                value={form.start_date || ""}
                onChange={(e) => setForm({ ...form, start_date: e.target.value })}
                style={inputStyle}
                max={new Date(Date.now() - 86400000).toISOString().split('T')[0]}
              />
              <span style={{ fontSize: 11, color: "#9ca3af" }}>追赶模式从此日期开始回放</span>
            </div>
            {form.signal_source === "rdagent_task" && (
              <div>
                <label style={labelStyle}>Loop ID</label>
                <input value={form.signal_loop_id} onChange={(e) => setForm({ ...form, signal_loop_id: e.target.value })} type="number" style={inputStyle} placeholder="留空=最新SOTA" />
              </div>
            )}
            <div>
              <label style={labelStyle}>初始资金</label>
              <input value={form.initial_capital} onChange={(e) => setForm({ ...form, initial_capital: Number(e.target.value) })} type="number" style={inputStyle} />
            </div>
            <div>
              <label style={labelStyle}>持仓股票数</label>
              <input value={form.max_positions} onChange={(e) => setForm({ ...form, max_positions: Number(e.target.value) })} type="number" style={inputStyle} />
            </div>
            <div>
              <label style={labelStyle}>单只最大仓位</label>
              <input value={form.max_position_pct} onChange={(e) => setForm({ ...form, max_position_pct: Number(e.target.value) })} type="number" step="0.01" style={inputStyle} />
            </div>
            <div>
              <label style={labelStyle}>换手限制</label>
              <input value={form.max_turnover_pct} onChange={(e) => setForm({ ...form, max_turnover_pct: Number(e.target.value) })} type="number" step="0.01" style={inputStyle} />
            </div>
            <div>
              <label style={labelStyle}>基准</label>
              <input value={form.benchmark} onChange={(e) => setForm({ ...form, benchmark: e.target.value })} style={inputStyle} />
            </div>
            {!isCatchupMode && (
              <div>
                <label style={labelStyle}>策略执行时间</label>
                <input
                  value={form.execute_time}
                  onChange={(e) => setForm({ ...form, execute_time: e.target.value })}
                  style={{ ...inputStyle, borderColor: executeTimeError ? "#ef4444" : "#d1d5db" }}
                  placeholder="HH:MM"
                />
                {executeTimeError ? (
                  <div style={{ fontSize: 11, color: "#ef4444", marginTop: 2 }}>{executeTimeError}</div>
                ) : (
                  <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 2 }}>盘后数据入库后执行，建议 17:00-18:00</div>
                )}
              </div>
            )}
          </div>

          <div style={{ marginTop: 16 }}>
            <label style={{ ...labelStyle, marginBottom: 8 }}>手续费配置</label>
            <FeeEditor fees={fees} onChange={setFees} errors={feeErrors} />
          </div>

          {/* 日内执行设置 - 追赶模式下隐藏 */}
          {!isCatchupMode && (
          <div style={{ marginTop: 16 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
              <label style={{ fontSize: 13, display: "flex", alignItems: "center", gap: 4, fontWeight: 600 }}>
                <input
                  type="checkbox"
                  checked={form.enable_intraday}
                  onChange={(e) => {
                    const on = e.target.checked;
                    setForm({ ...form, enable_intraday: on, intraday_strategy: on && form.intraday_strategy === "CLOSE_PRICE" ? "TWAP" : form.intraday_strategy });
                  }}
                /> 启用日内执行
              </label>
              <span style={{ fontSize: 11, color: "#9ca3af" }}>
                启用后按前一交易日选股结果在盘中分步执行买卖
              </span>
            </div>
            {form.enable_intraday && (
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, padding: 12, background: "#f9fafb", borderRadius: 8 }}>
                <div>
                  <label style={labelStyle}>执行模式</label>
                  <select
                    value={form.intraday_exec_mode}
                    onChange={(e) => setForm({ ...form, intraday_exec_mode: e.target.value })}
                    style={inputStyle}
                  >
                    <option value="replay">盘后回放</option>
                    <option value="live">盘中实时</option>
                  </select>
                  <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 2 }}>
                    {form.intraday_exec_mode === "live"
                      ? "盘中实时读取 TDX 分钟线逐步执行"
                      : "盘后获取全日分钟线一次性回放执行"}
                  </div>
                </div>
                <div>
                  <label style={labelStyle}>执行算法</label>
                  <select
                    value={form.intraday_strategy}
                    onChange={(e) => setForm({ ...form, intraday_strategy: e.target.value, intraday_config: {} })}
                    style={inputStyle}
                  >
                    {execAlgos.length > 0 ? (
                      execAlgos.filter((a) => a.algo_code !== "CLOSE_PRICE").map((a) => (
                        <option key={a.algo_code} value={a.algo_code}>{a.algo_name}</option>
                      ))
                    ) : (
                      <>
                        <option value="TWAP">TWAP 均匀拆分</option>
                        <option value="VWAP">VWAP 成交量加权</option>
                        <option value="SBB_EMA">SBB-EMA 择时</option>
                        <option value="AC_OPTIMAL">AC 最优执行</option>
                        <option value="POV">POV 参与率</option>
                      </>
                    )}
                  </select>
                  {execAlgos.find((a) => a.algo_code === form.intraday_strategy)?.description && (
                    <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 2 }}>
                      {execAlgos.find((a) => a.algo_code === form.intraday_strategy)?.description?.slice(0, 60)}
                    </div>
                  )}
                </div>
                <div>
                  <label style={labelStyle}>执行频率</label>
                  <select
                    value={form.intraday_freq}
                    onChange={(e) => setForm({ ...form, intraday_freq: e.target.value })}
                    style={inputStyle}
                  >
                    {FREQ_OPTIONS.map((f) => (
                      <option key={f.value} value={f.value}>{f.label}</option>
                    ))}
                  </select>
                </div>
                {(() => {
                  const algo = execAlgos.find((a) => a.algo_code === form.intraday_strategy);
                  const schema = algo?.param_schema?.properties;
                  if (!schema) return null;
                  return Object.entries(schema).map(([key, spec]: [string, any]) => (
                    <div key={key}>
                      <label style={labelStyle}>{spec.description || key}</label>
                      <input
                        value={form.intraday_config?.[key] ?? spec.default ?? ""}
                        onChange={(e) => setForm({
                          ...form,
                          intraday_config: { ...form.intraday_config, [key]: Number(e.target.value) || e.target.value },
                        })}
                        type={spec.type === "number" || spec.type === "integer" ? "number" : "text"}
                        step={spec.type === "number" ? "any" : undefined}
                        style={inputStyle}
                        placeholder={spec.default != null ? String(spec.default) : ""}
                      />
                    </div>
                  ));
                })()}
              </div>
            )}
          </div>
          )}

          <div style={{ display: "flex", gap: 12, marginTop: 16 }}>
            <label style={{ fontSize: 13, display: "flex", alignItems: "center", gap: 4 }}>
              <input type="checkbox" checked={form.auto_run} onChange={(e) => setForm({ ...form, auto_run: e.target.checked })} /> 自动运行
            </label>
            <label style={{ fontSize: 13, display: "flex", alignItems: "center", gap: 4 }}>
              <input type="checkbox" checked={form.enable_factor_attribution} onChange={(e) => setForm({ ...form, enable_factor_attribution: e.target.checked })} /> 因子归因
            </label>
            <label style={{ fontSize: 13, display: "flex", alignItems: "center", gap: 4 }}>
              <input type="checkbox" checked={form.enable_live_ic} onChange={(e) => setForm({ ...form, enable_live_ic: e.target.checked })} /> 实盘 IC
            </label>
          </div>

          <button
            onClick={createPortfolio}
            disabled={loading}
            style={{ ...btnStyle, marginTop: 16, opacity: loading ? 0.5 : 1 }}
          >
            {loading ? "创建中..." : "创建模拟盘"}
          </button>
        </div>
        );
      })()}

      {/* 模拟盘列表 */}
      {portfolios.map((p) => {
        const sc = statusColors[p.status] || statusColors.created;
        const isEditing = editingId === p.id;
        return (
          <div key={p.id} style={cardStyle}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div>
                <span style={{ fontSize: 16, fontWeight: 600 }}>{p.portfolio_name}</span>
                <span style={{
                  marginLeft: 10, fontSize: 11, padding: "2px 8px",
                  borderRadius: 4, background: sc.bg, color: sc.color, fontWeight: 500,
                }}>
                  {sc.label}
                </span>
              </div>
              <div style={{ display: "flex", gap: 6 }}>
                {!isEditing && (
                  <button onClick={() => startEdit(p.id)} style={{ ...btnSmallStyle, background: "#6366f1" }}>编辑</button>
                )}
                {p.status === "created" && (
                  <>
                    <button onClick={() => controlPortfolio(p.id, "start")} style={btnSmallStyle}>启动</button>
                    <button onClick={() => startCatchup(p.id)}
                      style={{ ...btnSmallStyle, background: "#059669" }}>历史追赶</button>
                  </>
                )}
                {(p.status === "paused" || p.status === "stopped") && (
                  <button onClick={() => controlPortfolio(p.id, "start")} style={btnSmallStyle}>启动</button>
                )}
                {p.status === "catching_up" && catchupProgress[p.id]?.error && (
                  <button onClick={() => startCatchup(p.id)}
                    style={{ ...btnSmallStyle, background: "#dc2626" }}>重试追赶</button>
                )}
                {p.status === "caught_up" && (
                  <button onClick={() => goLive(p.id)}
                    style={{ ...btnSmallStyle, background: "linear-gradient(135deg, #7c3aed, #6d28d9)" }}>
                    上线运行
                  </button>
                )}
                {p.status === "running" && (
                  <button onClick={() => controlPortfolio(p.id, "pause")}
                    style={{ ...btnSmallStyle, background: "#f59e0b" }}>暂停</button>
                )}
                {(p.status === "running" || p.status === "paused") && (
                  <button onClick={() => controlPortfolio(p.id, "stop")}
                    style={{ ...btnSmallStyle, background: "#dc2626" }}>停止</button>
                )}
                {(p.status === "created" || p.status === "stopped" || p.status === "caught_up") && (
                  <button onClick={() => deletePortfolio(p.id)}
                    style={{ ...btnSmallStyle, background: "#6b7280" }}>删除</button>
                )}
              </div>
            </div>

            {/* 编辑面板 */}
            {isEditing ? (
              <div style={{ marginTop: 16, paddingTop: 16, borderTop: "1px solid #e5e7eb" }}>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 12 }}>
                  <div>
                    <label style={labelStyle}>名称</label>
                    <input value={editForm.portfolio_name || ""} onChange={(e) => setEditForm({ ...editForm, portfolio_name: e.target.value })} style={inputStyle} />
                  </div>
                  <div>
                    <label style={labelStyle}>持仓数</label>
                    <input value={editForm.max_positions || 20} onChange={(e) => setEditForm({ ...editForm, max_positions: Number(e.target.value) })} type="number" style={inputStyle} />
                  </div>
                  <div>
                    <label style={labelStyle}>单只最大仓位</label>
                    <input value={editForm.max_position_pct || 0.1} onChange={(e) => setEditForm({ ...editForm, max_position_pct: Number(e.target.value) })} type="number" step="0.01" style={inputStyle} />
                  </div>
                  <div>
                    <label style={labelStyle}>换手限制</label>
                    <input value={editForm.max_turnover_pct || 0.3} onChange={(e) => setEditForm({ ...editForm, max_turnover_pct: Number(e.target.value) })} type="number" step="0.01" style={inputStyle} />
                  </div>
                  <div>
                    <label style={labelStyle}>基准</label>
                    <input value={editForm.benchmark || ""} onChange={(e) => setEditForm({ ...editForm, benchmark: e.target.value })} style={inputStyle} />
                  </div>
                  <div>
                    <label style={labelStyle}>策略执行时间</label>
                    <input
                      value={editForm.execute_time || "17:30"}
                      onChange={(e) => setEditForm({ ...editForm, execute_time: e.target.value })}
                      style={{ ...inputStyle, borderColor: editExecuteTimeError ? "#ef4444" : "#d1d5db" }}
                      placeholder="HH:MM"
                    />
                    {editExecuteTimeError ? (
                      <div style={{ fontSize: 11, color: "#ef4444", marginTop: 2 }}>{editExecuteTimeError}</div>
                    ) : (
                      <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 2 }}>盘后数据入库后执行</div>
                    )}
                  </div>
                  <div>
                    <label style={labelStyle}>启动日期</label>
                    <input
                      type="date"
                      value={editForm.start_date || ""}
                      onChange={(e) => setEditForm({ ...editForm, start_date: e.target.value })}
                      style={inputStyle}
                      max={new Date(Date.now() - 86400000).toISOString().split('T')[0]}
                      disabled={p.status !== "created" && p.status !== "stopped"}
                    />
                    <span style={{ fontSize: 11, color: "#9ca3af" }}>
                      {p.status !== "created" && p.status !== "stopped" ? "仅未启动/已停止可修改" : "追赶起始日期"}
                    </span>
                  </div>
                </div>
                <div style={{ marginTop: 12 }}>
                  <label style={{ ...labelStyle, marginBottom: 8 }}>手续费配置</label>
                  <FeeEditor fees={editFees} onChange={setEditFees} errors={editFeeErrors} />
                </div>
                {/* 编辑 - 日内执行设置 */}
                <div style={{ marginTop: 12 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                    <label style={{ fontSize: 13, display: "flex", alignItems: "center", gap: 4, fontWeight: 600 }}>
                      <input
                        type="checkbox"
                        checked={editForm.enable_intraday ?? false}
                        onChange={(e) => {
                          const on = e.target.checked;
                          setEditForm({ ...editForm, enable_intraday: on, intraday_strategy: on && (editForm.intraday_strategy === "CLOSE_PRICE" || !editForm.intraday_strategy) ? "TWAP" : editForm.intraday_strategy });
                        }}
                      /> 启用日内执行
                    </label>
                    <span style={{ fontSize: 11, color: "#9ca3af" }}>
                      启用后按前一交易日选股结果在盘中分步执行买卖
                    </span>
                  </div>
                  {editForm.enable_intraday && (
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, padding: 12, background: "#f9fafb", borderRadius: 8 }}>
                      <div>
                        <label style={labelStyle}>执行模式</label>
                        <select
                          value={editForm.intraday_exec_mode || "replay"}
                          onChange={(e) => setEditForm({ ...editForm, intraday_exec_mode: e.target.value })}
                          style={inputStyle}
                        >
                          <option value="replay">盘后回放</option>
                          <option value="live">盘中实时</option>
                        </select>
                        <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 2 }}>
                          {editForm.intraday_exec_mode === "live"
                            ? "盘中实时读取 TDX 分钟线逐步执行"
                            : "盘后获取全日分钟线一次性回放执行"}
                        </div>
                      </div>
                      <div>
                        <label style={labelStyle}>执行算法</label>
                        <select
                          value={editForm.intraday_strategy || "TWAP"}
                          onChange={(e) => setEditForm({ ...editForm, intraday_strategy: e.target.value, intraday_config: {} })}
                          style={inputStyle}
                        >
                          {execAlgos.length > 0 ? (
                            execAlgos.filter((a) => a.algo_code !== "CLOSE_PRICE").map((a) => (
                              <option key={a.algo_code} value={a.algo_code}>{a.algo_name}</option>
                            ))
                          ) : (
                            <>
                              <option value="TWAP">TWAP 均匀拆分</option>
                              <option value="VWAP">VWAP 成交量加权</option>
                              <option value="SBB_EMA">SBB-EMA 择时</option>
                              <option value="AC_OPTIMAL">AC 最优执行</option>
                              <option value="POV">POV 参与率</option>
                            </>
                          )}
                        </select>
                      </div>
                      <div>
                        <label style={labelStyle}>执行频率</label>
                        <select
                          value={editForm.intraday_freq || "5m"}
                          onChange={(e) => setEditForm({ ...editForm, intraday_freq: e.target.value })}
                          style={inputStyle}
                        >
                          {FREQ_OPTIONS.map((f) => (
                            <option key={f.value} value={f.value}>{f.label}</option>
                          ))}
                        </select>
                      </div>
                      {(() => {
                        const algo = execAlgos.find((a) => a.algo_code === editForm.intraday_strategy);
                        const schema = algo?.param_schema?.properties;
                        if (!schema) return null;
                        return Object.entries(schema).map(([key, spec]: [string, any]) => (
                          <div key={key}>
                            <label style={labelStyle}>{spec.description || key}</label>
                            <input
                              value={editForm.intraday_config?.[key] ?? spec.default ?? ""}
                              onChange={(e) => setEditForm({
                                ...editForm,
                                intraday_config: { ...editForm.intraday_config, [key]: Number(e.target.value) || e.target.value },
                              })}
                              type={spec.type === "number" || spec.type === "integer" ? "number" : "text"}
                              step={spec.type === "number" ? "any" : undefined}
                              style={inputStyle}
                              placeholder={spec.default != null ? String(spec.default) : ""}
                            />
                          </div>
                        ));
                      })()}
                    </div>
                  )}
                </div>
                <div style={{ display: "flex", gap: 12, marginTop: 12 }}>
                  <label style={{ fontSize: 13, display: "flex", alignItems: "center", gap: 4 }}>
                    <input type="checkbox" checked={editForm.auto_run ?? true} onChange={(e) => setEditForm({ ...editForm, auto_run: e.target.checked })} /> 自动运行
                  </label>
                  <label style={{ fontSize: 13, display: "flex", alignItems: "center", gap: 4 }}>
                    <input type="checkbox" checked={editForm.enable_factor_attribution ?? true} onChange={(e) => setEditForm({ ...editForm, enable_factor_attribution: e.target.checked })} /> 因子归因
                  </label>
                  <label style={{ fontSize: 13, display: "flex", alignItems: "center", gap: 4 }}>
                    <input type="checkbox" checked={editForm.enable_live_ic ?? true} onChange={(e) => setEditForm({ ...editForm, enable_live_ic: e.target.checked })} /> 实盘 IC
                  </label>
                </div>
                <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
                  <button onClick={saveEdit} disabled={saving || hasEditFeeErrors || !!editExecuteTimeError} style={{ ...btnStyle, opacity: saving || hasEditFeeErrors || !!editExecuteTimeError ? 0.5 : 1 }}>
                    {saving ? "保存中..." : "保存修改"}
                  </button>
                  <button onClick={() => setEditingId(null)} style={{ ...btnSmallStyle, background: "#6b7280", padding: "6px 14px" }}>取消</button>
                </div>
              </div>
            ) : (
              <>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 12, marginTop: 12, fontSize: 13, color: "#6b7280" }}>
                  <div>来源: <b style={{ color: "#111" }}>{p.signal_source}</b></div>
                  <div>来源ID: <b style={{ color: "#111" }}>{p.signal_source_id}</b></div>
                  <div>初始资金: <b style={{ color: "#111" }}>{(p.initial_capital / 10000).toFixed(0)}万</b></div>
                  <div>持仓数: <b style={{ color: "#111" }}>{p.max_positions}</b></div>
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 12, marginTop: 6, fontSize: 13, color: "#6b7280" }}>
                  <div>执行时间: <b style={{ color: "#111" }}>{p.execute_time ? String(p.execute_time).slice(0, 5) : "17:30"}</b></div>
                  <div>执行算法: <b style={{ color: "#111" }}>{p.enable_intraday ? `${p.intraday_strategy || "TWAP"} (${p.intraday_exec_mode === "live" ? "盘中实时" : "盘后回放"})` : "收盘价"}</b></div>
                  {p.start_date && <div>启动日期: <b style={{ color: "#111" }}>{p.start_date}</b></div>}
                  <div>创建时间: <span style={{ color: "#9ca3af" }}>{new Date(p.created_at).toLocaleString()}</span></div>
                </div>
              </>
            )}

            {/* 追赶进度条 */}
            {p.status === "catching_up" && (() => {
              const prog = catchupProgress[p.id];
              return (
                <div style={{
                  marginTop: 12, padding: 12, background: prog?.error ? "#fef2f2" : "#ecfdf5",
                  borderRadius: 8, fontSize: 13,
                }}>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                    <span style={{ color: prog?.error ? "#dc2626" : "#059669", fontWeight: 600 }}>
                      {prog?.error ? "追赶失败" : "追赶进度"}: {prog ? `${prog.completed_days}/${prog.total_days} 天` : "加载中..."}
                    </span>
                    <span style={{ color: "#6b7280" }}>
                      {prog?.current_date ? `当前: ${prog.current_date}` : ""}
                    </span>
                  </div>
                  <div style={{
                    width: "100%", height: 8, background: prog?.error ? "#fecaca" : "#d1fae5",
                    borderRadius: 4, overflow: "hidden",
                  }}>
                    <div style={{
                      width: `${prog?.pct ?? 0}%`, height: "100%",
                      background: prog?.error ? "#ef4444" : "linear-gradient(90deg, #059669, #10b981)",
                      borderRadius: 4, transition: "width 0.5s ease",
                    }} />
                  </div>
                  {prog?.error && (
                    <div style={{ marginTop: 6, padding: 8, background: "#fee2e2", borderRadius: 4, color: "#991b1b", fontSize: 12 }}>
                      {prog.error_time && <div style={{ fontSize: 10, color: "#7f1d1d", marginBottom: 4 }}>失败时间: {prog.error_time}</div>}
                      {prog.error}
                    </div>
                  )}
                  <div style={{ textAlign: "right", fontSize: 11, color: "#6b7280", marginTop: 4 }}>
                    {prog ? `${prog.pct.toFixed(1)}%` : ""}
                  </div>
                </div>
              );
            })()}

            {/* caught_up 配置 */}
            {p.status === "caught_up" && (() => {
              const now = new Date();
              const hour = now.getHours();
              const minute = now.getMinutes();
              const inTradingHours = (hour === 9 && minute >= 30) || (hour === 10) || (hour === 11 && minute <= 30) || (hour === 13) || (hour === 14) || (hour === 15 && minute === 0);
              let nextTime = "";
              if (inTradingHours) {
                if (hour < 11 || (hour === 11 && minute <= 30)) nextTime = "11:31";
                else if (hour < 15) nextTime = "15:01";
              }
              const cfg = caughtUpConfig[p.id] || { execute_time: "17:30", enable_intraday: false, intraday_exec_mode: "replay" };
              return (
                <div style={{
                  marginTop: 12, padding: 16, background: "#f5f3ff",
                  borderRadius: 8, fontSize: 13,
                }}>
                  <div style={{ color: "#7c3aed", fontWeight: 600, marginBottom: 12 }}>历史追赶已完成，配置执行参数后上线</div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 12 }}>
                    <div>
                      <label style={{ ...labelStyle, color: "#6b7280" }}>策略执行时间</label>
                      <input
                        value={cfg.execute_time}
                        onChange={(e) => setCaughtUpConfig(prev => ({ ...prev, [p.id]: { ...cfg, execute_time: e.target.value } }))}
                        style={inputStyle}
                        placeholder="HH:MM"
                      />
                    </div>
                    <div>
                      <label style={{ ...labelStyle, color: "#6b7280" }}>执行模式</label>
                      <select
                        value={cfg.intraday_exec_mode}
                        onChange={(e) => setCaughtUpConfig(prev => ({ ...prev, [p.id]: { ...cfg, intraday_exec_mode: e.target.value } }))}
                        style={inputStyle}
                      >
                        <option value="replay">盘后回放</option>
                        <option value="live">盘中实时</option>
                      </select>
                    </div>
                    <div style={{ display: "flex", alignItems: "flex-end" }}>
                      <label style={{ fontSize: 12, display: "flex", alignItems: "center", gap: 4 }}>
                        <input
                          type="checkbox"
                          checked={cfg.enable_intraday}
                          onChange={(e) => setCaughtUpConfig(prev => ({ ...prev, [p.id]: { ...cfg, enable_intraday: e.target.checked } }))}
                        /> 启用日内执行
                      </label>
                    </div>
                  </div>
                  {nextTime && <div style={{ fontSize: 11, color: "#9333ea", marginBottom: 8 }}>交易时段禁止上线，下次可操作: {nextTime}</div>}
                </div>
              );
            })()}
          </div>
        );
      })}

      {portfolios.length === 0 && !showCreate && (
        <div style={{ textAlign: "center", color: "#9ca3af", padding: 40 }}>
          暂无模拟盘，点击右上角创建
        </div>
      )}
    </div>
  );
}

const cardStyle: React.CSSProperties = { background: "#fff", borderRadius: 12, padding: 20, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" };
const labelStyle: React.CSSProperties = { display: "block", fontSize: 12, fontWeight: 500, color: "#6b7280", marginBottom: 4 };
const inputStyle: React.CSSProperties = { width: "100%", padding: "6px 10px", border: "1px solid #d1d5db", borderRadius: 6, fontSize: 13, outline: "none" };
const btnStyle: React.CSSProperties = { padding: "8px 16px", background: "linear-gradient(135deg, #2563eb, #1d4ed8)", color: "#fff", border: "none", borderRadius: 6, fontSize: 13, fontWeight: 600, cursor: "pointer" };
const btnSmallStyle: React.CSSProperties = { padding: "4px 12px", background: "#2563eb", color: "#fff", border: "none", borderRadius: 4, fontSize: 12, cursor: "pointer" };
