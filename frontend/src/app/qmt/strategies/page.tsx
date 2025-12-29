"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface Strategy {
  strategy_id: string;
  strategy_name: string;
  strategy_type: string;
  description: string;
  enabled: boolean;
  config: Record<string, any>;
  schedule_config: Record<string, any>;
  risk_config: Record<string, any>;
  created_at: string | null;
  updated_at: string | null;
}

interface QmtStatus {
  enabled: boolean;
  connected: boolean;
  mode?: string | null;
  account_id?: string | null;
  provider?: string | null;
  userdata_path?: string | null;
  session_id?: string | null;
  last_error?: string | null;
}

interface TradeIntent {
  id: number;
  strategy_id: string;
  symbol: string;
  side: string;
  quantity: number;
  price_type: string;
  price: number | null;
  reason: string;
  status: string;
  order_id: string | null;
  order_sysid: string | null;
  error_message: string | null;
  created_at: string | null;
  executed_at: string | null;
  signal_data: Record<string, any>;
  latest_order_status?: number | null;
  latest_order_status_msg?: string | null;
  traded_volume?: number | null;
  traded_price?: number | null;
}

interface Execution {
  id: number;
  strategy_id: string;
  execution_type: string;
  trigger_source: string | null;
  status: string;
  start_time: string | null;
  end_time: string | null;
  duration_seconds: number | null;
  symbols_processed: number;
  signals_generated: number;
  signals_executed: number;
  error_message: string | null;
  metrics: Record<string, any>;
  // 后端补充的 QMT 明细
  qmt_orders?: Array<Record<string, any>>;
  qmt_trades?: Array<Record<string, any>>;
}

interface StrategyMonitorItem {
  strategy_id: string;
  orders_count: number;
  trades_count: number;
  total_position_profit: number;
  total_daily_profit: number;
  market_value: number;
  weight_asset: number;
  symbols: string[];
}

interface StrategyMonitorSummary {
  timestamp: string;
  strategy_id: string;
  account: Record<string, any>;
  pnl: {
    total_position_profit: number;
    total_daily_profit: number;
    market_value: number;
    weight_asset: number;
  };
  positions: Array<Record<string, any>>;
  orders: Array<Record<string, any>>;
  trades: Array<Record<string, any>>;
}

function formatDateTime(s?: string | null) {
  if (!s) return "-";
  try {
    const d = new Date(s);
    return d.toLocaleString("zh-CN");
  } catch {
    return s;
  }
}

export default function StrategiesPage() {
  const [activeTab, setActiveTab] = useState<"strategies" | "intents" | "executions" | "monitor">("strategies");
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [intents, setIntents] = useState<TradeIntent[]>([]);
  const [executions, setExecutions] = useState<Execution[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // 交易意图自动刷新配置
  const [intentRefreshIntervalSec, setIntentRefreshIntervalSec] = useState<number>(0);

  // QMT 连接状态
  const [qmtStatus, setQmtStatus] = useState<QmtStatus | null>(null);
  const [qmtConnecting, setQmtConnecting] = useState(false);
  const [qmtError, setQmtError] = useState<string | null>(null);

  const [showAddModal, setShowAddModal] = useState(false);
  const [editingStrategy, setEditingStrategy] = useState<Strategy | null>(null);

  // 表单状态
  const [formStrategyId, setFormStrategyId] = useState("");
  const [formStrategyName, setFormStrategyName] = useState("");
  const [formStrategyType, setFormStrategyType] = useState("MA_CROSS");
  const [formDescription, setFormDescription] = useState("");
  const [formEnabled, setFormEnabled] = useState(true);
  
  // 策略配置参数（表单输入）
  const [formMaShort, setFormMaShort] = useState(5);
  const [formMaLong, setFormMaLong] = useState(20);
  const [formMaPeriod, setFormMaPeriod] = useState(20);
  const [formVolumeRatio, setFormVolumeRatio] = useState(1.5);
  const [formPeriod, setFormPeriod] = useState("1d");
  const [formSymbols, setFormSymbols] = useState("");
  const [formPositionSize, setFormPositionSize] = useState(0.1);
  const [formPriceType, setFormPriceType] = useState("LIMIT");
  
  // 调度配置（表单输入）
  const [formScheduleType, setFormScheduleType] = useState("daily");
  const [formScheduleTime, setFormScheduleTime] = useState("09:30");
  const [formScheduleInterval, setFormScheduleInterval] = useState(15);
  
  // 风控配置（表单输入）
  const [formMaxPositionPct, setFormMaxPositionPct] = useState(0.2);
  const [formMaxTotalPositionPct, setFormMaxTotalPositionPct] = useState(0.8);
  const [formMaxRiskPerTrade, setFormMaxRiskPerTrade] = useState(0.02);
  
  // JSON配置（保留，用于高级用户）
  const [formConfigJson, setFormConfigJson] = useState("{}");
  const [formScheduleConfig, setFormScheduleConfig] = useState("{}");
  const [formRiskConfig, setFormRiskConfig] = useState("{}");
  const [useAdvancedMode, setUseAdvancedMode] = useState(false);

  // 执行记录筛选：按策略ID
  const [executionStrategyFilter, setExecutionStrategyFilter] = useState<string>("");

  // 策略级监控数据
  const [strategyMonitors, setStrategyMonitors] = useState<StrategyMonitorItem[]>([]);
  const [selectedMonitorStrategyId, setSelectedMonitorStrategyId] = useState<string>("");
  const [strategyMonitorSummary, setStrategyMonitorSummary] = useState<StrategyMonitorSummary | null>(null);

  useEffect(() => {
    loadData();
  }, [activeTab]);

  // 当处于 Intents 标签页时，按配置的间隔自动刷新
  useEffect(() => {
    if (activeTab !== "intents") return;
    if (!intentRefreshIntervalSec || intentRefreshIntervalSec <= 0) return;
    if (qmtStatus && qmtStatus.enabled && !qmtStatus.connected) return;

    const timer = setInterval(() => {
      loadData();
    }, intentRefreshIntervalSec * 1000);

    return () => clearInterval(timer);
  }, [activeTab, intentRefreshIntervalSec, qmtStatus]);

  // 进入页面时自动检查并建立 miniQMT 连接
  useEffect(() => {
    ensureQmtConnected();
  }, []);

  async function loadData() {
    setLoading(true);
    setError(null);
    try {
      if (activeTab === "strategies") {
        const res = await fetch(`${API_BASE}/strategies/list?enabled_only=false`);
        if (!res.ok) throw new Error(`请求失败: ${res.status}`);
        const data = await res.json();
        setStrategies(data.strategies || []);
      } else if (activeTab === "intents") {
        const res = await fetch(`${API_BASE}/strategies/intents?limit=100`);
        if (!res.ok) throw new Error(`请求失败: ${res.status}`);
        const data = await res.json();
        setIntents(data.intents || []);
      } else if (activeTab === "executions") {
        // 确保已有策略列表用于筛选下拉选项
        if (strategies.length === 0) {
          try {
            const stratRes = await fetch(`${API_BASE}/strategies/list?enabled_only=false`);
            if (stratRes.ok) {
              const stratData = await stratRes.json();
              setStrategies(stratData.strategies || []);
            }
          } catch {
            // 忽略策略列表加载错误，不阻塞执行记录
          }
        }

        const params = new URLSearchParams();
        params.set("limit", "100");
        if (executionStrategyFilter) {
          params.set("strategy_id", executionStrategyFilter);
        }
        const res = await fetch(`${API_BASE}/strategies/executions?${params.toString()}`);
        if (!res.ok) throw new Error(`请求失败: ${res.status}`);
        const data = await res.json();
        setExecutions(data.executions || []);
      } else if (activeTab === "monitor") {
        // 策略级监控列表
        const res = await fetch(`${API_BASE}/qmt/monitor/strategies`);
        if (!res.ok) throw new Error(`请求失败: ${res.status}`);
        const data = await res.json();
        const items: StrategyMonitorItem[] = (data.items || []).map((x: any) => ({
          strategy_id: String(x.strategy_id),
          orders_count: Number(x.orders_count || 0),
          trades_count: Number(x.trades_count || 0),
          total_position_profit: Number(x.total_position_profit || 0),
          total_daily_profit: Number(x.total_daily_profit || 0),
          market_value: Number(x.market_value || 0),
          weight_asset: Number(x.weight_asset || 0),
          symbols: Array.isArray(x.symbols) ? x.symbols.map((s: any) => String(s)) : [],
        }));
        setStrategyMonitors(items);

        // 如果当前没有选中的策略，则默认选中第一个
        if (!selectedMonitorStrategyId && items.length > 0) {
          setSelectedMonitorStrategyId(items[0].strategy_id);
          // 立即加载该策略详情
          await loadSingleStrategyMonitor(items[0].strategy_id);
        } else if (selectedMonitorStrategyId) {
          // 已有选中策略，则刷新其详情
          await loadSingleStrategyMonitor(selectedMonitorStrategyId);
        } else {
          setStrategyMonitorSummary(null);
        }
      }
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }

  async function loadSingleStrategyMonitor(strategyId: string) {
    if (!strategyId) {
      setStrategyMonitorSummary(null);
      return;
    }
    try {
      const res = await fetch(`${API_BASE}/qmt/monitor/strategy/${encodeURIComponent(strategyId)}/summary`);
      if (!res.ok) throw new Error(`请求失败: ${res.status}`);
      const data = await res.json();
      setStrategyMonitorSummary(data as StrategyMonitorSummary);
    } catch (e: any) {
      // 不把错误打到全局 error，避免干扰其它标签
      console.error("加载策略监控详情失败", e);
      setStrategyMonitorSummary(null);
    }
  }

  async function ensureQmtConnected() {
    try {
      setQmtError(null);

      // 1. 查询当前状态
      const res = await fetch(`${API_BASE}/qmt/status`);
      if (!res.ok) {
        throw new Error(`QMT 状态查询失败: ${res.status}`);
      }
      const status: QmtStatus = await res.json();
      setQmtStatus(status);

      // 2. 如已启用但未连接，则尝试自动连接
      if (status.enabled && !status.connected) {
        setQmtConnecting(true);
        const connRes = await fetch(`${API_BASE}/qmt/connect`, {
          method: "POST",
        });
        if (!connRes.ok) {
          const err = await connRes.json().catch(() => null);
          throw new Error(err?.detail || `QMT 连接失败: ${connRes.status}`);
        }
        const connData = await connRes.json();
        if (connData?.status) {
          setQmtStatus(connData.status as QmtStatus);
        }
      }
    } catch (e: any) {
      setQmtError(e?.message || "QMT 连接检查失败");
    } finally {
      setQmtConnecting(false);
    }
  }

  async function handleSaveStrategy() {
    try {
      let configJson, scheduleConfig, riskConfig;
      
      if (useAdvancedMode) {
        // 高级模式：使用JSON配置
        try {
          configJson = JSON.parse(formConfigJson);
          scheduleConfig = formScheduleConfig ? JSON.parse(formScheduleConfig) : {};
          riskConfig = formRiskConfig ? JSON.parse(formRiskConfig) : {};
        } catch (e) {
          alert("JSON 格式错误，请检查配置");
          return;
        }
      } else {
        // 表单模式：从表单输入构建配置
        // 解析股票代码列表
        const symbols = formSymbols.split(/[,\n\s]+/).filter(s => s.trim()).map(s => {
          s = s.trim();
          // 如果没有后缀，自动添加
          if (!s.includes(".")) {
            if (s.startsWith("6") || s.startsWith("9")) {
              return s + ".SH";
            } else {
              return s + ".SZ";
            }
          }
          return s;
        });
        
        // 构建策略配置
        if (formStrategyType === "MA_CROSS") {
          configJson = {
            ma_short: formMaShort,
            ma_long: formMaLong,
            period: formPeriod,
            symbols: symbols,
            position_size: formPositionSize,
            price_type: formPriceType,
          };
        } else if (formStrategyType === "TREND_FOLLOWING") {
          configJson = {
            ma_period: formMaPeriod,
            volume_ratio: formVolumeRatio,
            period: formPeriod,
            symbols: symbols,
            position_size: formPositionSize,
            price_type: formPriceType,
          };
        } else {
          configJson = {
            symbols: symbols,
            position_size: formPositionSize,
            price_type: formPriceType,
          };
        }
        
        // 构建调度配置
        if (formScheduleType === "realtime") {
          scheduleConfig = {
            type: "realtime",
          };
        } else if (formScheduleType === "minute") {
          scheduleConfig = {
            type: "minute",
            interval: formScheduleInterval,
          };
        } else if (formScheduleType === "hourly") {
          scheduleConfig = {
            type: "hourly",
            time: formScheduleTime,
          };
        } else {
          scheduleConfig = {
            type: "daily",
            time: formScheduleTime,
          };
        }
        
        // 构建风控配置
        riskConfig = {
          max_position_pct: formMaxPositionPct,
          max_total_position_pct: formMaxTotalPositionPct,
          max_risk_per_trade: formMaxRiskPerTrade,
        };
      }

      const res = await fetch(`${API_BASE}/strategies/config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          strategy_id: formStrategyId,
          strategy_name: formStrategyName,
          strategy_type: formStrategyType,
          description: formDescription,
          enabled: formEnabled,
          config_json: configJson,
          schedule_config: scheduleConfig,
          risk_config: riskConfig,
        }),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "保存失败");
      }

      alert("保存成功");
      setShowAddModal(false);
      resetForm();
      loadData();
    } catch (e: any) {
      alert(e?.message || "保存失败");
    }
  }

  async function handleDeleteStrategy(strategyId: string) {
    if (!confirm("确定要删除这个策略吗？")) return;

    try {
      const res = await fetch(`${API_BASE}/strategies/config/${strategyId}`, {
        method: "DELETE",
      });

      if (!res.ok) throw new Error("删除失败");

      alert("删除成功");
      loadData();
    } catch (e: any) {
      alert(e?.message || "删除失败");
    }
  }

  async function handleToggleEnabled(strategyId: string, enabled: boolean) {
    try {
      const res = await fetch(
        `${API_BASE}/strategies/config/${strategyId}/enable?enabled=${!enabled}`,
        {
          method: "PATCH",
        }
      );

      if (!res.ok) throw new Error("操作失败");

      loadData();
    } catch (e: any) {
      alert(e?.message || "操作失败");
    }
  }

  function resetForm() {
    setFormStrategyId("");
    setFormStrategyName("");
    setFormStrategyType("MA_CROSS");
    setFormDescription("");
    setFormEnabled(true);
    
    // 重置表单参数
    setFormMaShort(5);
    setFormMaLong(20);
    setFormMaPeriod(20);
    setFormVolumeRatio(1.5);
    setFormPeriod("1d");
    setFormSymbols("");
    setFormPositionSize(0.1);
    setFormPriceType("LIMIT");
    
    setFormScheduleType("daily");
    setFormScheduleTime("09:30");
    setFormScheduleInterval(15);
    
    setFormMaxPositionPct(0.2);
    setFormMaxTotalPositionPct(0.8);
    setFormMaxRiskPerTrade(0.02);
    
    setFormConfigJson("{}");
    setFormScheduleConfig("{}");
    setFormRiskConfig("{}");
    setUseAdvancedMode(false);
    setEditingStrategy(null);
  }

  function handleEdit(strategy: Strategy) {
    setEditingStrategy(strategy);
    setFormStrategyId(strategy.strategy_id);
    setFormStrategyName(strategy.strategy_name);
    setFormStrategyType(strategy.strategy_type);
    setFormDescription(strategy.description);
    setFormEnabled(strategy.enabled);
    
    // 从配置中提取参数
    const config = strategy.config || {};
    if (strategy.strategy_type === "MA_CROSS") {
      setFormMaShort(config.ma_short || 5);
      setFormMaLong(config.ma_long || 20);
      setFormPeriod(config.period || "1d");
      setFormSymbols((config.symbols || []).join("\n"));
      setFormPositionSize(config.position_size || 0.1);
      setFormPriceType(config.price_type || "LIMIT");
    } else if (strategy.strategy_type === "TREND_FOLLOWING") {
      setFormMaPeriod(config.ma_period || 20);
      setFormVolumeRatio(config.volume_ratio || 1.5);
      setFormPeriod(config.period || "1d");
      setFormSymbols((config.symbols || []).join("\n"));
      setFormPositionSize(config.position_size || 0.1);
      setFormPriceType(config.price_type || "LIMIT");
    } else {
      setFormSymbols((config.symbols || []).join("\n"));
      setFormPositionSize(config.position_size || 0.1);
      setFormPriceType(config.price_type || "LIMIT");
    }
    
    // 调度配置
    const scheduleConfig = strategy.schedule_config || {};
    setFormScheduleType(scheduleConfig.type || "daily");
    setFormScheduleTime(scheduleConfig.time || "09:30");
    setFormScheduleInterval(scheduleConfig.interval || 15);
    
    // 风控配置
    const riskConfig = strategy.risk_config || {};
    setFormMaxPositionPct(riskConfig.max_position_pct || 0.2);
    setFormMaxTotalPositionPct(riskConfig.max_total_position_pct || 0.8);
    setFormMaxRiskPerTrade(riskConfig.max_risk_per_trade || 0.02);
    
    // JSON配置（用于显示）
    setFormConfigJson(JSON.stringify(config, null, 2));
    setFormScheduleConfig(JSON.stringify(scheduleConfig, null, 2));
    setFormRiskConfig(JSON.stringify(riskConfig, null, 2));
    setUseAdvancedMode(false);
    setShowAddModal(true);
  }

  function handleAddNew() {
    resetForm();
    setShowAddModal(true);
  }

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>📊 策略管理</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          管理交易策略配置、查看交易意图和执行记录
        </p>
      </section>

      {/* QMT 连接状态 */}
      <section
        style={{
          marginBottom: 16,
          padding: 12,
          borderRadius: 8,
          border: "1px solid #e5e7eb",
          backgroundColor: "#f9fafb",
          fontSize: 13,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontWeight: 600 }}>QMT 连接状态：</span>
          {qmtConnecting && <span>正在连接...</span>}
          {!qmtConnecting && qmtStatus && (
            <span>
              {qmtStatus.enabled ? (
                qmtStatus.connected ? (
                  <>
                    已连接（模式: {qmtStatus.mode || "未知"}, 账户: {qmtStatus.account_id || "未配置"}
                    )
                  </>
                ) : (
                  <>未连接（已启用，请检查 QMT 终端或 /qmt/connect 接口）</>
                )
              ) : (
                <>未启用（MINIQMT_ENABLED=false）</>
              )}
            </span>
          )}
        </div>
        {qmtError && (
          <div style={{ marginTop: 4, color: "#b91c1c" }}>错误：{qmtError}</div>
        )}
      </section>

      <div
        style={{
          display: "flex",
          gap: 8,
          marginBottom: 16,
        }}
      >
        <button
          type="button"
          onClick={() => setActiveTab("strategies")}
          style={{
            padding: "8px 16px",
            borderRadius: 999,
            border: "none",
            cursor: "pointer",
            fontWeight: 600,
            backgroundColor:
              activeTab === "strategies" ? "#4f46e5" : "rgba(15,23,42,0.06)",
            color: activeTab === "strategies" ? "#fff" : "#111827",
          }}
        >
          📋 策略配置
        </button>
        <button
          type="button"
          onClick={() => setActiveTab("intents")}
          style={{
            padding: "8px 16px",
            borderRadius: 999,
            border: "none",
            cursor: "pointer",
            fontWeight: 600,
            backgroundColor:
              activeTab === "intents" ? "#4f46e5" : "rgba(15,23,42,0.06)",
            color: activeTab === "intents" ? "#fff" : "#111827",
          }}
        >
          💼 交易意图
        </button>
        <button
          type="button"
          onClick={() => setActiveTab("executions")}
          style={{
            padding: "8px 16px",
            borderRadius: 999,
            border: "none",
            cursor: "pointer",
            fontWeight: 600,
            backgroundColor:
              activeTab === "executions" ? "#4f46e5" : "rgba(15,23,42,0.06)",
            color: activeTab === "executions" ? "#fff" : "#111827",
          }}
        >
          📈 执行记录
        </button>
        <button
          type="button"
          onClick={() => setActiveTab("monitor")}
          style={{
            padding: "8px 16px",
            borderRadius: 999,
            border: "none",
            cursor: "pointer",
            fontWeight: 600,
            backgroundColor:
              activeTab === "monitor" ? "#4f46e5" : "rgba(15,23,42,0.06)",
            color: activeTab === "monitor" ? "#fff" : "#111827",
          }}
        >
          🚨 策略监控
        </button>
      </div>

      {error && (
        <div
          style={{
            padding: 12,
            background: "#fee",
            border: "1px solid #fcc",
            borderRadius: 8,
            marginBottom: 16,
            color: "#c00",
          }}
        >
          {error}
        </div>
      )}

      {activeTab === "strategies" && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 20,
            boxShadow: "0 1px 3px rgba(0,0,0,0.1)",
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
            <h2 style={{ margin: 0 }}>策略列表</h2>
            <button
              type="button"
              onClick={handleAddNew}
              style={{
                padding: "8px 16px",
                background: "#4f46e5",
                color: "#fff",
                border: "none",
                borderRadius: 6,
                cursor: "pointer",
                fontWeight: 600,
              }}
            >
              + 新建策略
            </button>
          </div>

          {loading ? (
            <div>加载中...</div>
          ) : strategies.length === 0 ? (
            <div style={{ padding: 40, textAlign: "center", color: "#666" }}>
              暂无策略配置
            </div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ background: "#f5f5f5" }}>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>策略ID</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>策略名称</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>类型</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>状态</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {strategies.map((s) => (
                    <tr key={s.strategy_id} style={{ borderBottom: "1px solid #eee" }}>
                      <td style={{ padding: 12 }}>{s.strategy_id}</td>
                      <td style={{ padding: 12 }}>{s.strategy_name}</td>
                      <td style={{ padding: 12 }}>{s.strategy_type}</td>
                      <td style={{ padding: 12 }}>
                        <span
                          style={{
                            padding: "4px 8px",
                            borderRadius: 4,
                            background: s.enabled ? "#d4edda" : "#f8d7da",
                            color: s.enabled ? "#155724" : "#721c24",
                            fontSize: 12,
                          }}
                        >
                          {s.enabled ? "启用" : "禁用"}
                        </span>
                      </td>
                      <td style={{ padding: 12 }}>
                        <div style={{ display: "flex", gap: 8 }}>
                          <button
                            type="button"
                            onClick={() => handleToggleEnabled(s.strategy_id, s.enabled)}
                            style={{
                              padding: "4px 8px",
                              background: s.enabled ? "#ffc107" : "#28a745",
                              color: "#fff",
                              border: "none",
                              borderRadius: 4,
                              cursor: "pointer",
                              fontSize: 12,
                            }}
                          >
                            {s.enabled ? "禁用" : "启用"}
                          </button>
                          <button
                            type="button"
                            onClick={() => handleEdit(s)}
                            style={{
                              padding: "4px 8px",
                              background: "#17a2b8",
                              color: "#fff",
                              border: "none",
                              borderRadius: 4,
                              cursor: "pointer",
                              fontSize: 12,
                            }}
                          >
                            编辑
                          </button>
                          <button
                            type="button"
                            onClick={() => handleDeleteStrategy(s.strategy_id)}
                            style={{
                              padding: "4px 8px",
                              background: "#dc3545",
                              color: "#fff",
                              border: "none",
                              borderRadius: 4,
                              cursor: "pointer",
                              fontSize: 12,
                            }}
                          >
                            删除
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {activeTab === "monitor" && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 20,
            boxShadow: "0 1px 3px rgba(0,0,0,0.1)",
          }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: 16,
            }}
          >
            <h2 style={{ margin: 0 }}>QMT 策略级监控</h2>
            <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13 }}>
              <span>选择策略：</span>
              <select
                value={selectedMonitorStrategyId}
                onChange={async (e) => {
                  const sid = e.target.value;
                  setSelectedMonitorStrategyId(sid);
                  await loadSingleStrategyMonitor(sid);
                }}
                style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ddd" }}
              >
                <option value="">请选择策略</option>
                {strategyMonitors.map((s) => (
                  <option key={s.strategy_id} value={s.strategy_id}>
                    {s.strategy_id}
                  </option>
                ))}
              </select>
              <button
                type="button"
                onClick={loadData}
                style={{
                  padding: "4px 10px",
                  borderRadius: 4,
                  border: "1px solid #ddd",
                  background: "#fff",
                  cursor: "pointer",
                }}
              >
                刷新
              </button>
            </div>
          </div>

          {loading && <div>加载中...</div>}

          {!loading && strategyMonitors.length === 0 && (
            <div style={{ padding: 40, textAlign: "center", color: "#666" }}>
              暂无策略监控数据（请确认有策略通过 QMT 下过单）
            </div>
          )}

          {!loading && strategyMonitors.length > 0 && (
            <>
              {/* 总览表 */}
              <div style={{ overflowX: "auto", marginBottom: 24 }}>
                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ background: "#f5f5f5" }}>
                      <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>策略ID</th>
                      <th style={{ padding: 12, textAlign: "right", borderBottom: "2px solid #ddd" }}>委托数</th>
                      <th style={{ padding: 12, textAlign: "right", borderBottom: "2px solid #ddd" }}>成交数</th>
                      <th style={{ padding: 12, textAlign: "right", borderBottom: "2px solid #ddd" }}>持仓总盈亏</th>
                      <th style={{ padding: 12, textAlign: "right", borderBottom: "2px solid #ddd" }}>当日盈亏</th>
                      <th style={{ padding: 12, textAlign: "right", borderBottom: "2px solid #ddd" }}>市值</th>
                      <th style={{ padding: 12, textAlign: "right", borderBottom: "2px solid #ddd" }}>资产占比</th>
                    </tr>
                  </thead>
                  <tbody>
                    {strategyMonitors.map((m) => {
                      const totalPnl = m.total_position_profit || 0;
                      const dailyPnl = m.total_daily_profit || 0;
                      return (
                        <tr
                          key={m.strategy_id}
                          style={{
                            borderBottom: "1px solid #eee",
                            backgroundColor:
                              selectedMonitorStrategyId === m.strategy_id ? "#eef2ff" : "transparent",
                            cursor: "pointer",
                          }}
                          onClick={async () => {
                            setSelectedMonitorStrategyId(m.strategy_id);
                            await loadSingleStrategyMonitor(m.strategy_id);
                          }}
                        >
                          <td style={{ padding: 12 }}>{m.strategy_id}</td>
                          <td style={{ padding: 12, textAlign: "right" }}>{m.orders_count}</td>
                          <td style={{ padding: 12, textAlign: "right" }}>{m.trades_count}</td>
                          <td
                            style={{
                              padding: 12,
                              textAlign: "right",
                              color: totalPnl > 0 ? "#b91c1c" : totalPnl < 0 ? "#15803d" : "#111827",
                            }}
                          >
                            {totalPnl.toFixed(2)}
                          </td>
                          <td
                            style={{
                              padding: 12,
                              textAlign: "right",
                              color: dailyPnl > 0 ? "#b91c1c" : dailyPnl < 0 ? "#15803d" : "#111827",
                            }}
                          >
                            {dailyPnl.toFixed(2)}
                          </td>
                          <td style={{ padding: 12, textAlign: "right" }}>{m.market_value.toFixed(2)}</td>
                          <td style={{ padding: 12, textAlign: "right" }}>{(m.weight_asset * 100).toFixed(2)}%</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {/* 单个策略详情 */}
              {strategyMonitorSummary && (
                <div
                  style={{
                    marginTop: 8,
                    paddingTop: 16,
                    borderTop: "1px solid #e5e7eb",
                  }}
                >
                  <h3 style={{ marginTop: 0, marginBottom: 12 }}>当前策略详情：{strategyMonitorSummary.strategy_id}</h3>

                  {/* 概要卡片 */}
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
                      gap: 12,
                      marginBottom: 16,
                    }}
                  >
                    <div
                      style={{
                        padding: 12,
                        borderRadius: 8,
                        background: "#f9fafb",
                        border: "1px solid #e5e7eb",
                      }}
                    >
                      <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>持仓总盈亏</div>
                      <div
                        style={{
                          fontSize: 18,
                          fontWeight: 600,
                          color:
                            strategyMonitorSummary.pnl.total_position_profit > 0
                              ? "#b91c1c"
                              : strategyMonitorSummary.pnl.total_position_profit < 0
                              ? "#15803d"
                              : "#111827",
                        }}
                      >
                        {strategyMonitorSummary.pnl.total_position_profit.toFixed(2)}
                      </div>
                    </div>
                    <div
                      style={{
                        padding: 12,
                        borderRadius: 8,
                        background: "#f9fafb",
                        border: "1px solid #e5e7eb",
                      }}
                    >
                      <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>当日盈亏</div>
                      <div
                        style={{
                          fontSize: 18,
                          fontWeight: 600,
                          color:
                            strategyMonitorSummary.pnl.total_daily_profit > 0
                              ? "#b91c1c"
                              : strategyMonitorSummary.pnl.total_daily_profit < 0
                              ? "#15803d"
                              : "#111827",
                        }}
                      >
                        {strategyMonitorSummary.pnl.total_daily_profit.toFixed(2)}
                      </div>
                    </div>
                    <div
                      style={{
                        padding: 12,
                        borderRadius: 8,
                        background: "#f9fafb",
                        border: "1px solid #e5e7eb",
                      }}
                    >
                      <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>策略相关市值</div>
                      <div style={{ fontSize: 18, fontWeight: 600 }}>
                        {strategyMonitorSummary.pnl.market_value.toFixed(2)}
                      </div>
                    </div>
                    <div
                      style={{
                        padding: 12,
                        borderRadius: 8,
                        background: "#f9fafb",
                        border: "1px solid #e5e7eb",
                      }}
                    >
                      <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>资产占比</div>
                      <div style={{ fontSize: 18, fontWeight: 600 }}>
                        {(strategyMonitorSummary.pnl.weight_asset * 100).toFixed(2)}%
                      </div>
                    </div>
                  </div>

                  {/* 持仓明细表 */}
                  <div style={{ marginTop: 12 }}>
                    <h4 style={{ marginTop: 0, marginBottom: 8 }}>相关持仓</h4>
                    {strategyMonitorSummary.positions.length === 0 ? (
                      <div style={{ padding: 12, color: "#6b7280" }}>当前策略暂无关联持仓</div>
                    ) : (
                      <div style={{ overflowX: "auto" }}>
                        <table style={{ width: "100%", borderCollapse: "collapse" }}>
                          <thead>
                            <tr style={{ background: "#f5f5f5" }}>
                              <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #ddd" }}>代码</th>
                              <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #ddd" }}>名称</th>
                              <th style={{ padding: 8, textAlign: "right", borderBottom: "2px solid #ddd" }}>数量</th>
                              <th style={{ padding: 8, textAlign: "right", borderBottom: "2px solid #ddd" }}>市值</th>
                              <th style={{ padding: 8, textAlign: "right", borderBottom: "2px solid #ddd" }}>持仓盈亏</th>
                              <th style={{ padding: 8, textAlign: "right", borderBottom: "2px solid #ddd" }}>当日盈亏</th>
                              <th style={{ padding: 8, textAlign: "right", borderBottom: "2px solid #ddd" }}>资产占比</th>
                            </tr>
                          </thead>
                          <tbody>
                            {strategyMonitorSummary.positions.map((p, idx) => {
                              const posPnl = Number(p.position_profit || 0);
                              const dailyPnl = Number(p.float_profit || 0);
                              const mv = Number(p.market_value || 0);
                              const weightAsset = Number(p.weight_asset || 0);
                              return (
                                <tr key={idx} style={{ borderBottom: "1px solid #eee" }}>
                                  <td style={{ padding: 8 }}>{p.stock_code}</td>
                                  <td style={{ padding: 8 }}>{p.stock_name}</td>
                                  <td style={{ padding: 8, textAlign: "right" }}>{p.quantity}</td>
                                  <td style={{ padding: 8, textAlign: "right" }}>{mv.toFixed(2)}</td>
                                  <td
                                    style={{
                                      padding: 8,
                                      textAlign: "right",
                                      color: posPnl > 0 ? "#b91c1c" : posPnl < 0 ? "#15803d" : "#111827",
                                    }}
                                  >
                                    {posPnl.toFixed(2)}
                                  </td>
                                  <td
                                    style={{
                                      padding: 8,
                                      textAlign: "right",
                                      color: dailyPnl > 0 ? "#b91c1c" : dailyPnl < 0 ? "#15803d" : "#111827",
                                    }}
                                  >
                                    {dailyPnl.toFixed(2)}
                                  </td>
                                  <td style={{ padding: 8, textAlign: "right" }}>{(weightAsset * 100).toFixed(2)}%</td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </>
          )}
        </section>
      )}

      {activeTab === "intents" && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 20,
            boxShadow: "0 1px 3px rgba(0,0,0,0.1)",
          }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: 16,
            }}
          >
            <h2 style={{ margin: 0 }}>交易意图记录</h2>
            <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13 }}>
              <span>自动刷新间隔：</span>
              <select
                value={intentRefreshIntervalSec}
                onChange={(e) => setIntentRefreshIntervalSec(Number(e.target.value))}
                aria-label="交易意图自动刷新间隔"
                style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ddd" }}
              >
                <option value={5}>5 秒</option>
                <option value={10}>10 秒</option>
                <option value={30}>30 秒</option>
                <option value={60}>1 分钟</option>
              </select>
              <button
                type="button"
                onClick={loadData}
                style={{
                  padding: "4px 10px",
                  borderRadius: 4,
                  border: "1px solid #ddd",
                  background: "#fff",
                  cursor: "pointer",
                }}
              >
                手动刷新
              </button>
            </div>
          </div>
          {loading ? (
            <div>加载中...</div>
          ) : intents.length === 0 ? (
            <div style={{ padding: 40, textAlign: "center", color: "#666" }}>
              暂无交易意图记录
            </div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ background: "#f5f5f5" }}>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>策略ID</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>股票</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>方向</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>数量</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>价格</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>意图状态</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>QMT委托状态</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>成交(股/价)</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>订单ID</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>创建时间</th>
                  </tr>
                </thead>
                <tbody>
                  {intents.map((i) => (
                    <tr key={i.id} style={{ borderBottom: "1px solid #eee" }}>
                      <td style={{ padding: 12 }}>{i.strategy_id}</td>
                      <td style={{ padding: 12 }}>{i.symbol}</td>
                      <td style={{ padding: 12 }}>{i.side}</td>
                      <td style={{ padding: 12 }}>{i.quantity}</td>
                      <td style={{ padding: 12 }}>{i.price?.toFixed(2) || "-"}</td>
                      <td style={{ padding: 12 }}>
                        <span
                          style={{
                            padding: "4px 8px",
                            borderRadius: 4,
                            background:
                              i.status === "EXECUTED"
                                ? "#d4edda"
                                : i.status === "FAILED"
                                ? "#f8d7da"
                                : "#fff3cd",
                            color:
                              i.status === "EXECUTED"
                                ? "#155724"
                                : i.status === "FAILED"
                                ? "#721c24"
                                : "#856404",
                            fontSize: 12,
                          }}
                        >
                          {i.status}
                        </span>
                      </td>
                      <td style={{ padding: 12 }}>
                        {i.latest_order_status_msg || "-"}
                      </td>
                      <td style={{ padding: 12 }}>
                        {i.traded_volume != null && i.traded_volume > 0
                          ? `${i.traded_volume} / ${
                              i.traded_price != null ? i.traded_price.toFixed(2) : "-"
                            }`
                          : "-"}
                      </td>
                      <td style={{ padding: 12 }}>{i.order_id || "-"}</td>
                      <td style={{ padding: 12 }}>{formatDateTime(i.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {activeTab === "executions" && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 20,
            boxShadow: "0 1px 3px rgba(0,0,0,0.1)",
          }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: 16,
            }}
          >
            <h2 style={{ margin: 0 }}>策略执行记录</h2>
            <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13 }}>
              <span>按策略筛选：</span>
              <select
                value={executionStrategyFilter}
                onChange={(e) => {
                  setExecutionStrategyFilter(e.target.value);
                  // 立刻刷新当前标签页数据
                  // 注意：依赖 activeTab 和 executionStrategyFilter 的 useEffect 不存在，这里直接调用
                  setTimeout(() => {
                    loadData();
                  }, 0);
                }}
                style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ddd" }}
              >
                <option value="">全部策略</option>
                {strategies.map((s) => (
                  <option key={s.strategy_id} value={s.strategy_id}>
                    {s.strategy_id} - {s.strategy_name}
                  </option>
                ))}
              </select>
            </div>
          </div>
          {loading ? (
            <div>加载中...</div>
          ) : executions.length === 0 ? (
            <div style={{ padding: 40, textAlign: "center", color: "#666" }}>
              暂无执行记录
            </div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ background: "#f5f5f5" }}>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>策略ID</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>执行类型</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>状态</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>处理股票数</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>生成信号数</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>执行信号数</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>开始时间</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>关联委托/成交</th>
                  </tr>
                </thead>
                <tbody>
                  {executions.map((e) => {
                    const orders = e.qmt_orders || [];
                    const trades = e.qmt_trades || [];
                    return (
                      <>
                        <tr key={e.id} style={{ borderBottom: "1px solid #eee" }}>
                          <td style={{ padding: 12 }}>{e.strategy_id}</td>
                          <td style={{ padding: 12 }}>{e.execution_type}</td>
                          <td style={{ padding: 12 }}>
                            <span
                              style={{
                                padding: "4px 8px",
                                borderRadius: 4,
                                background:
                                  e.status === "SUCCESS"
                                    ? "#d4edda"
                                    : e.status === "FAILED"
                                    ? "#f8d7da"
                                    : "#fff3cd",
                                color:
                                  e.status === "SUCCESS"
                                    ? "#155724"
                                    : e.status === "FAILED"
                                    ? "#721c24"
                                    : "#856404",
                                fontSize: 12,
                              }}
                            >
                              {e.status}
                            </span>
                          </td>
                          <td style={{ padding: 12 }}>{e.symbols_processed}</td>
                          <td style={{ padding: 12 }}>{e.signals_generated}</td>
                          <td style={{ padding: 12 }}>{e.signals_executed}</td>
                          <td style={{ padding: 12 }}>{formatDateTime(e.start_time)}</td>
                          <td style={{ padding: 12 }}>
                            委托 {orders.length} 笔 / 成交 {trades.length} 笔
                          </td>
                        </tr>
                        {(orders.length > 0 || trades.length > 0) && (
                          <tr>
                            <td
                              colSpan={8}
                              style={{
                                padding: 12,
                                borderBottom: "1px solid #eee",
                                backgroundColor: "#fafafa",
                              }}
                            >
                              <div
                                style={{
                                  display: "flex",
                                  gap: 16,
                                  flexWrap: "wrap",
                                }}
                              >
                                {orders.length > 0 && (
                                  <div style={{ fontSize: 12, minWidth: 260 }}>
                                    <div style={{ fontWeight: 600, marginBottom: 4 }}>
                                      关联委托
                                    </div>
                                    <table
                                      style={{
                                        width: "100%",
                                        borderCollapse: "collapse",
                                      }}
                                    >
                                      <thead>
                                        <tr>
                                          <th
                                            style={{
                                              textAlign: "left",
                                              padding: 4,
                                              borderBottom: "1px solid #e5e7eb",
                                            }}
                                          >
                                            代码
                                          </th>
                                          <th
                                            style={{
                                              textAlign: "right",
                                              padding: 4,
                                              borderBottom: "1px solid #e5e7eb",
                                            }}
                                          >
                                            量/价
                                          </th>
                                          <th
                                            style={{
                                              textAlign: "left",
                                              padding: 4,
                                              borderBottom: "1px solid #e5e7eb",
                                            }}
                                          >
                                            状态
                                          </th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {orders.map((o, idx) => (
                                          <tr key={idx}>
                                            <td
                                              style={{
                                                padding: 4,
                                                borderBottom: "1px solid #f3f4f6",
                                              }}
                                            >
                                              {o.stock_code}
                                            </td>
                                            <td
                                              style={{
                                                padding: 4,
                                                borderBottom: "1px solid #f3f4f6",
                                                textAlign: "right",
                                              }}
                                            >
                                              {o.order_volume} / {o.price}
                                            </td>
                                            <td
                                              style={{
                                                padding: 4,
                                                borderBottom: "1px solid #f3f4f6",
                                              }}
                                            >
                                              {o.status_msg || `状态: ${o.order_status}`}
                                            </td>
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>
                                )}
                                {trades.length > 0 && (
                                  <div style={{ fontSize: 12, minWidth: 260 }}>
                                    <div style={{ fontWeight: 600, marginBottom: 4 }}>
                                      关联成交
                                    </div>
                                    <table
                                      style={{
                                        width: "100%",
                                        borderCollapse: "collapse",
                                      }}
                                    >
                                      <thead>
                                        <tr>
                                          <th
                                            style={{
                                              textAlign: "left",
                                              padding: 4,
                                              borderBottom: "1px solid #e5e7eb",
                                            }}
                                          >
                                            代码
                                          </th>
                                          <th
                                            style={{
                                              textAlign: "right",
                                              padding: 4,
                                              borderBottom: "1px solid #e5e7eb",
                                            }}
                                          >
                                            量/价
                                          </th>
                                          <th
                                            style={{
                                              textAlign: "left",
                                              padding: 4,
                                              borderBottom: "1px solid #e5e7eb",
                                            }}
                                          >
                                            时间
                                          </th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {trades.map((t, idx) => (
                                          <tr key={idx}>
                                            <td
                                              style={{
                                                padding: 4,
                                                borderBottom: "1px solid #f3f4f6",
                                              }}
                                            >
                                              {t.stock_code}
                                            </td>
                                            <td
                                              style={{
                                                padding: 4,
                                                borderBottom: "1px solid #f3f4f6",
                                                textAlign: "right",
                                              }}
                                            >
                                              {t.traded_volume} / {t.traded_price}
                                            </td>
                                            <td
                                              style={{
                                                padding: 4,
                                                borderBottom: "1px solid #f3f4f6",
                                              }}
                                            >
                                              {t.traded_time}
                                            </td>
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>
                                )}
                              </div>
                            </td>
                          </tr>
                        )}
                      </>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {showAddModal && (
        <div
          style={{
            position: "fixed",
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: "rgba(0,0,0,0.5)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 24,
              maxWidth: 600,
              width: "90%",
              maxHeight: "90vh",
              overflow: "auto",
            }}
          >
            <h2 style={{ margin: "0 0 20px 0" }}>
              {editingStrategy ? "编辑策略" : "新建策略"}
            </h2>

            <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  策略ID *
                </label>
                <input
                  type="text"
                  value={formStrategyId}
                  onChange={(e) => setFormStrategyId(e.target.value)}
                  placeholder="例如: ma_cross_001"
                  style={{ width: "100%", padding: "8px 12px", border: "1px solid #ddd", borderRadius: 6 }}
                  disabled={!!editingStrategy}
                />
              </div>

              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  策略名称 *
                </label>
                <input
                  type="text"
                  value={formStrategyName}
                  onChange={(e) => setFormStrategyName(e.target.value)}
                  placeholder="例如: 双均线策略"
                  style={{ width: "100%", padding: "8px 12px", border: "1px solid #ddd", borderRadius: 6 }}
                />
              </div>

              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  策略类型 *
                </label>
                <select
                  value={formStrategyType}
                  onChange={(e) => setFormStrategyType(e.target.value)}
                  style={{ width: "100%", padding: "8px 12px", border: "1px solid #ddd", borderRadius: 6 }}
                >
                  <option value="MA_CROSS">双均线策略</option>
                  <option value="TREND_FOLLOWING">趋势跟踪策略</option>
                  <option value="GRID">网格交易策略</option>
                </select>
              </div>

              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  描述
                </label>
                <textarea
                  value={formDescription}
                  onChange={(e) => setFormDescription(e.target.value)}
                  placeholder="策略描述"
                  rows={3}
                  style={{ width: "100%", padding: "8px 12px", border: "1px solid #ddd", borderRadius: 6 }}
                />
              </div>

              <div>
                <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <input
                    type="checkbox"
                    checked={formEnabled}
                    onChange={(e) => setFormEnabled(e.target.checked)}
                  />
                  <span>启用策略</span>
                </label>
              </div>

              <div style={{ marginTop: 8, marginBottom: 8 }}>
                <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <input
                    type="checkbox"
                    checked={useAdvancedMode}
                    onChange={(e) => setUseAdvancedMode(e.target.checked)}
                  />
                  <span style={{ fontSize: 12, color: "#666" }}>高级模式（使用JSON配置）</span>
                </label>
              </div>

              {!useAdvancedMode ? (
                <>
                  {/* 策略参数配置 */}
                  <div style={{ borderTop: "1px solid #eee", paddingTop: 16 }}>
                    <h3 style={{ margin: "0 0 12px 0", fontSize: 16 }}>策略参数</h3>
                    
                    {formStrategyType === "MA_CROSS" && (
                      <>
                        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                          <div>
                            <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                              短期均线周期 *
                            </label>
                            <input
                              type="number"
                              value={formMaShort}
                              onChange={(e) => setFormMaShort(parseInt(e.target.value) || 5)}
                              min={1}
                              max={100}
                              style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                            />
                          </div>
                          <div>
                            <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                              长期均线周期 *
                            </label>
                            <input
                              type="number"
                              value={formMaLong}
                              onChange={(e) => setFormMaLong(parseInt(e.target.value) || 20)}
                              min={1}
                              max={200}
                              style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                            />
                          </div>
                        </div>
                      </>
                    )}

                    {formStrategyType === "TREND_FOLLOWING" && (
                      <>
                        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                          <div>
                            <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                              均线周期 *
                            </label>
                            <input
                              type="number"
                              value={formMaPeriod}
                              onChange={(e) => setFormMaPeriod(parseInt(e.target.value) || 20)}
                              min={1}
                              max={200}
                              style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                            />
                          </div>
                          <div>
                            <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                              成交量放大倍数 *
                            </label>
                            <input
                              type="number"
                              step="0.1"
                              value={formVolumeRatio}
                              onChange={(e) => setFormVolumeRatio(parseFloat(e.target.value) || 1.5)}
                              min={1}
                              max={10}
                              style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                            />
                          </div>
                        </div>
                      </>
                    )}

                    <div style={{ marginTop: 12 }}>
                      <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                        数据周期 *
                      </label>
                      <select
                        value={formPeriod}
                        onChange={(e) => setFormPeriod(e.target.value)}
                        style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                      >
                        <option value="1m">1分钟线</option>
                        <option value="5m">5分钟线</option>
                        <option value="15m">15分钟线（日内交易）</option>
                        <option value="30m">30分钟线</option>
                        <option value="1d">日线（日频交易）</option>
                      </select>
                      <div style={{ fontSize: 11, color: "#666", marginTop: 4 }}>
                        {formPeriod === "15m" && "适合日内交易，每15分钟执行"}
                        {formPeriod === "1d" && "适合日频交易，每日执行"}
                      </div>
                    </div>

                    <div style={{ marginTop: 12 }}>
                      <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                        股票代码列表 *（每行一个，或逗号分隔）
                      </label>
                      <textarea
                        value={formSymbols}
                        onChange={(e) => setFormSymbols(e.target.value)}
                        placeholder="600519.SH&#10;000001.SZ&#10;或: 600519.SH, 000001.SZ"
                        rows={4}
                        style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6, fontSize: 12 }}
                      />
                      <div style={{ fontSize: 11, color: "#666", marginTop: 4 }}>
                        格式：600519.SH（上海）或 000001.SZ（深圳），如果只输入6位数字会自动识别市场
                      </div>
                    </div>

                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginTop: 12 }}>
                      <div>
                        <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                          仓位大小 *
                        </label>
                        <input
                          type="number"
                          step="0.01"
                          value={formPositionSize}
                          onChange={(e) => setFormPositionSize(parseFloat(e.target.value) || 0.1)}
                          min={0.01}
                          max={1}
                          style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                        />
                        <div style={{ fontSize: 11, color: "#666", marginTop: 2 }}>
                          {formPositionSize * 100}% 仓位
                        </div>
                      </div>
                      <div>
                        <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                          价格类型 *
                        </label>
                        <select
                          value={formPriceType}
                          onChange={(e) => setFormPriceType(e.target.value)}
                          style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                        >
                          <option value="LIMIT">限价</option>
                          <option value="MARKET">市价</option>
                        </select>
                      </div>
                    </div>
                  </div>

                  {/* 调度配置 */}
                  <div style={{ borderTop: "1px solid #eee", paddingTop: 16, marginTop: 16 }}>
                    <h3 style={{ margin: "0 0 12px 0", fontSize: 16 }}>执行方式</h3>
                    
                    <div>
                      <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                        执行类型 *
                      </label>
                      <select
                        value={formScheduleType}
                        onChange={(e) => setFormScheduleType(e.target.value)}
                        style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                      >
                        <option value="daily">日频（每日执行）</option>
                        <option value="realtime">实时（行情触发）</option>
                        <option value="minute">分钟级（定时执行）</option>
                        <option value="hourly">小时级（每小时执行）</option>
                      </select>
                      <div style={{ fontSize: 11, color: "#666", marginTop: 4 }}>
                        {formScheduleType === "daily" && "适合日频策略，每日固定时间执行"}
                        {formScheduleType === "realtime" && "实时行情触发，收到行情更新时自动执行"}
                        {formScheduleType === "minute" && "适合日内策略，每N分钟执行一次"}
                        {formScheduleType === "hourly" && "每小时执行一次"}
                      </div>
                    </div>

                    {formScheduleType === "daily" && (
                      <div style={{ marginTop: 12 }}>
                        <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                          执行时间 *
                        </label>
                        <input
                          type="time"
                          value={formScheduleTime}
                          onChange={(e) => setFormScheduleTime(e.target.value)}
                          style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                        />
                      </div>
                    )}

                    {formScheduleType === "minute" && (
                      <div style={{ marginTop: 12 }}>
                        <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                          执行间隔（分钟） *
                        </label>
                        <input
                          type="number"
                          value={formScheduleInterval}
                          onChange={(e) => setFormScheduleInterval(parseInt(e.target.value) || 15)}
                          min={1}
                          max={60}
                          style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                        />
                        <div style={{ fontSize: 11, color: "#666", marginTop: 4 }}>
                          每 {formScheduleInterval} 分钟执行一次
                        </div>
                      </div>
                    )}

                    {formScheduleType === "hourly" && (
                      <div style={{ marginTop: 12 }}>
                        <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                          执行时间（分钟） *
                        </label>
                        <input
                          type="number"
                          value={formScheduleTime.split(":")[1] || "0"}
                          onChange={(e) => {
                            const min = e.target.value || "0";
                            setFormScheduleTime(`:${min.padStart(2, "0")}`);
                          }}
                          min={0}
                          max={59}
                          style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                        />
                        <div style={{ fontSize: 11, color: "#666", marginTop: 4 }}>
                          每小时的第 {formScheduleTime.split(":")[1] || "0"} 分钟执行
                        </div>
                      </div>
                    )}
                  </div>

                  {/* 风控配置 */}
                  <div style={{ borderTop: "1px solid #eee", paddingTop: 16, marginTop: 16 }}>
                    <h3 style={{ margin: "0 0 12px 0", fontSize: 16 }}>风控参数（可选）</h3>
                    
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                      <div>
                        <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                          单股最大仓位比例
                        </label>
                        <input
                          type="number"
                          step="0.01"
                          value={formMaxPositionPct}
                          onChange={(e) => setFormMaxPositionPct(parseFloat(e.target.value) || 0.2)}
                          min={0}
                          max={1}
                          style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                        />
                        <div style={{ fontSize: 11, color: "#666", marginTop: 2 }}>
                          {formMaxPositionPct * 100}%
                        </div>
                      </div>
                      <div>
                        <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                          总仓位最大比例
                        </label>
                        <input
                          type="number"
                          step="0.01"
                          value={formMaxTotalPositionPct}
                          onChange={(e) => setFormMaxTotalPositionPct(parseFloat(e.target.value) || 0.8)}
                          min={0}
                          max={1}
                          style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                        />
                        <div style={{ fontSize: 11, color: "#666", marginTop: 2 }}>
                          {formMaxTotalPositionPct * 100}%
                        </div>
                      </div>
                    </div>

                    <div style={{ marginTop: 12 }}>
                      <label style={{ display: "block", marginBottom: 4, fontSize: 13, fontWeight: 600 }}>
                        单笔最大风险比例
                      </label>
                      <input
                        type="number"
                        step="0.001"
                        value={formMaxRiskPerTrade}
                        onChange={(e) => setFormMaxRiskPerTrade(parseFloat(e.target.value) || 0.02)}
                        min={0}
                        max={1}
                        style={{ width: "100%", padding: "6px 8px", border: "1px solid #ddd", borderRadius: 6 }}
                      />
                      <div style={{ fontSize: 11, color: "#666", marginTop: 2 }}>
                        {formMaxRiskPerTrade * 100}%
                      </div>
                    </div>
                  </div>
                </>
              ) : (
                <>
                  {/* 高级模式：JSON配置 */}
                  <div>
                    <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                      策略配置 (JSON) *
                    </label>
                    <div style={{ fontSize: 12, color: "#666", marginBottom: 4 }}>
                      {formStrategyType === "MA_CROSS" && (
                        <span>
                          示例: {"{"}"ma_short": 5, "ma_long": 20, "period": "15m", "symbols": ["600519.SH"], "position_size": 0.1, "price_type": "LIMIT"{"}"}
                        </span>
                      )}
                      {formStrategyType === "TREND_FOLLOWING" && (
                        <span>
                          示例: {"{"}"ma_period": 20, "volume_ratio": 1.5, "period": "15m", "symbols": ["600519.SH"], "position_size": 0.1, "price_type": "LIMIT"{"}"}
                        </span>
                      )}
                    </div>
                    <textarea
                      value={formConfigJson}
                      onChange={(e) => setFormConfigJson(e.target.value)}
                      rows={8}
                      style={{
                        width: "100%",
                        padding: "8px 12px",
                        border: "1px solid #ddd",
                        borderRadius: 6,
                        fontFamily: "monospace",
                        fontSize: 12,
                      }}
                    />
                  </div>

                  <div>
                    <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                      调度配置 (JSON)
                    </label>
                    <div style={{ fontSize: 12, color: "#666", marginBottom: 4 }}>
                      示例: {"{"}"type": "daily", "time": "09:30"{"}"} 或 {"{"}"type": "realtime"{"}"} 或 {"{"}"type": "minute", "interval": 15{"}"}
                    </div>
                    <textarea
                      value={formScheduleConfig}
                      onChange={(e) => setFormScheduleConfig(e.target.value)}
                      rows={5}
                      style={{
                        width: "100%",
                        padding: "8px 12px",
                        border: "1px solid #ddd",
                        borderRadius: 6,
                        fontFamily: "monospace",
                        fontSize: 12,
                      }}
                    />
                  </div>

                  <div>
                    <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                      风控配置 (JSON)
                    </label>
                    <textarea
                      value={formRiskConfig}
                      onChange={(e) => setFormRiskConfig(e.target.value)}
                      rows={5}
                      style={{
                        width: "100%",
                        padding: "8px 12px",
                        border: "1px solid #ddd",
                        borderRadius: 6,
                        fontFamily: "monospace",
                        fontSize: 12,
                      }}
                    />
                  </div>
                </>
              )}
            </div>

            <div style={{ display: "flex", gap: 12, marginTop: 24 }}>
              <button
                type="button"
                onClick={handleSaveStrategy}
                style={{
                  flex: 1,
                  padding: "10px 20px",
                  background: "#4f46e5",
                  color: "#fff",
                  border: "none",
                  borderRadius: 6,
                  cursor: "pointer",
                  fontWeight: 600,
                }}
              >
                保存
              </button>
              <button
                type="button"
                onClick={() => {
                  setShowAddModal(false);
                  resetForm();
                }}
                style={{
                  flex: 1,
                  padding: "10px 20px",
                  background: "#6c757d",
                  color: "#fff",
                  border: "none",
                  borderRadius: 6,
                  cursor: "pointer",
                  fontWeight: 600,
                }}
              >
                取消
              </button>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}

