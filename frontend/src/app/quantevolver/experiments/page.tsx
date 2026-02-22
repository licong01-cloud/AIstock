"use client";

import { useEffect, useState, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Experiment = {
  experiment_id: string;
  experiment_name: string;
  status: string;
  factor_names?: string[];
  model_id?: string;
  strategy_id?: string;
  workspace_path?: string;
  wsl_command?: string;
  result_metrics?: any;
  created_at?: string;
  updated_at?: string;
};

const KEY_METRICS = [
  { key: "IC", label: "IC", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.03 },
  { key: "ICIR", label: "ICIR", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.3 },
  { key: "Rank IC", label: "Rank IC", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.05 },
  { key: "Rank ICIR", label: "Rank ICIR", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.3 },
  { key: "annualized_return", label: "年化收益", fmt: (v: number) => (v * 100).toFixed(2) + "%", good: (v: number) => v > 0.1 },
  { key: "max_drawdown", label: "最大回撤", fmt: (v: number) => (v * 100).toFixed(2) + "%", good: (v: number) => v > -0.2 },
  { key: "sharpe", label: "Sharpe", fmt: (v: number) => v.toFixed(3), good: (v: number) => v > 1.0 },
  { key: "information_ratio", label: "IR", fmt: (v: number) => v.toFixed(3), good: (v: number) => v > 0.5 },
];

const MODEL_NAMES: Record<string, string> = {
  LGBModel: "LightGBM",
  linear: "线性模型",
  XGBModel: "XGBoost",
  CatBoostModel: "CatBoost",
  DNNModel: "深度神经网络",
  TabNetModel: "TabNet",
};

const STATUS_MAP: Record<string, { label: string; cls: string; border: string }> = {
  created:   { label: "已创建", cls: "bg-blue-100 text-blue-700",   border: "border-l-blue-500" },
  running:   { label: "运行中", cls: "bg-amber-100 text-amber-700", border: "border-l-amber-500" },
  completed: { label: "已完成", cls: "bg-green-100 text-green-700", border: "border-l-green-500" },
  failed:    { label: "失败",   cls: "bg-red-100 text-red-700",     border: "border-l-red-500" },
};

function getMetrics(exp: Experiment): Record<string, any> {
  if (!exp.result_metrics) return {};
  if (typeof exp.result_metrics === "string") {
    try { return JSON.parse(exp.result_metrics); } catch { return {}; }
  }
  return exp.result_metrics;
}

export default function ExperimentsPage() {
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [actionId, setActionId] = useState<string | null>(null);
  const [actionType, setActionType] = useState<string>("");
  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);

  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 3500);
  }, []);

  async function loadExperiments() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API}/quantevolver/experiments?limit=50`);
      const data = await res.json();
      setExperiments(data.items || []);
      setTotal(data.total || 0);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    }
    setLoading(false);
  }

  useEffect(() => { loadExperiments(); }, []);

  async function syncResult(expId: string) {
    setActionId(expId);
    setActionType("sync");
    try {
      const res = await fetch(`${API}/quantevolver/experiments/${expId}/sync-results`, { method: "POST" });
      const data = await res.json();
      if (data.ok) {
        showToast("同步成功！回测指标已更新", true);
        loadExperiments();
      } else {
        showToast("同步失败: " + (data.error || "未知错误"), false);
      }
    } catch (e: any) {
      showToast("同步失败: " + (e?.message || ""), false);
    }
    setActionId(null);
    setActionType("");
  }

  async function regenerateExperiment(expId: string) {
    setActionId(expId);
    setActionType("regen");
    try {
      const res = await fetch(`${API}/quantevolver/experiments/${expId}/regenerate`, { method: "POST" });
      const data = await res.json();
      if (data.ok) {
        showToast(`脚本已重新生成 (${data.factor_count} 个因子)`, true);
        loadExperiments();
      } else {
        showToast("重新生成失败: " + (data.error || "未知错误"), false);
      }
    } catch (e: any) {
      showToast("重新生成失败: " + (e?.message || ""), false);
    }
    setActionId(null);
    setActionType("");
  }

  return (
    <main className="p-6">
      {/* Toast */}
      {toast && (
        <div className={`fixed top-4 right-4 z-50 px-4 py-3 rounded-lg shadow-lg text-sm font-medium transition-all ${toast.ok ? "bg-green-600 text-white" : "bg-red-600 text-white"}`}>
          {toast.msg}
        </div>
      )}

      {/* Banner */}
      <section className="rounded-2xl p-5 text-white mb-5"
        style={{ background: "linear-gradient(135deg, #ef4444 0%, #f59e0b 100%)" }}>
        <h1 className="text-2xl font-bold m-0">实验历史</h1>
        <p className="mt-2 text-sm opacity-90">查看QLib回测实验记录，同步WSL执行结果，重新生成实验脚本</p>
      </section>

      {/* 工具栏 */}
      <section className="bg-white rounded-xl p-4 shadow-sm mb-4">
        <div className="flex items-center gap-3">
          <button onClick={loadExperiments} disabled={loading}
            className="px-3 py-1.5 text-xs font-semibold rounded-lg border border-gray-300 bg-white cursor-pointer hover:bg-gray-50 disabled:opacity-50">
            {loading ? "加载中..." : "🔄 刷新"}
          </button>
          <span className="text-xs text-gray-400">共 {total} 条实验记录</span>
          {error && <span className="text-xs text-red-600 ml-2">{error}</span>}
        </div>
      </section>

      {/* 实验列表 - 横向列表布局（白色表格背景，因子库风格） */}
      <div className="bg-white rounded-xl shadow-sm overflow-hidden">
        {/* 表头 */}
        <div className="grid gap-3 px-4 py-3 bg-white border-b-2 border-gray-200 text-xs font-semibold text-gray-700"
          style={{ gridTemplateColumns: "100px 140px 80px 140px 100px 80px 100px 100px 100px 140px" }}>
          <div>实验ID</div>
          <div>实验时间</div>
          <div className="text-center">因子数量</div>
          <div className="text-center">模型</div>
          <div className="text-center">策略</div>
          <div className="text-center">IC</div>
          <div className="text-center">年化收益</div>
          <div className="text-center">最大回撤</div>
          <div className="text-center">同步状态</div>
          <div className="text-center">操作</div>
        </div>

        {/* 数据行 */}
        {experiments.map(exp => {
          const expanded = expandedId === exp.experiment_id;
          const sm = STATUS_MAP[exp.status] || { label: exp.status, cls: "bg-gray-100 text-gray-600", border: "border-l-gray-400" };
          const metrics = getMetrics(exp);
          const hasMetrics = Object.keys(metrics).length > 0;
          const isActioning = actionId === exp.experiment_id;
          const ic = metrics["IC"];
          const annRet = metrics["annualized_return"];
          const maxDD = metrics["max_drawdown"];

          return (
            <div key={exp.experiment_id} className="border-b border-gray-100 last:border-b-0 bg-white">
              {/* 主行 - 点击展开/收起 */}
              <div className="grid gap-3 px-4 py-3 text-xs hover:bg-gray-50 cursor-pointer transition-colors items-center"
                style={{ gridTemplateColumns: "100px 140px 80px 140px 100px 80px 100px 100px 100px 140px" }}
                onClick={() => setExpandedId(expanded ? null : exp.experiment_id)}>
                
                {/* 实验ID */}
                <div className="text-gray-600 font-mono text-[10px]">
                  {exp.experiment_id || "-"}
                </div>

                {/* 实验时间 */}
                <div className="text-gray-700">
                  {exp.created_at ? new Date(exp.created_at).toLocaleString("zh-CN", { 
                    month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" 
                  }) : "-"}
                </div>

                {/* 因子数量 */}
                <div className="text-center">
                  <span className="px-2 py-0.5 rounded text-[11px] font-semibold bg-violet-100 text-violet-700">
                    {exp.factor_names?.length || 0}
                  </span>
                </div>

                {/* 模型 */}
                <div className="text-center">
                  <span className="px-2 py-0.5 rounded text-[11px] font-semibold bg-blue-100 text-blue-700 truncate inline-block max-w-full">
                    {MODEL_NAMES[exp.model_id || ""] || exp.model_id || "默认"}
                  </span>
                </div>

                {/* 策略 */}
                <div className="text-center">
                  <span className="px-2 py-0.5 rounded text-[11px] font-semibold bg-green-100 text-green-700 truncate inline-block max-w-full">
                    {exp.strategy_id || "TopkDropout"}
                  </span>
                </div>

                {/* IC */}
                <div className="text-center font-mono">
                  {ic != null ? (
                    <span className={ic > 0.03 ? "text-green-600 font-semibold" : "text-red-600"}>
                      {(typeof ic === "number" ? ic : parseFloat(ic)).toFixed(4)}
                    </span>
                  ) : "-"}
                </div>

                {/* 年化收益 */}
                <div className="text-center font-mono">
                  {annRet != null ? (
                    <span className={annRet > 0.1 ? "text-green-600 font-semibold" : "text-red-600"}>
                      {((typeof annRet === "number" ? annRet : parseFloat(annRet)) * 100).toFixed(2)}%
                    </span>
                  ) : "-"}
                </div>

                {/* 最大回撤 */}
                <div className="text-center font-mono">
                  {maxDD != null ? (
                    <span className={maxDD > -0.2 ? "text-green-600 font-semibold" : "text-red-600"}>
                      {((typeof maxDD === "number" ? maxDD : parseFloat(maxDD)) * 100).toFixed(2)}%
                    </span>
                  ) : "-"}
                </div>

                {/* 同步状态 */}
                <div className="text-center">
                  <span className={`px-2 py-0.5 rounded-full text-[10px] font-semibold ${sm.cls}`}>
                    {sm.label}
                  </span>
                </div>

                {/* 操作按钮 */}
                <div className="flex gap-1 justify-center" onClick={e => e.stopPropagation()}>
                  <button onClick={() => syncResult(exp.experiment_id)}
                    disabled={isActioning}
                    className="px-2 py-1 text-[10px] font-medium rounded border border-blue-300 text-blue-700 bg-blue-50 cursor-pointer hover:bg-blue-100 disabled:opacity-50">
                    {isActioning && actionType === "sync" ? "同步中" : "同步"}
                  </button>
                  <button onClick={() => regenerateExperiment(exp.experiment_id)}
                    disabled={isActioning}
                    className="px-2 py-1 text-[10px] font-medium rounded border border-purple-300 text-purple-700 bg-purple-50 cursor-pointer hover:bg-purple-100 disabled:opacity-50">
                    {isActioning && actionType === "regen" ? "生成中" : "重生成"}
                  </button>
                  <button onClick={(e) => { e.stopPropagation(); setExpandedId(expanded ? null : exp.experiment_id); }}
                    className="px-2 py-1 text-[10px] font-medium rounded border border-gray-300 text-gray-700 bg-white cursor-pointer hover:bg-gray-50">
                    {expanded ? "收起" : "详情"}
                  </button>
                </div>
              </div>

              {/* 展开详情 */}
              {expanded && (
                <div className="px-4 pb-4 pt-0 border-t border-gray-100">
                  <div className="pt-3 space-y-3">

                    {/* 因子列表 */}
                    {exp.factor_names && exp.factor_names.length > 0 && (
                      <div>
                        <div className="text-xs font-semibold text-gray-700 mb-1.5">因子列表 ({exp.factor_names.length})</div>
                        <div className="grid gap-1 p-2 bg-violet-50 rounded-lg border border-violet-100 max-h-48 overflow-auto"
                          style={{ gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))" }}>
                          {exp.factor_names.map(fn => (
                            <span key={fn} className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-white border border-gray-200 break-all">
                              {fn}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* 模型和策略 */}
                    <div className="flex gap-6 text-xs">
                      <div>
                        <strong className="text-gray-600">模型：</strong>
                        <span className="ml-1 px-2 py-0.5 rounded text-[11px] font-semibold bg-blue-100 text-blue-700">
                          {MODEL_NAMES[exp.model_id || ""] || exp.model_id || "默认"}
                        </span>
                      </div>
                      <div>
                        <strong className="text-gray-600">策略：</strong>
                        <span className="ml-1 px-2 py-0.5 rounded text-[11px] font-semibold bg-green-100 text-green-700">
                          {exp.strategy_id || "默认"}
                        </span>
                      </div>
                    </div>

                    {/* 完整回测指标表格 */}
                    {hasMetrics && (
                      <div>
                        <div className="text-xs font-semibold text-gray-700 mb-1.5">回测指标详情</div>
                        <div className="bg-gray-50 rounded-lg border border-gray-200 overflow-hidden">
                          <table className="w-full text-[11px]" style={{ borderCollapse: "collapse" }}>
                            <thead>
                              <tr className="border-b-2 border-gray-200 text-left">
                                <th className="px-3 py-1.5 font-semibold text-gray-600">指标</th>
                                <th className="px-3 py-1.5 font-semibold text-gray-600">值</th>
                              </tr>
                            </thead>
                            <tbody>
                              {Object.entries(metrics).map(([k, v]) => {
                                const km = KEY_METRICS.find(m => m.key === k);
                                const numVal = typeof v === "number" ? v : parseFloat(String(v));
                                const isNum = !isNaN(numVal);
                                const isGood = km && isNum ? km.good(numVal) : null;
                                return (
                                  <tr key={k} className="border-b border-gray-100">
                                    <td className="px-3 py-1 text-gray-500">{km?.label || k}</td>
                                    <td className={`px-3 py-1 font-semibold font-mono ${isGood === true ? "text-green-600" : isGood === false ? "text-red-600" : "text-gray-800"}`}>
                                      {isNum && km ? km.fmt(numVal) : String(v)}
                                    </td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                    {/* 工作目录 */}
                    {exp.workspace_path && (
                      <div className="text-xs">
                        <strong className="text-gray-600">工作目录：</strong>
                        <span className="font-mono text-[11px] text-gray-500 ml-1">{exp.workspace_path}</span>
                      </div>
                    )}

                    {/* WSL命令 */}
                    {exp.wsl_command && (
                      <div>
                        <div className="text-xs font-semibold text-gray-700 mb-1.5">WSL执行命令</div>
                        <pre className="bg-slate-800 text-slate-200 p-3 rounded-lg text-[11px] overflow-auto max-h-36 whitespace-pre-wrap">
                          {exp.wsl_command}
                        </pre>
                        <button
                          onClick={() => {
                            navigator.clipboard.writeText(exp.wsl_command || "");
                            showToast("已复制到剪贴板", true);
                          }}
                          className="mt-2 px-3 py-1 text-[11px] border border-gray-300 rounded bg-white cursor-pointer hover:bg-gray-50">
                          复制命令
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {!loading && experiments.length === 0 && (
        <div className="text-center py-16 text-gray-400 bg-white rounded-xl shadow-sm">
          暂无实验记录。请先在组合配置页面生成QLib配置。
        </div>
      )}
    </main>
  );
}
