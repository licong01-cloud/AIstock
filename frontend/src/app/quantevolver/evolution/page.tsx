"use client";

import React, { useState, useEffect } from "react";
import { 
  Play, Square, Terminal, GitMerge, FileCode2, 
  Activity, ArrowRight, DownloadCloud, CheckCircle2,
  AlertCircle
} from "lucide-react";

export default function EvolutionDashboard() {
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null);
  const [activeLoop, setActiveLoop] = useState<number | null>(null);
  const [logs, setLogs] = useState<string[]>([
    "[System] 演进控制中心已启动...",
    "[System] 等待连接至 AIstock 演进调度引擎..."
  ]);

  // Mock data for UI demonstration
  const mockTasks = [
    { id: "Evo_20260221_01", name: "Alpha挖掘与调优-001", status: "running", currentLoop: 3, maxLoops: 10 },
    { id: "Evo_20260220_02", name: "稳健收益策略演进", status: "completed", currentLoop: 10, maxLoops: 10 },
  ];

  const mockLoops = [
    { index: 0, status: "completed", isSota: false, action: "initial", score: 65 },
    { index: 1, status: "completed", isSota: true, action: "factor_adjust", score: 72 },
    { index: 2, status: "completed", isSota: false, action: "param_tune", score: 68 },
    { index: 3, status: "running", isSota: false, action: "factor_adjust", score: 0 },
  ];

  return (
    <div className="flex h-[calc(100vh-4rem)] bg-slate-900 text-slate-300 font-sans overflow-hidden">
      
      {/* 左侧：任务控制中心与实时终端 */}
      <div className="w-1/3 flex flex-col border-r border-slate-700">
        <div className="p-4 border-b border-slate-700 bg-slate-800 flex justify-between items-center">
          <h2 className="text-lg font-bold text-white flex items-center gap-2">
            <Activity className="text-blue-400" size={20} />
            演进控制中心
          </h2>
          <button className="px-3 py-1 bg-blue-600 hover:bg-blue-500 text-white rounded text-sm font-medium flex items-center gap-1 transition-colors">
            <Play size={16} />
            新建演进任务
          </button>
        </div>

        {/* 任务列表 */}
        <div className="flex-1 overflow-y-auto p-4 space-y-3">
          <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">活跃任务</div>
          {mockTasks.map(task => (
            <div 
              key={task.id} 
              className={`p-3 rounded-lg border cursor-pointer transition-colors ${
                activeTaskId === task.id 
                  ? "bg-slate-800 border-blue-500" 
                  : "bg-slate-800/50 border-slate-700 hover:border-slate-500"
              }`}
              onClick={() => setActiveTaskId(task.id)}
            >
              <div className="flex justify-between items-start mb-2">
                <div className="font-medium text-slate-200">{task.name}</div>
                {task.status === 'running' ? (
                  <span className="flex h-2 w-2 relative mt-1">
                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                    <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
                  </span>
                ) : (
                  <CheckCircle2 size={14} className="text-slate-500" />
                )}
              </div>
              <div className="text-xs text-slate-400 flex justify-between">
                <span>{task.id}</span>
                <span>Loop: {task.currentLoop} / {task.maxLoops}</span>
              </div>
            </div>
          ))}
        </div>

        {/* 实时终端 */}
        <div className="h-64 border-t border-slate-700 bg-black flex flex-col">
          <div className="px-3 py-1 bg-slate-800 text-xs font-mono text-slate-400 flex items-center gap-2 border-b border-slate-700">
            <Terminal size={14} />
            Live Logs
          </div>
          <div className="flex-1 overflow-y-auto p-2 font-mono text-[11px] text-green-400 space-y-1">
            {logs.map((log, i) => (
              <div key={i}>{log}</div>
            ))}
            <div className="animate-pulse">_</div>
          </div>
        </div>
      </div>

      {/* 中间：演进血脉拓扑树 */}
      <div className="w-1/4 flex flex-col border-r border-slate-700 bg-slate-800/30">
        <div className="p-4 border-b border-slate-700 bg-slate-800 flex items-center gap-2">
          <GitMerge className="text-purple-400" size={20} />
          <h2 className="text-lg font-bold text-white">演进拓扑 (Evolution Tree)</h2>
        </div>
        <div className="flex-1 overflow-y-auto p-6 relative">
          {/* 简单的垂直时间线样式 */}
          <div className="absolute left-10 top-0 bottom-0 w-0.5 bg-slate-700 z-0"></div>
          
          <div className="space-y-8 relative z-10">
            {mockLoops.map(loop => (
              <div 
                key={loop.index} 
                className="flex items-center gap-4 cursor-pointer group"
                onClick={() => setActiveLoop(loop.index)}
              >
                <div className={`w-8 h-8 rounded-full flex items-center justify-center border-2 transition-transform group-hover:scale-110 ${
                  loop.isSota 
                    ? "bg-yellow-900/50 border-yellow-500 shadow-[0_0_10px_rgba(234,179,8,0.5)]" 
                    : loop.status === "completed"
                      ? "bg-slate-800 border-green-500"
                      : "bg-slate-800 border-blue-500 animate-pulse"
                }`}>
                  {loop.isSota ? "⭐" : loop.index}
                </div>
                <div className={`flex-1 p-3 rounded-lg border transition-colors ${
                  activeLoop === loop.index ? "border-blue-500 bg-slate-800" : "border-slate-700 bg-slate-800/50"
                }`}>
                  <div className="font-medium text-slate-200 text-sm">LOOP {loop.index}</div>
                  <div className="text-xs text-slate-400 mt-1 uppercase">{loop.action}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* 右侧：LOOP 深度看板 */}
      <div className="flex-1 flex flex-col bg-slate-900 overflow-y-auto">
        {activeLoop !== null ? (
          <>
            <div className="p-4 border-b border-slate-700 bg-slate-800 flex justify-between items-center">
              <h2 className="text-lg font-bold text-white flex items-center gap-2">
                <FileCode2 className="text-emerald-400" size={20} />
                LOOP {activeLoop} 详情看板
              </h2>
              <button className="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded text-sm font-bold flex items-center gap-2 transition-colors shadow-lg shadow-emerald-900/20">
                <DownloadCloud size={16} />
                一键同步实体资产至实盘
              </button>
            </div>
            
            <div className="p-6 space-y-6">
              {/* Agent 诊断报告 */}
              <div className="bg-slate-800 rounded-lg border border-slate-700 p-4 shadow-sm">
                <h3 className="text-sm font-bold text-blue-400 uppercase tracking-wider mb-3 flex items-center gap-2">
                  <AlertCircle size={16} />
                  Agent 结案陈词 & 决策逻辑
                </h3>
                <div className="text-sm text-slate-300 space-y-2 leading-relaxed bg-slate-900 p-4 rounded border border-slate-700 font-serif">
                  <p><strong className="text-slate-100">诊断 (Analyst):</strong> 本轮多空收益不对称，多头表现优异但空头衰减极快，且相关性矩阵显示新加入的 3 个因子存在轻微共线性。</p>
                  <p><strong className="text-slate-100">决策 (Researcher):</strong> 建议进入调参分支，提升模型对特征的正则化惩罚以抑制空头过拟合。</p>
                  <p><strong className="text-slate-100">审查 (Reviewer):</strong> 配置合法，将 LightGBM 的 reg_lambda 从 0.1 提升至 1.0。</p>
                </div>
              </div>

              {/* 回测表现雷达/指标 (Mock) */}
              <div className="grid grid-cols-2 gap-4">
                <div className="bg-slate-800 rounded-lg border border-slate-700 p-4 shadow-sm">
                  <h3 className="text-sm font-bold text-slate-400 uppercase tracking-wider mb-3">核心指标</h3>
                  <div className="space-y-3">
                    <div className="flex justify-between items-end border-b border-slate-700/50 pb-2">
                      <span className="text-sm text-slate-400">Rank IC</span>
                      <span className="text-lg font-mono text-emerald-400">0.054</span>
                    </div>
                    <div className="flex justify-between items-end border-b border-slate-700/50 pb-2">
                      <span className="text-sm text-slate-400">ICIR</span>
                      <span className="text-lg font-mono text-emerald-400">0.68</span>
                    </div>
                    <div className="flex justify-between items-end pb-2">
                      <span className="text-sm text-slate-400">Max Drawdown</span>
                      <span className="text-lg font-mono text-rose-400">-12.4%</span>
                    </div>
                  </div>
                </div>
                
                <div className="bg-slate-800 rounded-lg border border-slate-700 p-4 shadow-sm">
                  <h3 className="text-sm font-bold text-slate-400 uppercase tracking-wider mb-3">多维分析雷达图 (Mock)</h3>
                  <div className="h-32 flex items-center justify-center text-slate-600 border border-dashed border-slate-700 rounded bg-slate-900/50">
                    [ 雷达图表区域 (基于 ECharts/Recharts) ]
                  </div>
                </div>
              </div>

              {/* 配置 Diff */}
              <div className="bg-slate-800 rounded-lg border border-slate-700 p-4 shadow-sm">
                <h3 className="text-sm font-bold text-slate-400 uppercase tracking-wider mb-3">Config Diff (对比上一轮)</h3>
                <div className="font-mono text-sm p-4 bg-slate-900 rounded border border-slate-700 overflow-x-auto">
                  <div className="text-slate-500">  model_params:</div>
                  <div className="text-rose-400 bg-rose-900/20 px-2">-   reg_lambda: 0.1</div>
                  <div className="text-emerald-400 bg-emerald-900/20 px-2">+   reg_lambda: 1.0</div>
                  <div className="text-slate-500">  factor_list:</div>
                  <div className="text-slate-500">    - Alpha158_Vol</div>
                  <div className="text-slate-500">    ... (保持不变)</div>
                </div>
              </div>
            </div>
          </>
        ) : (
          <div className="flex-1 flex flex-col items-center justify-center text-slate-500">
            <ArrowRight size={48} className="mb-4 opacity-20" />
            <p className="text-lg">请在左侧选择一个任务并在拓扑树中点击具体的 LOOP</p>
          </div>
        )}
      </div>
      
    </div>
  );
}
