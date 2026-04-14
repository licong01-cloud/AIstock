"use client";

import React from "react";

/**
 * 统一的「策略与执行配置」卡片
 *
 * 用于所有 QE 场景（单次实验、演进 Loop 详情、演进 Loop 总览、演进手风琴）
 * 展示：模型、策略、日内执行算法、尾盘处理策略、行业黑名单、HMM 板块轮动、股票池
 *
 * 数据来源优先级:
 *   activeTask (task 级覆盖) > loop_config > model_params > experiment 直接字段
 */

export interface StrategyConfigSource {
  /** Loop 的 config_json (含 factor_list, model_id, strategy_id, model_params) */
  loopConfig?: any;
  /** Task 级配置（可覆盖 loop 配置） */
  taskConfig?: {
    strategy_id?: string;
    execution_algo?: string;
    unfilled_handler?: string;
    enable_sector_hmm?: boolean;
  };
  /** 独立实验的扁平字段（experiments/[id] 场景） */
  experiment?: any;
}

interface StrategyConfigCardProps {
  source: StrategyConfigSource;
  /** 标题颜色（方便融入不同页面主题） */
  titleColor?: string;
}

export function StrategyConfigCard({ source, titleColor = "#0ea5e9" }: StrategyConfigCardProps) {
  const { loopConfig, taskConfig, experiment } = source;

  const cfg: any = loopConfig || {};
  const mp: any = cfg.model_params || {};
  const exp: any = experiment || {};

  // 字段合并: task 级 > loop config (顶层) > model_params > experiment
  // 统一用 || 确保空字符串 "" 被视为未配置（前端创建时默认给 ""）
  const modelId: string | undefined =
    cfg.model_id || cfg.model_type || exp.model_id;
  const strategyId: string | undefined =
    taskConfig?.strategy_id || cfg.strategy_id || exp.strategy_id;
  const executionAlgo: string | undefined =
    taskConfig?.execution_algo || mp.execution_algo || exp.execution_algo;
  const unfilledHandler: string | undefined =
    taskConfig?.unfilled_handler || mp.unfilled_handler || exp.unfilled_handler;
  const sectorBlacklist: string[] =
    mp.sector_blacklist || cfg.sector_blacklist || exp.sector_blacklist || [];
  const blacklistEnabled = Array.isArray(sectorBlacklist) && sectorBlacklist.length > 0;
  // enable_sector_hmm 是 boolean，必须用 ?? （false 是合法值，不能被 || 跳过）
  const hmmEnabled: boolean =
    taskConfig?.enable_sector_hmm ?? mp.enable_sector_hmm ?? exp.enable_sector_hmm ?? false;
  const hmmPreset: string | undefined =
    mp.hmm_signal_preset || exp.hmm_signal_preset;
  const stockPool: string | undefined =
    mp.stock_pool || cfg.stock_pool || exp.stock_pool;

  const renderBadge = (enabled: boolean, onLabel: string = "已启用", offLabel: string = "未启用") => (
    <span
      style={{
        display: "inline-block",
        padding: "2px 10px",
        borderRadius: "12px",
        fontSize: "11px",
        fontWeight: 700,
        backgroundColor: enabled ? "#dcfce7" : "#f1f5f9",
        color: enabled ? "#166534" : "#64748b",
        border: enabled ? "1px solid #bbf7d0" : "1px solid #e2e8f0",
      }}
    >
      {enabled ? onLabel : offLabel}
    </span>
  );

  const items: { label: string; render: React.ReactNode }[] = [
    {
      label: "模型 (Model)",
      render: modelId ? (
        <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#0f172a" }}>
          {modelId}
        </span>
      ) : (
        <span style={{ color: "#94a3b8", fontSize: 12 }}>—</span>
      ),
    },
    {
      label: "交易策略 (Strategy)",
      render: strategyId ? (
        <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#0f172a" }}>
          {strategyId}
        </span>
      ) : (
        <span style={{ color: "#94a3b8", fontSize: 12 }}>—</span>
      ),
    },
    {
      label: "日内执行算法",
      render: executionAlgo ? (
        <span
          style={{
            display: "inline-block",
            padding: "2px 10px",
            borderRadius: 4,
            fontSize: 11,
            fontFamily: "monospace",
            fontWeight: 700,
            backgroundColor: "#eff6ff",
            color: "#1d4ed8",
            border: "1px solid #bfdbfe",
          }}
        >
          {executionAlgo}
        </span>
      ) : (
        <span style={{ color: "#94a3b8", fontSize: 12 }}>默认 (close)</span>
      ),
    },
    {
      label: "尾盘处理策略",
      render: unfilledHandler ? (
        <span
          style={{
            display: "inline-block",
            padding: "2px 10px",
            borderRadius: 4,
            fontSize: 11,
            fontFamily: "monospace",
            fontWeight: 700,
            backgroundColor: "#fef3c7",
            color: "#92400e",
            border: "1px solid #fde68a",
          }}
        >
          {unfilledHandler}
        </span>
      ) : (
        <span style={{ color: "#94a3b8", fontSize: 12 }}>未配置</span>
      ),
    },
    {
      label: "行业黑名单",
      render: (
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {renderBadge(blacklistEnabled)}
          {blacklistEnabled && (
            <span style={{ fontSize: 11, color: "#64748b" }}>({sectorBlacklist.length} 行业)</span>
          )}
        </div>
      ),
    },
    {
      label: "HMM 板块轮动",
      render: (
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {renderBadge(hmmEnabled)}
          {hmmEnabled && hmmPreset && (
            <span
              style={{
                display: "inline-block",
                padding: "1px 8px",
                borderRadius: 4,
                fontSize: 11,
                fontFamily: "monospace",
                backgroundColor: "#ede9fe",
                color: "#6d28d9",
                border: "1px solid #ddd6fe",
              }}
            >
              {hmmPreset}
            </span>
          )}
        </div>
      ),
    },
  ];

  if (stockPool) {
    items.push({
      label: "股票池 (Stock Pool)",
      render: (
        <span style={{ fontFamily: "monospace", fontSize: 12, color: "#334155" }}>{stockPool}</span>
      ),
    });
  }

  return (
    <div
      style={{
        backgroundColor: "#ffffff",
        borderRadius: "8px",
        border: "1px solid #e2e8f0",
        padding: "20px",
        boxShadow: "0 1px 3px rgba(0,0,0,0.05)",
      }}
    >
      <h3
        style={{
          margin: "0 0 16px 0",
          fontSize: "13px",
          fontWeight: 700,
          color: titleColor,
          textTransform: "uppercase",
          letterSpacing: "0.05em",
        }}
      >
        策略与执行配置
      </h3>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: "10px 24px" }}>
        {items.map((it) => (
          <div
            key={it.label}
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              padding: "8px 12px",
              backgroundColor: "#f8fafc",
              borderRadius: 6,
              border: "1px solid #e2e8f0",
            }}
          >
            <span style={{ fontSize: 11, color: "#64748b", fontWeight: 600 }}>{it.label}</span>
            <div>{it.render}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
