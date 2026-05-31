"use client";

import React from "react";

/**
 * Unified strategy/execution config card.
 *
 * QE experiment detail stores runtime settings such as execution algo, HMM and
 * stock pool in custom_params; evolution loops may store the same values in
 * model_params/config_json. This card displays the effective persisted config.
 */

export interface StrategyConfigSource {
  /** Loop config_json, including factor_list/model_id/strategy_id/model_params. */
  loopConfig?: any;
  /** Task-level config can override loop config. */
  taskConfig?: {
    strategy_id?: string;
    execution_algo?: string;
    execution_algo_params?: any;
    unfilled_handler?: string;
    unfilled_handler_params?: any;
    enable_sector_hmm?: boolean;
    hmm_model_version_id?: string;
    hmm_signal_preset?: string;
    sector_blacklist?: any;
    sector_blacklist_snapshot?: any;
    sector_blacklist_enabled?: boolean;
    blacklist_enabled?: boolean;
    stock_pool?: string;
  };
  /** Standalone experiment fields for experiments/[id]. */
  experiment?: any;
}

interface StrategyConfigCardProps {
  source: StrategyConfigSource;
  /** Title color for embedding in different pages. */
  titleColor?: string;
}

type JsonObjectResult = { value: Record<string, any>; error?: string };

function isPlainObject(value: any): value is Record<string, any> {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

function parseJsonObject(value: any, label: string): JsonObjectResult {
  if (value == null || value === "") return { value: {} };
  if (isPlainObject(value)) return { value };
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (isPlainObject(parsed)) return { value: parsed };
      return { value: {}, error: `${label} 不是对象，实际类型为 ${Array.isArray(parsed) ? "array" : typeof parsed}` };
    } catch (err: any) {
      return { value: {}, error: `${label} JSON 解析失败: ${err?.message || String(err)}` };
    }
  }
  return { value: {}, error: `${label} 类型不支持: ${typeof value}` };
}

function firstText(...values: any[]): string | undefined {
  for (const value of values) {
    if (typeof value === "string" && value.trim() !== "") return value.trim();
  }
  return undefined;
}

function firstPresent(...values: any[]): any {
  for (const value of values) {
    if (value !== undefined && value !== null && value !== "") return value;
  }
  return undefined;
}

function boolOrUndefined(value: any): boolean | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes", "y", "on", "enabled"].includes(normalized)) return true;
    if (["false", "0", "no", "n", "off", "disabled"].includes(normalized)) return false;
  }
  return Boolean(value);
}

function normalizeStringList(value: any): string[] {
  if (Array.isArray(value)) {
    return value
      .map((item) => {
        if (typeof item === "string") return item;
        if (isPlainObject(item)) {
          return item.sw2_name || item.sw2_code || item.name || item.code || JSON.stringify(item);
        }
        return String(item);
      })
      .filter((item) => item.trim() !== "");
  }
  if (typeof value === "string" && value.trim() !== "") {
    if (value.trim().startsWith("[")) {
      try {
        return normalizeStringList(JSON.parse(value));
      } catch {
        return [value];
      }
    }
    return value.split(",").map((item) => item.trim()).filter(Boolean);
  }
  return [];
}

function formatParamPreview(params: Record<string, any>): string | undefined {
  const entries = Object.entries(params || {}).filter(([, value]) => value !== undefined && value !== null && value !== "");
  if (entries.length === 0) return undefined;
  return entries
    .slice(0, 3)
    .map(([key, value]) => {
      if (typeof value === "object") return `${key}=...`;
      return `${key}=${String(value)}`;
    })
    .join(", ");
}

export function StrategyConfigCard({ source, titleColor = "#0ea5e9" }: StrategyConfigCardProps) {
  const { loopConfig, taskConfig, experiment } = source;

  const cfg: any = loopConfig || {};
  const exp: any = experiment || {};

  const cfgCustomResult = parseJsonObject(cfg.custom_params, "loopConfig.custom_params");
  const expCustomResult = parseJsonObject(exp.custom_params, "experiment.custom_params");
  const modelParamsResult = parseJsonObject(cfg.model_params, "loopConfig.model_params");
  const strategyParamsResult = parseJsonObject(cfg.strategy_params, "loopConfig.strategy_params");
  const taskExecParamsResult = parseJsonObject(taskConfig?.execution_algo_params, "taskConfig.execution_algo_params");
  const cfgExecParamsResult = parseJsonObject(cfg.execution_algo_params, "loopConfig.execution_algo_params");
  const expExecParamsResult = parseJsonObject(exp.execution_algo_params, "experiment.execution_algo_params");
  const taskUnfilledParamsResult = parseJsonObject(taskConfig?.unfilled_handler_params, "taskConfig.unfilled_handler_params");
  const cfgUnfilledParamsResult = parseJsonObject(cfg.unfilled_handler_params, "loopConfig.unfilled_handler_params");
  const strategyUnfilledParamsResult = parseJsonObject(strategyParamsResult.value.unfilled_handler_params, "loopConfig.strategy_params.unfilled_handler_params");
  const blacklistSnapshotResult = parseJsonObject(firstPresent(
    taskConfig?.sector_blacklist_snapshot,
    cfg.sector_blacklist_snapshot,
    cfgCustomResult.value.sector_blacklist_snapshot,
    expCustomResult.value.sector_blacklist_snapshot,
    exp.sector_blacklist_snapshot,
  ), "sector_blacklist_snapshot");

  const cfgCustom = cfgCustomResult.value;
  const expCustom = expCustomResult.value;
  const mp = modelParamsResult.value;
  const strategyParams = strategyParamsResult.value;
  const taskExecParams = taskExecParamsResult.value;
  const cfgExecParams = cfgExecParamsResult.value;
  const expExecParams = expExecParamsResult.value;
  const taskUnfilledParams = taskUnfilledParamsResult.value;
  const cfgUnfilledParams = cfgUnfilledParamsResult.value;
  const strategyUnfilledParams = strategyUnfilledParamsResult.value;
  const blacklistSnapshot = blacklistSnapshotResult.value;

  const parseErrors = [
    cfgCustomResult.error,
    expCustomResult.error,
    modelParamsResult.error,
    strategyParamsResult.error,
    taskExecParamsResult.error,
    cfgExecParamsResult.error,
    expExecParamsResult.error,
    taskUnfilledParamsResult.error,
    cfgUnfilledParamsResult.error,
    strategyUnfilledParamsResult.error,
    blacklistSnapshotResult.error,
  ].filter(Boolean) as string[];

  const modelId: string | undefined = firstText(cfg.model_id, cfg.model_type, exp.model_id);
  const strategyId: string | undefined = firstText(taskConfig?.strategy_id, cfg.strategy_id, exp.strategy_id);

  const executionAlgo: string | undefined = firstText(
    taskConfig?.execution_algo,
    cfg.execution_algo,
    strategyParams.execution_algo,
    mp.execution_algo,
    cfgCustom.execution_algo,
    expCustom.execution_algo,
    exp.execution_algo,
  );

  const executionAlgoParams = firstPresent(
    Object.keys(taskExecParams).length ? taskExecParams : undefined,
    Object.keys(cfgExecParams).length ? cfgExecParams : undefined,
    strategyParams.execution_algo_params,
    mp.execution_algo_params,
    cfgCustom.execution_algo_params,
    expCustom.execution_algo_params,
    Object.keys(expExecParams).length ? expExecParams : undefined,
  ) || {};

  const unfilledHandler: string | undefined = firstText(
    taskConfig?.unfilled_handler,
    cfg.unfilled_handler,
    strategyParams.unfilled_handler,
    mp.unfilled_handler,
    cfgCustom.unfilled_handler,
    expCustom.unfilled_handler,
    exp.unfilled_handler,
    executionAlgoParams.unfilled_handler,
  );

  const unfilledBackupDepth = firstPresent(
    taskUnfilledParams.backup_depth,
    cfgUnfilledParams.backup_depth,
    strategyUnfilledParams.backup_depth,
    strategyParams.unfilled_backup_depth,
    mp.unfilled_backup_depth,
    cfgCustom.unfilled_backup_depth,
    expCustom.unfilled_backup_depth,
    executionAlgoParams.unfilled_backup_depth,
  );

  const sectorBlacklist = normalizeStringList(firstPresent(
    taskConfig?.sector_blacklist,
    cfg.sector_blacklist,
    strategyParams.sector_blacklist,
    mp.sector_blacklist,
    cfgCustom.sector_blacklist,
    expCustom.sector_blacklist,
    exp.sector_blacklist,
  ));

  const stockPool: string | undefined = firstText(
    taskConfig?.stock_pool,
    cfg.stock_pool,
    strategyParams.stock_pool,
    mp.stock_pool,
    cfgCustom.stock_pool,
    expCustom.stock_pool,
    exp.stock_pool,
  );
  const stockPoolFiltered = !!stockPool && stockPool !== "all";

  const explicitBlacklistEnabled = boolOrUndefined(firstPresent(
    taskConfig?.sector_blacklist_enabled,
    taskConfig?.blacklist_enabled,
    cfg.sector_blacklist_enabled,
    cfg.blacklist_enabled,
    strategyParams.sector_blacklist_enabled,
    strategyParams.blacklist_enabled,
    mp.sector_blacklist_enabled,
    mp.blacklist_enabled,
    cfgCustom.sector_blacklist_enabled,
    cfgCustom.blacklist_enabled,
    expCustom.sector_blacklist_enabled,
    expCustom.blacklist_enabled,
    exp.sector_blacklist_enabled,
    exp.blacklist_enabled,
  ));
  const blacklistSnapshotItems = Array.isArray(blacklistSnapshot.items) ? blacklistSnapshot.items : [];
  const blacklistEnabled = explicitBlacklistEnabled ?? (blacklistSnapshotItems.length > 0 || sectorBlacklist.length > 0 || stockPoolFiltered);
  const blacklistDetailItems = blacklistSnapshotItems.length > 0
    ? blacklistSnapshotItems.map((item: any) => ({
      code: item?.sw2_code || item?.code || "",
      name: item?.sw2_name || item?.name || item?.sw2_code || item?.code || "",
      sw1Name: item?.sw1_name || "",
      reason: item?.reason || "",
      effectiveFrom: item?.effective_from || "",
      effectiveTo: item?.effective_to || "",
    }))
    : sectorBlacklist.map((item) => ({ code: item, name: item, sw1Name: "", reason: "", effectiveFrom: "", effectiveTo: "" }));
  const blacklistWarning = typeof blacklistSnapshot.warning === "string" ? blacklistSnapshot.warning : "";

  const hmmEnabled = boolOrUndefined(firstPresent(
    taskConfig?.enable_sector_hmm,
    cfg.enable_sector_hmm,
    strategyParams.enable_sector_hmm,
    mp.enable_sector_hmm,
    cfgCustom.enable_sector_hmm,
    expCustom.enable_sector_hmm,
    exp.enable_sector_hmm,
  )) ?? false;

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

  const emptyValue = <span style={{ color: "#94a3b8", fontSize: 12 }}>—</span>;
  const mutedValue = (text: string) => <span style={{ color: "#94a3b8", fontSize: 12 }}>{text}</span>;

  const executionParamPreview = isPlainObject(executionAlgoParams) ? formatParamPreview(executionAlgoParams) : undefined;
  const labelType = firstText(
    cfg.label_type,
    strategyParams.label_type,
    mp.label_type,
    cfgCustom.label_type,
    expCustom.label_type,
    exp.label_type,
  ) || "close";
  const labelHorizon = firstPresent(
    cfg.label_horizon,
    strategyParams.label_horizon,
    mp.label_horizon,
    cfgCustom.label_horizon,
    expCustom.label_horizon,
    exp.label_horizon,
    1,
  );
  const holdThresh = firstPresent(
    cfg.hold_thresh,
    strategyParams.hold_thresh,
    mp.hold_thresh,
    cfgCustom.hold_thresh,
    expCustom.hold_thresh,
  );

  const items: { label: string; render: React.ReactNode }[] = [
    {
      label: "模型 (Model)",
      render: modelId ? (
        <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#0f172a" }}>
          {modelId}
        </span>
      ) : emptyValue,
    },
    {
      label: "交易策略 (Strategy)",
      render: strategyId ? (
        <span style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 700, color: "#0f172a" }}>
          {strategyId}
        </span>
      ) : emptyValue,
    },
    {
      label: "训练标签期限",
      render: (
        <span style={{ fontSize: 12, fontWeight: 700, color: "#0f766e" }}>
          {String(labelType)} / {String(labelHorizon)}d
        </span>
      ),
    },
    {
      label: "最短持仓",
      render: holdThresh !== undefined ? (
        <span style={{ fontSize: 12, fontWeight: 700, color: "#92400e" }}>
          {String(holdThresh)}d
        </span>
      ) : mutedValue("未覆盖"),
    },
    {
      label: "分钟线执行策略",
      render: executionAlgo ? (
        <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap", justifyContent: "flex-end" }}>
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
          {executionParamPreview && <span style={{ fontSize: 10, color: "#64748b" }}>{executionParamPreview}</span>}
        </div>
      ) : mutedValue("默认 (close)") ,
    },
    {
      label: "尾盘处理策略",
      render: unfilledHandler ? (
        <div style={{ display: "flex", alignItems: "center", gap: 6, justifyContent: "flex-end" }}>
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
          {unfilledBackupDepth !== undefined && <span style={{ fontSize: 10, color: "#64748b" }}>backup={String(unfilledBackupDepth)}</span>}
        </div>
      ) : mutedValue("未配置"),
    },
    {
      label: "行业黑名单",
      render: (
        <div style={{ display: "flex", alignItems: "center", gap: 8, justifyContent: "flex-end", flexWrap: "wrap" }}>
          {renderBadge(blacklistEnabled)}
          {blacklistEnabled && blacklistDetailItems.length > 0 && (
            <span style={{ fontSize: 11, color: "#64748b" }}>({blacklistDetailItems.length} 行业)</span>
          )}
        </div>
      ),
    },
    {
      label: "HMM 板块轮动",
      render: (
        <div style={{ display: "flex", alignItems: "center", gap: 8, justifyContent: "flex-end", flexWrap: "wrap" }}>
          {renderBadge(hmmEnabled)}
        </div>
      ),
    },
  ];

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
      {parseErrors.length > 0 && (
        <div style={{ marginBottom: 12, padding: "8px 10px", borderRadius: 6, backgroundColor: "#fef2f2", border: "1px solid #fecaca", color: "#b91c1c", fontSize: 12 }}>
          配置解析异常：{parseErrors.join("；")}
        </div>
      )}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "10px 24px" }}>
        {items.map((it) => (
          <div
            key={it.label}
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 12,
              padding: "8px 12px",
              backgroundColor: "#f8fafc",
              borderRadius: 6,
              border: "1px solid #e2e8f0",
              minWidth: 0,
            }}
          >
            <span style={{ fontSize: 11, color: "#64748b", fontWeight: 600, flexShrink: 0 }}>{it.label}</span>
            <div style={{ minWidth: 0, textAlign: "right", overflowWrap: "anywhere" }}>{it.render}</div>
          </div>
        ))}
      </div>
      {blacklistEnabled && blacklistDetailItems.length > 0 && (
        <div style={{ marginTop: 12, padding: "10px 12px", backgroundColor: "#fff7ed", border: "1px solid #fed7aa", borderRadius: 6 }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, marginBottom: 8 }}>
            <span style={{ fontSize: 11, color: "#9a3412", fontWeight: 700 }}>行业黑名单配置</span>
            <span style={{ fontSize: 10, color: "#9a3412", fontFamily: "monospace" }}>
              {blacklistSnapshot.as_of_date ? `as_of=${blacklistSnapshot.as_of_date}` : ""}
            </span>
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {blacklistDetailItems.map((item: any, idx: number) => (
              <span
                key={`${item.code || item.name}-${idx}`}
                title={[item.sw1Name, item.code, item.reason].filter(Boolean).join(" / ")}
                style={{
                  padding: "3px 8px",
                  borderRadius: 12,
                  backgroundColor: "#ffedd5",
                  color: "#9a3412",
                  border: "1px solid #fdba74",
                  fontSize: 11,
                  fontWeight: 600,
                }}
              >
                {item.name || item.code}
                {item.code && item.name !== item.code ? ` (${item.code})` : ""}
              </span>
            ))}
          </div>
          {stockPoolFiltered && (
            <div style={{ marginTop: 8, fontSize: 10, color: "#9a3412", fontFamily: "monospace" }}>
              stock_pool={stockPool}
            </div>
          )}
          {blacklistWarning && (
            <div style={{ marginTop: 8, fontSize: 11, color: "#b45309" }}>
              {blacklistWarning}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
