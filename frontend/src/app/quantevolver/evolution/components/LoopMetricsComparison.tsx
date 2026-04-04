"use client";

import React from "react";
import { BarChart3, TrendingUp } from "lucide-react";

interface LoopMetricsComparisonProps {
  loops: any[];
  taskType?: string;
  onLoopSelect?: (loopIndex: number) => void;
  selectedLoopIndex?: number;
}

interface LoopMetrics {
  loop_index: number;
  label?: string;
  status?: string;
  metrics?: {
    IC?: number;
    Sharpe?: number;
    annualized_return?: number;
    max_drawdown?: number;
    [key: string]: any;
  };
}

export default function LoopMetricsComparison({
  loops,
  taskType,
  onLoopSelect,
  selectedLoopIndex,
}: LoopMetricsComparisonProps) {
  if (!loops || loops.length === 0) {
    return null;
  }

  // 提取每个 Loop 的指标
  const loopMetricsList: (LoopMetrics & { sourceLoop: number })[] = loops
    .filter(loop => loop.metrics_json && Object.keys(loop.metrics_json).length > 0)
    .map((loop, idx) => ({
      loop_index: loop.loop_index,
      label: loop.config_json?.label || `Loop ${loop.loop_index}`,
      status: loop.status,
      metrics: loop.metrics_json || {},
      sourceLoop: idx,
    }));

  if (loopMetricsList.length === 0) {
    return null;
  }

  // 策略演进任务：使用 strategy_evo_config 中的配置标签
  if (taskType === "strategy_evo") {
    // 尝试从 task 的 strategy_evo_config 读取 loop 配置
    const taskLoops: LoopMetrics[] = [];
    // 由于 loopMetricsList 已经有了，直接使用即可
  }

  // 计算最优 Loop：优先级 1. 年化收益（越高越好），2. 最大回撤（绝对值越小越好）
  const bestLoop = loopMetricsList.reduce((best, current) => {
    const currentAnnReturn = current.metrics?.annualized_return ?? -Infinity;
    const bestAnnReturn = best.metrics?.annualized_return ?? -Infinity;
    const currentMaxDD = Math.abs(current.metrics?.max_drawdown ?? Infinity);
    const bestMaxDD = Math.abs(best.metrics?.max_drawdown ?? Infinity);

    // 年化收益优先
    if (currentAnnReturn > bestAnnReturn) {
      return current;
    }
    // 年化收益相同时，比较最大回撤
    if (currentAnnReturn === bestAnnReturn && currentMaxDD < bestMaxDD) {
      return current;
    }
    return best;
  });

  // 格式化百分比值
  const formatPercent = (val: number | undefined): string => {
    if (val === undefined || val === null) return "-";
    const percent = val * 100;
    return `${percent >= 0 ? "+" : ""}${percent.toFixed(2)}%`;
  };

  const formatNumber = (val: number | undefined, decimals: number = 4): string => {
    if (val === undefined || val === null) return "-";
    return val.toFixed(decimals);
  };

  // 状态样式
  const getStatusStyle = (status?: string): React.CSSProperties => {
    switch (status) {
      case "completed":
        return { color: "#16a34a", fontWeight: 500 };
      case "running":
        return { color: "#ea580c", fontWeight: 500 };
      case "failed":
        return { color: "#dc2626", fontWeight: 500 };
      case "pending":
        return { color: "#64748b", fontWeight: 500 };
      default:
        return { color: "#64748b", fontWeight: 500 };
    }
  };

  const getStatusText = (status?: string): string => {
    switch (status) {
      case "completed": return "完成";
      case "running": return "运行中";
      case "failed": return "失败";
      case "pending": return "等待";
      case "processing": return "处理中";
      default: return status || "-";
    }
  };

  return (
    <div style={{
      backgroundColor: "#f8fafc",
      border: "1px solid #e5e7eb",
      borderRadius: "8px",
      padding: "16px",
      marginTop: "16px",
    }}>
      <div style={{
        display: "flex",
        alignItems: "center",
        gap: "8px",
        marginBottom: "12px",
      }}>
        <BarChart3 size={18} color="#64748b" />
        <h3 style={{ margin: 0, fontSize: "15px", fontWeight: 600, color: "#1e293b" }}>
          Loop 指标对比
        </h3>
        {bestLoop && (
          <div style={{
            marginLeft: "auto",
            fontSize: "12px",
            color: "#64748b",
            display: "flex",
            alignItems: "center",
            gap: "4px",
          }}>
            <span>最优配置：</span>
            <span style={{
              padding: "2px 8px",
              backgroundColor: "#fef3c7",
              color: "#d97706",
              borderRadius: "12px",
              fontSize: "11px",
              fontWeight: 600,
            }}>
              Loop {bestLoop.loop_index} - {bestLoop.label}
            </span>
          </div>
        )}
      </div>

      {/* 指标对比表 */}
      <div style={{
        overflowX: "auto",
        border: "1px solid #e5e7eb",
        borderRadius: "6px",
        backgroundColor: "#fff",
      }}>
        <table style={{
          width: "100%",
          borderCollapse: "collapse",
          fontSize: "13px",
        }}>
          <thead>
            <tr style={{
              backgroundColor: "#f1f5f9",
              borderBottom: "2px solid #e5e7eb",
            }}>
              <th style={{
                padding: "10px 12px",
                textAlign: "left",
                fontWeight: 600,
                color: "#475569",
                borderRight: "1px solid #e5e7eb",
              }}>Loop</th>
              <th style={{
                padding: "10px 12px",
                textAlign: "left",
                fontWeight: 600,
                color: "#475569",
                borderRight: "1px solid #e5e7eb",
              }}>标签</th>
              <th style={{
                padding: "10px 12px",
                textAlign: "right",
                fontWeight: 600,
                color: "#475569",
                borderRight: "1px solid #e5e7eb",
              }}>IC</th>
              <th style={{
                padding: "10px 12px",
                textAlign: "right",
                fontWeight: 600,
                color: "#475569",
                borderRight: "1px solid #e5e7eb",
              }}>Sharpe</th>
              <th style={{
                padding: "10px 12px",
                textAlign: "right",
                fontWeight: 600,
                color: "#475569",
                borderRight: "1px solid #e5e7eb",
              }}>
                年化收益
              </th>
              <th style={{
                padding: "10px 12px",
                textAlign: "right",
                fontWeight: 600,
                color: "#475569",
                borderRight: "1px solid #e5e7eb",
              }}>
                最大回撤
              </th>
              <th style={{
                padding: "10px 12px",
                textAlign: "center",
                fontWeight: 600,
                color: "#475569",
              }}>状态</th>
            </tr>
          </thead>
          <tbody>
            {loopMetricsList.map((loop, idx) => {
              const isBest = loop.loop_index === bestLoop.loop_index;
              const isSelected = loop.loop_index === selectedLoopIndex;
              return (
                <tr
                  key={loop.loop_index}
                  onClick={() => onLoopSelect && onLoopSelect(loop.loop_index)}
                  style={{
                    cursor: onLoopSelect ? "pointer" : "default",
                    backgroundColor: isSelected ? "#f5f3ff" : (idx % 2 === 0 ? "#fff" : "#f8fafc"),
                    borderBottom: "1px solid #e5e7eb",
                    transition: "background-color 0.2s",
                  }}
                  onMouseEnter={(e) => {
                    if (onLoopSelect && !isSelected) {
                      e.currentTarget.style.backgroundColor = "#f1f5f9";
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (onLoopSelect && !isSelected) {
                      e.currentTarget.style.backgroundColor = idx % 2 === 0 ? "#fff" : "#f8fafc";
                    }
                  }}
                >
                  <td style={{
                    padding: "10px 12px",
                    textAlign: "left",
                    borderRight: "1px solid #e5e7eb",
                    fontWeight: isBest ? 600 : 400,
                  }}>
                    {isBest && <TrendingUp size={14} color="#16a34a" style={{ verticalAlign: "middle", marginRight: "4px" }} />}
                    L{loop.loop_index}
                  </td>
                  <td style={{
                    padding: "10px 12px",
                    textAlign: "left",
                    borderRight: "1px solid #e5e7eb",
                    fontWeight: isBest ? 600 : 400,
                    color: isBest ? "#16a34a" : "inherit",
                    maxWidth: "150px",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}>
                    {loop.label || `-`}
                  </td>
                  <td style={{
                    padding: "10px 12px",
                    textAlign: "right",
                    borderRight: "1px solid #e5e7eb",
                    fontWeight: isBest ? 600 : 400,
                  }}>
                    {formatNumber(loop.metrics?.IC, 4)}
                  </td>
                  <td style={{
                    padding: "10px 12px",
                    textAlign: "right",
                    borderRight: "1px solid #e5e7eb",
                    fontWeight: isBest ? 600 : 400,
                  }}>
                    {formatNumber(loop.metrics?.Sharpe, 2)}
                  </td>
                  <td style={{
                    padding: "10px 12px",
                    textAlign: "right",
                    borderRight: "1px solid #e5e7eb",
                    fontWeight: isBest ? 600 : 400,
                    color: (loop.metrics?.annualized_return ?? 0) > 0 ? "#16a34a" : (loop.metrics?.annualized_return ?? 0) < 0 ? "#dc2626" : "inherit",
                  }}>
                    {formatPercent(loop.metrics?.annualized_return)}
                  </td>
                  <td style={{
                    padding: "10px 12px",
                    textAlign: "right",
                    borderRight: "1px solid #e5e7eb",
                    fontWeight: isBest ? 600 : 400,
                    color: "#16a34a",
                  }}>
                    {formatPercent(loop.metrics?.max_drawdown)}
                  </td>
                  <td style={{
                    padding: "10px 12px",
                    textAlign: "center",
                    ...getStatusStyle(loop.status),
                  }}>
                    {getStatusText(loop.status)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* 图例说明 */}
      <div style={{
        marginTop: "12px",
        fontSize: "12px",
        color: "#64748b",
        display: "flex",
        gap: "16px",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: "4px" }}>
          <TrendingUp size={14} color="#16a34a" />
          <span>最优配置（年化收益优先，最大回撤第二）</span>
        </div>
      </div>
    </div>
  );
}
