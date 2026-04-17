"use client";

import React, { useEffect, useState } from "react";

interface GroupResult {
  group_name: string;
  factor_names: string[];
  model_id: string;
  dataset_type: string;
  group_ic: number | null;
  group_icir: number | null;
  group_sharpe: number | null;
  meta_weight: number | null;
  assigned_node_id: string | null;
  status: string;
  error_message: string | null;
}

interface MetaWeightEntry {
  as_of_date: string;
  method: string;
  weights: Record<string, number>;
  combined_ic: number | null;
}

interface MultiAlphaResultsProps {
  experimentId: string;
  apiBase: string;
}

/**
 * Multi-Alpha 实验结果展示组件。
 * 显示各组 IC/权重/状态 + Meta-Model 权重分布。
 */
export default function MultiAlphaResults({
  experimentId,
  apiBase,
}: MultiAlphaResultsProps) {
  const [groups, setGroups] = useState<GroupResult[]>([]);
  const [metaHistory, setMetaHistory] = useState<MetaWeightEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchResults = async () => {
      try {
        const resp = await fetch(
          `${apiBase}/quantevolver/experiments/${experimentId}/multi-alpha/results`
        );
        if (!resp.ok) {
          // 404 表示还没有结果 (未运行完) — 区别于其他 HTTP 错误
          if (resp.status === 404) {
            setError(null);
            setGroups([]);
            return;
          }
          throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
        }
        const data = await resp.json();
        if (data.ok) {
          setGroups(Array.isArray(data.groups) ? data.groups : []);
          setMetaHistory(Array.isArray(data.meta_weights_history) ? data.meta_weights_history : []);
        } else {
          setError(data.detail || "Failed to load results");
        }
      } catch (e: any) {
        setError(e.message || "网络请求失败");
      } finally {
        setLoading(false);
      }
    };
    fetchResults();
  }, [experimentId, apiBase]);

  if (loading)
    return (
      <div className="text-sm text-gray-500 p-4">加载 Multi-Alpha 结果...</div>
    );
  if (error)
    return <div className="text-sm text-red-500 p-4">{error}</div>;
  if (groups.length === 0)
    return (
      <div className="text-sm text-gray-500 p-4">暂无 Multi-Alpha 组结果</div>
    );

  // Compute combined stats
  const completedGroups = groups.filter((g) => g.group_ic !== null);
  const totalWeight = completedGroups.reduce(
    (s, g) => s + (g.meta_weight || 0),
    0
  );

  return (
    <div className="space-y-6">
      {/* Summary bar */}
      <div className="flex items-center gap-6 p-3 bg-purple-50 dark:bg-purple-900/20 rounded-lg">
        <div>
          <div className="text-xs text-gray-500">组数</div>
          <div className="text-lg font-bold text-purple-600">
            {groups.length}
          </div>
        </div>
        <div>
          <div className="text-xs text-gray-500">总因子数</div>
          <div className="text-lg font-bold">
            {groups.reduce((s, g) => s + (g.factor_names?.length || 0), 0)}
          </div>
        </div>
        {metaHistory.length > 0 && metaHistory[0].combined_ic && (
          <div>
            <div className="text-xs text-gray-500">合成 IC</div>
            <div className="text-lg font-bold text-green-600">
              {metaHistory[0].combined_ic.toFixed(4)}
            </div>
          </div>
        )}
      </div>

      {/* Group table */}
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="bg-gray-50 dark:bg-gray-800">
            <tr>
              <th className="px-3 py-2 text-left">组</th>
              <th className="px-3 py-2 text-left">因子数</th>
              <th className="px-3 py-2 text-left">模型</th>
              <th className="px-3 py-2 text-right">IC</th>
              <th className="px-3 py-2 text-right">ICIR</th>
              <th className="px-3 py-2 text-right">权重</th>
              <th className="px-3 py-2 text-left">节点</th>
              <th className="px-3 py-2 text-left">状态</th>
            </tr>
          </thead>
          <tbody>
            {groups.map((g) => (
              <tr
                key={g.group_name}
                className="border-b border-gray-100 dark:border-gray-700"
              >
                <td className="px-3 py-2 font-medium">{g.group_name}</td>
                <td className="px-3 py-2">
                  {g.factor_names?.length || 0}
                </td>
                <td className="px-3 py-2 text-gray-500">
                  {(g.model_id || "")
                    .replace("__seed_", "")
                    .replace("__", "")}
                </td>
                <td className="px-3 py-2 text-right font-mono">
                  {g.group_ic?.toFixed(4) || "—"}
                </td>
                <td className="px-3 py-2 text-right font-mono">
                  {g.group_icir?.toFixed(3) || "—"}
                </td>
                <td className="px-3 py-2 text-right font-mono">
                  {g.meta_weight !== null
                    ? `${(g.meta_weight * 100).toFixed(1)}%`
                    : "—"}
                </td>
                <td className="px-3 py-2 text-gray-500">
                  {g.assigned_node_id || "—"}
                </td>
                <td className="px-3 py-2">
                  <StatusBadge status={g.status} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Weight bar chart (simple CSS bars) */}
      {completedGroups.length > 0 && totalWeight > 0 && (
        <div className="space-y-2">
          <div className="text-xs font-medium text-gray-500">
            Meta-Model 权重分布
          </div>
          {completedGroups.map((g) => {
            const pct =
              totalWeight > 0 ? ((g.meta_weight || 0) / totalWeight) * 100 : 0;
            return (
              <div key={g.group_name} className="flex items-center gap-2">
                <div className="w-28 text-xs truncate">{g.group_name}</div>
                <div className="flex-1 h-4 bg-gray-100 dark:bg-gray-700 rounded overflow-hidden">
                  <div
                    className="h-full bg-purple-500 rounded transition-all"
                    style={{ width: `${pct}%` }}
                  />
                </div>
                <div className="w-12 text-xs text-right font-mono">
                  {pct.toFixed(1)}%
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    pending: "bg-gray-100 text-gray-600",
    running: "bg-blue-100 text-blue-700",
    completed: "bg-green-100 text-green-700",
    failed: "bg-red-100 text-red-700",
  };
  return (
    <span
      className={`px-1.5 py-0.5 rounded text-xs ${colors[status] || colors.pending}`}
    >
      {status}
    </span>
  );
}
