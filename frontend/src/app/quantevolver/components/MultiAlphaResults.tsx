"use client";

import React, { useEffect, useState } from "react";

interface GroupResult {
  group_name: string;
  factor_names?: string[];
  model_id?: string | null;
  dataset_type?: string | null;
  group_ic?: number | null;
  group_icir?: number | null;
  group_sharpe?: number | null;
  meta_weight?: number | null;
  assigned_node_id?: string | null;
  status: string;
  error_message?: string | null;
}

interface MetaWeightEntry {
  as_of_date: string | null;
  method: string;
  weights: Record<string, number>;
  combined_ic: number | null;
}

interface MultiAlphaResultsProps {
  experimentId: string;
  apiBase: string;
}

const STATUS_COLORS: Record<string, string> = {
  pending: "bg-gray-100 text-gray-600",
  running: "bg-blue-100 text-blue-700",
  completed: "bg-green-100 text-green-700",
  failed: "bg-red-100 text-red-700",
};

function formatNum(value: number | null | undefined, digits: number) {
  return typeof value === "number" ? value.toFixed(digits) : "-";
}

/**
 * Multi-Alpha result panel.
 * It renders an explicit not-ready/error state instead of an empty success table.
 */
export default function MultiAlphaResults({ experimentId, apiBase }: MultiAlphaResultsProps) {
  const [groups, setGroups] = useState<GroupResult[]>([]);
  const [metaHistory, setMetaHistory] = useState<MetaWeightEntry[]>([]);
  const [ready, setReady] = useState(false);
  const [stage, setStage] = useState<string | null>(null);
  const [artifactStatus, setArtifactStatus] = useState<string | null>(null);
  const [artifactErrors, setArtifactErrors] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchResults = async () => {
      try {
        const resp = await fetch(
          `${apiBase}/quantevolver/experiments/${experimentId}/multi-alpha/results`
        );
        if (resp.status === 404) {
          setError("Multi-Alpha 结果尚未生成，请等待分组训练、合成和统一回测完成。");
          setGroups([]);
          setReady(false);
          return;
        }
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) {
          throw new Error(data.detail || `HTTP ${resp.status}: ${resp.statusText}`);
        }
        if (!data.ok) {
          throw new Error(data.detail || "Multi-Alpha 结果接口返回失败");
        }

        const nextGroups = Array.isArray(data.groups) ? data.groups : [];
        setGroups(nextGroups);
        setMetaHistory(Array.isArray(data.meta_weights_history) ? data.meta_weights_history : []);
        setReady(Boolean(data.ready));
        setStage(data.stage || null);
        setArtifactStatus(data.artifact_status || null);
        setArtifactErrors(Array.isArray(data.artifact_errors) ? data.artifact_errors : []);
        setError(null);
      } catch (e: any) {
        setError(e.message || "网络请求失败");
        setReady(false);
      } finally {
        setLoading(false);
      }
    };
    fetchResults();
  }, [experimentId, apiBase]);

  if (loading) {
    return <div className="p-4 text-sm text-gray-500">加载 Multi-Alpha 结果...</div>;
  }

  const completedGroups = groups.filter((g) => g.group_ic !== null && g.group_ic !== undefined);
  const totalWeight = completedGroups.reduce((s, g) => s + (g.meta_weight || 0), 0);
  const showNotReady = !ready || error || artifactErrors.length > 0;

  return (
    <div className="space-y-6">
      {showNotReady && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
          <div className="font-semibold">Multi-Alpha 结果尚未就绪</div>
          <div className="mt-1">
            {error || artifactErrors.join("; ") || "后端仍在等待或校验必要产物，暂不展示空结果表。"}
          </div>
          {(stage || artifactStatus) && (
            <div className="mt-2 text-xs text-amber-700">
              stage={stage || "-"} / artifact_status={artifactStatus || "-"}
            </div>
          )}
        </div>
      )}

      {groups.length === 0 ? (
        <div className="p-4 text-sm text-gray-500">暂无 Multi-Alpha 分组状态。</div>
      ) : (
        <>
          <div className="flex items-center gap-6 rounded-lg bg-purple-50 p-3 dark:bg-purple-900/20">
            <div>
              <div className="text-xs text-gray-500">组数</div>
              <div className="text-lg font-bold text-purple-600">{groups.length}</div>
            </div>
            <div>
              <div className="text-xs text-gray-500">总因子数</div>
              <div className="text-lg font-bold">
                {groups.reduce((s, g) => s + (g.factor_names?.length || 0), 0)}
              </div>
            </div>
            {metaHistory.length > 0 && typeof metaHistory[0].combined_ic === "number" && (
              <div>
                <div className="text-xs text-gray-500">合成 IC</div>
                <div className="text-lg font-bold text-green-600">
                  {metaHistory[0].combined_ic.toFixed(4)}
                </div>
              </div>
            )}
          </div>

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
                  <tr key={g.group_name} className="border-b border-gray-100 dark:border-gray-700">
                    <td className="px-3 py-2 font-medium">{g.group_name}</td>
                    <td className="px-3 py-2">{g.factor_names?.length || 0}</td>
                    <td className="px-3 py-2 text-gray-500">
                      {(g.model_id || "").replace("__seed_", "").replace("__", "")}
                    </td>
                    <td className="px-3 py-2 text-right font-mono">{formatNum(g.group_ic, 4)}</td>
                    <td className="px-3 py-2 text-right font-mono">{formatNum(g.group_icir, 3)}</td>
                    <td className="px-3 py-2 text-right font-mono">
                      {typeof g.meta_weight === "number" ? `${(g.meta_weight * 100).toFixed(1)}%` : "-"}
                    </td>
                    <td className="px-3 py-2 text-gray-500">{g.assigned_node_id || "-"}</td>
                    <td className="px-3 py-2">
                      <StatusBadge status={g.status} />
                      {g.error_message && <span className="ml-2 text-red-600">{g.error_message}</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {ready && completedGroups.length > 0 && totalWeight > 0 && (
            <div className="space-y-2">
              <div className="text-xs font-medium text-gray-500">Meta-Model 权重分布</div>
              {completedGroups.map((g) => {
                const pct = totalWeight > 0 ? ((g.meta_weight || 0) / totalWeight) * 100 : 0;
                return (
                  <div key={g.group_name} className="flex items-center gap-2">
                    <div className="w-28 truncate text-xs">{g.group_name}</div>
                    <div className="h-4 flex-1 overflow-hidden rounded bg-gray-100 dark:bg-gray-700">
                      <div className="h-full rounded bg-purple-500 transition-all" style={{ width: `${pct}%` }} />
                    </div>
                    <div className="w-12 text-right font-mono text-xs">{pct.toFixed(1)}%</div>
                  </div>
                );
              })}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`rounded px-1.5 py-0.5 text-xs ${STATUS_COLORS[status] || STATUS_COLORS.pending}`}>
      {status}
    </span>
  );
}
