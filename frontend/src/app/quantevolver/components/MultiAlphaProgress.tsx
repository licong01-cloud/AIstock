"use client";

import React, { useEffect, useState } from "react";

interface GroupStatus {
  group_name: string;
  model_id: string;
  assigned_node_id: string | null;
  status: string;
  group_ic: number | null;
}

interface MultiAlphaProgressProps {
  experimentId: string;
  apiBase: string;
  pollInterval?: number;
}

/**
 * Multi-Alpha 分布式执行进度展示。
 * 按节点分组显示各 Alpha Group 的训练状态。
 */
export default function MultiAlphaProgress({
  experimentId,
  apiBase,
  pollInterval = 10000,
}: MultiAlphaProgressProps) {
  const [groups, setGroups] = useState<GroupStatus[]>([]);
  const [loading, setLoading] = useState(true);
  const [fetchError, setFetchError] = useState<string | null>(null);

  useEffect(() => {
    let failCount = 0;
    const fetchStatus = async () => {
      try {
        const resp = await fetch(
          `${apiBase}/quantevolver/experiments/${experimentId}/multi-alpha/results`
        );
        if (resp.status === 404) {
          // 正常：实验刚提交，还没有组数据
          setGroups([]);
          setLoading(false);
          return;
        }
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        if (data.ok) {
          setGroups(Array.isArray(data.groups) ? data.groups : []);
          failCount = 0;
          setFetchError(null);
        }
      } catch (e: any) {
        failCount++;
        if (failCount >= 3) {
          setFetchError(`进度查询失败: ${e.message || e}`);
        }
      } finally {
        setLoading(false);
      }
    };

    fetchStatus();
    const timer = setInterval(fetchStatus, pollInterval);
    return () => clearInterval(timer);
  }, [experimentId, apiBase, pollInterval]);

  if (loading) return <div className="text-sm text-gray-500">加载进度...</div>;
  if (fetchError) return <div className="text-sm text-red-500">{fetchError}</div>;

  // Group by node
  const byNode: Record<string, GroupStatus[]> = {};
  for (const g of groups) {
    const node = g.assigned_node_id || "default";
    if (!byNode[node]) byNode[node] = [];
    byNode[node].push(g);
  }

  const allComplete = groups.every((g) => g.status === "completed");
  const anyFailed = groups.some((g) => g.status === "failed");
  const completedCount = groups.filter((g) => g.status === "completed").length;

  return (
    <div className="space-y-4">
      {/* Overall progress */}
      <div className="flex items-center gap-3">
        <div className="flex-1 h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full transition-all ${
              anyFailed ? "bg-red-500" : allComplete ? "bg-green-500" : "bg-blue-500"
            }`}
            style={{
              width: `${groups.length > 0 ? (completedCount / groups.length) * 100 : 0}%`,
            }}
          />
        </div>
        <span className="text-xs text-gray-500">
          {completedCount}/{groups.length} 组
        </span>
      </div>

      {/* Per-node breakdown */}
      {Object.entries(byNode).map(([nodeId, nodeGroups]) => (
        <div key={nodeId} className="space-y-2">
          <div className="text-xs font-medium text-gray-500">
            {nodeId === "wsl2-5080"
              ? "本机 (RTX 5080)"
              : nodeId === "rdagent-node1"
                ? "远端 (192.168.50.215)"
                : nodeId}
          </div>
          {nodeGroups.map((g) => (
            <div
              key={g.group_name}
              className="flex items-center gap-3 text-xs"
            >
              <StatusIcon status={g.status} />
              <span className="w-28 truncate font-medium">
                {g.group_name}
              </span>
              <span className="text-gray-400">
                {(g.model_id || "").replace("__seed_", "").replace("__", "")}
              </span>
              <span className="ml-auto font-mono">
                {g.group_ic !== null ? `IC=${g.group_ic.toFixed(4)}` : ""}
              </span>
            </div>
          ))}
        </div>
      ))}

      {/* Meta-Model status */}
      <div className="flex items-center gap-2 text-xs">
        <StatusIcon status={allComplete ? "completed" : "pending"} />
        <span className="font-medium">Meta-Model</span>
        <span className="text-gray-400">
          {allComplete
            ? "所有组完成，合成中"
            : `等待 ${groups.length - completedCount} 个组完成`}
        </span>
      </div>
    </div>
  );
}

function StatusIcon({ status }: { status: string }) {
  switch (status) {
    case "completed":
      return <span className="text-green-500">&#10003;</span>;
    case "running":
      return <span className="text-blue-500 animate-pulse">&#9679;</span>;
    case "failed":
      return <span className="text-red-500">&#10007;</span>;
    default:
      return <span className="text-gray-300">&#9675;</span>;
  }
}
