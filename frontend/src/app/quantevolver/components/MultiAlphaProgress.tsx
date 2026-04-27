"use client";

import React, { useEffect, useState } from "react";

interface GroupStatus {
  group_name: string;
  model_id?: string | null;
  assigned_node_id?: string | null;
  status: string;
  group_ic?: number | null;
  error_message?: string | null;
}

interface MultiAlphaProgressProps {
  experimentId: string;
  apiBase: string;
  pollInterval?: number;
}

const STAGE_LABELS: Record<string, string> = {
  pending_setup: "等待提交",
  pending_groups: "等待分组训练",
  group_training: "分组训练中",
  result_collection: "结果收集中",
  artifact_validation: "校验回测产物",
  failed_artifact: "产物校验失败",
  group_failed: "分组训练失败",
  failed: "执行失败",
  completed: "已完成",
};

const ARTIFACT_LABELS: Record<string, string> = {
  not_started: "产物未开始生成",
  pending: "产物待生成",
  validating: "产物校验中",
  failed: "产物校验失败",
  ready: "产物已就绪",
};

/**
 * Multi-Alpha execution progress.
 * Shows group training plus artifact validation/result collection stages.
 */
export default function MultiAlphaProgress({
  experimentId,
  apiBase,
  pollInterval = 10000,
}: MultiAlphaProgressProps) {
  const [groups, setGroups] = useState<GroupStatus[]>([]);
  const [stage, setStage] = useState<string>("pending_setup");
  const [artifactStatus, setArtifactStatus] = useState<string>("not_started");
  const [artifactErrors, setArtifactErrors] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [fetchError, setFetchError] = useState<string | null>(null);

  useEffect(() => {
    let failCount = 0;
    const fetchStatus = async () => {
      try {
        const statusResp = await fetch(
          `${apiBase}/quantevolver/experiments/${experimentId}/run-status`
        );
        if (!statusResp.ok) throw new Error(`run-status HTTP ${statusResp.status}`);
        const statusData = await statusResp.json();
        const lifecycle = statusData.multi_alpha || {};
        const nextStage = statusData.multi_alpha_stage || lifecycle.stage || "pending_setup";
        const nextArtifact = statusData.artifact_status || lifecycle.artifact_status || "not_started";
        const nextErrors = statusData.error
          ? [statusData.error]
          : Array.isArray(lifecycle.artifact_errors)
            ? lifecycle.artifact_errors
            : [];

        setStage(nextStage);
        setArtifactStatus(nextArtifact);
        setArtifactErrors(nextErrors);

        if (Array.isArray(lifecycle.groups) && lifecycle.groups.length > 0) {
          setGroups(lifecycle.groups);
        }

        const resp = await fetch(
          `${apiBase}/quantevolver/experiments/${experimentId}/multi-alpha/results`
        );
        if (resp.status === 404) {
          setLoading(false);
          failCount = 0;
          setFetchError(null);
          return;
        }
        if (resp.status === 409) {
          const data = await resp.json().catch(() => ({}));
          throw new Error(data.detail || "Multi-Alpha artifacts are not ready");
        }
        if (!resp.ok) throw new Error(`results HTTP ${resp.status}`);
        const data = await resp.json();
        if (data.ok) {
          setGroups(Array.isArray(data.groups) ? data.groups : []);
          if (data.stage) setStage(data.stage);
          if (data.artifact_status) setArtifactStatus(data.artifact_status);
          if (Array.isArray(data.artifact_errors)) setArtifactErrors(data.artifact_errors);
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

  const byNode: Record<string, GroupStatus[]> = {};
  for (const g of groups) {
    const node = g.assigned_node_id || "default";
    if (!byNode[node]) byNode[node] = [];
    byNode[node].push(g);
  }

  const allComplete = groups.length > 0 && groups.every((g) => g.status === "completed");
  const anyFailed = groups.some((g) => g.status === "failed") || artifactStatus === "failed";
  const completedCount = groups.filter((g) => g.status === "completed").length;
  const metaDone = stage === "completed";
  const metaRunning = stage === "result_collection" || stage === "artifact_validation";

  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs">
        <div className="flex items-center justify-between gap-3">
          <div className="font-semibold text-slate-700">
            当前阶段：{STAGE_LABELS[stage] || stage}
          </div>
          <div className={artifactStatus === "failed" ? "text-red-600" : "text-slate-500"}>
            {ARTIFACT_LABELS[artifactStatus] || artifactStatus}
          </div>
        </div>
        {artifactErrors.length > 0 && (
          <div className="mt-2 rounded border border-red-200 bg-red-50 p-2 text-red-700">
            {artifactErrors.join("; ")}
          </div>
        )}
        {fetchError && (
          <div className="mt-2 rounded border border-red-200 bg-red-50 p-2 text-red-700">
            {fetchError}
          </div>
        )}
      </div>

      <div className="flex items-center gap-3">
        <div className="h-2 flex-1 overflow-hidden rounded-full bg-gray-200 dark:bg-gray-700">
          <div
            className={`h-full rounded-full transition-all ${
              anyFailed ? "bg-red-500" : allComplete ? "bg-green-500" : "bg-blue-500"
            }`}
            style={{ width: `${groups.length > 0 ? (completedCount / groups.length) * 100 : 0}%` }}
          />
        </div>
        <span className="text-xs text-gray-500">
          {completedCount}/{groups.length} 组
        </span>
      </div>

      {Object.entries(byNode).map(([nodeId, nodeGroups]) => (
        <div key={nodeId} className="space-y-2">
          <div className="text-xs font-medium text-gray-500">
            {nodeId === "wsl2-5080"
              ? "本机 WSL"
              : nodeId === "rdagent-node1"
                ? "远端 CPU 节点"
                : nodeId}
          </div>
          {nodeGroups.map((g) => (
            <div key={g.group_name} className="flex items-center gap-3 text-xs">
              <StatusIcon status={g.status} />
              <span className="w-28 truncate font-medium">{g.group_name}</span>
              <span className="text-gray-400">
                {(g.model_id || "").replace("__seed_", "").replace("__", "")}
              </span>
              {g.error_message && <span className="text-red-500">{g.error_message}</span>}
              <span className="ml-auto font-mono">
                {typeof g.group_ic === "number" ? `IC=${g.group_ic.toFixed(4)}` : ""}
              </span>
            </div>
          ))}
        </div>
      ))}

      <div className="flex items-center gap-2 text-xs">
        <StatusIcon status={metaDone ? "completed" : metaRunning ? "running" : anyFailed ? "failed" : "pending"} />
        <span className="font-medium">Meta-Model / 统一回测</span>
        <span className="text-gray-400">
          {metaDone
            ? "合成和统一回测已完成"
            : metaRunning
              ? "正在合成预测、统一回测或校验产物"
              : allComplete
                ? "等待结果收集"
                : `等待 ${Math.max(groups.length - completedCount, 0)} 个组完成`}
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
      return <span className="animate-pulse text-blue-500">&#9679;</span>;
    case "failed":
      return <span className="text-red-500">&#10007;</span>;
    default:
      return <span className="text-gray-300">&#9675;</span>;
  }
}
