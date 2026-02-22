"use client";

import React from "react";
import { TransformJob, fmtTime } from "./types";
import { StatusBadge } from "./StatusBadge";

type Props = {
  jobs: TransformJob[];
  loading: boolean;
  onRefresh: () => void;
  onViewProgress: (jobId: string, factorName: string) => void;
};

export function JobsTable({ jobs, loading, onRefresh, onViewProgress }: Props) {
  return (
    <div className="bg-white rounded-xl border shadow-sm">
      <div className="flex items-center justify-between px-4 py-3 border-b">
        <span className="text-sm font-medium text-gray-700">
          最近改造任务（最多100条）
        </span>
        <button
          onClick={onRefresh}
          className="text-xs text-blue-600 hover:text-blue-800 font-medium"
        >
          刷新
        </button>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-16 text-gray-400 text-sm">
          加载中...
        </div>
      ) : jobs.length === 0 ? (
        <div className="flex items-center justify-center py-16 text-gray-400 text-sm">
          暂无改造任务
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-xs text-gray-500 uppercase tracking-wide">
                <th className="px-4 py-3 text-left">因子名称</th>
                <th className="px-4 py-3 text-left">来源</th>
                <th className="px-4 py-3 text-center">状态</th>
                <th className="px-4 py-3 text-center">LLM重试</th>
                <th className="px-4 py-3 text-left">错误信息</th>
                <th className="px-4 py-3 text-left">创建时间</th>
                <th className="px-4 py-3 text-left">完成时间</th>
                <th className="px-4 py-3 text-center">操作</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {jobs.map((job) => (
                <tr key={job.job_id} className="hover:bg-gray-50 transition-colors">
                  <td
                    className="px-4 py-3 font-mono text-xs text-gray-900 max-w-[12rem] truncate"
                    title={job.factor_name}
                  >
                    {job.factor_name}
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-500">{job.factor_source}</td>
                  <td className="px-4 py-3 text-center">
                    <StatusBadge status={job.status} />
                  </td>
                  <td className="px-4 py-3 text-center text-xs text-gray-500">
                    {job.llm_retry_count}/{job.max_llm_retries}
                  </td>
                  <td
                    className="px-4 py-3 text-xs text-red-500 max-w-[16rem] truncate"
                    title={job.error_message ?? ""}
                  >
                    {job.error_message || "-"}
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-400 whitespace-nowrap">
                    {fmtTime(job.created_at)}
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-400 whitespace-nowrap">
                    {fmtTime(job.completed_at)}
                  </td>
                  <td className="px-4 py-3 text-center">
                    <button
                      onClick={() => onViewProgress(job.job_id, job.factor_name)}
                      className="text-xs text-blue-600 hover:text-blue-800 font-medium px-2 py-1 rounded hover:bg-blue-50"
                    >
                      查看进度
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
