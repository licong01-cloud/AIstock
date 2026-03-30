"use client";

import React, { useState, useMemo } from "react";
import { TransformJob, fmtTime } from "./types";
import { StatusBadge } from "./StatusBadge";

const PAGE_SIZE_OPTIONS = [20, 50, 100];

type SortKey = "status" | "created_at" | null;
type SortDir = "asc" | "desc";

type Props = {
  jobs: TransformJob[];
  loading: boolean;
  onRefresh: () => void;
  onViewProgress: (jobId: string, factorName: string) => void;
};

/** 状态排序权重: 进行中 > 待处理 > 成功 > 失败 */
const STATUS_ORDER: Record<string, number> = {
  RULE_TRANSFORMING: 0,
  COMPILE_TESTING: 1,
  EXECUTION_TESTING: 2,
  LLM_REPAIRING: 3,
  ANALYSIS_REVIEWING: 4,
  PENDING: 5,
  SUCCESS: 6,
  FAILED: 7,
};

/** 根据任务状态返回行背景色（inline style，因项目未启用 Tailwind JIT） */
function rowBgStyle(status: string): React.CSSProperties {
  switch (status) {
    case "SUCCESS":
      return { backgroundColor: "#dcfce7" };       // 淡绿色
    case "FAILED":
      return { backgroundColor: "#fee2e2" };       // 淡红色
    case "RULE_TRANSFORMING":
    case "COMPILE_TESTING":
    case "EXECUTION_TESTING":
    case "LLM_REPAIRING":
    case "ANALYSIS_REVIEWING":
      return { backgroundColor: "#dbeafe" };       // 淡蓝色
    case "PENDING":
    default:
      return {};
  }
}

/** 排序箭头指示器 */
function SortArrow({ active, dir }: { active: boolean; dir: SortDir }) {
  if (!active) return <span className="text-gray-300 ml-1">↕</span>;
  return <span className="text-blue-600 ml-1">{dir === "asc" ? "↑" : "↓"}</span>;
}

export function JobsTable({ jobs, loading, onRefresh, onViewProgress }: Props) {
  const [pageSize, setPageSize] = useState(100);
  const [currentPage, setCurrentPage] = useState(1);
  const [sortKey, setSortKey] = useState<SortKey>(null);
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  /** 点击列头排序: asc → desc → 取消 */
  const handleSort = (key: SortKey) => {
    if (sortKey !== key) {
      setSortKey(key);
      setSortDir("asc");
    } else if (sortDir === "asc") {
      setSortDir("desc");
    } else {
      setSortKey(null);
      setSortDir("asc");
    }
    setCurrentPage(1);
  };

  /** 排序后的数据 */
  const sorted = useMemo(() => {
    if (!sortKey) return jobs;
    const arr = [...jobs];
    arr.sort((a, b) => {
      let cmp = 0;
      if (sortKey === "status") {
        cmp = (STATUS_ORDER[a.status] ?? 99) - (STATUS_ORDER[b.status] ?? 99);
      } else if (sortKey === "created_at") {
        const ta = a.created_at ? new Date(a.created_at).getTime() : 0;
        const tb = b.created_at ? new Date(b.created_at).getTime() : 0;
        cmp = ta - tb;
      }
      return sortDir === "asc" ? cmp : -cmp;
    });
    return arr;
  }, [jobs, sortKey, sortDir]);

  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize));
  const safePage = Math.min(currentPage, totalPages);
  const paged = sorted.slice((safePage - 1) * pageSize, safePage * pageSize);

  return (
    <div className="bg-white rounded-xl border shadow-sm">
      <div className="flex items-center justify-between px-4 py-3 border-b">
        <span className="text-sm font-medium text-gray-700">
          改造任务（共 {jobs.length} 条）
        </span>
        <div className="flex items-center gap-3">
          <select
            value={pageSize}
            onChange={(e) => { setPageSize(Number(e.target.value)); setCurrentPage(1); }}
            title="每页显示数量"
            className="border rounded px-2 py-1.5 text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {PAGE_SIZE_OPTIONS.map((s) => (
              <option key={s} value={s}>每页 {s}</option>
            ))}
          </select>
          <button
            onClick={onRefresh}
            className="text-xs text-blue-600 hover:text-blue-800 font-medium"
          >
            刷新
          </button>
        </div>
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
                <th className="px-3 py-3 text-left whitespace-nowrap">因子名称</th>
                <th
                  className="px-3 py-3 text-center whitespace-nowrap cursor-pointer select-none hover:text-gray-700"
                  style={{ minWidth: 100 }}
                  onClick={() => handleSort("status")}
                >
                  状态
                  <SortArrow active={sortKey === "status"} dir={sortDir} />
                </th>
                <th className="px-3 py-3 text-center whitespace-nowrap">LLM重试</th>
                <th className="px-3 py-3 text-left whitespace-nowrap">错误信息</th>
                <th
                  className="px-3 py-3 text-left whitespace-nowrap cursor-pointer select-none hover:text-gray-700"
                  style={{ minWidth: 155 }}
                  onClick={() => handleSort("created_at")}
                >
                  创建时间
                  <SortArrow active={sortKey === "created_at"} dir={sortDir} />
                </th>
                <th className="px-3 py-3 text-left whitespace-nowrap" style={{ minWidth: 155 }}>完成时间</th>
                <th className="px-3 py-3 text-center whitespace-nowrap" style={{ minWidth: 90 }}>操作</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {paged.map((job) => (
                <tr key={job.job_id} style={rowBgStyle(job.status)}>
                  <td
                    className="px-3 py-2.5 font-mono text-xs text-gray-900 whitespace-nowrap"
                    title={job.factor_name}
                  >
                    {job.factor_name}
                  </td>
                  <td className="px-3 py-2.5 text-center whitespace-nowrap">
                    <StatusBadge status={job.status} />
                  </td>
                  <td className="px-3 py-2.5 text-center text-xs text-gray-500 whitespace-nowrap">
                    {job.llm_retry_count}/{job.max_llm_retries}
                  </td>
                  <td
                    className="px-3 py-2.5 text-xs text-red-500 max-w-[20rem] truncate"
                    title={job.error_message ?? ""}
                  >
                    {job.error_message || "-"}
                  </td>
                  <td className="px-3 py-2.5 text-xs text-gray-400 whitespace-nowrap">
                    {fmtTime(job.created_at)}
                  </td>
                  <td className="px-3 py-2.5 text-xs text-gray-400 whitespace-nowrap">
                    {fmtTime(job.completed_at)}
                  </td>
                  <td className="px-3 py-2.5 text-center whitespace-nowrap">
                    <button
                      onClick={() => onViewProgress(job.job_id, job.factor_name)}
                      className="text-xs text-blue-600 hover:text-blue-800 font-medium px-2 py-1 rounded hover:bg-blue-50 whitespace-nowrap"
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

      {/* 分页导航 */}
      {!loading && sorted.length > 0 && totalPages > 1 && (
        <div className="flex items-center justify-center gap-2 px-4 py-3 border-t">
          <button
            onClick={() => setCurrentPage(1)}
            disabled={safePage === 1}
            className="px-2 py-1 text-xs rounded border disabled:opacity-40 hover:bg-gray-50 transition-colors"
          >
            «
          </button>
          <button
            onClick={() => setCurrentPage(safePage - 1)}
            disabled={safePage === 1}
            className="px-2 py-1 text-xs rounded border disabled:opacity-40 hover:bg-gray-50 transition-colors"
          >
            ‹
          </button>
          {Array.from({ length: Math.min(7, totalPages) }, (_, i) => {
            let page: number;
            if (totalPages <= 7) {
              page = i + 1;
            } else if (safePage <= 4) {
              page = i + 1;
            } else if (safePage >= totalPages - 3) {
              page = totalPages - 6 + i;
            } else {
              page = safePage - 3 + i;
            }
            return (
              <button
                key={page}
                onClick={() => setCurrentPage(page)}
                className={`px-2.5 py-1 text-xs rounded border transition-colors ${
                  page === safePage
                    ? "bg-blue-600 text-white border-blue-600"
                    : "hover:bg-gray-50"
                }`}
              >
                {page}
              </button>
            );
          })}
          <button
            onClick={() => setCurrentPage(safePage + 1)}
            disabled={safePage === totalPages}
            className="px-2 py-1 text-xs rounded border disabled:opacity-40 hover:bg-gray-50 transition-colors"
          >
            ›
          </button>
          <button
            onClick={() => setCurrentPage(totalPages)}
            disabled={safePage === totalPages}
            className="px-2 py-1 text-xs rounded border disabled:opacity-40 hover:bg-gray-50 transition-colors"
          >
            »
          </button>
          <span className="text-xs text-gray-500 ml-2">
            第 {safePage} / {totalPages} 页
          </span>
        </div>
      )}
    </div>
  );
}
