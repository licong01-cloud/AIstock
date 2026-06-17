"use client";

import React, { useState, useMemo } from "react";
import { FactorItem, fmtTime } from "./types";
import { StatusBadge } from "./StatusBadge";

const PAGE_SIZE_OPTIONS = [10, 20, 30, 50, 100];

type SortKey = "transformation_status" | "last_transformation_at" | null;
type SortDir = "asc" | "desc";

type Props = {
  factors: FactorItem[];
  loading: boolean;
  total: number;
  selectedFactors: Set<string>;
  statusFilter: string;
  sourceFilter: string;
  searchText: string;
  actionLoading: string | null;
  availableSources: string[];
  pageSize: number;
  currentPage: number;
  totalPages: number;
  onStatusFilterChange: (v: string) => void;
  onSourceFilterChange: (v: string) => void;
  onSearchChange: (v: string) => void;
  onToggleSelect: (name: string) => void;
  onToggleAll: () => void;
  onTransform: (factorName: string, source: string) => void;
  onViewCode: (factorName: string, source: string) => void;
  onReset: (factorName: string, source: string) => void;
  onRefresh: () => void;
  onPageChange: (page: number) => void;
  onPageSizeChange: (size: number) => void;
};

/** 状态排序权重 */
const STATUS_ORDER: Record<string, number> = {
  RULE_TRANSFORMING: 0,
  COMPILE_TESTING: 1,
  EXECUTION_TESTING: 2,
  LLM_REPAIRING: 3,
  ANALYSIS_REVIEWING: 4,
  SUCCESS: 5,
  FAILED: 6,
  PENDING: 7,
};

/** 根据改造状态返回行背景色（inline style，因项目未启用 Tailwind JIT） */
function rowBgStyle(status: string | null): React.CSSProperties {
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

export function FactorTable({
  factors, loading, total, selectedFactors,
  statusFilter, sourceFilter, searchText, actionLoading,
  availableSources, pageSize, currentPage, totalPages,
  onStatusFilterChange, onSourceFilterChange, onSearchChange,
  onToggleSelect, onToggleAll,
  onTransform, onViewCode, onReset, onRefresh,
  onPageChange, onPageSizeChange,
}: Props) {
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
    onPageChange(1);
  };

  /** 排序后的数据（仅对当前页传入的 factors 排序） */
  const sorted = useMemo(() => {
    if (!sortKey) return factors;
    const arr = [...factors];
    arr.sort((a, b) => {
      let cmp = 0;
      if (sortKey === "transformation_status") {
        const sa = STATUS_ORDER[a.transformation_status ?? "PENDING"] ?? 99;
        const sb = STATUS_ORDER[b.transformation_status ?? "PENDING"] ?? 99;
        cmp = sa - sb;
      } else if (sortKey === "last_transformation_at") {
        const ta = a.last_transformation_at ? new Date(a.last_transformation_at).getTime() : 0;
        const tb = b.last_transformation_at ? new Date(b.last_transformation_at).getTime() : 0;
        cmp = ta - tb;
      }
      return sortDir === "asc" ? cmp : -cmp;
    });
    return arr;
  }, [factors, sortKey, sortDir]);

  return (
    <div className="bg-white rounded-xl border shadow-sm">
      {/* 工具栏 */}
      <div className="flex flex-wrap gap-3 items-center px-4 py-3 border-b">
        <input
          type="text"
          placeholder="搜索因子名..."
          value={searchText}
          onChange={(e) => onSearchChange(e.target.value)}
          className="border rounded px-3 py-1.5 text-sm w-48 focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <select
          value={statusFilter}
          onChange={(e) => onStatusFilterChange(e.target.value)}
          title="按状态筛选"
          className="border rounded px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="all">全部状态</option>
          <option value="PENDING">待处理</option>
          <option value="SUCCESS">改造成功</option>
          <option value="FAILED">改造失败</option>
          <option value="LLM_REPAIRING">LLM修复中</option>
          <option value="no_code">无原始代码</option>
        </select>
        <select
          value={sourceFilter}
          onChange={(e) => onSourceFilterChange(e.target.value)}
          title="按来源筛选"
          className="border rounded px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="all">全部来源</option>
          {availableSources.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
        <span className="text-xs text-gray-500 ml-auto">
          显示 {(currentPage - 1) * pageSize + 1}–{Math.min(currentPage * pageSize, total)} / {total} 个因子
          {selectedFactors.size > 0 && `，已选 ${selectedFactors.size} 个`}
        </span>
        <select
          value={pageSize}
          onChange={(e) => onPageSizeChange(Number(e.target.value))}
          title="每页显示数量"
          className="border rounded px-2 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
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

      {/* 表格内容 */}
      {loading ? (
        <div className="flex items-center justify-center py-16 text-gray-400 text-sm">
          加载中...
        </div>
      ) : factors.length === 0 ? (
        <div className="flex items-center justify-center py-16 text-gray-400 text-sm">
          暂无数据
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-xs text-gray-500 uppercase tracking-wide">
                <th className="px-3 py-3 text-left w-10">
                  <input
                    type="checkbox"
                    checked={selectedFactors.size === factors.length && factors.length > 0}
                    onChange={onToggleAll}
                    aria-label="全选"
                    className="rounded"
                  />
                </th>
                <th className="px-3 py-3 text-left whitespace-nowrap">因子名称</th>
                <th className="px-3 py-3 text-left whitespace-nowrap">来源</th>
                <th className="px-3 py-3 text-center whitespace-nowrap">SOTA</th>
                <th className="px-3 py-3 text-center whitespace-nowrap">IC</th>
                <th className="px-3 py-3 text-center whitespace-nowrap">Sharpe</th>
                <th className="px-3 py-3 text-center whitespace-nowrap" style={{ minWidth: 72 }}>原始代码</th>
                <th
                  className="px-3 py-3 text-center whitespace-nowrap cursor-pointer select-none hover:text-gray-700"
                  style={{ minWidth: 100 }}
                  onClick={() => handleSort("transformation_status")}
                >
                  改造状态
                  <SortArrow active={sortKey === "transformation_status"} dir={sortDir} />
                </th>
                <th className="px-3 py-3 text-center whitespace-nowrap" style={{ minWidth: 72 }}>非官方代码</th>
                <th className="px-3 py-3 text-left whitespace-nowrap">QE文件路径</th>
                <th
                  className="px-3 py-3 text-left whitespace-nowrap cursor-pointer select-none hover:text-gray-700"
                  style={{ minWidth: 155 }}
                  onClick={() => handleSort("last_transformation_at")}
                >
                  上次改造
                  <SortArrow active={sortKey === "last_transformation_at"} dir={sortDir} />
                </th>
                <th className="px-3 py-3 text-center whitespace-nowrap" style={{ minWidth: 160 }}>操作</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {sorted.map((f) => (
                <tr
                  key={`${f.factor_name}-${f.source}`}
                  style={rowBgStyle(f.transformation_status)}
                >
                  <td className="px-3 py-2.5">
                    <input
                      type="checkbox"
                      checked={selectedFactors.has(f.factor_name)}
                      onChange={() => onToggleSelect(f.factor_name)}
                      aria-label={`选择 ${f.factor_name}`}
                      className="rounded"
                    />
                  </td>
                  <td
                    className="px-3 py-2.5 font-mono text-xs text-gray-900 whitespace-nowrap"
                    title={f.factor_name}
                  >
                    {f.factor_name}
                  </td>
                  <td className="px-3 py-2.5 text-xs whitespace-nowrap">
                    <span className={f.source === "alpha158" ? "text-blue-700 font-semibold" : "text-gray-500"}>
                      {f.source}
                    </span>
                  </td>
                  <td className="px-3 py-2.5 text-center whitespace-nowrap">
                    {f.is_sota_factor ? (
                      <span className="text-yellow-500 text-xs font-bold">★ SOTA</span>
                    ) : (
                      <span className="text-gray-300 text-xs">-</span>
                    )}
                  </td>
                  <td className="px-3 py-2.5 text-center text-xs whitespace-nowrap">
                    {f.ic != null ? (
                      <span className={f.ic > 0 ? "text-green-600" : "text-red-500"}>
                        {f.ic.toFixed(4)}
                      </span>
                    ) : "-"}
                  </td>
                  <td className="px-3 py-2.5 text-center text-xs whitespace-nowrap">
                    {f.sharpe != null ? (
                      <span className={f.sharpe > 0 ? "text-green-600" : "text-red-500"}>
                        {f.sharpe.toFixed(3)}
                      </span>
                    ) : "-"}
                  </td>
                  <td className="px-3 py-2.5 text-center whitespace-nowrap">
                    {f.has_original_code ? (
                      <span className="text-green-500 text-xs">✓ 有</span>
                    ) : (
                      <span className="text-gray-300 text-xs">✗ 无</span>
                    )}
                  </td>
                  <td className="px-3 py-2.5 text-center whitespace-nowrap">
                    <StatusBadge status={f.transformation_status} />
                  </td>
                  <td className="px-3 py-2.5 text-center whitespace-nowrap">
                    {f.has_realtime_code ? (
                      <span className="text-green-500 text-xs">✓ 有</span>
                    ) : (
                      <span className="text-gray-300 text-xs">✗ 无</span>
                    )}
                  </td>
                  <td className="px-3 py-2.5 text-xs whitespace-nowrap" title={f.qe_code_path || ""}>
                    {f.qe_code_path ? (
                      <span className="font-mono text-green-700">{f.qe_code_path}</span>
                    ) : (
                      <span className="text-gray-300">-</span>
                    )}
                  </td>
                  <td className="px-3 py-2.5 text-xs text-gray-400 whitespace-nowrap">
                    {fmtTime(f.last_transformation_at)}
                  </td>
                  <td className="px-3 py-2.5 whitespace-nowrap">
                    <div className="flex items-center gap-1.5 justify-center flex-nowrap">
                      {f.has_original_code && (
                        <button
                          onClick={() => onTransform(f.factor_name, f.source)}
                          disabled={actionLoading === f.factor_name}
                          className="px-2 py-1 text-xs bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 transition-colors whitespace-nowrap"
                        >
                          {actionLoading === f.factor_name ? "..." : "改造"}
                        </button>
                      )}
                      <button
                        onClick={() => onViewCode(f.factor_name, f.source)}
                        className="px-2 py-1 text-xs bg-gray-100 text-gray-600 rounded hover:bg-gray-200 transition-colors whitespace-nowrap"
                      >
                        查看
                      </button>
                      {(f.transformation_status === "SUCCESS" ||
                        f.transformation_status === "FAILED") && (
                        <button
                          onClick={() => onReset(f.factor_name, f.source)}
                          className="px-2 py-1 text-xs bg-orange-50 text-orange-600 rounded hover:bg-orange-100 transition-colors whitespace-nowrap"
                        >
                          重置
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* 分页导航 */}
      {!loading && factors.length > 0 && totalPages > 1 && (
        <div className="flex items-center justify-center gap-2 px-4 py-3 border-t">
          <button
            onClick={() => onPageChange(1)}
            disabled={currentPage === 1}
            className="px-2 py-1 text-xs rounded border disabled:opacity-40 hover:bg-gray-50 transition-colors"
          >
            «
          </button>
          <button
            onClick={() => onPageChange(currentPage - 1)}
            disabled={currentPage === 1}
            className="px-2 py-1 text-xs rounded border disabled:opacity-40 hover:bg-gray-50 transition-colors"
          >
            ‹
          </button>
          {Array.from({ length: Math.min(7, totalPages) }, (_, i) => {
            let page: number;
            if (totalPages <= 7) {
              page = i + 1;
            } else if (currentPage <= 4) {
              page = i + 1;
            } else if (currentPage >= totalPages - 3) {
              page = totalPages - 6 + i;
            } else {
              page = currentPage - 3 + i;
            }
            return (
              <button
                key={page}
                onClick={() => onPageChange(page)}
                className={`px-2.5 py-1 text-xs rounded border transition-colors ${
                  page === currentPage
                    ? "bg-blue-600 text-white border-blue-600"
                    : "hover:bg-gray-50"
                }`}
              >
                {page}
              </button>
            );
          })}
          <button
            onClick={() => onPageChange(currentPage + 1)}
            disabled={currentPage === totalPages}
            className="px-2 py-1 text-xs rounded border disabled:opacity-40 hover:bg-gray-50 transition-colors"
          >
            ›
          </button>
          <button
            onClick={() => onPageChange(totalPages)}
            disabled={currentPage === totalPages}
            className="px-2 py-1 text-xs rounded border disabled:opacity-40 hover:bg-gray-50 transition-colors"
          >
            »
          </button>
          <span className="text-xs text-gray-500 ml-2">
            第 {currentPage} / {totalPages} 页
          </span>
        </div>
      )}
    </div>
  );
}
