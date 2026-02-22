"use client";

import React from "react";
import { CodeDetail } from "./types";
import { StatusBadge } from "./StatusBadge";

type Props = {
  data: CodeDetail;
  tab: "original" | "realtime";
  onTabChange: (tab: "original" | "realtime") => void;
  onClose: () => void;
};

export function CodeModal({ data, tab, onTabChange, onClose }: Props) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      style={{ backgroundColor: "rgba(0,0,0,0.65)" }}
      onClick={onClose}
    >
      <div
        className="rounded-xl shadow-2xl flex flex-col"
        style={{ width: "min(92vw, 1000px)", maxHeight: "88vh", background: "#ffffff" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-6 py-4 border-b">
          <div>
            <h3 className="font-semibold text-gray-900">{data.factor_name}</h3>
            <p className="text-xs text-gray-500 mt-0.5">
              来源: {data.source}&nbsp;·&nbsp;
              状态: <StatusBadge status={data.transformation_status} />
            </p>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex rounded-lg border overflow-hidden">
              <button
                onClick={() => onTabChange("original")}
                className={`px-3 py-1.5 text-sm font-medium transition-colors ${
                  tab === "original"
                    ? "bg-blue-600 text-white"
                    : "bg-white text-gray-600 hover:bg-gray-50"
                }`}
              >
                原始代码
              </button>
              <button
                onClick={() => onTabChange("realtime")}
                className={`px-3 py-1.5 text-sm font-medium transition-colors ${
                  tab === "realtime"
                    ? "bg-green-600 text-white"
                    : "bg-white text-gray-600 hover:bg-gray-50"
                }`}
              >
                改造后代码
              </button>
            </div>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-600 text-xl font-bold leading-none"
            >
              ×
            </button>
          </div>
        </div>
        <div className="flex-1 overflow-auto p-4 flex flex-col gap-2">
          {tab === "original" && data.asset_path && (
            <div className="flex items-center gap-2 px-3 py-2 bg-blue-50 border border-blue-200 rounded-lg">
              <span className="text-xs text-blue-600 font-medium shrink-0">📁 原始文件路径:</span>
              <span className="text-xs font-mono text-blue-800 truncate" title={data.asset_path}>
                {data.asset_path}
              </span>
              {data._original_code_source === "filesystem" && (
                <span className="ml-auto text-xs text-blue-500 shrink-0">✓ 文件系统</span>
              )}
            </div>
          )}
          {tab === "original" && data._original_code_error && (
            <div className="px-3 py-2 bg-red-50 border border-red-200 rounded-lg">
              <span className="text-xs text-red-600">⚠ 读取原始代码失败: {data._original_code_error}</span>
            </div>
          )}
          {tab === "realtime" && data.qe_code_path && (
            <div className="flex items-center gap-2 px-3 py-2 bg-green-50 border border-green-200 rounded-lg">
              <span className="text-xs text-green-600 font-medium shrink-0">📄 改造后文件路径:</span>
              <span className="text-xs font-mono text-green-800 truncate" title={data.qe_code_path}>
                {data.qe_code_path}
              </span>
              {data._transformed_code_source === "filesystem" && (
                <span className="ml-auto text-xs text-green-500 shrink-0">✓ 文件系统</span>
              )}
            </div>
          )}
          {tab === "realtime" && data._transformed_code_error && (
            <div className="px-3 py-2 bg-red-50 border border-red-200 rounded-lg">
              <span className="text-xs text-red-600">⚠ 读取改造后代码失败: {data._transformed_code_error}</span>
            </div>
          )}
          <pre className="text-xs font-mono bg-gray-900 text-gray-100 p-4 rounded-lg overflow-auto whitespace-pre-wrap min-h-32 flex-1">
            {tab === "original"
              ? data.code_text || "（无原始代码，请确认 asset_path 字段已填充且文件存在）"
              : data.realtime_code_text || "（尚未改造或改造失败，qe_code_path 文件不存在）"}
          </pre>
        </div>
      </div>
    </div>
  );
}
