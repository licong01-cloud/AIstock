"use client";

import React from "react";

interface MultiAlphaModeToggleProps {
  alphaMode: "single" | "multi";
  onModeChange: (mode: "single" | "multi") => void;
}

/**
 * Alpha 模式切换组件 — 在 Compose Step 1 顶部显示。
 * 切换 single / multi 模式，multi 模式启用分组因子编辑。
 */
export default function MultiAlphaModeToggle({
  alphaMode,
  onModeChange,
}: MultiAlphaModeToggleProps) {
  return (
    <div className="flex items-center gap-4 mb-4 p-3 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
      <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
        Alpha 模式:
      </span>
      <div className="flex gap-2">
        <button
          className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
            alphaMode === "single"
              ? "bg-blue-600 text-white shadow-sm"
              : "bg-white dark:bg-gray-700 text-gray-600 dark:text-gray-300 border border-gray-300 dark:border-gray-600 hover:bg-gray-100 dark:hover:bg-gray-600"
          }`}
          onClick={() => onModeChange("single")}
        >
          单 Alpha
        </button>
        <button
          className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
            alphaMode === "multi"
              ? "bg-purple-600 text-white shadow-sm"
              : "bg-white dark:bg-gray-700 text-gray-600 dark:text-gray-300 border border-gray-300 dark:border-gray-600 hover:bg-gray-100 dark:hover:bg-gray-600"
          }`}
          onClick={() => onModeChange("multi")}
        >
          多 Alpha
        </button>
      </div>
      {alphaMode === "multi" && (
        <span className="text-xs text-purple-600 dark:text-purple-400">
          Phase 3: 因子分组建模 + Meta-Model 合成
        </span>
      )}
    </div>
  );
}
