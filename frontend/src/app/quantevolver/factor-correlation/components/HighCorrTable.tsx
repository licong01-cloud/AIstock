"use client";

import React, { useState, useMemo } from "react";

interface HighCorrPair {
  factor_a: string;
  factor_b: string;
  correlation: number;
}

interface Props {
  pairs: HighCorrPair[];
  onSelectPair: (fa: string, fb: string) => void;
  onGoToDedup?: (factorName: string) => void;
}

const THRESHOLD_OPTIONS = [0.5, 0.6, 0.7, 0.8, 0.9];
const PAGE_SIZE = 20;

type SortKey = "abs_corr" | "corr" | "factor_a" | "factor_b" | "pair_count";
type SortDir = "asc" | "desc";

function corrColor(corr: number): string {
  const abs = Math.abs(corr);
  if (corr < 0) return "#2563eb"; // 蓝色（负相关）
  if (abs >= 0.8) return "#dc2626"; // 红
  if (abs >= 0.6) return "#ea580c"; // 橙
  return "#ca8a04"; // 黄
}

function rowBg(corr: number): React.CSSProperties {
  const abs = Math.abs(corr);
  if (abs >= 0.8) return { backgroundColor: "#fee2e2" };
  if (abs >= 0.6) return { backgroundColor: "#fef3c7" };
  return {};
}

export default function HighCorrTable({ pairs, onSelectPair, onGoToDedup }: Props) {
  const [threshold, setThreshold] = useState(0.5);
  const [currentPage, setCurrentPage] = useState(1);
  const [sortKey, setSortKey] = useState<SortKey>("abs_corr");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(prev => prev === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir(key === "factor_a" || key === "factor_b" ? "asc" : "desc");
    }
    setCurrentPage(1);
  };

  const sortIndicator = (key: SortKey) => {
    if (sortKey !== key) return " ↕";
    return sortDir === "asc" ? " ↑" : " ↓";
  };

  // 统计每个因子在当前阈值下参与的高相关对数量
  const pairCountMap = useMemo(() => {
    const map = new Map<string, number>();
    for (const p of pairs) {
      if (Math.abs(p.correlation) >= threshold) {
        map.set(p.factor_a, (map.get(p.factor_a) ?? 0) + 1);
        map.set(p.factor_b, (map.get(p.factor_b) ?? 0) + 1);
      }
    }
    return map;
  }, [pairs, threshold]);

  const filtered = useMemo(() => {
    const result = pairs.filter((p) => Math.abs(p.correlation) >= threshold);

    result.sort((a, b) => {
      let cmp = 0;
      switch (sortKey) {
        case "abs_corr":
          cmp = Math.abs(a.correlation) - Math.abs(b.correlation);
          break;
        case "corr":
          cmp = a.correlation - b.correlation;
          break;
        case "factor_a":
          cmp = a.factor_a.localeCompare(b.factor_a);
          break;
        case "factor_b":
          cmp = a.factor_b.localeCompare(b.factor_b);
          break;
        case "pair_count": {
          const maxA = Math.max(pairCountMap.get(a.factor_a) ?? 0, pairCountMap.get(a.factor_b) ?? 0);
          const maxB = Math.max(pairCountMap.get(b.factor_a) ?? 0, pairCountMap.get(b.factor_b) ?? 0);
          cmp = maxA - maxB;
          if (cmp === 0) cmp = Math.abs(a.correlation) - Math.abs(b.correlation);
          break;
        }
      }
      return sortDir === "asc" ? cmp : -cmp;
    });

    return result;
  }, [pairs, threshold, sortKey, sortDir, pairCountMap]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(currentPage, totalPages);
  const paged = filtered.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE);

  const thStyle: React.CSSProperties = {
    padding: "10px 12px",
    cursor: "pointer",
    userSelect: "none",
    whiteSpace: "nowrap",
  };

  return (
    <div>
      {/* 工具栏 */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          marginBottom: 12,
          flexWrap: "wrap",
        }}
      >
        <label style={{ fontSize: 13, color: "#6b7280" }}>
          相关性阈值:
        </label>
        <select
          value={threshold}
          onChange={(e) => {
            setThreshold(Number(e.target.value));
            setCurrentPage(1);
          }}
          style={{
            padding: "6px 10px",
            border: "1px solid #e5e7eb",
            borderRadius: 8,
            fontSize: 13,
            outline: "none",
          }}
        >
          {THRESHOLD_OPTIONS.map((t) => (
            <option key={t} value={t}>
              |corr| &ge; {t}
            </option>
          ))}
        </select>
        <span style={{ fontSize: 12, color: "#9ca3af", marginLeft: "auto" }}>
          共 {filtered.length} 对
          {" | "}
          <span style={{ color: "#ef4444", fontWeight: 600 }}>{filtered.filter(p => Math.abs(p.correlation) >= 0.8).length}</span> 对 |corr|≥0.8
          {" "}
          <span style={{ color: "#f59e0b", fontWeight: 600 }}>{filtered.filter(p => Math.abs(p.correlation) >= 0.7 && Math.abs(p.correlation) < 0.8).length}</span> 对 ≥0.7
        </span>
      </div>

      {/* 表格 */}
      {filtered.length === 0 ? (
        <div style={{ color: "#9ca3af", textAlign: "center", padding: 48 }}>
          无满足条件的高相关因子对
        </div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 13,
            }}
          >
            <thead>
              <tr
                style={{
                  borderBottom: "2px solid #e5e7eb",
                  fontSize: 12,
                  color: "#6b7280",
                  textTransform: "uppercase",
                }}
              >
                <th style={{ padding: "10px 12px", textAlign: "left" }}>#</th>
                <th
                  style={{ ...thStyle, textAlign: "left" }}
                  onClick={() => handleSort("factor_a")}
                >
                  因子 A{sortIndicator("factor_a")}
                </th>
                <th
                  style={{ ...thStyle, textAlign: "left" }}
                  onClick={() => handleSort("factor_b")}
                >
                  因子 B{sortIndicator("factor_b")}
                </th>
                <th
                  style={{ ...thStyle, textAlign: "center" }}
                  onClick={() => handleSort("abs_corr")}
                >
                  |相关系数|{sortIndicator("abs_corr")}
                </th>
                <th
                  style={{ ...thStyle, textAlign: "center" }}
                  onClick={() => handleSort("corr")}
                >
                  原始值{sortIndicator("corr")}
                </th>
                <th
                  style={{ ...thStyle, textAlign: "center" }}
                  onClick={() => handleSort("pair_count")}
                >
                  高相关对数{sortIndicator("pair_count")}
                </th>
                <th style={{ padding: "10px 12px", textAlign: "center" }}>
                  操作
                </th>
              </tr>
            </thead>
            <tbody>
              {paged.map((p, i) => (
                <tr
                  key={`${p.factor_a}-${p.factor_b}`}
                  style={{
                    borderBottom: "1px solid #f3f4f6",
                    ...rowBg(p.correlation),
                  }}
                >
                  <td
                    style={{
                      padding: "8px 12px",
                      color: "#9ca3af",
                      fontSize: 12,
                    }}
                  >
                    {(safePage - 1) * PAGE_SIZE + i + 1}
                  </td>
                  <td
                    style={{
                      padding: "8px 12px",
                      fontFamily: "monospace",
                      fontSize: 12,
                    }}
                  >
                    {p.factor_a}
                  </td>
                  <td
                    style={{
                      padding: "8px 12px",
                      fontFamily: "monospace",
                      fontSize: 12,
                    }}
                  >
                    {p.factor_b}
                  </td>
                  <td
                    style={{
                      padding: "8px 12px",
                      textAlign: "center",
                      fontWeight: 700,
                      fontFamily: "monospace",
                      color: corrColor(p.correlation),
                    }}
                  >
                    {Math.abs(p.correlation).toFixed(4)}
                  </td>
                  <td
                    style={{
                      padding: "8px 12px",
                      textAlign: "center",
                      fontFamily: "monospace",
                      fontSize: 12,
                      color: p.correlation < 0 ? "#2563eb" : "#374151",
                    }}
                  >
                    {p.correlation >= 0 ? "+" : ""}{p.correlation.toFixed(4)}
                  </td>
                  <td
                    style={{
                      padding: "8px 12px",
                      textAlign: "center",
                      fontSize: 12,
                      fontWeight: 600,
                      color: Math.max(pairCountMap.get(p.factor_a) ?? 0, pairCountMap.get(p.factor_b) ?? 0) >= 5 ? "#dc2626" : "#374151",
                    }}
                  >
                    {pairCountMap.get(p.factor_a) ?? 0} / {pairCountMap.get(p.factor_b) ?? 0}
                  </td>
                  <td style={{ padding: "8px 12px", textAlign: "center" }}>
                    <button
                      onClick={() =>
                        onSelectPair(p.factor_a, p.factor_b)
                      }
                      style={{
                        padding: "4px 12px",
                        fontSize: 12,
                        fontWeight: 500,
                        background: "#f3f4f6",
                        color: "#4b5563",
                        border: "none",
                        borderRadius: 6,
                        cursor: "pointer",
                        marginRight: onGoToDedup ? 4 : 0,
                      }}
                    >
                      查看详情
                    </button>
                    {onGoToDedup && (
                      <button
                        onClick={() => onGoToDedup(p.factor_a)}
                        style={{
                          padding: "4px 10px",
                          fontSize: 11,
                          fontWeight: 500,
                          background: "#f3f4f6",
                          color: "#7c3aed",
                          border: "none",
                          borderRadius: 6,
                          cursor: "pointer",
                        }}
                      >
                        批量去重
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* 分页 */}
      {totalPages > 1 && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            gap: 6,
            marginTop: 16,
          }}
        >
          <PgBtn label="«" onClick={() => setCurrentPage(1)} disabled={safePage === 1} />
          <PgBtn label="‹" onClick={() => setCurrentPage(safePage - 1)} disabled={safePage === 1} />
          {Array.from({ length: Math.min(7, totalPages) }, (_, i) => {
            let page: number;
            if (totalPages <= 7) page = i + 1;
            else if (safePage <= 4) page = i + 1;
            else if (safePage >= totalPages - 3) page = totalPages - 6 + i;
            else page = safePage - 3 + i;
            return (
              <PgBtn
                key={page}
                label={String(page)}
                onClick={() => setCurrentPage(page)}
                active={page === safePage}
              />
            );
          })}
          <PgBtn label="›" onClick={() => setCurrentPage(safePage + 1)} disabled={safePage === totalPages} />
          <PgBtn label="»" onClick={() => setCurrentPage(totalPages)} disabled={safePage === totalPages} />
          <span style={{ fontSize: 12, color: "#9ca3af", marginLeft: 8 }}>
            {safePage} / {totalPages}
          </span>
        </div>
      )}
    </div>
  );
}

function PgBtn({
  label,
  onClick,
  disabled,
  active,
}: {
  label: string;
  onClick: () => void;
  disabled?: boolean;
  active?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        padding: "4px 10px",
        fontSize: 12,
        border: "1px solid #e5e7eb",
        borderRadius: 6,
        cursor: disabled ? "default" : "pointer",
        opacity: disabled ? 0.4 : 1,
        background: active ? "#7c3aed" : "#fff",
        color: active ? "#fff" : "#374151",
        fontWeight: active ? 600 : 400,
      }}
    >
      {label}
    </button>
  );
}
