"use client";

import { useEffect, useMemo, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface CloudStrategy {
  id: string;
  name: string;
  desc: string;
  keyword: string;
}

interface CloudSearchResponse {
  success: boolean;
  error: string | null;
  keyword: string;
  total: number;
  items: any[];
}

function formatNumber(v: number | null | undefined, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return v.toFixed(digits);
}

export default function CloudScreeningPage() {
  const [keyword, setKeyword] = useState("");
  const [pageSize, setPageSize] = useState(100);

  const [savedStrategies, setSavedStrategies] = useState<
    { name: string; keyword: string }[]
  >([]);
  const [newStrategyName, setNewStrategyName] = useState("");
  const [selectedCustomName, setSelectedCustomName] = useState("不使用自定义策略");

  const [hotStrategies, setHotStrategies] = useState<CloudStrategy[]>([]);
  const [selectedHotIndex, setSelectedHotIndex] = useState(0);

  const [loadingHot, setLoadingHot] = useState(false);
  const [loadingSearch, setLoadingSearch] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchResult, setSearchResult] = useState<CloudSearchResponse | null>(
    null,
  );

  const [selectedCodes, setSelectedCodes] = useState<Record<string, boolean>>({});
  const [categories, setCategories] = useState<
    { id: number; name: string; description?: string | null }[]
  >([]);
  const [targetCatId, setTargetCatId] = useState<number | "new" | null>(null);
  const [newCatName, setNewCatName] = useState("");
  const [addingWatchlist, setAddingWatchlist] = useState(false);
  const [opMessage, setOpMessage] = useState<string | null>(null);

  const [resultPage, setResultPage] = useState(1);
  const [resultPageSize, setResultPageSize] = useState(20);

  async function loadHotStrategies() {
    setLoadingHot(true);
    try {
      const res = await fetch(`${API_BASE}/cloud-screening/hot-strategies`);
      if (!res.ok) throw new Error(`热门策略请求失败: ${res.status}`);
      const data = await res.json();
      setHotStrategies(data.strategies || []);
    } catch (e: any) {
      setError(e?.message || "获取热门策略失败");
    } finally {
      setLoadingHot(false);
    }
  }

  useEffect(() => {
    loadHotStrategies();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function loadCategories() {
    try {
      const res = await fetch(`${API_BASE}/watchlist/categories`);
      if (!res.ok) return;
      const data = await res.json();
      setCategories(data || []);
      if ((data || []).length && targetCatId === null) {
        setTargetCatId(data[0].id);
      }
    } catch {
      // 分类加载失败时忽略，后续操作前用户可重试
    }
  }

  useEffect(() => {
    loadCategories();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function handleSaveStrategy() {
    const k = keyword.trim();
    const n = newStrategyName.trim();
    if (!k) {
      setError("请输入要保存的选股条件文本");
      return;
    }
    if (!n) {
      setError("请输入策略名称");
      return;
    }
    setError(null);
    setSavedStrategies((prev) => {
      const updated: { name: string; keyword: string }[] = [];
      let replaced = false;
      for (const it of prev) {
        if (it.name === n) {
          updated.push({ name: n, keyword: k });
          replaced = true;
        } else {
          updated.push(it);
        }
      }
      if (!replaced) {
        updated.push({ name: n, keyword: k });
      }
      return updated;
    });
  }

  async function handleSearch() {
    setLoadingSearch(true);
    setError(null);
    setSearchResult(null);

    // 优先级：输入框 > 自定义策略 > 热门策略
    let effectiveKeyword = keyword.trim();

    if (!effectiveKeyword && selectedCustomName !== "不使用自定义策略") {
      const s = savedStrategies.find((x) => x.name === selectedCustomName);
      if (s) effectiveKeyword = s.keyword.trim();
    }

    if (!effectiveKeyword && selectedHotIndex > 0 && hotStrategies.length > 0) {
      const s = hotStrategies[selectedHotIndex - 1];
      effectiveKeyword = (s.keyword || s.name || "").trim();
    }

    if (!effectiveKeyword) {
      setError("请输入自定义关键词或选择一个热门策略");
      setLoadingSearch(false);
      return;
    }

    try {
      const body = { keyword: effectiveKeyword, page_size: pageSize };
      const res = await fetch(`${API_BASE}/cloud-screening/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) throw new Error(`云选股请求失败: ${res.status}`);
      const data: CloudSearchResponse = await res.json();
      setSearchResult(data);
      setSelectedCodes({});
      setResultPage(1);
      if (!data.success && data.error) {
        setError(data.error);
      }
    } catch (e: any) {
      setError(e?.message || "云选股请求失败");
      setSearchResult(null);
    } finally {
      setLoadingSearch(false);
    }
  }

  const rows = useMemo(() => searchResult?.items || [], [searchResult]);

  const totalPages = useMemo(
    () =>
      rows.length
        ? Math.max(1, Math.ceil(rows.length / (resultPageSize || 20)))
        : 1,
    [rows, resultPageSize],
  );

  const pagedRows = useMemo(() => {
    if (!rows.length) return [] as any[];
    const size = resultPageSize || 20;
    const maxPage = Math.max(1, Math.ceil(rows.length / size));
    const current = Math.min(Math.max(1, resultPage), maxPage);
    const start = (current - 1) * size;
    return rows.slice(start, start + size);
  }, [rows, resultPage, resultPageSize]);

  const currentPage = useMemo(
    () => {
      if (!rows.length) return 1;
      const size = resultPageSize || 20;
      const maxPage = Math.max(1, Math.ceil(rows.length / size));
      return Math.min(Math.max(1, resultPage), maxPage);
    },
    [rows, resultPage, resultPageSize],
  );

  const allColumns = useMemo(() => {
    if (!rows.length) return [] as string[];
    const cols = new Set<string>();
    for (const r of rows) {
      Object.keys(r || {}).forEach((k) => cols.add(k));
    }
    // 去掉在前面单独显示的字段
    ["code", "name", "名称", "市场码"].forEach((k) => {
      if (cols.has(k)) cols.delete(k);
    });
    return Array.from(cols);
  }, [rows]);

  const selectedRows = useMemo(() => {
    if (!rows.length) return [] as any[];
    return rows.filter((row) => {
      const codeKey = String(
        row?.code ?? row?.["代码"] ?? "",
      ).trim();
      return codeKey && selectedCodes[codeKey];
    });
  }, [rows, selectedCodes]);

  const selectedBaseCodes = useMemo(() => {
    if (!selectedRows.length) return [] as string[];
    const out: string[] = [];
    for (const row of selectedRows) {
      const codeKey = String(
        row?.code ?? row?.["代码"] ?? "",
      ).trim();
      if (codeKey) out.push(codeKey);
    }
    return out;
  }, [selectedRows]);

  function toggleCodeSelected(code: string) {
    if (!code) return;
    setSelectedCodes((prev) => ({ ...prev, [code]: !prev[code] }));
  }

  async function handleAddToWatchlist() {
    if (!selectedBaseCodes.length) {
      setOpMessage("请先在表格中勾选要加入自选股的股票。");
      return;
    }

    let catId = targetCatId;
    if (catId === "new") {
      const name = newCatName.trim();
      if (!name) {
        setOpMessage("请输入新建分类名称。");
        return;
      }
      try {
        const res = await fetch(`${API_BASE}/watchlist/categories`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name, description: null }),
        });
        if (!res.ok) {
          throw new Error(`创建自选分类失败: ${res.status}`);
        }
        const data = await res.json();
        catId = data.id as number;
        setTargetCatId(catId);
        // 重新加载分类列表
        loadCategories();
      } catch (e: any) {
        setOpMessage(e?.message || "创建自选分类失败。");
        return;
      }
    }

    if (catId == null || typeof catId !== "number") {
      setOpMessage("请选择要加入的自选股分类。");
      return;
    }

    setAddingWatchlist(true);
    setOpMessage(null);
    try {
      const res = await fetch(`${API_BASE}/watchlist/items/bulk-add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          codes: selectedBaseCodes,
          category_id: catId,
          on_conflict: "ignore",
        }),
      });
      if (!res.ok) {
        throw new Error(`批量加入自选股失败: ${res.status}`);
      }
      const data = await res.json();
      const added =
        (data?.inserted as number | undefined) ??
        (data?.added as number | undefined) ??
        selectedBaseCodes.length;
      setOpMessage(
        `已将 ${selectedBaseCodes.length} 只股票加入自选股（新增记录约 ${added} 条）。`,
      );
    } catch (e: any) {
      setOpMessage(e?.message || "批量加入自选股失败。");
    } finally {
      setAddingWatchlist(false);
    }
  }

  function handlePrefillBatchAnalysis() {
    const codes = selectedBaseCodes;
    if (!codes.length) {
      setOpMessage("请先在表格中勾选要批量分析的股票。");
      return;
    }
    try {
      if (typeof window !== "undefined") {
        window.localStorage.setItem(
          "analysis_prefill_batch_codes",
          codes.join("\n"),
        );
      }
      setOpMessage(
        `已将 ${codes.length} 只股票代码写入批量分析预填（analysis_prefill_batch_codes）。`,
      );
    } catch {
      setOpMessage("写入批量分析预填失败，请稍后重试。");
    }
  }

  // 简单 CSV 导出
  function handleDownloadCsv() {
    if (!rows.length) return;
    const cols = allColumns;
    const lines: string[] = [];
    lines.push(cols.join(","));
    for (const r of rows) {
      const values = cols.map((c) => {
        const v = r?.[c];
        if (v === null || v === undefined) return "";
        const s = String(v).replace(/"/g, '""');
        if (s.includes(",") || s.includes("\n") || s.includes("\r")) {
          return `"${s}` + `"`;
        }
        return s;
      });
      lines.push(values.join(","));
    }
    const blob = new Blob(["\ufeff" + lines.join("\n")], {
      type: "text/csv;charset=utf-8;",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "cloud_screening_result.csv";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  const strategyOptions = useMemo(() => {
    const base = ["不使用热门策略"];
    return base.concat(hotStrategies.map((s, i) => `${i + 1}. ${s.name}`));
  }, [hotStrategies]);

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #38bdf8 0%, #0ea5e9 40%, #6366f1 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 24,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 22 }}>☁ 云选股（东方财富智能选股）</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          通过东方财富智能选股/热门策略接口获取候选股票列表，用作策略参考，与本地指标选股互补。
        </p>
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
          marginBottom: 16,
          fontSize: 13,
        }}
      >
        <h2 style={{ marginTop: 0, fontSize: 16 }}>条件与策略选择</h2>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "minmax(0, 2fr) minmax(0, 1fr)",
            gap: 16,
            alignItems: "flex-start",
          }}
        >
          <div>
            <label style={{ display: "block", marginBottom: 4 }}>
              自定义选股关键词/策略描述
            </label>
            <input
              type="text"
              value={keyword}
              onChange={(e) => setKeyword(e.target.value)}
              placeholder="例如：高成长、银行、人气龙头、半导体等"
              style={{ width: "100%", padding: "6px 8px" }}
            />

            <div
              style={{
                display: "grid",
                gridTemplateColumns: "minmax(0, 2fr) minmax(0, 1fr)",
                gap: 8,
                marginTop: 12,
              }}
            >
              <div>
                <label style={{ display: "block", marginBottom: 4 }}>
                  保存当前条件为自定义策略
                </label>
                <input
                  type="text"
                  value={newStrategyName}
                  onChange={(e) => setNewStrategyName(e.target.value)}
                  placeholder="策略名称（如：高成长龙头）"
                  style={{ width: "100%", padding: "6px 8px" }}
                />
              </div>
              <div style={{ display: "flex", alignItems: "flex-end" }}>
                <button
                  onClick={handleSaveStrategy}
                  style={{
                    padding: "6px 12px",
                    borderRadius: 8,
                    border: "none",
                    background: "#4f46e5",
                    color: "#fff",
                    fontSize: 13,
                    cursor: "pointer",
                  }}
                >
                  💾 保存当前条件
                </button>
              </div>
            </div>

            <div style={{ marginTop: 12 }}>
              <label style={{ display: "block", marginBottom: 4 }}>
                选择自定义云选股策略（可选）
              </label>
              <select
                value={selectedCustomName}
                onChange={(e) => setSelectedCustomName(e.target.value)}
                style={{ width: "100%" }}
              >
                <option value="不使用自定义策略">不使用自定义策略</option>
                {savedStrategies.map((s) => (
                  <option key={s.name} value={s.name}>
                    {s.name}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div>
            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", marginBottom: 4 }}>
                返回数量
              </label>
              <input
                type="number"
                min={10}
                max={500}
                step={10}
                value={pageSize}
                onChange={(e) => setPageSize(Number(e.target.value) || 0)}
                style={{ width: "100%" }}
              />
            </div>

            <div style={{ marginBottom: 12 }}>
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  marginBottom: 4,
                }}
              >
                <label>热门云选股策略（来自东方财富）</label>
                <button
                  onClick={loadHotStrategies}
                  disabled={loadingHot}
                  style={{
                    padding: "3px 8px",
                    borderRadius: 999,
                    border: "1px solid #ccc",
                    background: "#fafafa",
                    fontSize: 12,
                  }}
                >
                  {loadingHot ? "刷新中..." : "🔥 刷新热门策略"}
                </button>
              </div>
              <select
                value={selectedHotIndex}
                onChange={(e) => setSelectedHotIndex(Number(e.target.value))}
                style={{ width: "100%" }}
              >
                {strategyOptions.map((label, idx) => (
                  <option key={idx} value={idx}>
                    {label}
                  </option>
                ))}
              </select>

              {selectedHotIndex > 0 && hotStrategies[selectedHotIndex - 1] && (
                <p style={{ marginTop: 6, fontSize: 12, color: "#555" }}>
                  已选择策略：{hotStrategies[selectedHotIndex - 1].name}
                </p>
              )}
            </div>

            <button
              onClick={handleSearch}
              disabled={loadingSearch}
              style={{
                marginTop: 4,
                padding: "8px 16px",
                borderRadius: 999,
                border: "none",
                background: "#0ea5e9",
                color: "#fff",
                fontSize: 14,
                cursor: "pointer",
              }}
            >
              {loadingSearch ? "执行中..." : "🚀 执行云选股"}
            </button>
          </div>
        </div>

        {error && (
          <p style={{ color: "#b00020", marginTop: 10, fontSize: 13 }}>
            错误：{error}
          </p>
        )}
      </section>

      {searchResult && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 12,
            boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            fontSize: 12,
          }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: 8,
            }}
          >
            <div>
              <h2 style={{ margin: 0, fontSize: 16 }}>云选股结果</h2>
              <p style={{ margin: 0, color: "#555" }}>
                关键词：{searchResult.keyword || "(空)"} · 返回股票：
                {searchResult.total} 只
              </p>
            </div>
            <button
              onClick={handleDownloadCsv}
              disabled={!rows.length}
              style={{
                padding: "4px 10px",
                borderRadius: 999,
                border: "1px solid #ccc",
                background: rows.length ? "#fafafa" : "#f3f4f6",
                fontSize: 12,
              }}
            >
              💾 导出为 CSV
            </button>
          </div>
          {rows.length > 0 && (
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                marginBottom: 8,
                fontSize: 12,
                color: "#555",
              }}
            >
              <div>
                共 {rows.length} 只股票，当前第 {currentPage} / {totalPages} 页
              </div>
              <div
                style={{
                  display: "flex",
                  gap: 8,
                  alignItems: "center",
                }}
              >
                <span>每页显示：</span>
                <select
                  value={resultPageSize}
                  onChange={(e) => {
                    const v = Number(e.target.value) || 20;
                    setResultPageSize(v);
                    setResultPage(1);
                  }}
                  style={{ padding: "2px 4px", fontSize: 12 }}
                >
                  <option value={10}>10</option>
                  <option value={20}>20</option>
                  <option value={50}>50</option>
                  <option value={100}>100</option>
                </select>
                <button
                  type="button"
                  onClick={() => setResultPage(Math.max(1, currentPage - 1))}
                  disabled={currentPage <= 1}
                  style={{
                    padding: "4px 8px",
                    borderRadius: 8,
                    border: "1px solid #ccc",
                    background:
                      currentPage <= 1 ? "#f3f4f6" : "#fafafa",
                    cursor:
                      currentPage <= 1 ? "default" : "pointer",
                  }}
                >
                  上一页
                </button>
                <button
                  type="button"
                  onClick={() =>
                    setResultPage(
                      Math.min(totalPages, currentPage + 1),
                    )
                  }
                  disabled={currentPage >= totalPages}
                  style={{
                    padding: "4px 8px",
                    borderRadius: 8,
                    border: "1px solid #ccc",
                    background:
                      currentPage >= totalPages ? "#f3f4f6" : "#fafafa",
                    cursor:
                      currentPage >= totalPages ? "default" : "pointer",
                  }}
                >
                  下一页
                </button>
              </div>
            </div>
          )}

          {rows.length === 0 ? (
            <p style={{ color: "#777" }}>
              尚无云选股结果，请输入关键词或选择热门策略后点击“执行云选股”。
            </p>
          ) : (
            <div
              style={{
                maxHeight: 520,
                borderRadius: 6,
                border: "1px solid #eee",
                overflowX: "auto",
                overflowY: "auto",
              }}
            >
              <table
                style={{
                  borderCollapse: "collapse",
                  tableLayout: "auto",
                  width: "max-content",
                  minWidth: "100%",
                }}
              >
                <thead>
                  <tr
                    style={{
                      background: "#fafafa",
                      position: "sticky",
                      top: 0,
                    }}
                  >
                    <th
                      style={{ padding: 6, textAlign: "center" }}
                    >
                      选择
                    </th>
                    <th
                      style={{ padding: 6, textAlign: "right" }}
                    >
                      序号
                    </th>
                    <th
                      style={{ padding: 6, textAlign: "left" }}
                    >
                      代码
                    </th>
                    <th
                      style={{
                        padding: 6,
                        textAlign: "left",
                        whiteSpace: "nowrap",
                      }}
                    >
                      名称
                    </th>
                    {allColumns.map((c) => (
                      <th
                        key={c}
                        style={{
                          padding: 6,
                          textAlign: "right",
                        }}
                      >
                        {c}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pagedRows.map((row, idx) => {
                    const codeKey = String(
                      row?.code ?? row?.["代码"] ?? "",
                    ).trim();
                    const name = String(
                      row?.name ?? row?.["名称"] ?? "",
                    );
                    const globalIndex = (currentPage - 1) * (resultPageSize || 20) + idx + 1;
                    return (
                      <tr
                        key={idx}
                        style={{
                          borderTop: "1px solid #f0f0f0",
                          background: idx % 2 === 0 ? "#fff" : "#fcfcfc",
                        }}
                      >
                        <td
                          style={{
                            padding: 6,
                            textAlign: "center",
                          }}
                        >
                          <input
                            type="checkbox"
                            checked={codeKey ? !!selectedCodes[codeKey] : false}
                            onChange={() => toggleCodeSelected(codeKey)}
                          />
                        </td>
                        <td
                          style={{
                            padding: 6,
                            textAlign: "right",
                            fontFamily: "monospace",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {globalIndex}
                        </td>
                        <td
                          style={{
                            padding: 6,
                            textAlign: "left",
                            fontFamily: "monospace",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {codeKey}
                        </td>
                        <td
                          style={{
                            padding: 6,
                            textAlign: "left",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {name}
                        </td>
                        {allColumns.map((c) => {
                          const v = row?.[c];
                          const isNumeric = typeof v === "number";
                          return (
                            <td
                              key={c}
                              style={{
                                padding: 6,
                                textAlign: isNumeric ? "right" : "left",
                              }}
                            >
                              {isNumeric ? formatNumber(v, 2) : String(v ?? "")}
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {rows.length > 0 && (
            <div
              style={{
                marginTop: 12,
                borderTop: "1px solid #eee",
                paddingTop: 8,
              }}
            >
              <h3 style={{ fontSize: 13, margin: "0 0 6px" }}>批量操作</h3>
              <p style={{ fontSize: 12, color: "#555", margin: "0 0 6px" }}>
                已选择 {selectedRows.length} 只股票。
              </p>
              <div
                style={{
                  display: "flex",
                  flexWrap: "wrap",
                  gap: 12,
                  alignItems: "flex-end",
                  fontSize: 12,
                }}
              >
                <div>
                  <label style={{ display: "block", marginBottom: 4 }}>
                    自选股分类
                  </label>
                  <select
                    value={
                      targetCatId === "new"
                        ? "new"
                        : targetCatId ?? ""
                    }
                    onChange={(e) => {
                      const v = e.target.value;
                      if (v === "new") setTargetCatId("new");
                      else if (v === "") setTargetCatId(null);
                      else setTargetCatId(Number(v));
                    }}
                    style={{ minWidth: 160 }}
                  >
                    <option value="">(请选择分类)</option>
                    {categories.map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.name}
                      </option>
                    ))}
                    <option value="new">新建分类...</option>
                  </select>
                </div>
                {targetCatId === "new" && (
                  <div>
                    <label style={{ display: "block", marginBottom: 4 }}>
                      新建分类名称
                    </label>
                    <input
                      type="text"
                      value={newCatName}
                      onChange={(e) => setNewCatName(e.target.value)}
                      placeholder="例如：云选股候选"
                      style={{ minWidth: 160, padding: "4px 6px" }}
                    />
                  </div>
                )}

                <button
                  onClick={handleAddToWatchlist}
                  disabled={addingWatchlist}
                  style={{
                    padding: "6px 12px",
                    borderRadius: 999,
                    border: "none",
                    background: "#22c55e",
                    color: "#fff",
                    fontSize: 12,
                    cursor: "pointer",
                  }}
                >
                  {addingWatchlist ? "加入中..." : "⭐ 加入自选股"}
                </button>

                <button
                  onClick={handlePrefillBatchAnalysis}
                  style={{
                    padding: "6px 12px",
                    borderRadius: 999,
                    border: "1px solid #ccc",
                    background: "#fafafa",
                    fontSize: 12,
                  }}
                >
                  📊 批量分析选中股票
                </button>
              </div>

              {opMessage && (
                <p
                  style={{
                    marginTop: 6,
                    fontSize: 12,
                    color: "#555",
                  }}
                >
                  {opMessage}
                </p>
              )}
            </div>
          )}
        </section>
      )}
    </main>
  );
}
