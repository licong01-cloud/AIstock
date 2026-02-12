"use client";

import { useEffect, useMemo, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface WatchlistCategory {
  id: number;
  name: string;
  description?: string | null;
}

interface WatchlistItem {
  id: number;
  code: string;
  name: string;
  category_names?: string;
  created_at?: string | null;
  updated_at?: string | null;
  last_analysis_time?: string | null;
  last_rating?: string | null;
  last_conclusion?: string | null;
  entry_price?: number | null;
  entry_rank?: number | null;
  entry_task_id?: string | null;
  entry_loop_id?: number | null;
  entry_as_of?: string | null;
  last?: number | null;
  pct_change?: number | null;
  pct_since_entry?: number | null;
  open?: number | null;
  prev_close?: number | null;
  high?: number | null;
  low?: number | null;
  volume_hand?: number | null;
  amount?: number | null;
  rating?: string | null;
}

interface ListItemsResponse {
  data: WatchlistItem[];
  total: number;
}

interface Task {
  id: string;
  name: string;
  createdAt?: string | null;
  updatedAt?: string | null;
}

interface TasksResponse {
  tasks: Task[];
}

interface PricesResponse {
  prices: Record<string, {
    latestPrice?: number | null;
    openPrice?: number | null;
    closePrice?: number | null;
    highPrice?: number | null;
    lowPrice?: number | null;
    rating?: string | null;
  } | null>;
}

function formatPct(v: number | null | undefined) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return `${v.toFixed(2)}%`;
}

function formatAmount(v: number | null | undefined) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  if (Math.abs(v) >= 1e8) return `${(v / 1e8).toFixed(2)}亿`;
  return `${(v / 1e4).toFixed(2)}万`;
}

type SortBy =
  | "code"
  | "name"
  | "category"
  | "created_at"
  | "updated_at"
  | "last_analysis_time"
  | "last_rating"
  | "entry_price"
  | "entry_rank"
  | "last"
  | "pct_change"
  | "pct_since_entry"
  | "open"
  | "prev_close"
  | "high"
  | "low"
  | "volume_hand"
  | "amount";

const SORTABLE_FIELDS: SortBy[] = [
  "code",
  "name",
  "category",
  "created_at",
  "updated_at",
  "last_analysis_time",
  "last_rating",
  "entry_price",
  "entry_rank",
  "last",
  "pct_change",
  "pct_since_entry",
  "open",
  "prev_close",
  "high",
  "low",
  "volume_hand",
  "amount",
];

const UNABLE_SORT_FIELDS: SortBy[] = [
  "last",
  "pct_change",
  "pct_since_entry",
  "open",
  "prev_close",
  "high",
  "low",
  "volume_hand",
  "amount",
];

function displayCode(code: string): string {
  const c = (code || "").trim();
  if (!c) return "";
  if (c.includes(".")) {
    return c.split(".")[0] || c;
  }
  return c;
}

function sortItems(
  items: WatchlistItem[],
  sortBy: SortBy,
  sortDir: "asc" | "desc",
  pricesRefreshed: boolean,
): WatchlistItem[] {
  const reverse = sortDir === "desc";
  const cloned = [...items];
  
  // 如果价格未刷新，价格相关字段不可排序
  if (!pricesRefreshed && UNABLE_SORT_FIELDS.includes(sortBy)) {
    return cloned;
  }
  
  cloned.sort((a, b) => {
    let va: any;
    let vb: any;
    
    if (sortBy === "category") {
      va = a.category_names ?? "";
      vb = b.category_names ?? "";
    } else if (sortBy === "code") {
      va = displayCode(a.code);
      vb = displayCode(b.code);
    } else if (sortBy === "name") {
      va = a.name ?? "";
      vb = b.name ?? "";
    } else if (sortBy === "last_analysis_time") {
      va = a.last_analysis_time ?? "";
      vb = b.last_analysis_time ?? "";
    } else if (sortBy === "last_rating") {
      va = a.last_rating ?? "";
      vb = b.last_rating ?? "";
    } else if (sortBy === "created_at") {
      va = a.created_at ?? "";
      vb = b.created_at ?? "";
    } else {
      va = a.updated_at ?? "";
      vb = b.updated_at ?? "";
    }
    
    // 数值字段排序
    if (["entry_price", "entry_rank", "last", "pct_change", "pct_since_entry", "open", "prev_close", "high", "low", "volume_hand", "amount"].includes(sortBy)) {
      const numA = (a as any)[sortBy] as number | null | undefined;
      const numB = (b as any)[sortBy] as number | null | undefined;
      const aNull = numA === null || numA === undefined || Number.isNaN(numA);
      const bNull = numB === null || numB === undefined || Number.isNaN(numB);
      
      if (aNull && bNull) {
        return displayCode(a.code).localeCompare(displayCode(b.code));
      }
      if (aNull) return 1;
      if (bNull) return -1;
      
      const diff = Number(numA) - Number(numB);
      if (diff === 0) {
        return displayCode(a.code).localeCompare(displayCode(b.code));
      }
      return reverse ? -diff : diff;
    }
    
    // 字符串字段排序
    const sa = String(va).toLowerCase();
    const sb = String(vb).toLowerCase();
    if (sa === sb) {
      return displayCode(a.code).localeCompare(displayCode(b.code));
    }
    const cmp = sa < sb ? -1 : 1;
    return reverse ? -cmp : cmp;
  });
  
  return cloned;
}

function formatDateTime(value?: string | null): string {
  if (!value) return "-";
  let s = String(value).trim();
  if (!s) return "-";
  s = s.replace("T", " ");
  const dotIndex = s.indexOf(".");
  if (dotIndex >= 0) {
    s = s.slice(0, dotIndex);
  }
  s = s.replace(/Z$/, "");
  s = s.replace(/[+-]\d{2}:?\d{2}$/, "");
  s = s.trim();
  if (s.length >= 19) return s.slice(0, 19); // YYYY-MM-DD HH:MM:SS
  if (s.length >= 10) return s.slice(0, 10); // YYYY-MM-DD
  return s;
}

function formatDate(value?: string | null): string {
  const dt = formatDateTime(value);
  if (dt === "-") return "-";
  if (dt.length >= 10) return dt.slice(0, 10);
  return dt;
}

export default function WatchlistPageEnhanced() {
  const [categories, setCategories] = useState<WatchlistCategory[]>([]);
  const [currentCatId, setCurrentCatId] = useState<number | null>(null);

  const [sortBy, setSortBy] = useState<SortBy>("updated_at");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);

  // 内存缓存的全量数据
  const [allItems, setAllItems] = useState<WatchlistItem[]>([]);
  const [items, setItems] = useState<WatchlistItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // 价格刷新状态
  const [pricesRefreshed, setPricesRefreshed] = useState(false);
  const [refreshingPrices, setRefreshingPrices] = useState(false);

  // TASK筛选
  const [tasks, setTasks] = useState<Task[]>([]);
  const [selectedTaskId, setSelectedTaskId] = useState<string>("");

  // 全选
  const [selectedIds, setSelectedIds] = useState<number[]>([]);

  const currentCatName = useMemo(
    () => categories.find((c) => c.id === currentCatId)?.name,
    [categories, currentCatId],
  );

  // 加载TASK列表
  async function loadTasks() {
    try {
      const res = await fetch(`${API_BASE}/tasks`);
      if (!res.ok) throw new Error(`TASK请求失败: ${res.status}`);
      const data: TasksResponse = await res.json();
      setTasks(data.tasks || []);
    } catch {
      setTasks([]);
    }
  }

  // 加载分类列表
  async function loadCategories() {
    try {
      const res = await fetch(`${API_BASE}/watchlist/categories`);
      if (!res.ok) throw new Error(`分类请求失败: ${res.status}`);
      const data: WatchlistCategory[] = await res.json();
      setCategories(data);
      if (data.length && currentCatId == null) {
        setCurrentCatId(null);
      }
    } catch {
      // 忽略分类加载错误，界面仍可使用
    }
  }

  // 加载全量数据（不分页）
  async function loadAllItems() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/watchlist/all`);
      if (!res.ok) throw new Error(`列表请求失败: ${res.status}`);
      const data: ListItemsResponse = await res.json();
      
      const rawData = data.data || [];
      setAllItems(rawData);
      setTotal(rawData.length);
      
      // 应用筛选和排序
      let filtered = rawData;
      
      // TASK筛选
      if (selectedTaskId) {
        filtered = filtered.filter(item => item.entry_task_id === selectedTaskId);
      }
      
      // 排序
      filtered = sortItems(filtered, sortBy, sortDir, pricesRefreshed);
      
      // 分页
      const start = Math.max(0, (page - 1) * pageSize);
      const end = start + pageSize;
      const pageItems = filtered.slice(start, end);
      
      setItems(pageItems);
      setTotal(filtered.length);
    } catch (e: any) {
      setError(e?.message || "未知错误");
      setItems([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  }

  // 刷新价格
  async function refreshPrices() {
    setRefreshingPrices(true);
    setError(null);
    try {
      const codes = allItems.map(item => item.code);
      const res = await fetch(`${API_BASE}/stocks/prices`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ codes }),
      });
      
      if (!res.ok) throw new Error(`价格刷新失败: ${res.status}`);
      const data: PricesResponse = await res.json();
      
      // 更新数据中的价格信息
      const updatedItems = allItems.map(item => {
        const priceData = data.prices[item.code];
        if (!priceData) return item;
        
        return {
          ...item,
          last: priceData.latestPrice,
          open: priceData.openPrice,
          prev_close: priceData.closePrice,
          high: priceData.highPrice,
          low: priceData.lowPrice,
          rating: priceData.rating,
        };
      });
      
      setAllItems(updatedItems);
      setPricesRefreshed(true);
      
      // 重新应用筛选和排序
      let filtered = updatedItems;
      
      // TASK筛选
      if (selectedTaskId) {
        filtered = filtered.filter(item => item.entry_task_id === selectedTaskId);
      }
      
      // 排序
      filtered = sortItems(filtered, sortBy, sortDir, pricesRefreshed);
      
      // 分页
      const start = Math.max(0, (page - 1) * pageSize);
      const end = start + pageSize;
      const pageItems = filtered.slice(start, end);
      
      setItems(pageItems);
      setTotal(filtered.length);
    } catch (e: any) {
      setError(e?.message || "价格刷新失败");
    } finally {
      setRefreshingPrices(false);
    }
  }

  // 全选/取消全选
  function toggleSelectAll(checked: boolean) {
    if (checked) {
      setSelectedIds(items.map(item => item.id));
    } else {
      setSelectedIds([]);
    }
  }

  // 单个选择
  function toggleSelect(id: number, checked: boolean) {
    setSelectedIds((prev) => {
      const set = new Set(prev);
      if (checked) set.add(id);
      else set.delete(id);
      return Array.from(set);
    });
  }

  // 切换排序
  function toggleSort(field: SortBy) {
    if (sortBy === field) {
      // 切换方向：asc -> desc -> 无
      if (sortDir === "asc") {
        setSortDir("desc");
      } else if (sortDir === "desc") {
        setSortDir("asc");
      }
    } else {
      setSortBy(field);
      setSortDir("desc");
    }
    setPage(1);
  }

  useEffect(() => {
    loadCategories();
    loadTasks();
    loadAllItems();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    // 重新应用筛选和排序
    let filtered = allItems;
    
    // TASK筛选
    if (selectedTaskId) {
      filtered = filtered.filter(item => item.entry_task_id === selectedTaskId);
    }
    
    // 排序
    filtered = sortItems(filtered, sortBy, sortDir, pricesRefreshed);
    
    // 分页
    const start = Math.max(0, (page - 1) * pageSize);
    const end = start + pageSize;
    const pageItems = filtered.slice(start, end);
    
    setItems(pageItems);
    setTotal(filtered.length);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sortBy, sortDir, page, pageSize, selectedTaskId, pricesRefreshed]);

  const totalPages = Math.max(1, Math.ceil((total || 0) / pageSize));
  const allSelected = items.length > 0 && selectedIds.length === items.length;
  const someSelected = selectedIds.length > 0;

  return (
    <main style={{ padding: 24 }}>
      <section style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>⭐ 自选股票池（增强版）</h1>
        <p style={{ marginTop: 4, fontSize: 13, color: "#666" }}>
          支持全选、TASK筛选、价格刷新、内存排序等功能。
        </p>
      </section>

      {/* 控制栏 */}
      <section
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 12,
          marginBottom: 12,
          fontSize: 13,
          alignItems: "center",
        }}
      >
        <div>
          <span style={{ marginRight: 6 }}>分类：</span>
          <select
            value={currentCatId ?? ""}
            onChange={(e) => {
              const v = e.target.value === "" ? null : Number(e.target.value);
              setCurrentCatId(v);
              setPage(1);
            }}
            style={{ minWidth: 160 }}
          >
            <option value="">全部</option>
            {categories.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name}
              </option>
            ))}
          </select>
        </div>

        <div>
          <span style={{ marginRight: 6 }}>来源TASK：</span>
          <select
            value={selectedTaskId}
            onChange={(e) => {
              setSelectedTaskId(e.target.value);
              setPage(1);
            }}
            style={{ minWidth: 160 }}
          >
            <option value="">全部</option>
            {tasks.map((t) => (
              <option key={t.id} value={t.id}>
                {t.name}
              </option>
            ))}
          </select>
        </div>

        <button
          type="button"
          onClick={refreshPrices}
          disabled={refreshingPrices}
          style={{
            padding: "4px 10px",
            borderRadius: 8,
            border: "1px solid #cbd5e1",
            background: pricesRefreshed ? "#dcfce7" : "#f8fafc",
            fontSize: 12,
          }}
        >
          {refreshingPrices ? "刷新中..." : "刷新价格"}
        </button>

        <button
          type="button"
          onClick={() => {
            loadAllItems();
          }}
          disabled={loading}
          style={{
            padding: "4px 10px",
            borderRadius: 8,
            border: "1px solid #cbd5e1",
            background: "#f8fafc",
            fontSize: 12,
          }}
        >
          {loading ? "刷新中..." : "刷新"}
        </button>

        <span style={{ color: "#777" }}>
          {currentCatName ? `当前分类：${currentCatName}` : "全部分类"}
          {total ? ` · 共 ${total} 条` : ""}
          {totalPages ? ` · 第 ${page}/${totalPages} 页` : ""}
          {pricesRefreshed ? " · 价格已刷新" : " · 价格未刷新"}
        </span>
      </section>

      {error && <p style={{ color: "#b00020", fontSize: 13 }}>错误：{error}</p>}

      {/* 数据表格 */}
      <section
        style={{
          background: "#fff",
          borderRadius: 10,
          boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
          padding: 8,
          marginTop: 8,
        }}
      >
        <div
          style={{
            maxHeight: 520,
            overflow: "auto",
            borderRadius: 6,
            border: "1px solid #eee",
          }}
        >
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 12,
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
                <th style={{ padding: 6 }}>
                  <input
                    type="checkbox"
                    checked={allSelected}
                    ref={(el) => {
                      if (el && allSelected) el.indeterminate = false;
                      if (el && someSelected && !allSelected) el.indeterminate = true;
                    }}
                    onChange={(e) => toggleSelectAll(e.target.checked)}
                  />
                </th>
                <th
                  style={{ padding: 6, textAlign: "left", cursor: "pointer" }}
                  onClick={() => toggleSort("code")}
                >
                  代码 {sortBy === "code" && (sortDir === "asc" ? "↑" : "↓")}
                </th>
                <th
                  style={{ padding: 6, textAlign: "left", cursor: "pointer" }}
                  onClick={() => toggleSort("name")}
                >
                  名称 {sortBy === "name" && (sortDir === "asc" ? "↑" : "↓")}
                </th>
                <th
                  style={{ padding: 6, textAlign: "left", cursor: "pointer" }}
                  onClick={() => toggleSort("category")}
                >
                  分类 {sortBy === "category" && (sortDir === "asc" ? "↑" : "↓")}
                </th>
                <th
                  style={{ padding: 6, textAlign: "left", cursor: "pointer" }}
                  onClick={() => toggleSort("entry_rank")}
                >
                  Rank {sortBy === "entry_rank" && (sortDir === "asc" ? "↑" : "↓")}
                </th>
                <th
                  style={{ padding: 6, textAlign: "right", cursor: "pointer" }}
                  onClick={() => toggleSort("entry_price")}
                >
                  加入价格 {sortBy === "entry_price" && (sortDir === "asc" ? "↑" : "↓")}
                </th>
                <th
                  style={{
                    padding: 6,
                    textAlign: "right",
                    cursor: pricesRefreshed ? "pointer" : "not-allowed",
                    color: pricesRefreshed ? "#333" : "#999",
                  }}
                  onClick={() => pricesRefreshed && toggleSort("last")}
                >
                  最新价 {sortBy === "last" && (sortDir === "asc" ? "↑" : "↓")}
                  {!pricesRefreshed && "*"}
                </th>
                <th
                  style={{
                    padding: 6,
                    textAlign: "right",
                    cursor: pricesRefreshed ? "pointer" : "not-allowed",
                    color: pricesRefreshed ? "#333" : "#999",
                  }}
                  onClick={() => pricesRefreshed && toggleSort("pct_change")}
                >
                  涨幅% {sortBy === "pct_change" && (sortDir === "asc" ? "↑" : "↓")}
                  {!pricesRefreshed && "*"}
                </th>
                <th
                  style={{
                    padding: 6,
                    textAlign: "right",
                    cursor: pricesRefreshed ? "pointer" : "not-allowed",
                    color: pricesRefreshed ? "#333" : "#999",
                  }}
                  onClick={() => pricesRefreshed && toggleSort("pct_since_entry")}
                >
                  加入以来涨幅 {sortBy === "pct_since_entry" && (sortDir === "asc" ? "↑" : "↓")}
                  {!pricesRefreshed && "*"}
                </th>
                <th
                  style={{
                    padding: 6,
                    textAlign: "right",
                    cursor: pricesRefreshed ? "pointer" : "not-allowed",
                    color: pricesRefreshed ? "#333" : "#999",
                  }}
                  onClick={() => pricesRefreshed && toggleSort("open")}
                >
                  开盘 {sortBy === "open" && (sortDir === "asc" ? "↑" : "↓")}
                  {!pricesRefreshed && "*"}
                </th>
                <th
                  style={{
                    padding: 6,
                    textAlign: "right",
                    cursor: pricesRefreshed ? "pointer" : "not-allowed",
                    color: pricesRefreshed ? "#333" : "#999",
                  }}
                  onClick={() => pricesRefreshed && toggleSort("prev_close")}
                >
                  昨收 {sortBy === "prev_close" && (sortDir === "asc" ? "↑" : "↓")}
                  {!pricesRefreshed && "*"}
                </th>
                <th
                  style={{
                    padding: 6,
                    textAlign: "right",
                    cursor: pricesRefreshed ? "pointer" : "not-allowed",
                    color: pricesRefreshed ? "#333" : "#999",
                  }}
                  onClick={() => pricesRefreshed && toggleSort("high")}
                >
                  最高 {sortBy === "high" && (sortDir === "asc" ? "↑" : "↓")}
                  {!pricesRefreshed && "*"}
                </th>
                <th
                  style={{
                    padding: 6,
                    textAlign: "right",
                    cursor: pricesRefreshed ? "pointer" : "not-allowed",
                    color: pricesRefreshed ? "#333" : "#999",
                  }}
                  onClick={() => pricesRefreshed && toggleSort("low")}
                >
                  最低 {sortBy === "low" && (sortDir === "asc" ? "↑" : "↓")}
                  {!pricesRefreshed && "*"}
                </th>
                <th
                  style={{
                    padding: 6,
                    textAlign: "right",
                    cursor: pricesRefreshed ? "pointer" : "not-allowed",
                    color: pricesRefreshed ? "#333" : "#999",
                  }}
                  onClick={() => pricesRefreshed && toggleSort("volume_hand")}
                >
                  成交量(手) {sortBy === "volume_hand" && (sortDir === "asc" ? "↑" : "↓")}
                  {!pricesRefreshed && "*"}
                </th>
                <th
                  style={{
                    padding: 6,
                    textAlign: "right",
                    cursor: pricesRefreshed ? "pointer" : "not-allowed",
                    color: pricesRefreshed ? "#333" : "#999",
                  }}
                  onClick={() => pricesRefreshed && toggleSort("amount")}
                >
                  成交额 {sortBy === "amount" && (sortDir === "asc" ? "↑" : "↓")}
                  {!pricesRefreshed && "*"}
                </th>
                <th
                  style={{ padding: 6, textAlign: "left" }}
                >
                  投资评级
                </th>
                <th
                  style={{ padding: 6, textAlign: "left", cursor: "pointer" }}
                  onClick={() => toggleSort("created_at")}
                >
                  加入时间 {sortBy === "created_at" && (sortDir === "asc" ? "↑" : "↓")}
                </th>
                <th
                  style={{ padding: 6, textAlign: "left", cursor: "pointer" }}
                  onClick={() => toggleSort("last_analysis_time")}
                >
                  分析时间 {sortBy === "last_analysis_time" && (sortDir === "asc" ? "↑" : "↓")}
                </th>
              </tr>
            </thead>
            <tbody>
              {items.map((row) => {
                const joinDate = row.created_at
                  ? formatDate(row.created_at)
                  : "-";
                const selected = selectedIds.includes(row.id);
                return (
                  <tr
                    key={row.id}
                    style={{
                      borderTop: "1px solid #f0f0f0",
                      background: selected ? "#eff6ff" : "#fff",
                    }}
                  >
                    <td style={{ padding: 6, textAlign: "center" }}>
                      <input
                        type="checkbox"
                        checked={selected}
                        onChange={(e) => toggleSelect(row.id, e.target.checked)}
                      />
                    </td>
                    <td style={{ padding: 6, fontFamily: "monospace" }}>
                      {displayCode(row.code)}
                    </td>
                    <td style={{ padding: 6 }}>{row.name}</td>
                    <td style={{ padding: 6 }}>{row.category_names || "-"}</td>
                    <td style={{ padding: 6, textAlign: "right", color: "#6b7280" }}>
                      {row.entry_rank != null ? String(row.entry_rank) : "-"}
                    </td>
                    <td style={{ padding: 6, textAlign: "right", color: "#6b7280" }}>
                      {row.entry_price != null ? row.entry_price.toFixed(3) : "-"}
                    </td>
                    <td style={{ padding: 6, textAlign: "right" }}>
                      {row.last != null ? row.last.toFixed(3) : "-"}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        textAlign: "right",
                        color:
                          (row.pct_change ?? 0) > 0
                            ? "#e53935"
                            : (row.pct_change ?? 0) < 0
                              ? "#1e88e5"
                              : "#333",
                      }}
                    >
                      {row.pct_change != null
                        ? Number(row.pct_change).toFixed(3)
                        : "-"}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        textAlign: "right",
                        fontWeight: 600,
                        color:
                          (row.pct_since_entry ?? 0) > 0
                            ? "#e53935"
                            : (row.pct_since_entry ?? 0) < 0
                              ? "#1e88e5"
                              : "#333",
                      }}
                    >
                      {row.pct_since_entry != null
                        ? `${row.pct_since_entry.toFixed(2)}%`
                        : "-"}
                    </td>
                    <td style={{ padding: 6, textAlign: "right" }}>
                      {row.open != null ? row.open.toFixed(3) : "-"}
                    </td>
                    <td style={{ padding: 6, textAlign: "right" }}>
                      {row.prev_close != null ? row.prev_close.toFixed(3) : "-"}
                    </td>
                    <td style={{ padding: 6, textAlign: "right" }}>
                      {row.high != null ? row.high.toFixed(3) : "-"}
                    </td>
                    <td style={{ padding: 6, textAlign: "right" }}>
                      {row.low != null ? row.low.toFixed(3) : "-"}
                    </td>
                    <td style={{ padding: 6, textAlign: "right" }}>
                      {row.volume_hand != null
                        ? Number(row.volume_hand).toFixed(0)
                        : "-"}
                    </td>
                    <td style={{ padding: 6, textAlign: "right" }}>
                      {formatAmount(row.amount ?? null)}
                    </td>
                    <td style={{ padding: 6 }}>{row.rating || "N/A"}</td>
                    <td style={{ padding: 6 }}>{joinDate}</td>
                    <td style={{ padding: 6 }}>
                      {row.last_analysis_time
                        ? formatDateTime(row.last_analysis_time)
                        : "N/A"}
                    </td>
                  </tr>
                );
              })}
              {items.length === 0 && !loading && (
                <tr>
                  <td colSpan={17} style={{ padding: 10, textAlign: "center" }}>
                    暂无自选股票。
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* 分页 */}
        <section
          style={{
            marginTop: 12,
            paddingTop: 8,
            borderTop: "1px solid #e5e7eb",
            fontSize: 13,
            display: "flex",
            gap: 8,
            alignItems: "center",
          }}
        >
          <button
            type="button"
            onClick={() => setPage(Math.max(1, page - 1))}
            disabled={page <= 1}
            style={{
              padding: "4px 10px",
              borderRadius: 6,
              border: "1px solid #cbd5e1",
              background: page <= 1 ? "#f9fafb" : "#fff",
              cursor: page <= 1 ? "not-allowed" : "pointer",
            }}
          >
            上一页
          </button>
          <span>
            第 {page} / {totalPages} 页
          </span>
          <button
            type="button"
            onClick={() => setPage(Math.min(totalPages, page + 1))}
            disabled={page >= totalPages}
            style={{
              padding: "4px 10px",
              borderRadius: 6,
              border: "1px solid #cbd5e1",
              background: page >= totalPages ? "#f9fafb" : "#fff",
              cursor: page >= totalPages ? "not-allowed" : "pointer",
            }}
          >
            下一页
          </button>
          
          <select
            value={pageSize}
            onChange={(e) => {
              setPageSize(Number(e.target.value));
              setPage(1);
            }}
            style={{
              padding: "4px 6px",
              borderRadius: 6,
              border: "1px solid #e5e7eb",
            }}
          >
            <option value={20}>20条/页</option>
            <option value={50}>50条/页</option>
            <option value={100}>100条/页</option>
            <option value={200}>200条/页</option>
          </select>
        </section>

        {/* 选中状态 */}
        <section
          style={{
            marginTop: 12,
            paddingTop: 8,
            borderTop: "1px solid #e5e7eb",
            fontSize: 13,
          }}
        >
          <div style={{ fontWeight: 600 }}>
            当前已选中 {selectedIds.length} 条
          </div>
        </section>
      </section>
    </main>
  );
}
