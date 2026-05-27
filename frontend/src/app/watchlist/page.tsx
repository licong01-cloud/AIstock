"use client";

import { useEffect, useMemo, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface WatchlistCategory {
  id: number;
  name: string;
  description?: string | null;
}

export default WatchlistPage;


interface WatchlistItem {
  id: number;
  code: string;
  name: string;
  category_names?: string;
  category_ids?: number[];
  created_at?: string | null;
  updated_at?: string | null;
  last_analysis_time?: string | null;
  last_rating?: string | null;
  last_conclusion?: string | null;
  entry_price?: number | null;
  entry_rank?: number | null;
  entry_source?: string | null;
  entry_task_id?: string | null;
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
}

interface ListItemsResponse {
  total: number;
  items: WatchlistItem[];
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
  prices: Record<
    string,
    {
      latestPrice?: number | null;
      openPrice?: number | null;
      closePrice?: number | null;
      highPrice?: number | null;
      lowPrice?: number | null;
      rating?: string | null;
    } | null
  >;
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

type SortByPersistent =
  | "code"
  | "name"
  | "category"
  | "entry_price"
  | "entry_rank"
  | "created_at"
  | "updated_at"
  | "last_analysis_time"
  | "last_rating";

type SortByRealtime =
  | "last"
  | "pct_change"
  | "pct_since_entry"
  | "open"
  | "prev_close"
  | "high"
  | "low"
  | "volume_hand"
  | "amount";

type SortBy = SortByPersistent | SortByRealtime;

const PERSISTENT_SORT_KEYS: SortByPersistent[] = [
  "code",
  "name",
  "category",
  "created_at",
  "updated_at",
  "last_analysis_time",
  "last_rating",
];

const REALTIME_SORT_KEYS: SortByRealtime[] = [
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

const DEFAULT_SORT_FIELD: SortBy = "updated_at";
const DEFAULT_SORT_DIR: "asc" | "desc" = "desc";

const REALTIME_HINT = "请先刷新价格";

interface NumericFilter {
  op: ">=" | "<=" | ">" | "<" | "=";
  enabled: boolean;
  value: number;
}

interface DateFilter {
  op: ">=" | "<=" | ">" | "<" | "=";
  enabled: boolean;
  value: string; // YYYY-MM-DD
}

interface SearchFilters {
  code: string;
  name: string;
  category: string;
  rating: string;
  entry_rank: NumericFilter;
  num: {
    last: NumericFilter;
    pct_change: NumericFilter;
    open: NumericFilter;
    prev_close: NumericFilter;
    high: NumericFilter;
    low: NumericFilter;
    volume_hand: NumericFilter;
    amount: NumericFilter;
  };
  date: {
    created_at: DateFilter;
    last_analysis_time: DateFilter;
  };
}

const DEFAULT_NUMERIC_FILTER: NumericFilter = {
  op: ">=",
  enabled: false,
  value: 0,
};

const DEFAULT_DATE_FILTER: DateFilter = {
  op: ">=",
  enabled: false,
  value: "",
};

function displayCode(code: string): string {
  const c = (code || "").trim();
  if (!c) return "";
  if (c.includes(".")) {
    return c.split(".")[0] || c;
  }
  return c;
}

function cmpNumeric(val: number | null | undefined, op: string, target: number): boolean {
  if (!Number.isFinite(target)) return true;
  if (val === null || val === undefined || Number.isNaN(val)) return false;
  const v = Number(val);
  if (op === ">=") return v >= target;
  if (op === "<=") return v <= target;
  if (op === ">") return v > target;
  if (op === "<") return v < target;
  return v === target;
}

function toDateOnly(iso: string | null | undefined): string | null {
  if (!iso) return null;
  const s = String(iso);
  if (!s) return null;
  if (s.includes("T")) return s.split("T", 1)[0];
  return s.slice(0, 10);
}

function cmpDate(valIso: string | null | undefined, op: string, targetDate: string): boolean {
  if (!targetDate) return true;
  const v = toDateOnly(valIso);
  if (!v) return false;
  const t = targetDate;
  if (op === ">=") return v >= t;
  if (op === "<=") return v <= t;
  if (op === ">") return v > t;
  if (op === "<") return v < t;
  return v === t;
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

function sortItemsRealtime(
  items: WatchlistItem[],
  sortBy: SortByRealtime,
  sortDir: "asc" | "desc",
): WatchlistItem[] {
  const reverse = sortDir === "desc";
  const cloned = [...items];
  cloned.sort((a, b) => {
    const va = (a as any)[sortBy] as number | null | undefined;
    const vb = (b as any)[sortBy] as number | null | undefined;
    const aNull = va === null || va === undefined || Number.isNaN(va);
    const bNull = vb === null || vb === undefined || Number.isNaN(vb);
    if (aNull && bNull) {
      return displayCode(a.code).localeCompare(displayCode(b.code));
    }
    if (aNull) return 1;
    if (bNull) return -1;
    const diff = Number(va) - Number(vb);
    if (diff === 0) {
      return displayCode(a.code).localeCompare(displayCode(b.code));
    }
    return reverse ? -diff : diff;
  });
  return cloned;
}

function canSortRealtime(field: SortBy, pricesRefreshed: boolean): boolean {
  if (!REALTIME_SORT_KEYS.includes(field as SortByRealtime)) return true;
  return pricesRefreshed;
}

function sortItemsPersistent(
  items: WatchlistItem[],
  sortBy: SortByPersistent,
  sortDir: "asc" | "desc",
): WatchlistItem[] {
  const reverse = sortDir === "desc";
  const cloned = [...items];
  cloned.sort((a, b) => {
    if (sortBy === "entry_price" || sortBy === "entry_rank") {
      const va = (a as any)[sortBy] as number | null | undefined;
      const vb = (b as any)[sortBy] as number | null | undefined;
      const aNull = va === null || va === undefined || Number.isNaN(va);
      const bNull = vb === null || vb === undefined || Number.isNaN(vb);
      if (aNull && bNull) {
        return displayCode(a.code).localeCompare(displayCode(b.code));
      }
      if (aNull) return 1;
      if (bNull) return -1;
      const diff = Number(va) - Number(vb);
      if (diff === 0) {
        return displayCode(a.code).localeCompare(displayCode(b.code));
      }
      return reverse ? -diff : diff;
    }
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

function WatchlistPage() {
  const [categories, setCategories] = useState<WatchlistCategory[]>([]);
  const [currentCatId, setCurrentCatId] = useState<number | null>(null);

  const [sortBy, setSortBy] = useState<SortBy>(DEFAULT_SORT_FIELD);
  const [sortDir, setSortDir] = useState<"asc" | "desc">(DEFAULT_SORT_DIR);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(5); // seconds

  const [allItems, setAllItems] = useState<WatchlistItem[]>([]);
  const [items, setItems] = useState<WatchlistItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [pricesRefreshed, setPricesRefreshed] = useState(false);
  const [refreshingPrices, setRefreshingPrices] = useState(false);

  const [searchActive, setSearchActive] = useState(false);
  const [searchFilters, setSearchFilters] = useState<SearchFilters>({
    code: "",
    name: "",
    category: "",
    rating: "",
    entry_rank: { ...DEFAULT_NUMERIC_FILTER },
    num: {
      last: { ...DEFAULT_NUMERIC_FILTER },
      pct_change: { ...DEFAULT_NUMERIC_FILTER },
      open: { ...DEFAULT_NUMERIC_FILTER },
      prev_close: { ...DEFAULT_NUMERIC_FILTER },
      high: { ...DEFAULT_NUMERIC_FILTER },
      low: { ...DEFAULT_NUMERIC_FILTER },
      volume_hand: { ...DEFAULT_NUMERIC_FILTER },
      amount: { ...DEFAULT_NUMERIC_FILTER },
    },
    date: {
      created_at: { ...DEFAULT_DATE_FILTER },
      last_analysis_time: { ...DEFAULT_DATE_FILTER },
    },
  });

  const [selectedIds, setSelectedIds] = useState<number[]>([]);

  // 卡片展开/折叠状态
  const [expandedCards, setExpandedCards] = useState<Set<string>>(new Set());

  const toggleCard = (cardId: string) => {
    setExpandedCards((prev) => {
      const next = new Set(prev);
      if (next.has(cardId)) {
        next.delete(cardId);
      } else {
        next.add(cardId);
      }
      return next;
    });
  };

  // 可折叠卡片组件
  const CollapsibleCard = ({ title, cardId, children }: { title: string; cardId: string; children: React.ReactNode }) => {
    const isExpanded = expandedCards.has(cardId);
    return (
      <section
        style={{
          background: "#f9fafb",
          borderRadius: 10,
          marginBottom: 8,
          fontSize: 13,
        }}
      >
        <div
          onClick={() => toggleCard(cardId)}
          style={{
            padding: "8px 12px",
            cursor: "pointer",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            background: "#f3f4f6",
            borderRadius: "10px 10px 0 0",
            borderBottom: isExpanded ? "1px solid #e5e7eb" : "none",
          }}
        >
          <h3 style={{ margin: 0, fontSize: 14 }}>
            {title}
            <span style={{ fontSize: 12, color: "#9ca3af", marginLeft: 8 }}>
              {isExpanded ? "▼" : "▶"}
            </span>
          </h3>
        </div>
        {isExpanded && (
          <div style={{ padding: "12px" }}>{children}</div>
        )}
      </section>
    );
  };

  // 分类管理与添加到自选相关表单状态
  const [newCatName, setNewCatName] = useState("");
  const [newCatDesc, setNewCatDesc] = useState("");
  const [renameTargetName, setRenameTargetName] = useState("");
  const [renameNewName, setRenameNewName] = useState("");
  const [renameNewDesc, setRenameNewDesc] = useState("");
  const [deleteTargetName, setDeleteTargetName] = useState("");

  const [singleAddCode, setSingleAddCode] = useState("");
  const [singleAddName, setSingleAddName] = useState("");
  const [singleAddMode, setSingleAddMode] = useState<"existing" | "new">(
    "existing",
  );
  const [singleAddExistingCats, setSingleAddExistingCats] = useState<string[]>(
    [],
  );
  const [singleAddNewCatName, setSingleAddNewCatName] = useState("");

  const [batchAddCodes, setBatchAddCodes] = useState("");
  const [batchAddCatChoice, setBatchAddCatChoice] = useState("");
  const [batchAddNewCatName, setBatchAddNewCatName] = useState("");
  const [batchAddMoveIfExists, setBatchAddMoveIfExists] = useState(false);

  // 批量操作
  const [bulkOpType, setBulkOpType] = useState(
    "新增" as
      | "新增"
      | "修改分类"
      | "添加到分类"
      | "从分类移除"
      | "删除"
      | "批量分析",
  );
  const [bulkTargetCatName, setBulkTargetCatName] = useState("");
  const [bulkNewCatName, setBulkNewCatName] = useState("");
  const [bulkAddCatNames, setBulkAddCatNames] = useState<string[]>([]);
  const [bulkRemoveCatNames, setBulkRemoveCatNames] = useState<string[]>([]);
  const [bulkAddCodes, setBulkAddCodes] = useState("");

  const currentCatName = useMemo(
    () => categories.find((c) => c.id === currentCatId)?.name || "",
    [categories, currentCatId],
  );

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const allSelected =
    items.length > 0 && items.every((it) => selectedIds.includes(it.id));
  const someSelected =
    items.length > 0 && items.some((it) => selectedIds.includes(it.id));

  const nameToCatId = useMemo(() => {
    const map: Record<string, number> = {};
    categories.forEach((c) => {
      map[c.name] = c.id;
    });
    return map;
  }, [categories]);


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

  function matchesCategory(item: WatchlistItem): boolean {
    if (currentCatId == null) return true;
    if (Array.isArray(item.category_ids)) {
      return item.category_ids.includes(currentCatId);
    }
    const catName = categories.find((c) => c.id === currentCatId)?.name;
    if (!catName) return false;
    return (item.category_names || "").includes(catName);
  }

  function applyFiltersAndSort(nextItems: WatchlistItem[]) {
    let filtered = nextItems.filter((item) => matchesCategory(item));

    if (searchActive) {
      const f = searchFilters;
      const tCode = f.code.trim().toLowerCase();
      const tName = f.name.trim().toLowerCase();
      const tCat = f.category.trim().toLowerCase();
      const tRating = f.rating.trim().toLowerCase();

      function okText(it: WatchlistItem): boolean {
        const ts = (it.code || "").toLowerCase();
        const c6 = displayCode(it.code).toLowerCase();
        if (tCode && !c6.includes(tCode) && !ts.includes(tCode)) return false;
        if (tName && !(it.name || "").toLowerCase().includes(tName)) return false;
        if (tCat && !(it.category_names || "").toLowerCase().includes(tCat))
          return false;
        if (
          tRating &&
          !(it.last_rating || "").toLowerCase().includes(tRating)
        )
          return false;
        return true;
      }

      function okEntryRank(it: WatchlistItem): boolean {
        const er = f.entry_rank;
        if (!er.enabled) return true;
        return cmpNumeric(it.entry_rank, er.op, er.value);
      }

      function okNumeric(it: WatchlistItem): boolean {
        const n = f.num;
        const mapping: [keyof typeof n, keyof WatchlistItem][] = [
          ["last", "last"],
          ["pct_change", "pct_change"],
          ["open", "open"],
          ["prev_close", "prev_close"],
          ["high", "high"],
          ["low", "low"],
          ["volume_hand", "volume_hand"],
          ["amount", "amount"],
        ];
        for (const [k, field] of mapping) {
          const nf = n[k];
          if (!nf.enabled) continue;
          if (!cmpNumeric((it as any)[field], nf.op, nf.value)) return false;
        }
        return true;
      }

      function okDate(it: WatchlistItem): boolean {
        const d = f.date;
        if (d.created_at.enabled) {
          if (!cmpDate(it.created_at ?? null, d.created_at.op, d.created_at.value))
            return false;
        }
        if (d.last_analysis_time.enabled) {
          if (
            !cmpDate(
              it.last_analysis_time ?? null,
              d.last_analysis_time.op,
              d.last_analysis_time.value,
            )
          )
            return false;
        }
        return true;
      }

      filtered = filtered.filter((it) => okText(it) && okEntryRank(it) && okNumeric(it) && okDate(it));
    }

    const canSort = canSortRealtime(sortBy, pricesRefreshed);
    const effectiveSort = canSort ? sortBy : DEFAULT_SORT_FIELD;
    const effectiveDir = canSort ? sortDir : DEFAULT_SORT_DIR;

    if (PERSISTENT_SORT_KEYS.includes(effectiveSort as SortByPersistent)) {
      filtered = sortItemsPersistent(
        filtered,
        effectiveSort as SortByPersistent,
        effectiveDir,
      );
    } else {
      filtered = sortItemsRealtime(
        filtered,
        effectiveSort as SortByRealtime,
        effectiveDir,
      );
    }

    const totalLocal = filtered.length;
    const start = Math.max(0, (page - 1) * pageSize);
    const end = start + pageSize;
    const pageItems = filtered.slice(start, end);
    setItems(pageItems);
    setTotal(totalLocal);
  }

  async function loadAllItems() {
    setLoading(true);
    setError(null);
    try {
      const all: WatchlistItem[] = [];
      let fetched = 0;
      let totalRemote = 0;
      const pageSizeServer = 200;
      let p = 1;
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const params = new URLSearchParams();
        params.set("page", String(p));
        params.set("page_size", String(pageSizeServer));
        params.set("sort_by", "updated_at");
        params.set("sort_dir", "desc");
        const res = await fetch(
          `${API_BASE}/watchlist/items?${params.toString()}`,
        );
        if (!res.ok) throw new Error(`列表请求失败: ${res.status}`);
        const data: ListItemsResponse = await res.json();
        const batch = data.items || [];
        if (p === 1) totalRemote = data.total || batch.length || 0;
        all.push(...batch);
        fetched += batch.length;
        if (batch.length === 0 || fetched >= totalRemote) break;
        p += 1;
        if (p > 200) break;
      }
      setAllItems(all);
      setPricesRefreshed(false);
      applyFiltersAndSort(all);
    } catch (e: any) {
      setError(e?.message || "未知错误");
      setItems([]);
      setTotal(0);
      setAllItems([]);
    } finally {
      setLoading(false);
    }
  }

  async function refreshPrices() {
    if (refreshingPrices) return;
    setRefreshingPrices(true);
    setError(null);
    try {
      let filtered = allItems.filter((item) => matchesCategory(item));
      const codes = filtered.map((item) => item.code).filter(Boolean);
      if (codes.length === 0) {
        setPricesRefreshed(true);
        return;
      }
      const res = await fetch(`${API_BASE}/stocks/prices`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ codes }),
      });
      if (!res.ok) throw new Error(`价格刷新失败: ${res.status}`);
      const data: PricesResponse = await res.json();
      const updated = allItems.map((item) => {
        const priceData = data.prices[item.code];
        if (!priceData) return item;
        const rawLast = priceData.latestPrice ?? item.last;
        const newPrevClose = priceData.closePrice ?? item.prev_close;
        // 停牌/盘前: latestPrice=0 → 用 prev_close 作为有效价格
        const effectivePrice =
          rawLast != null && rawLast > 0
            ? rawLast
            : newPrevClose != null && newPrevClose > 0
              ? newPrevClose
              : item.last;
        // 重新计算涨跌幅
        let newPctChange = item.pct_change;
        if (effectivePrice != null && effectivePrice > 0 && newPrevClose != null && newPrevClose > 0) {
          newPctChange = ((effectivePrice - newPrevClose) / newPrevClose) * 100;
        }
        let newPctSinceEntry = item.pct_since_entry;
        if (effectivePrice != null && effectivePrice > 0 && item.entry_price != null && item.entry_price > 0) {
          newPctSinceEntry = ((effectivePrice - item.entry_price) / item.entry_price) * 100;
        }
        return {
          ...item,
          last: effectivePrice,
          open: priceData.openPrice ?? item.open,
          prev_close: newPrevClose,
          high: priceData.highPrice ?? item.high,
          low: priceData.lowPrice ?? item.low,
          last_rating: priceData.rating ?? item.last_rating,
          pct_change: newPctChange,
          pct_since_entry: newPctSinceEntry,
        };
      });
      setAllItems(updated);
      setPricesRefreshed(true);
      applyFiltersAndSort(updated);
    } catch (e: any) {
      setError(e?.message || "价格刷新失败");
    } finally {
      setRefreshingPrices(false);
    }
  }

  async function loadPageItems() {
    if (searchActive) {
      await loadAllAndFilter();
      return;
    }
    await loadAllItems();
  }

  async function loadAllAndFilter() {
    // 搜索模式：拉取当前分类下所有条目并在前端过滤+排序+分页
    setLoading(true);
    setError(null);
    try {
      const all: WatchlistItem[] = [];
      let fetched = 0;
      let totalRemote = 0;
      const pageSizeServer = 200;
      let p = 1;
      // 先尝试最多取若干页，直到达到 total
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const params = new URLSearchParams();
        if (currentCatId != null) {
          params.set("category_id", String(currentCatId));
        }
        params.set("page", String(p));
        params.set("page_size", String(pageSizeServer));
        params.set("sort_by", "updated_at");
        params.set("sort_dir", "desc");
        const res = await fetch(
          `${API_BASE}/watchlist/items?${params.toString()}`,
        );
        if (!res.ok) throw new Error(`列表请求失败: ${res.status}`);
        const data: ListItemsResponse = await res.json();
        const batch = data.items || [];
        if (p === 1) totalRemote = data.total || batch.length || 0;
        all.push(...batch);
        fetched += batch.length;
        if (batch.length === 0 || fetched >= totalRemote) break;
        p += 1;
        if (p > 100) break;
      }

      // 文本过滤
      const f = searchFilters;
      const tCode = f.code.trim().toLowerCase();
      const tName = f.name.trim().toLowerCase();
      const tCat = f.category.trim().toLowerCase();
      const tRating = f.rating.trim().toLowerCase();

      function okText(it: WatchlistItem): boolean {
        const ts = (it.code || "").toLowerCase();
        const c6 = displayCode(it.code).toLowerCase();
        if (tCode && !c6.includes(tCode) && !ts.includes(tCode)) return false;
        if (tName && !(it.name || "").toLowerCase().includes(tName)) return false;
        if (tCat && !(it.category_names || "").toLowerCase().includes(tCat))
          return false;
        if (
          tRating &&
          !(it.last_rating || "")
            .toLowerCase()
            .includes(tRating)
        )
          return false;
        return true;
      }

      function okNumeric(it: WatchlistItem): boolean {
        const n = f.num;
        const mapping: [keyof typeof n, keyof WatchlistItem][] = [
          ["last", "last"],
          ["pct_change", "pct_change"],
          ["open", "open"],
          ["prev_close", "prev_close"],
          ["high", "high"],
          ["low", "low"],
          ["volume_hand", "volume_hand"],
          ["amount", "amount"],
        ];
        for (const [k, field] of mapping) {
          const nf = n[k];
          if (!nf.enabled) continue;
          if (!cmpNumeric((it as any)[field], nf.op, nf.value)) return false;
        }
        return true;
      }

      function okDate(it: WatchlistItem): boolean {
        const d = f.date;
        if (d.created_at.enabled) {
          if (!cmpDate(it.created_at ?? null, d.created_at.op, d.created_at.value))
            return false;
        }
        if (d.last_analysis_time.enabled) {
          if (
            !cmpDate(
              it.last_analysis_time ?? null,
              d.last_analysis_time.op,
              d.last_analysis_time.value,
            )
          )
            return false;
        }
        return true;
      }

      let filtered = all.filter((it) => okText(it) && okNumeric(it) && okDate(it));

      // 分类筛选（在搜索模式下也需要应用）
      filtered = filtered.filter((item) => matchesCategory(item));

      // 排序
      if (PERSISTENT_SORT_KEYS.includes(sortBy as SortByPersistent)) {
        filtered = sortItemsPersistent(filtered, sortBy as SortByPersistent, sortDir);
      } else {
        filtered = sortItemsRealtime(filtered, sortBy as SortByRealtime, sortDir);
      }

      const totalLocal = filtered.length;
      const start = Math.max(0, (page - 1) * pageSize);
      const end = start + pageSize;
      const pageItems = filtered.slice(start, end);
      setItems(pageItems);
      setTotal(totalLocal);
    } catch (e: any) {
      setError(e?.message || "未知错误");
      setItems([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadCategories();
    loadAllItems();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (searchActive) {
      loadAllAndFilter();
    } else {
      applyFiltersAndSort(allItems);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    currentCatId,
    sortBy,
    sortDir,
    page,
    pageSize,
    searchActive,
    pricesRefreshed,
    allItems,
  ]);

  useEffect(() => {
    if (!autoRefresh) return undefined;
    const id = setInterval(() => {
      if (searchActive) {
        loadAllAndFilter();
      } else {
        applyFiltersAndSort(allItems);
      }
    }, Math.max(2, refreshInterval) * 1000);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    autoRefresh,
    refreshInterval,
    currentCatId,
    sortBy,
    sortDir,
    page,
    pageSize,
    searchActive,
    pricesRefreshed,
    allItems,
  ]);

  function toggleSelect(id: number, checked: boolean) {
    setSelectedIds((prev) => {
      const set = new Set(prev);
      if (checked) set.add(id);
      else set.delete(id);
      return Array.from(set);
    });
  }

  function toggleSelectAll(checked: boolean) {
    if (!checked) {
      const pageIds = new Set(items.map((it) => it.id));
      setSelectedIds((prev) => prev.filter((id) => !pageIds.has(id)));
      return;
    }
    const merged = new Set(selectedIds);
    items.forEach((it) => merged.add(it.id));
    setSelectedIds(Array.from(merged));
  }

  function toggleSort(field: SortBy) {
    if (!canSortRealtime(field, pricesRefreshed)) {
      setError(REALTIME_HINT);
      return;
    }
    if (sortBy === field) {
      setSortDir((prev) => (prev === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(field);
      setSortDir("desc");
    }
  }

  function handleJumpHistory(item: WatchlistItem) {
    const code6 = displayCode(item.code);
    if (typeof window !== "undefined") {
      window.localStorage.setItem("analysis_prefill_history_q", code6);
      window.location.href = "/analysis";
    }
  }

  function handleJumpAnalyze(item: WatchlistItem) {
    const code6 = displayCode(item.code);
    if (typeof window !== "undefined") {
      window.localStorage.setItem("analysis_prefill_single_code", code6);
      window.location.href = "/analysis";
    }
  }

  async function ensureDefaultCategoryId(): Promise<number> {
    const existingId = nameToCatId["默认"];
    if (existingId) return existingId;
    try {
      const res = await fetch(`${API_BASE}/watchlist/categories`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: "默认", description: "默认分类" }),
      });
      const data = await res.json();
      const cid = Number(data.id);
      await loadCategories();
      return cid;
    } catch {
      throw new Error("创建默认分类失败");
    }
  }

  async function handleCreateCategory() {
    const n = newCatName.trim();
    if (!n) return;
    try {
      const res = await fetch(`${API_BASE}/watchlist/categories`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: n, description: newCatDesc.trim() || null }),
      });
      if (!res.ok) throw new Error(String(res.status));
      setNewCatName("");
      setNewCatDesc("");
      await loadCategories();
    } catch (e: any) {
      setError(e?.message || "创建分类失败");
    }
  }

  async function handleRenameCategory() {
    const target = categories.find((c) => c.name === renameTargetName);
    const nn = renameNewName.trim();
    if (!target || !nn) return;
    try {
      const res = await fetch(
        `${API_BASE}/watchlist/categories/${target.id}`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: nn, description: renameNewDesc.trim() || null }),
        },
      );
      if (!res.ok) throw new Error(String(res.status));
      setRenameNewName("");
      setRenameNewDesc("");
      await loadCategories();
    } catch (e: any) {
      setError(e?.message || "重命名分类失败");
    }
  }

  async function handleDeleteCategory() {
    const target = categories.find((c) => c.name === deleteTargetName);
    if (!target) return;
    try {
      const res = await fetch(
        `${API_BASE}/watchlist/categories/${target.id}`,
        { method: "DELETE" },
      );
      if (!res.ok) throw new Error(String(res.status));
      const data = await res.json();
      if (!data.success) {
        setError("删除失败：分类需为空");
      }
      if (currentCatId === target.id) {
        setCurrentCatId(null);
      }
      await loadCategories();
    } catch (e: any) {
      setError(e?.message || "删除分类失败");
    }
  }

  async function handleSingleAdd() {
    const code = singleAddCode.trim();
    if (!code) {
      setError("请输入股票代码");
      return;
    }
    try {
      let primaryCid: number | null = null;
      const extraCids: number[] = [];
      if (singleAddMode === "new") {
        const n = singleAddNewCatName.trim();
        if (!n) {
          setError("请输入新建分类名称");
          return;
        }
        const res = await fetch(`${API_BASE}/watchlist/categories`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: n, description: null }),
        });
        const data = await res.json();
        primaryCid = Number(data.id);
        await loadCategories();
      } else {
        const picked = singleAddExistingCats
          .map((n) => nameToCatId[n])
          .filter((id): id is number => !!id);
        if (!picked.length) {
          primaryCid = await ensureDefaultCategoryId();
        } else {
          primaryCid = picked[0];
          extraCids.push(...picked.slice(1));
        }
      }
      if (!primaryCid) {
        throw new Error("无法确定目标分类");
      }
      const resAdd = await fetch(`${API_BASE}/watchlist/items/add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          code,
          category_id: primaryCid,
          name: singleAddName.trim() || null,
          extra_category_ids: extraCids,
        }),
      });
      if (!resAdd.ok) throw new Error(String(resAdd.status));
      setSingleAddCode("");
      setSingleAddName("");
      setSingleAddExistingCats([]);
      setSingleAddNewCatName("");
      setPage(1);
      await loadPageItems();
    } catch (e: any) {
      setError(e?.message || "添加失败");
    }
  }

  async function handleBatchAdd() {
    const raw = batchAddCodes.replace(/\n/g, ",");
    const list = raw
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (!list.length) {
      setError("请输入至少一个股票代码");
      return;
    }
    try {
      let targetCatId: number | null = null;
      const choice = batchAddCatChoice || "默认";
      if (choice === "新建分类...") {
        const n = batchAddNewCatName.trim();
        if (!n) {
          setError("请输入新建分类名称");
          return;
        }
        const res = await fetch(`${API_BASE}/watchlist/categories`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: n, description: null }),
        });
        const data = await res.json();
        targetCatId = Number(data.id);
        await loadCategories();
      } else {
        const id = nameToCatId[choice];
        if (id) {
          targetCatId = id;
        } else {
          const res = await fetch(`${API_BASE}/watchlist/categories`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: choice, description: null }),
          });
          const data = await res.json();
          targetCatId = Number(data.id);
          await loadCategories();
        }
      }
      if (!targetCatId) throw new Error("无法确定目标分类");
      const resAdd = await fetch(`${API_BASE}/watchlist/items/bulk-add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          codes: list,
          category_id: targetCatId,
          on_conflict: batchAddMoveIfExists ? "move" : "ignore",
        }),
      });
      if (!resAdd.ok) throw new Error(String(resAdd.status));
      setBatchAddCodes("");
      setBatchAddNewCatName("");
      setPage(1);
      await loadPageItems();
    } catch (e: any) {
      setError(e?.message || "批量添加失败");
    }
  }

  async function handleBulkExecute() {
    if (bulkOpType === "新增") {
      await handleBatchAdd();
      return;
    }
    if (!selectedIds.length) {
      setError("请先在列表中选择至少一条记录");
      return;
    }
    try {
      if (bulkOpType === "修改分类") {
        const target = categories.find((c) => c.name === bulkTargetCatName);
        if (!target) throw new Error("请选择分类");
        const res = await fetch(
          `${API_BASE}/watchlist/items/bulk-set-category`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ids: selectedIds, category_id: target.id }),
          },
        );
        if (!res.ok) throw new Error(String(res.status));
      } else if (bulkOpType === "添加到分类") {
        const catIds = bulkAddCatNames
          .map((n) => nameToCatId[n])
          .filter((id): id is number => !!id);
        if (!catIds.length) throw new Error("请选择至少一个分类");
        const res = await fetch(
          `${API_BASE}/watchlist/items/bulk-add-categories`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ids: selectedIds, category_ids: catIds }),
          },
        );
        if (!res.ok) throw new Error(String(res.status));
      } else if (bulkOpType === "从分类移除") {
        const catIds = bulkRemoveCatNames
          .map((n) => nameToCatId[n])
          .filter((id): id is number => !!id);
        if (!catIds.length) throw new Error("请选择至少一个分类");
        const res = await fetch(
          `${API_BASE}/watchlist/items/bulk-remove-categories`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ids: selectedIds, category_ids: catIds }),
          },
        );
        if (!res.ok) throw new Error(String(res.status));
      } else if (bulkOpType === "删除") {
        const yes =
          typeof window === "undefined" ||
          window.confirm("确认删除选中的自选记录？该操作不可恢复。");
        if (!yes) return;
        const res = await fetch(
          `${API_BASE}/watchlist/items/bulk-delete`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ids: selectedIds }),
          },
        );
        if (!res.ok) throw new Error(String(res.status));
      } else if (bulkOpType === "批量分析") {
        if (typeof window !== "undefined") {
          const codes: string[] = [];
          const idSet = new Set(selectedIds);
          items.forEach((it) => {
            if (idSet.has(it.id)) {
              codes.push(displayCode(it.code));
            }
          });
          window.localStorage.setItem(
            "analysis_prefill_batch_codes",
            codes.join("\n"),
          );
          window.location.href = "/analysis";
        }
      }
      await loadPageItems();
    } catch (e: any) {
      setError(e?.message || "批量操作失败");
    }
  }

  return (
    <main style={{ padding: 24 }}>
      <section style={{ marginBottom: 16 }}>
        <h1 data-testid="watchlist-title" style={{ margin: 0, fontSize: 22 }}>⭐ 自选股票池</h1>
      </section>

      {/* 列表控制条：排序、分页、自动刷新 */}
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
            data-testid="watchlist-category-filter"
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
          <span style={{ marginRight: 6 }}>排序字段：</span>
          <select
            title="排序字段"
            value={sortBy}
            onChange={(e) => {
              const next = e.target.value as SortBy;
              if (!canSortRealtime(next, pricesRefreshed)) {
                setError(REALTIME_HINT);
                return;
              }
              setSortBy(next);
              setPage(1);
            }}
          >
            <option value="code">代码</option>
            <option value="name">名称</option>
            <option value="category">分类</option>
            <option value="created_at">加入时间</option>
            <option value="updated_at">更新时间</option>
            <option value="last_analysis_time">最近分析时间</option>
            <option value="last_rating">投资评级</option>
            <option value="entry_price">加入价格</option>
            <option value="last" disabled={!pricesRefreshed}>
              最新价{!pricesRefreshed ? "*" : ""}
            </option>
            <option value="pct_change" disabled={!pricesRefreshed}>
              涨幅%{!pricesRefreshed ? "*" : ""}
            </option>
            <option value="pct_since_entry" disabled={!pricesRefreshed}>
              加入以来涨幅{!pricesRefreshed ? "*" : ""}
            </option>
            <option value="open" disabled={!pricesRefreshed}>
              开盘{!pricesRefreshed ? "*" : ""}
            </option>
            <option value="prev_close" disabled={!pricesRefreshed}>
              昨收{!pricesRefreshed ? "*" : ""}
            </option>
            <option value="high" disabled={!pricesRefreshed}>
              最高{!pricesRefreshed ? "*" : ""}
            </option>
            <option value="low" disabled={!pricesRefreshed}>
              最低{!pricesRefreshed ? "*" : ""}
            </option>
            <option value="volume_hand" disabled={!pricesRefreshed}>
              成交量(手){!pricesRefreshed ? "*" : ""}
            </option>
            <option value="amount" disabled={!pricesRefreshed}>
              成交额{!pricesRefreshed ? "*" : ""}
            </option>
          </select>
          {!pricesRefreshed && (
            <span style={{ marginLeft: 6, fontSize: 12, color: "#fff" }}>
              *需先刷新价格
            </span>
          )}
        </div>

        <div>
          <span style={{ marginRight: 6 }}>方向：</span>
          <select
            title="排序方向"
            value={sortDir}
            onChange={(e) => {
              setSortDir(e.target.value as "asc" | "desc");
              setPage(1);
            }}
          >
            <option value="desc">降序</option>
            <option value="asc">升序</option>
          </select>
        </div>

        <div>
          <span style={{ marginRight: 6 }}>每页条数：</span>
          <select
            title="每页条数"
            value={pageSize}
            onChange={(e) => {
              setPageSize(Number(e.target.value));
              setPage(1);
            }}
          >
            <option value={10}>10</option>
            <option value={20}>20</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <label style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={(e) => setAutoRefresh(e.target.checked)}
            />
            自动刷新
          </label>
          <select
            title="刷新间隔"
            value={refreshInterval}
            onChange={(e) => setRefreshInterval(Number(e.target.value))}
          >
            <option value={5}>5 秒</option>
            <option value={10}>10 秒</option>
            <option value={20}>20 秒</option>
            <option value={30}>30 秒</option>
            <option value={60}>1 分钟</option>
            <option value={300}>5 分钟</option>
            <option value={600}>10 分钟</option>
          </select>
        </div>

        <button
          type="button"
          onClick={() => {
            loadPageItems();
          }}
          disabled= {loading}
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

        <span style={{ color: "#fff" }}>
          {currentCatName ? `当前分类：${currentCatName}` : "全部分类"}
          {total ? ` · 共 ${total} 条` : ""}
          {totalPages ? ` · 第 ${page}/${totalPages} 页` : ""}
          {pricesRefreshed ? " · 价格已刷新" : " · 价格未刷新"}
        </span>
      </section>

      {error && <p style={{ color: "#b00020", fontSize: 13 }}>错误：{error}</p>}

      {/* 分类管理 */}
      <CollapsibleCard title="🗂 分类管理" cardId="category">
        <div style={{ display: "flex", flexWrap: "wrap", gap: 16 }}>
          <div style={{ minWidth: 220 }}>
            <div style={{ fontWeight: 600, marginBottom: 4 }}>新建分类</div>
            <input
              title="新建分类名称"
              value={newCatName}
              onChange={(e) => setNewCatName(e.target.value)}
              placeholder="分类名称"
              style={{
                width: "100%",
                marginBottom: 4,
                padding: "4px 6px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
              }}
            />
            <input
              title="新建分类描述"
              value={newCatDesc}
              onChange={(e) => setNewCatDesc(e.target.value)}
              placeholder="描述(可选)"
              style={{
                width: "100%",
                marginBottom: 6,
                padding: "4px 6px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
              }}
            />
            <button
              type="button"
              onClick={handleCreateCategory}
              disabled={!newCatName.trim()}
              style={{
                padding: "4px 10px",
                borderRadius: 999,
                border: "1px solid #22c55e",
                background: newCatName.trim() ? "#dcfce7" : "#e5e7eb",
                fontSize: 12,
              }}
            >
              创建
            </button>
          </div>

          <div style={{ minWidth: 220 }}>
            <div style={{ fontWeight: 600, marginBottom: 4 }}>重命名分类</div>
            <select
              title="选择要重命名的分类"
              value={renameTargetName}
              onChange={(e) => setRenameTargetName(e.target.value)}
              style={{
                width: "100%",
                marginBottom: 4,
                padding: "4px 6px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
              }}
            >
              <option value="">(选择分类)</option>
              {categories
                .filter((c) => c.name !== "默认" && c.name !== "持仓股票")
                .map((c) => (
                  <option key={c.id} value={c.name}>
                    {c.name}
                  </option>
                ))}
            </select>
            <input
              title="新分类名称"
              value={renameNewName}
              onChange={(e) => setRenameNewName(e.target.value)}
              placeholder="新名称"
              style={{
                width: "100%",
                marginBottom: 4,
                padding: "4px 6px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
              }}
            />
            <input
              title="新分类描述"
              value={renameNewDesc}
              onChange={(e) => setRenameNewDesc(e.target.value)}
              placeholder="新描述(可选)"
              style={{
                width: "100%",
                marginBottom: 6,
                padding: "4px 6px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
              }}
            />
            <button
              type="button"
              onClick={handleRenameCategory}
              disabled={!renameTargetName || !renameNewName.trim()}
              style={{
                padding: "4px 10px",
                borderRadius: 999,
                border: "1px solid #3b82f6",
                background:
                  renameTargetName && renameNewName.trim()
                    ? "#dbeafe"
                    : "#e5e7eb",
                fontSize: 12,
              }}
            >
              重命名
            </button>
          </div>

          <div style={{ minWidth: 220 }}>
            <div style={{ fontWeight: 600, marginBottom: 4 }}>删除分类(需为空)</div>
            <select
              title="选择要删除的分类"
              value={deleteTargetName}
              onChange={(e) => setDeleteTargetName(e.target.value)}
              style={{
                width: "100%",
                marginBottom: 6,
                padding: "4px 6px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
              }}
            >
              <option value="">(选择分类)</option>
              {categories
                .filter((c) => c.name !== "默认" && c.name !== "持仓股票")
                .map((c) => (
                  <option key={c.id} value={c.name}>
                    {c.name}
                  </option>
                ))}
            </select>
            <button
              type="button"
              onClick={handleDeleteCategory}
              disabled={!deleteTargetName}
              style={{
                padding: "4px 10px",
                borderRadius: 999,
                border: "1px solid #ef4444",
                background: deleteTargetName ? "#fee2e2" : "#e5e7eb",
                fontSize: 12,
              }}
            >
              删除
            </button>
          </div>
        </div>
      </CollapsibleCard>

      {/* 添加到自选 */}
      <CollapsibleCard title="➕ 添加到自选" cardId="add">
        <div
          style={{
            display: "flex",
            gap: 16,
          }}
        >
          {/* 左侧：单个添加 */}
          <div style={{ flex: 1 }}>
            <div style={{ marginBottom: 8, fontWeight: 600 }}>单个添加</div>
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: 8,
                marginBottom: 8,
              }}
            >
              <input
                title="股票代码"
                value={singleAddCode}
                onChange={(e) => setSingleAddCode(e.target.value)}
                placeholder="股票代码，如 600519"
                style={{
                  minWidth: 160,
                  padding: "4px 6px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                }}
              />
              <input
                title="股票名称"
                value={singleAddName}
                onChange={(e) => setSingleAddName(e.target.value)}
                placeholder="名称(可选)"
                style={{
                  minWidth: 160,
                  padding: "4px 6px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                }}
              />
            </div>
            <div style={{ marginBottom: 4 }}>分类方式：</div>
            <div style={{ display: "flex", gap: 12, marginBottom: 8 }}>
              <label style={{ display: "flex", alignItems: "center", gap: 4 }}>
                <input
                  type="radio"
                  checked={singleAddMode === "existing"}
                  onChange={() => setSingleAddMode("existing")}
                />
                已有(可多选)
              </label>
              <label style={{ display: "flex", alignItems: "center", gap: 4 }}>
                <input
                  type="radio"
                  checked={singleAddMode === "new"}
                  onChange={() => setSingleAddMode("new")}
                />
                新建
              </label>
            </div>
            {singleAddMode === "existing" ? (
              <select
                title="选择分类(可按 Ctrl 多选)"
                multiple
                value={singleAddExistingCats}
                onChange={(e) => {
                  const opts = Array.from(e.target.selectedOptions).map(
                    (o) => o.value,
                  );
                  setSingleAddExistingCats(opts);
                }}
                style={{
                  minWidth: 220,
                  padding: "4px 6px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                  marginBottom: 8,
                }}
              >
                {categories.map((c) => (
                  <option key={c.id} value={c.name}>
                    {c.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                title="新建分类名称"
                value={singleAddNewCatName}
                onChange={(e) => setSingleAddNewCatName(e.target.value)}
                placeholder="新建分类名称"
                style={{
                  minWidth: 220,
                  padding: "4px 6px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                  marginBottom: 8,
                }}
              />
            )}
            <div>
              <button
                type="button"
                onClick={handleSingleAdd}
                style={{
                  padding: "4px 10px",
                  borderRadius: 999,
                  border: "1px solid #22c55e",
                  background: "#dcfce7",
                  fontSize: 12,
                }}
              >
                添加
              </button>
            </div>
          </div>

          {/* 右侧：批量添加 */}
          <div style={{ flex: 1 }}>
            <div style={{ marginBottom: 8, fontWeight: 600 }}>批量添加</div>
            <textarea
              title="批量添加代码"
              value={batchAddCodes}
              onChange={(e) => setBatchAddCodes(e.target.value)}
              placeholder="多个代码用逗号或换行分隔，如 600519,000001"
              rows={6}
              style={{
                width: "100%",
                padding: "4px 6px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
                marginBottom: 8,
              }}
            />
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: 8,
                alignItems: "center",
              }}
            >
              <select
                title="批量添加分类"
                value={batchAddCatChoice}
                onChange={(e) => setBatchAddCatChoice(e.target.value)}
                style={{
                  minWidth: 160,
                  padding: "4px 6px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                }}
              >
                <option value="默认">默认</option>
                {categories
                  .filter((c) => c.name !== "默认")
                  .map((c) => (
                    <option key={c.id} value={c.name}>
                      {c.name}
                    </option>
                  ))}
                <option value="新建分类...">新建分类...</option>
              </select>
              {batchAddCatChoice === "新建分类..." && (
                <input
                  title="批量新建分类名称"
                  value={batchAddNewCatName}
                  onChange={(e) => setBatchAddNewCatName(e.target.value)}
                  placeholder="新建分类名称"
                  style={{
                    minWidth: 160,
                    padding: "4px 6px",
                    borderRadius: 6,
                    border: "1px solid #e5e7eb",
                  }}
                />
              )}
              <label style={{ display: "flex", alignItems: "center", gap: 4 }}>
                <input
                  type="checkbox"
                  checked={batchAddMoveIfExists}
                  onChange={(e) => setBatchAddMoveIfExists(e.target.checked)}
                />
                存在则移动到此分类
              </label>
              <button
                type="button"
                onClick={handleBatchAdd}
                style={{
                  padding: "4px 10px",
                  borderRadius: 999,
                  border: "1px solid #3b82f6",
                  background: "#dbeafe",
                  fontSize: 12,
                }}
              >
                批量添加
              </button>
            </div>
          </div>
        </div>
      </CollapsibleCard>

      {/* 高级搜索 */}
      <CollapsibleCard title="🔎 搜索" cardId="search">
        <p style={{ margin: "4px 0 8px", fontSize: 12, color: "#6b7280" }}>
          文字为包含匹配，数字/日期支持比较条件；留空表示不筛选。
        </p>
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 8,
            marginBottom: 8,
          }}
        >
          <input
            title="代码包含"
            value={searchFilters.code}
            onChange={(e) =>
              setSearchFilters((prev) => ({ ...prev, code: e.target.value }))
            }
            placeholder="代码包含"
            style={{
              minWidth: 160,
              padding: "4px 6px",
              borderRadius: 6,
              border: "1px solid #e5e7eb",
            }}
          />
          <input
            title="名称包含"
            value={searchFilters.name}
            onChange={(e) =>
              setSearchFilters((prev) => ({ ...prev, name: e.target.value }))
            }
            placeholder="名称包含"
            style={{
              minWidth: 160,
              padding: "4px 6px",
              borderRadius: 6,
              border: "1px solid #e5e7eb",
            }}
          />
          <input
            title="分类包含"
            value={searchFilters.category}
            onChange={(e) =>
              setSearchFilters((prev) => ({
                ...prev,
                category: e.target.value,
              }))
            }
            placeholder="分类包含"
            style={{
              minWidth: 160,
              padding: "4px 6px",
              borderRadius: 6,
              border: "1px solid #e5e7eb",
            }}
          />
          <input
            title="投资评级包含"
            value={searchFilters.rating}
            onChange={(e) =>
              setSearchFilters((prev) => ({
                ...prev,
                rating: e.target.value,
              }))
            }
            placeholder="投资评级包含"
            style={{
              minWidth: 160,
              padding: "4px 6px",
              borderRadius: 6,
              border: "1px solid #e5e7eb",
            }}
          />
        </div>

        {/* Entry Rank 条件 */}
        <div style={{ marginBottom: 8 }}>
          <div style={{ marginBottom: 4, fontWeight: 500 }}>加入时Rank评分</div>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <select
              title="Rank比较符号"
              value={searchFilters.entry_rank.op}
              onChange={(e) =>
                setSearchFilters((prev) => ({
                  ...prev,
                  entry_rank: {
                    ...prev.entry_rank,
                    op: e.target.value as NumericFilter["op"],
                  },
                }))
              }
              style={{
                padding: "4px 6px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
              }}
            >
              <option value=">=">&gt;=</option>
              <option value="<=">&lt;=</option>
              <option value=">">&gt;</option>
              <option value="<">&lt;</option>
              <option value="=">=</option>
            </select>
            <input
              title="Rank阈值"
              type="number"
              value={searchFilters.entry_rank.value}
              onChange={(e) =>
                setSearchFilters((prev) => ({
                  ...prev,
                  entry_rank: {
                    ...prev.entry_rank,
                    value: Number(e.target.value || "0"),
                  },
                }))
              }
              placeholder="数值"
              style={{
                width: 100,
                padding: "4px 6px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
              }}
            />
            <label style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <input
                type="checkbox"
                checked={searchFilters.entry_rank.enabled}
                onChange={(e) =>
                  setSearchFilters((prev) => ({
                    ...prev,
                    entry_rank: {
                      ...prev.entry_rank,
                      enabled: e.target.checked,
                    },
                  }))
                }
              />
              启用
            </label>
          </div>
        </div>

        {/* 数值条件 */}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
            gap: 8,
            marginBottom: 8,
          }}
        >
          {([
            ["last", "最新价"],
            ["pct_change", "涨幅%"],
            ["open", "开盘"],
            ["prev_close", "昨收"],
            ["high", "最高"],
            ["low", "最低"],
            ["volume_hand", "成交量(手)"],
            ["amount", "成交额"],
          ] as const).map(([key, label]) => {
            const nf = searchFilters.num[key];
            return (
              <div key={key}>
                <div style={{ marginBottom: 2 }}>{label}</div>
                <div
                  style={{
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 4,
                    alignItems: "center",
                  }}
                >
                  <select
                    title={`${label}比较符号`}
                    value={nf.op}
                    onChange={(e) =>
                      setSearchFilters((prev) => ({
                        ...prev,
                        num: {
                          ...prev.num,
                          [key]: {
                            ...prev.num[key],
                            op: e.target.value as NumericFilter["op"],
                          },
                        },
                      }))
                    }
                    style={{
                      padding: "2px 4px",
                      borderRadius: 4,
                      border: "1px solid #e5e7eb",
                    }}
                  >
                    <option value=">=">&gt;=</option>
                    <option value="<=">&lt;=</option>
                    <option value=">">&gt;</option>
                    <option value="<">&lt;</option>
                    <option value="=">=</option>
                  </select>
                  <input
                    title={`${label}阈值`}
                    type="number"
                    value={nf.value}
                    onChange={(e) =>
                      setSearchFilters((prev) => ({
                        ...prev,
                        num: {
                          ...prev.num,
                          [key]: {
                            ...prev.num[key],
                            value: Number(e.target.value || "0"),
                          },
                        },
                      }))
                    }
                    style={{
                      flex: 1,
                      minWidth: 80,
                      maxWidth: 120,
                      padding: "2px 4px",
                      borderRadius: 4,
                      border: "1px solid #e5e7eb",
                    }}
                  />
                  <label
                    style={{ display: "flex", alignItems: "center", gap: 2 }}
                  >
                    <input
                      type="checkbox"
                      checked={nf.enabled}
                      onChange={(e) =>
                        setSearchFilters((prev) => ({
                          ...prev,
                          num: {
                            ...prev.num,
                            [key]: {
                              ...prev.num[key],
                              enabled: e.target.checked,
                            },
                          },
                        }))
                      }
                    />
                    启用
                  </label>
                </div>
              </div>
            );
          })}
        </div>

        {/* 日期条件 */}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
            gap: 8,
            marginBottom: 8,
          }}
        >
          {([
            ["created_at", "加入时间"],
            ["last_analysis_time", "分析时间"],
          ] as const).map(([key, label]) => {
            const df = searchFilters.date[key];
            return (
              <div key={key}>
                <div style={{ marginBottom: 2 }}>{label}</div>
                <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                  <select
                    title={`${label}比较符号`}
                    value={df.op}
                    onChange={(e) =>
                      setSearchFilters((prev) => ({
                        ...prev,
                        date: {
                          ...prev.date,
                          [key]: {
                            ...prev.date[key],
                            op: e.target.value as DateFilter["op"],
                          },
                        },
                      }))
                    }
                    style={{
                      padding: "2px 4px",
                      borderRadius: 4,
                      border: "1px solid #e5e7eb",
                    }}
                  >
                    <option value=">=">&gt;=</option>
                    <option value="<=">&lt;=</option>
                    <option value=">">&gt;</option>
                    <option value="<">&lt;</option>
                    <option value="=">=</option>
                  </select>
                  <input
                    title={`${label}日期`}
                    type="date"
                    value={df.value}
                    onChange={(e) =>
                      setSearchFilters((prev) => ({
                        ...prev,
                        date: {
                          ...prev.date,
                          [key]: {
                            ...prev.date[key],
                            value: e.target.value,
                          },
                        },
                      }))
                    }
                    style={{
                      flex: 1,
                      minWidth: 120,
                      maxWidth: 160,
                      padding: "2px 4px",
                      borderRadius: 4,
                      border: "1px solid #e5e7eb",
                    }}
                  />
                  <label
                    style={{ display: "flex", alignItems: "center", gap: 2 }}
                  >
                    <input
                      type="checkbox"
                      checked={df.enabled}
                      onChange={(e) =>
                        setSearchFilters((prev) => ({
                          ...prev,
                          date: {
                            ...prev.date,
                            [key]: {
                              ...prev.date[key],
                              enabled: e.target.checked,
                            },
                          },
                        }))
                      }
                    />
                    启用
                  </label>
                </div>
              </div>
            );
          })}
        </div>

        <div style={{ display: "flex", gap: 8 }}>
          <button
            type="button"
            onClick={() => {
              setSearchActive(true);
              setPage(1);
              loadAllAndFilter();
            }}
            style={{
              padding: "4px 10px",
              borderRadius: 999,
              border: "1px solid #3b82f6",
              background: "#dbeafe",
              fontSize: 12,
            }}
          >
            执行搜索
          </button>
          <button
            type="button"
            onClick={() => {
              setSearchActive(false);
              setSearchFilters({
                code: "",
                name: "",
                category: "",
                rating: "",
                entry_rank: { ...DEFAULT_NUMERIC_FILTER },
                num: {
                  last: { ...DEFAULT_NUMERIC_FILTER },
                  pct_change: { ...DEFAULT_NUMERIC_FILTER },
                  open: { ...DEFAULT_NUMERIC_FILTER },
                  prev_close: { ...DEFAULT_NUMERIC_FILTER },
                  high: { ...DEFAULT_NUMERIC_FILTER },
                  low: { ...DEFAULT_NUMERIC_FILTER },
                  volume_hand: { ...DEFAULT_NUMERIC_FILTER },
                  amount: { ...DEFAULT_NUMERIC_FILTER },
                },
                date: {
                  created_at: { ...DEFAULT_DATE_FILTER },
                  last_analysis_time: { ...DEFAULT_DATE_FILTER },
                },
              });
              setPage(1);
              loadPageItems();
            }}
            style={{
              padding: "4px 10px",
              borderRadius: 999,
              border: "1px solid #e5e7eb",
              background: "#f9fafb",
              fontSize: 12,
            }}
          >
            清空搜索
          </button>
        </div>
      </CollapsibleCard>

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
            data-testid="watchlist-items-table"
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
                      if (el && someSelected && !allSelected) {
                        el.indeterminate = true;
                      }
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
                  style={{ padding: 6, textAlign: "right", cursor: "pointer" }}
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
                <th style={{ padding: 6, textAlign: "left" }}>入池基准日</th>
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
                  style={{ padding: 6, textAlign: "left", cursor: "pointer" }}
                  onClick={() => toggleSort("last_rating")}
                >
                  投资评级 {sortBy === "last_rating" && (sortDir === "asc" ? "↑" : "↓")}
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
                <th style={{ padding: 6 }}>历史</th>
                <th style={{ padding: 6 }}>分析</th>
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
                    data-testid={`watchlist-row-${row.code}`}
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
                    <td data-testid={`watchlist-cell-rank-${row.code}`} style={{ padding: 6, textAlign: "right", color: "#6b7280" }}>
                      {row.entry_rank != null ? String(row.entry_rank) : "-"}
                    </td>
                    <td data-testid={`watchlist-cell-entry-price-${row.code}`} style={{ padding: 6, textAlign: "right", color: "#6b7280" }}>
                      {row.entry_price != null ? row.entry_price.toFixed(3) : "-"}
                    </td>
                    <td data-testid={`watchlist-cell-entry-as-of-${row.code}`} style={{ padding: 6, color: "#6b7280" }}>
                      {row.entry_as_of ? formatDate(row.entry_as_of) : "-"}
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
                    <td style={{ padding: 6 }}>{row.last_rating || "N/A"}</td>
                    <td style={{ padding: 6 }}>{joinDate}</td>
                    <td style={{ padding: 6 }}>
                      {row.last_analysis_time
                        ? formatDateTime(row.last_analysis_time)
                        : "N/A"}
                    </td>
                    <td style={{ padding: 6, textAlign: "center" }}>
                      <button
                        type="button"
                        onClick={() => handleJumpHistory(row)}
                        style={{
                          padding: "2px 8px",
                          borderRadius: 6,
                          border: "1px solid #cbd5e1",
                          background: "#f1f5f9",
                          fontSize: 11,
                          cursor: "pointer",
                        }}
                      >
                        历史
                      </button>
                    </td>
                    <td style={{ padding: 6, textAlign: "center" }}>
                      <button
                        type="button"
                        onClick={() => handleJumpAnalyze(row)}
                        style={{
                          padding: "2px 8px",
                          borderRadius: 6,
                          border: "1px solid #c4b5fd",
                          background: "#ede9fe",
                          fontSize: 11,
                          cursor: "pointer",
                        }}
                      >
                        分析
                      </button>
                    </td>
                  </tr>
                );
              })}
              {items.length === 0 && !loading && (
                <tr>
                  <td colSpan={23} style={{ padding: 10, textAlign: "center" }}>
                    暂无自选股票。
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* 批量操作 */}
        <section
          style={{
            marginTop: 12,
            paddingTop: 8,
            borderTop: "1px solid #e5e7eb",
            fontSize: 13,
          }}
        >
          <div style={{ marginBottom: 4, fontWeight: 600 }}>批量操作</div>
          <div style={{ marginBottom: 4, fontSize: 12, color: "#6b7280" }}>
            当前已选中 {selectedIds.length} 条
          </div>
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: 8,
              alignItems: "center",
              marginBottom: 8,
            }}
          >
            <select
              title="批量操作类型"
              value={bulkOpType}
              onChange={(e) =>
                setBulkOpType(e.target.value as typeof bulkOpType)
              }
              style={{
                minWidth: 140,
                padding: "4px 6px",
                borderRadius: 6,
                border: "1px solid #e5e7eb",
              }}
            >
              <option value="新增">新增</option>
              <option value="修改分类">修改分类</option>
              <option value="添加到分类">添加到分类</option>
              <option value="从分类移除">从分类移除</option>
              <option value="删除">删除</option>
              <option value="批量分析">批量分析</option>
            </select>

            {bulkOpType === "修改分类" && (
              <select
                title="目标分类"
                value={bulkTargetCatName}
                onChange={(e) => setBulkTargetCatName(e.target.value)}
                style={{
                  minWidth: 160,
                  padding: "4px 6px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                }}
              >
                <option value="">选择分类(替换)</option>
                {categories.map((c) => (
                  <option key={c.id} value={c.name}>
                    {c.name}
                  </option>
                ))}
              </select>
            )}

            {bulkOpType === "添加到分类" && (
              <select
                title="添加到分类"
                multiple
                value={bulkAddCatNames}
                onChange={(e) => {
                  const opts = Array.from(e.target.selectedOptions).map(
                    (o) => o.value,
                  );
                  setBulkAddCatNames(opts);
                }}
                style={{
                  minWidth: 180,
                  padding: "4px 6px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                }}
              >
                {categories.map((c) => (
                  <option key={c.id} value={c.name}>
                    {c.name}
                  </option>
                ))}
              </select>
            )}

            {bulkOpType === "从分类移除" && (
              <select
                title="从分类移除"
                multiple
                value={bulkRemoveCatNames}
                onChange={(e) => {
                  const opts = Array.from(e.target.selectedOptions).map(
                    (o) => o.value,
                  );
                  setBulkRemoveCatNames(opts);
                }}
                style={{
                  minWidth: 180,
                  padding: "4px 6px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                }}
              >
                {categories.map((c) => (
                  <option key={c.id} value={c.name}>
                    {c.name}
                  </option>
                ))}
              </select>
            )}

            {bulkOpType === "新增" && (
              <input
                title="批量新增股票代码"
                value={bulkAddCodes}
                onChange={(e) => setBulkAddCodes(e.target.value)}
                placeholder="股票代码，逗号或换行分隔"
                style={{
                  minWidth: 220,
                  padding: "4px 6px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                }}
              />
            )}

            <button
              type="button"
              onClick={handleBulkExecute}
              style={{
                padding: "4px 10px",
                borderRadius: 999,
                border: "1px solid #0f766e",
                background: "#ccfbf1",
                fontSize: 12,
              }}
            >
              执行
            </button>
          </div>
        </section>

        {/* 分页器 */}
        <div
          style={{
            marginTop: 8,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <button
            type="button"
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
            style={{
              padding: "4px 10px",
              borderRadius: 8,
              border: "1px solid #cbd5e1",
              background: page <= 1 ? "#e5e7eb" : "#f8fafc",
              fontSize: 12,
            }}
          >
            上一页
          </button>
          <span style={{ fontSize: 12 }}>
            第 {page} / {totalPages} 页 （共 {total} 条）
          </span>
          <button
            type="button"
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages}
            style={{
              padding: "4px 10px",
              borderRadius: 8,
              border: "1px solid #cbd5e1",
              background: page >= totalPages ? "#e5e7eb" : "#f8fafc",
              fontSize: 12,
            }}
          >
            下一页
          </button>
        </div>
      </section>
    </main>
  );
}
