"use client";

import type React from "react";
import { useState, useEffect, useCallback } from "react";

const BACKEND_BASE =
  process.env.NEXT_PUBLIC_TDX_BACKEND_BASE || "http://127.0.0.1:8001";

function buildBackendUrl(path: string) {
  return `${BACKEND_BASE.replace(/\/$/, "")}${path}`;
}

async function backendRequest<T = any>(
  method: string,
  path: string,
  body?: any,
): Promise<T> {
  const url = `${BACKEND_BASE.replace(/\/$/, "")}${path}`;
  const res = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(
      `请求失败: HTTP ${res.status} ${res.statusText}${text ? ` | ${text}` : ""}`,
    );
  }
  const text = await res.text();
  if (!text) return {} as T;
  return JSON.parse(text) as T;
}

interface ExportResponse {
  snapshot_id: string;
  freq: string;
  start: string;
  end: string;
  ts_codes?: string[];
  board_codes?: string[];
  rows: number;
}

interface SnapshotInfo {
  snapshot_id: string;
  path: string;
  has_daily: boolean;
  has_minute: boolean;
  has_board: boolean;
  has_board_index: boolean;
  has_board_member: boolean;
  has_factor_data: boolean;
  has_moneyflow: boolean;
  has_daily_basic: boolean;
  meta: any;
  created_at: string | null;
}

interface SnapshotListResponse {
  snapshots: SnapshotInfo[];
  total: number;
}

interface BinExportInfo {
  snapshot_id: string;
  bin_dir: string;
  created_at: string | null;
  modified_at: string | null;
  start?: string | null;
  end?: string | null;
  exchanges?: string[] | null;
  exclude_st?: boolean | null;
  exclude_delisted_or_paused?: boolean | null;
  freq_types?: string[] | null;
}

interface BinExportListResponse {
  items: BinExportInfo[];
  total: number;
}

type ExportType =
  | "daily"
  | "minute"
  | "board"
  | "board_index"
  | "board_member"
  | "factor"
  | "moneyflow"
  | "daily_basic";
type ExportMode = "full" | "incremental";
type ExportTab = "snapshot" | "bin";
type BinTab = "stock" | "index";

// 导出进度状态
type ExportStatus = "idle" | "preparing" | "loading" | "writing" | "done" | "error";

// 支持增量导出的类型
const INCREMENTAL_TYPES: ExportType[] = ["minute", "board", "board_index", "board_member", "factor"];

// 数据检查响应
interface DataCheckResponse {
  total_stocks: number;
  date_range: string[];
  trading_days: number;
  data_coverage: number;
  adj_factor_coverage: number;
  sample_data: Array<{
    datetime: string;
    instrument: string;
    "$close": number | null;
    "$factor": number | null;
    "$volume": number | null;
  }>;
  issues: string[];
}

// 数据预览响应
interface DataPreviewResponse {
  code: string;
  count: number;
  data: Array<Record<string, any>>;
}

interface FieldMapExportResponse {
  snapshot_id: string;
  csv_path: string;
  rows: number;
  written_h5: Record<string, number>;
  has_daily_basic: boolean;
  has_moneyflow: boolean;
}

interface IndexMarketInfo {
  market: string;
}

interface IndexMarketListResponse {
  items: IndexMarketInfo[];
  total: number;
}

interface IndexInfo {
  ts_code: string;
  name?: string | null;
  fullname?: string | null;
  market?: string | null;
}

interface IndexListResponse {
  items: IndexInfo[];
  total: number;
}

interface IndexBinExportResponse {
  snapshot_id: string;
  index_code: string;
  csv_dir: string;
  bin_dir: string;
  dump_bin_ok: boolean;
  check_ok: boolean | null;
  stdout_dump: string | null;
  stderr_dump: string | null;
  stdout_check: string | null;
  stderr_check: string | null;
}

interface IndexHealthCheckResponse {
  snapshot_id: string;
  bin_dir: string;
  has_index_file: boolean;
  index_count: number;
  check_ok: boolean | null;
  stdout_check: string | null;
  stderr_check: string | null;
}

export default function QlibPage() {
  // Snapshot 列表
  const [snapshots, setSnapshots] = useState<SnapshotInfo[]>([]);
  const [loadingList, setLoadingList] = useState(false);

  // 导出表单
  const [exportType, setExportType] = useState<ExportType>("daily");
  const [exportMode, setExportMode] = useState<ExportMode>("full");
  const [snapshotId, setSnapshotId] = useState<string>("qlib_export_" + new Date().toISOString().slice(0, 10).replace(/-/g, ""));
  const [exSh, setExSh] = useState<boolean>(true);
  const [exSz, setExSz] = useState<boolean>(true);
  const [exBj, setExBj] = useState<boolean>(true);
  const [excludeSt, setExcludeSt] = useState<boolean>(true);
  const [excludeDelistedOrPaused, setExcludeDelistedOrPaused] = useState<boolean>(true);
  const [start, setStart] = useState<string>("2025-11-01");
  const [end, setEnd] = useState<string>("2025-12-01");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ExportResponse | null>(null);

  // 导出进度
  const [exportStatus, setExportStatus] = useState<ExportStatus>("idle");
  const [exportProgress, setExportProgress] = useState(0);

  // 详情弹窗
  const [detailSnapshot, setDetailSnapshot] = useState<SnapshotInfo | null>(null);

  // 数据检查
  const [checkLoading, setCheckLoading] = useState(false);
  const [checkResult, setCheckResult] = useState<DataCheckResponse | null>(null);
  const [checkError, setCheckError] = useState<string | null>(null);
  const [previewCode, setPreviewCode] = useState<string>("601919.SH");
  const [previewResult, setPreviewResult] = useState<DataPreviewResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  // 字段说明（CSV + 写入 H5 attrs）
  const [fieldMapLoading, setFieldMapLoading] = useState(false);
  const [fieldMapError, setFieldMapError] = useState<string | null>(null);
  const [fieldMapResult, setFieldMapResult] = useState<FieldMapExportResponse | null>(null);

  // Qlib bin 导出（CSV -> bin）相关状态
  const [binSnapshotId, setBinSnapshotId] = useState<string>(
    "qlib_bin_" + new Date().toISOString().slice(0, 10).replace(/-/g, ""),
  );
  const [binStart, setBinStart] = useState<string>(start);
  const [binEnd, setBinEnd] = useState<string>(end);
  const [binFreq, setBinFreq] = useState<"day" | "1m">("day");
  const [binRunHealthCheck, setBinRunHealthCheck] = useState<boolean>(true);
  const [binLoading, setBinLoading] = useState<boolean>(false);
  const [binError, setBinError] = useState<string | null>(null);
  const [binResult, setBinResult] = useState<{
    snapshot_id: string;
    csv_dir: string;
    bin_dir: string;
    dump_bin_ok: boolean;
    check_ok: boolean | null;
    stdout_dump: string;
    stderr_dump: string;
    stdout_check?: string | null;
    stderr_check?: string | null;
  } | null>(null);

  // 指数导出相关状态
  const [indexMarkets, setIndexMarkets] = useState<IndexMarketInfo[]>([]);
  const [indexMarketsLoaded, setIndexMarketsLoaded] = useState(false);
  const [indexMarketsError, setIndexMarketsError] = useState<string | null>(null);
  const [selectedIndexMarkets, setSelectedIndexMarkets] = useState<string[]>([]);

  const [indices, setIndices] = useState<IndexInfo[]>([]);
  const [indicesLoading, setIndicesLoading] = useState(false);
  const [indicesError, setIndicesError] = useState<string | null>(null);
  const [selectedIndexCode, setSelectedIndexCode] = useState<string>("");

  const [indexStart, setIndexStart] = useState<string>(binStart);
  const [indexEnd, setIndexEnd] = useState<string>(binEnd);
  const [indexDataSource, setIndexDataSource] = useState<"tushare" | "tdx">("tushare");
  const [indexRunHealthCheck, setIndexRunHealthCheck] = useState<boolean>(true);
  const [indexLoading, setIndexLoading] = useState<boolean>(false);
  const [indexError, setIndexError] = useState<string | null>(null);
  const [indexResult, setIndexResult] = useState<IndexBinExportResponse | null>(null);
  const [indexShowDumpLog, setIndexShowDumpLog] = useState<boolean>(false);
  const [indexShowCheckLog, setIndexShowCheckLog] = useState<boolean>(false);

  const [indexHealthLoading, setIndexHealthLoading] = useState<boolean>(false);
  const [indexHealthError, setIndexHealthError] = useState<string | null>(null);
  const [indexHealthResult, setIndexHealthResult] = useState<IndexHealthCheckResponse | null>(null);
  const [showDumpLog, setShowDumpLog] = useState<boolean>(false);
  const [showCheckLog, setShowCheckLog] = useState<boolean>(false);

  // Qlib bin 导出列表
  const [binExports, setBinExports] = useState<BinExportInfo[]>([]);
  const [binExportsLoading, setBinExportsLoading] = useState(false);
  const [binExportsError, setBinExportsError] = useState<string | null>(null);

  // 导出区域标签页：HDF5 Snapshot vs Qlib bin
  const [exportTab, setExportTab] = useState<ExportTab>("snapshot");

  // Qlib bin 内部子标签：股票 vs 指数
  const [binTab, setBinTab] = useState<BinTab>("stock");

  // 记录导出时使用的过滤条件
  const [lastExportConfig, setLastExportConfig] = useState<{
    type: ExportType;
    mode: ExportMode;
    exchanges?: string[];
    start: string;
    end: string;
  } | null>(null);

  // 当导出类型改变时，如果不支持增量则重置为全量
  const handleExportTypeChange = (type: ExportType) => {
    setExportType(type);
    // moneyflow 和日线/板块全量导出不支持增量
    if (!INCREMENTAL_TYPES.includes(type)) {
      setExportMode("full");
    }
  };

  const handleExportFieldMapForSnapshot = async (sid: string) => {
    const id = (sid || "").trim();
    if (!id) return;

    try {
      const resp = await backendRequest<FieldMapExportResponse>(
        "POST",
        "/api/v1/qlib/field_map/export",
        {
          snapshot_id: id,
          write_to_h5: true,
        },
      );
      alert(`字段说明已生成\nCSV: ${resp.csv_path}\nrows: ${resp.rows}`);
    } catch (e: any) {
      alert(e?.message || "生成字段说明失败");
    }
  };

  const handleExportFieldMap = async () => {
    setFieldMapLoading(true);
    setFieldMapError(null);
    setFieldMapResult(null);

    try {
      const resp = await backendRequest<FieldMapExportResponse>(
        "POST",
        "/api/v1/qlib/field_map/export",
        {
          snapshot_id: snapshotId.trim(),
          write_to_h5: true,
        },
      );
      setFieldMapResult(resp);
    } catch (e: any) {
      setFieldMapError(e?.message || "生成字段说明失败");
    } finally {
      setFieldMapLoading(false);
    }
  };

  const formatDateTimeShanghai = (value?: string | null) => {
    if (!value) return "—";
    try {
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) return value;
      return d.toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      }).replace(/\//g, "-");
    } catch {
      return value;
    }
  };

  const logBox: React.CSSProperties = {
    fontFamily: "Menlo, Monaco, Consolas, 'Courier New', monospace",
    fontSize: 12,
    whiteSpace: "pre-wrap",
    background: "#0f172a",
    color: "#e5e7eb",
    padding: 12,
    borderRadius: 8,
    maxHeight: 260,
    overflow: "auto",
  };

  // 加载 Snapshot 列表
  const loadSnapshots = useCallback(async () => {
    setLoadingList(true);
    try {
      const resp = await backendRequest<SnapshotListResponse>("GET", "/api/v1/qlib/snapshots");
      setSnapshots(resp.snapshots || []);
    } catch (e: any) {
      console.error("加载 Snapshot 列表失败:", e);
    } finally {
      setLoadingList(false);
    }
  }, []);

  useEffect(() => {
    loadSnapshots();
  }, [loadSnapshots]);

  const loadBinExports = useCallback(async () => {
    setBinExportsLoading(true);
    setBinExportsError(null);
    try {
      const resp = await backendRequest<BinExportListResponse>("GET", "/api/v1/qlib/bin/exports");
      setBinExports(resp.items || []);
    } catch (e: any) {
      setBinExportsError(e?.message || "加载 Qlib bin 列表失败");
    } finally {
      setBinExportsLoading(false);
    }
  }, []);

  // 在切换到 bin 标签时加载 bin 列表
  useEffect(() => {
    if (exportTab === "bin") {
      loadBinExports();
    }
  }, [exportTab, loadBinExports]);

  // 在进入 "指数 bin 导出" 子标签时加载指数市场列表
  useEffect(() => {
    if (exportTab === "bin" && binTab === "index" && indexDataSource !== "tdx" && !indexMarketsLoaded) {
      (async () => {
        try {
          const resp = await backendRequest<IndexMarketListResponse>(
            "GET",
            "/api/v1/qlib/index/markets",
          );
          setIndexMarkets(resp.items || []);
          setIndexMarketsLoaded(true);
        } catch (e: any) {
          setIndexMarketsError(e?.message || "加载指数市场列表失败");
        }
      })();
    }
  }, [exportTab, binTab, indexMarketsLoaded, indexDataSource]);

  // 当选中的指数 market 变化时加载指数列表
  useEffect(() => {
    if (exportTab !== "bin" || binTab !== "index") return;
    if (indexDataSource === "tdx") return;
    if (selectedIndexMarkets.length === 0) {
      setIndices([]);
      setSelectedIndexCode("");
      return;
    }

    (async () => {
      setIndicesLoading(true);
      setIndicesError(null);
      try {
        const marketsParam = selectedIndexMarkets.join(",");
        const resp = await backendRequest<IndexListResponse>(
          "GET",
          `/api/v1/qlib/index/list?markets=${encodeURIComponent(marketsParam)}`,
        );
        setIndices(resp.items || []);
        if (resp.items && resp.items.length > 0) {
          setSelectedIndexCode(resp.items[0].ts_code);
        } else {
          setSelectedIndexCode("");
        }
      } catch (e: any) {
        setIndicesError(e?.message || "加载指数列表失败");
      } finally {
        setIndicesLoading(false);
      }
    })();
  }, [exportTab, binTab, selectedIndexMarkets, indexDataSource]);

  // 选择 TDX 数据源时，从后端按实际数据表生成可选指数列表
  useEffect(() => {
    if (exportTab !== "bin" || binTab !== "index") return;
    if (indexDataSource !== "tdx") return;

    (async () => {
      setIndicesLoading(true);
      setIndicesError(null);
      try {
        const resp = await backendRequest<IndexListResponse>(
          "GET",
          "/api/v1/qlib/index/list?data_source=tdx",
        );
        setIndices(resp.items || []);
        if (resp.items && resp.items.length > 0) {
          setSelectedIndexCode(resp.items[0].ts_code);
        } else {
          setSelectedIndexCode("");
        }
        setSelectedIndexMarkets([]);
      } catch (e: any) {
        setIndicesError(e?.message || "加载 TDX 指数列表失败");
        setIndices([]);
        setSelectedIndexCode("");
      } finally {
        setIndicesLoading(false);
      }
    })();
  }, [exportTab, binTab, indexDataSource]);

  // 删除 Snapshot
  const handleDelete = async (id: string) => {
    if (!confirm(`确定要删除 Snapshot "${id}" 吗？此操作不可恢复。`)) return;
    try {
      await backendRequest("DELETE", `/api/v1/qlib/snapshots/${encodeURIComponent(id)}`);
      loadSnapshots();
    } catch (e: any) {
      alert(`删除失败: ${e?.message || "未知错误"}`);
    }
  };

  // 模拟进度更新
  const simulateProgress = () => {
    setExportProgress(0);
    setExportStatus("preparing");
    
    const steps = [
      { status: "preparing" as ExportStatus, progress: 10, delay: 200 },
      { status: "loading" as ExportStatus, progress: 30, delay: 500 },
      { status: "loading" as ExportStatus, progress: 50, delay: 800 },
      { status: "writing" as ExportStatus, progress: 70, delay: 300 },
      { status: "writing" as ExportStatus, progress: 90, delay: 200 },
    ];

    let currentStep = 0;
    const runStep = () => {
      if (currentStep < steps.length) {
        const step = steps[currentStep];
        setExportStatus(step.status);
        setExportProgress(step.progress);
        currentStep++;
        setTimeout(runStep, step.delay);
      }
    };
    runStep();
  };

  // 导出
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setResult(null);
    simulateProgress();

    try {
      const exchanges: string[] = [];
      if (exSh) exchanges.push("sh");
      if (exSz) exchanges.push("sz");
      if (exBj) exchanges.push("bj");

      let endpoint = "";
      let payload: any = { snapshot_id: snapshotId.trim(), end };
      
      // 全量导出需要 start，增量导出不需要
      if (exportMode === "full") {
        payload.start = start;
      }

      // 根据导出类型和模式确定 endpoint
      if (exportType === "daily") {
        // 日频只支持全量
        endpoint = "/api/v1/qlib/snapshots/daily";
        payload.start = start;
        if (exchanges.length > 0) payload.exchanges = exchanges;
        payload.exclude_st = excludeSt;
        payload.exclude_delisted_or_paused = excludeDelistedOrPaused;
      } else if (exportType === "minute") {
        if (exportMode === "incremental") {
          endpoint = "/api/v1/qlib/snapshots/minute/incremental";
        } else {
          endpoint = "/api/v1/qlib/snapshots/minute";
          payload.start = start;
        }
        if (exchanges.length > 0) payload.exchanges = exchanges;
        payload.freq = "1m";
        payload.exclude_st = excludeSt;
        payload.exclude_delisted_or_paused = excludeDelistedOrPaused;
      } else if (exportType === "board") {
        if (exportMode === "incremental") {
          endpoint = "/api/v1/qlib/boards/daily/incremental";
        } else {
          endpoint = "/api/v1/qlib/boards/daily";
          payload.start = start;
        }
      } else if (exportType === "board_index") {
        if (exportMode === "incremental") {
          endpoint = "/api/v1/qlib/boards/index/incremental";
        } else {
          endpoint = "/api/v1/qlib/boards/index";
          payload.start = start;
        }
      } else if (exportType === "board_member") {
        if (exportMode === "incremental") {
          endpoint = "/api/v1/qlib/boards/member/incremental";
        } else {
          endpoint = "/api/v1/qlib/boards/member";
          payload.start = start;
        }
      } else if (exportType === "factor") {
        if (exportMode === "incremental") {
          endpoint = "/api/v1/qlib/factors/incremental";
        } else {
          endpoint = "/api/v1/qlib/factors";
          payload.start = start;
        }
        if (exchanges.length > 0) payload.exchanges = exchanges;
      } else if (exportType === "moneyflow") {
        // 个股资金流向只支持全量导出
        endpoint = "/api/v1/qlib/snapshots/moneyflow";
        payload.start = start;
        if (exchanges.length > 0) payload.exchanges = exchanges;
        payload.exclude_st = excludeSt;
        payload.exclude_delisted_or_paused = excludeDelistedOrPaused;
      } else if (exportType === "daily_basic") {
        // daily_basic 只支持全量导出，逻辑与 moneyflow/daily 一致
        endpoint = "/api/v1/qlib/snapshots/daily_basic";
        payload.start = start;
        if (exchanges.length > 0) payload.exchanges = exchanges;
        payload.exclude_st = excludeSt;
        payload.exclude_delisted_or_paused = excludeDelistedOrPaused;
      }

      // 记录导出配置
      setLastExportConfig({
        type: exportType,
        mode: exportMode,
        exchanges: (exportType === "daily" || exportType === "minute" || exportType === "factor" || exportType === "moneyflow" || exportType === "daily_basic")
          ? exchanges
          : undefined,
        start,
        end,
      });

      const resp = await backendRequest<ExportResponse>("POST", endpoint, payload);
      setResult(resp);
      setExportStatus("done");
      setExportProgress(100);
      loadSnapshots();
    } catch (e: any) {
      setError(e?.message || "导出失败");
      setExportStatus("error");
    } finally {
      setLoading(false);
    }
  };

  // 获取导出类型显示名称
  const getExportTypeName = (type: ExportType) => {
    const names: Record<ExportType, string> = {
      daily: "日频行情",
      minute: "分钟线",
      board: "板块日线",
      board_index: "板块索引",
      board_member: "板块成员",
      factor: "RD-Agent因子",
      moneyflow: "个股资金流向 (moneyflow.h5)",
      daily_basic: "每日指标 (daily_basic.h5)",
    };
    return names[type];
  };

  // 获取交易所显示名称
  const getExchangeNames = (exchanges: string[]) => {
    const names: Record<string, string> = {
      sh: "上交所",
      sz: "深交所",
      bj: "北交所",
    };
    return exchanges.map(e => names[e] || e).join("、");
  };

  // 获取进度状态文字
  const getProgressText = () => {
    switch (exportStatus) {
      case "preparing": return "准备中...";
      case "loading": return "读取数据...";
      case "writing": return "写入文件...";
      case "done": return "完成";
      case "error": return "失败";
      default: return "";
    }
  };

  // 数据检查
  const handleDataCheck = async () => {
    setCheckLoading(true);
    setCheckError(null);
    setCheckResult(null);
    
    try {
      const exchanges: string[] = [];
      if (exSh) exchanges.push("sh");
      if (exSz) exchanges.push("sz");
      if (exBj) exchanges.push("bj");
      
      const resp = await backendRequest<DataCheckResponse>("POST", "/api/v1/qlib/data/check", {
        start,
        end,
        exchanges: exchanges.length > 0 ? exchanges : undefined,
        check_adj_factor: true,
        sample_size: 5,
      });
      setCheckResult(resp);
    } catch (e: any) {
      setCheckError(e?.message || "检查失败");
    } finally {
      setCheckLoading(false);
    }
  };

  // 数据预览
  const handlePreview = async () => {
    if (!previewCode.trim()) return;
    setPreviewLoading(true);
    setPreviewResult(null);
    
    try {
      const resp = await backendRequest<DataPreviewResponse>(
        "GET",
        `/api/v1/qlib/data/preview?ts_code=${encodeURIComponent(previewCode)}&start=${start}&end=${end}&limit=10`
      );
      setPreviewResult(resp);
    } catch (e: any) {
      alert(`预览失败: ${e?.message || "未知错误"}`);
    } finally {
      setPreviewLoading(false);
    }
  };

  const cardStyle: React.CSSProperties = {
    padding: 16,
    borderRadius: 12,
    background: "#fff",
    boxShadow: "0 2px 8px rgba(0,0,0,0.06)",
    marginBottom: 16,
  };

  const inputStyle: React.CSSProperties = {
    width: "100%",
    padding: "8px 10px",
    borderRadius: 8,
    border: "1px solid #d4d4d4",
    fontSize: 14,
  };

  const btnPrimary: React.CSSProperties = {
    padding: "8px 16px",
    borderRadius: 8,
    border: "none",
    background: "#0ea5e9",
    color: "#fff",
    cursor: "pointer",
    fontSize: 14,
  };

  const btnSecondary: React.CSSProperties = {
    padding: "4px 10px",
    borderRadius: 6,
    border: "1px solid #d4d4d4",
    background: "#fff",
    color: "#374151",
    cursor: "pointer",
    fontSize: 12,
  };

  const btnDanger: React.CSSProperties = {
    padding: "4px 10px",
    borderRadius: 6,
    border: "none",
    background: "#ef4444",
    color: "#fff",
    cursor: "pointer",
    fontSize: 12,
  };

  const modalOverlay: React.CSSProperties = {
    position: "fixed",
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: "rgba(0,0,0,0.5)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    zIndex: 1000,
  };

  const modalContent: React.CSSProperties = {
    background: "#fff",
    borderRadius: 12,
    padding: 24,
    maxWidth: 600,
    width: "90%",
    maxHeight: "80vh",
    overflow: "auto",
  };

  const tabButtonBase: React.CSSProperties = {
    padding: "6px 10px",
    borderRadius: 999,
    border: "none",
    background: "transparent",
    color: "#374151",
    cursor: "pointer",
    fontSize: 13,
  };

  const tabButtonActive: React.CSSProperties = {
    ...tabButtonBase,
    background: "#0f766e",
    color: "#fff",
  };

  return (
    <main className="p-6 max-w-5xl mx-auto">
      <h1 className="text-2xl font-bold mb-1">Qlib Snapshot 管理</h1>
      <p className="text-sm text-gray-500 mb-6">
        从本地 TimescaleDB 导出数据到 Qlib Snapshot，供 RD-Agent / Qlib 回测使用。
      </p>

      {/* 导出（HDF5 Snapshot / Qlib bin）Tab 区域 */}
      <section style={cardStyle}>
        <h2 className="text-lg font-semibold mb-1">Qlib 数据导出</h2>
        <p className="text-sm text-gray-500 mb-4">
          在同一页面下，通过标签切换管理 HDF5 Snapshot 与 Qlib bin（CSV→bin）导出配置。
        </p>

        {/* Tab 切换：HDF5 Snapshot / Qlib bin（风格对齐本地数据管理页） */}
        <div className="mb-4 flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => setExportTab("snapshot")}
            style={exportTab === "snapshot" ? tabButtonActive : tabButtonBase}
          >
            HDF5 Snapshot 导出
          </button>
          <button
            type="button"
            onClick={() => setExportTab("bin")}
            style={exportTab === "bin" ? tabButtonActive : tabButtonBase}
          >
            Qlib bin 导出（CSV→bin）
          </button>
        </div>

        {exportTab === "snapshot" && (
          <form onSubmit={handleSubmit} className="space-y-4">
          {/* 导出数据集选择 */}
          <div>
            <label className="block text-sm font-medium mb-2">导出数据集</label>
            <select
              value={exportType}
              onChange={(e) => handleExportTypeChange(e.target.value as ExportType)}
              style={inputStyle}
            >
              <option value="daily">日频行情</option>
              <option value="minute">分钟线</option>
              <option value="board">板块日线</option>
              <option value="board_index">板块索引</option>
              <option value="board_member">板块成员</option>
              <option value="factor">RD-Agent因子</option>
              <option value="moneyflow">个股资金流向 (moneyflow.h5)</option>
              <option value="daily_basic">每日指标 (daily_basic.h5)</option>
            </select>
          </div>

          {/* 导出模式（仅支持增量的类型显示，moneyflow 一律全量） */}
          {INCREMENTAL_TYPES.includes(exportType) && exportType !== "moneyflow" && (
            <div>
              <label className="block text-sm font-medium mb-2">导出模式</label>
              <div className="flex gap-4">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="radio"
                    name="exportMode"
                    checked={exportMode === "full"}
                    onChange={() => setExportMode("full")}
                  />
                  <span className="text-sm">全量导出</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="radio"
                    name="exportMode"
                    checked={exportMode === "incremental"}
                    onChange={() => setExportMode("incremental")}
                  />
                  <span className="text-sm">增量导出</span>
                  <span className="text-xs text-gray-400">（从上次位置继续）</span>
                </label>
              </div>
            </div>
          )}

          {/* 日线 / 分钟线 / 资金流向 / 每日指标 专用：样本过滤（ST / 退市 / 暂停上市） */}
          {(exportType === "daily" || exportType === "minute" || exportType === "moneyflow" || exportType === "daily_basic") && (
            <div>
              <label className="block text-sm font-medium mb-2">样本过滤</label>
              <div className="flex flex-col gap-1 text-sm text-gray-700">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={excludeSt}
                    onChange={(e) => setExcludeSt(e.target.checked)}
                  />
                  <span>排除所有有过 ST 记录的股票（包括当前 ST）</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={excludeDelistedOrPaused}
                    onChange={(e) => setExcludeDelistedOrPaused(e.target.checked)}
                  />
                  <span>排除退市 / 当前暂停上市的股票</span>
                </label>
              </div>
            </div>
          )}

          {/* Snapshot ID */}
          <div>
            <label className="block text-sm font-medium mb-1">Snapshot ID</label>
            <input
              value={snapshotId}
              onChange={(e) => setSnapshotId(e.target.value)}
              style={inputStyle}
              placeholder="例如：qlib_daily_2025Q1_all"
            />
            <p className="text-xs text-gray-500 mt-1">
              将作为 qlib_snapshots/&lt;Snapshot ID&gt;/ 目录名
              {exportMode === "incremental" && "（增量导出需使用已存在的 Snapshot ID）"}
            </p>
          </div>

          {/* 交易所（日频、分钟线、因子数据、资金流向） */}
          {(exportType === "daily" || exportType === "minute" || exportType === "factor" || exportType === "moneyflow") && (
            <div>
              <label className="block text-sm font-medium mb-2">交易所范围</label>
              <div className="flex gap-4">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input type="checkbox" checked={exSh} onChange={(e) => setExSh(e.target.checked)} />
                  <span className="text-sm">上交所 (SH)</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input type="checkbox" checked={exSz} onChange={(e) => setExSz(e.target.checked)} />
                  <span className="text-sm">深交所 (SZ)</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input type="checkbox" checked={exBj} onChange={(e) => setExBj(e.target.checked)} />
                  <span className="text-sm">北交所 (BJ)</span>
                </label>
              </div>
            </div>
          )}

          {/* 日期范围 */}
          <div className={exportMode === "incremental" ? "" : "grid grid-cols-2 gap-4"}>
            {/* 开始日期（仅全量导出显示） */}
            {exportMode === "full" && (
              <div>
                <label className="block text-sm font-medium mb-1">开始日期</label>
                <input
                  type="date"
                  value={start}
                  onChange={(e) => setStart(e.target.value)}
                  style={inputStyle}
                />
              </div>
            )}
            <div>
              <label className="block text-sm font-medium mb-1">
                {exportMode === "incremental" ? "导出截止日期" : "结束日期"}
              </label>
              <input
                type="date"
                value={end}
                onChange={(e) => setEnd(e.target.value)}
                style={inputStyle}
              />
              {exportMode === "incremental" && (
                <p className="text-xs text-gray-500 mt-1">
                  将从上次导出位置继续，直到此日期
                </p>
              )}
            </div>
          </div>

          {/* 提交按钮 */}
          <div>
            <button
              type="submit"
              disabled={loading}
              style={{ ...btnPrimary, opacity: loading ? 0.6 : 1 }}
            >
              {loading ? "导出中..." : exportMode === "incremental" ? "增量导出" : "全量导出"}
            </button>
          </div>

          {/* 字段说明按钮：生成 aistock_field_map.csv 并写入 H5 attrs */}
          <div>
            <button
              type="button"
              disabled={fieldMapLoading}
              onClick={handleExportFieldMap}
              style={{ ...btnSecondary, opacity: fieldMapLoading ? 0.6 : 1 }}
            >
              {fieldMapLoading ? "生成字段说明中..." : "生成字段说明（CSV+写入H5）"}
            </button>
            <p className="text-xs text-gray-500 mt-1">
              将读取数据库列 comment（daily_basic / moneyflow_ts），生成 <code>metadata/aistock_field_map.csv</code>，并写入该 Snapshot 下的 <code>daily_basic.h5</code>/<code>moneyflow.h5</code> attrs。
            </p>
          </div>

          {/* 进度条 */}
          {loading && (
            <div className="space-y-2">
              <div className="flex justify-between text-xs text-gray-500">
                <span>{getProgressText()}</span>
                <span>{exportProgress}%</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div
                  className="bg-blue-500 h-2 rounded-full transition-all duration-300"
                  style={{ width: `${exportProgress}%` }}
                />
              </div>
            </div>
          )}

          {/* 错误提示 */}
          {error && (
            <div className="p-3 rounded-lg bg-red-50 text-red-700 text-sm">
              {error}
            </div>
          )}

          {fieldMapError && (
            <div className="p-3 rounded-lg bg-red-50 text-red-700 text-sm">
              {fieldMapError}
            </div>
          )}

          {fieldMapResult && (
            <div className="p-4 rounded-lg bg-blue-50 text-blue-800 text-sm space-y-2">
              <div className="font-medium text-base">📄 字段说明已生成</div>
              <div className="text-xs">
                <div><span className="text-blue-700">Snapshot ID:</span> {fieldMapResult.snapshot_id}</div>
                <div><span className="text-blue-700">CSV:</span> {fieldMapResult.csv_path}</div>
                <div><span className="text-blue-700">字段数:</span> {fieldMapResult.rows}</div>
                <div>
                  <span className="text-blue-700">写入 H5:</span>
                  {Object.keys(fieldMapResult.written_h5 || {}).length === 0
                    ? " 无"
                    : ""}
                </div>
                {Object.entries(fieldMapResult.written_h5 || {}).map(([p, n]) => (
                  <div key={p} className="ml-4">- {p}（{n} 列）</div>
                ))}
              </div>
            </div>
          )}

          {/* 成功提示 - 增强版，显示过滤条件 */}
          {result && lastExportConfig && (
            <div className="p-4 rounded-lg bg-green-50 text-green-700 text-sm space-y-2">
              <div className="font-medium text-base">
                ✅ {lastExportConfig.mode === "incremental" ? "增量导出成功" : "全量导出成功"}
              </div>
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div><span className="text-green-600">Snapshot ID:</span> {result.snapshot_id}</div>
                <div><span className="text-green-600">导出类型:</span> {getExportTypeName(lastExportConfig.type)}</div>
                <div><span className="text-green-600">导出模式:</span> {lastExportConfig.mode === "incremental" ? "增量" : "全量"}</div>
                <div><span className="text-green-600">时间区间:</span> {result.start} ~ {result.end}</div>
                <div><span className="text-green-600">频率:</span> {result.freq}</div>
                {lastExportConfig.exchanges && lastExportConfig.exchanges.length > 0 && (
                  <div><span className="text-green-600">交易所:</span> {getExchangeNames(lastExportConfig.exchanges)}</div>
                )}
                <div><span className="text-green-600">总行数:</span> {result.rows.toLocaleString()}</div>
                {result.ts_codes && (
                  <div><span className="text-green-600">股票/板块数:</span> {result.ts_codes.length.toLocaleString()}</div>
                )}
              </div>
              {lastExportConfig.mode === "incremental" && result.rows === 0 && (
                <div className="text-xs text-green-600 mt-2">
                  💡 没有新数据需要导出，已是最新状态
                </div>
              )}
            </div>
          )}
        </form>
        )}

        {exportTab === "bin" && (
          <div className="space-y-4">
            {/* Qlib bin 内部子标签：股票 / 指数 */}
            <div className="mb-2 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => setBinTab("stock")}
                style={binTab === "stock" ? tabButtonActive : tabButtonBase}
              >
                股票 bin 导出
              </button>
              <button
                type="button"
                onClick={() => setBinTab("index")}
                style={binTab === "index" ? tabButtonActive : tabButtonBase}
              >
                指数 bin 导出
              </button>
            </div>

            {/* 股票 bin 导出表单：保持原有逻辑不变 */}
            {binTab === "stock" && (
              <form
                className="space-y-4"
                onSubmit={async (e) => {
                  e.preventDefault();
                  setBinLoading(true);
                  setBinError(null);
                  setBinResult(null);
                  setShowDumpLog(false);
                  setShowCheckLog(false);

                  try {
                    const exchanges: string[] = [];
                    if (exSh) exchanges.push("sh");
                    if (exSz) exchanges.push("sz");
                    if (exBj) exchanges.push("bj");

                    const payload = {
                      snapshot_id: binSnapshotId.trim(),
                      start: binStart,
                      end: binEnd,
                      exchanges: exchanges.length > 0 ? exchanges : undefined,
                      run_health_check: binRunHealthCheck,
                      exclude_st: excludeSt,
                      exclude_delisted_or_paused: excludeDelistedOrPaused,
                      freq: binFreq,
                    };

                    const resp = await backendRequest<{
                      snapshot_id: string;
                      csv_dir: string;
                      bin_dir: string;
                      dump_bin_ok: boolean;
                      check_ok: boolean | null;
                      stdout_dump: string;
                      stderr_dump: string;
                      stdout_check?: string | null;
                      stderr_check?: string | null;
                    }>("POST", "/api/v1/qlib/bin/export", payload);

                    setBinResult(resp);
                  } catch (err: any) {
                    setBinError(err?.message || "导出 Qlib bin 失败");
                  } finally {
                    setBinLoading(false);
                  }
                }}
              >
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium mb-1">bin Snapshot ID</label>
                <input
                  value={binSnapshotId}
                  onChange={(e) => setBinSnapshotId(e.target.value)}
                  style={inputStyle}
                  placeholder="例如：qlib_bin_2025Q1_all"
                />
                <p className="text-xs text-gray-500 mt-1">
                  将作为 <code>QLIB_BIN_ROOT_WIN/&lt;Snapshot ID&gt;</code> 目录名，供 Qlib.init 使用。
                </p>
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">导出日期区间</label>
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <span className="block text-xs text-gray-500 mb-1">开始日期</span>
                    <input
                      type="date"
                      value={binStart}
                      onChange={(e) => setBinStart(e.target.value)}
                      style={inputStyle}
                    />
                  </div>
                  <div>
                    <span className="block text-xs text-gray-500 mb-1">结束日期</span>
                    <input
                      type="date"
                      value={binEnd}
                      onChange={(e) => setBinEnd(e.target.value)}
                      style={inputStyle}
                    />
                  </div>
                </div>
              </div>
            </div>

            {/* 导出频率：日线 / 分钟线 */}
            <div>
              <label className="block text-sm font-medium mb-1">导出频率</label>
              <select
                value={binFreq}
                onChange={(e) => setBinFreq(e.target.value as "day" | "1m")}
                style={inputStyle}
              >
                <option value="day">日线（日K）</option>
                <option value="1m">1 分钟线</option>
              </select>
              <p className="text-xs text-gray-500 mt-1">
                当前仅支持 day 和 1m；5m/15m 将在数据库准备好后扩展。
              </p>
            </div>

            {/* 交易所范围 */}
            <div>
              <label className="block text-sm font-medium mb-2">交易所范围</label>
              <div className="flex gap-4 flex-wrap">
                <label className="flex items-center gap-2 cursor-pointer text-sm">
                  <input
                    type="checkbox"
                    checked={exSh}
                    onChange={(e) => setExSh(e.target.checked)}
                  />
                  <span>上交所 (SH)</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer text-sm">
                  <input
                    type="checkbox"
                    checked={exSz}
                    onChange={(e) => setExSz(e.target.checked)}
                  />
                  <span>深交所 (SZ)</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer text-sm">
                  <input
                    type="checkbox"
                    checked={exBj}
                    onChange={(e) => setExBj(e.target.checked)}
                  />
                  <span>北交所 (BJ)</span>
                </label>
              </div>
            </div>

            {/* 样本过滤，与 HDF5 Snapshot 一致 */}
            <div>
              <label className="block text-sm font-medium mb-2">样本过滤</label>
              <div className="flex flex-col gap-1 text-sm text-gray-700">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={excludeSt}
                    onChange={(e) => setExcludeSt(e.target.checked)}
                  />
                  <span>排除所有有过 ST 记录的股票（包括当前 ST）</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={excludeDelistedOrPaused}
                    onChange={(e) => setExcludeDelistedOrPaused(e.target.checked)}
                  />
                  <span>排除退市 / 当前暂停上市的股票</span>
                </label>
              </div>
            </div>

            <div className="flex items-center justify-between flex-wrap gap-2">
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={binRunHealthCheck}
                  onChange={(e) => setBinRunHealthCheck(e.target.checked)}
                />
                <span>
                  运行 <code>check_data_health.py</code> 进行健康检查
                </span>
              </label>

              <button
                type="submit"
                disabled={binLoading}
                style={{ ...btnPrimary, opacity: binLoading ? 0.6 : 1 }}
              >
                {binLoading ? "导出 Qlib bin 中..." : "导出 Qlib bin"}
              </button>
            </div>

            {/* 错误提示 */}
            {binError && (
              <div className="p-3 rounded-lg bg-red-50 text-red-700 text-sm mt-2">{binError}</div>
            )}

            {/* 结果展示 */}
            {binTab === "stock" && binResult && (
              <div className="mt-3 space-y-3 text-sm">
                <div className="p-3 rounded-lg bg-green-50 text-green-700">
                  <div className="font-medium mb-1">Qlib bin 导出完成</div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-1 text-xs text-green-800">
                    <div className="text-xs">
                      <span className="font-semibold">Snapshot ID: </span>
                      <span className="font-mono">{binResult.snapshot_id}</span>
                    </div>
                    <div className="text-xs">
                      <span className="font-semibold">CSV 目录: </span>
                      <span className="font-mono">{binResult.csv_dir}</span>
                    </div>
                    <div className="text-xs">
                      <span className="font-semibold">bin 目录: </span>
                      <span className="font-mono">{binResult.bin_dir}</span>
                    </div>
                    <div className="text-xs">
                      <span className="font-semibold">dump_bin: </span>
                      <span>{binResult.dump_bin_ok ? "✅ 成功" : "❌ 失败"}</span>
                    </div>
                    {binResult.check_ok !== null && (
                      <div className="text-xs">
                        <span className="font-semibold">健康检查: </span>
                        <span>{binResult.check_ok ? "✅ 通过" : "❌ 存在问题"}</span>
                      </div>
                    )}
                  </div>
                </div>

                {/* 日志折叠区 */}
                <div className="space-y-2">
                  <div>
                    <button
                      type="button"
                      style={btnSecondary}
                      onClick={() => setShowDumpLog((v) => !v)}
                    >
                      {showDumpLog ? "收起 dump_bin 日志" : "查看 dump_bin 日志"}
                    </button>
                    {showDumpLog && (
                      <div className="mt-2" style={logBox}>
                        {(binResult.stdout_dump || "").trim() || "<无标准输出>"}
                        {binResult.stderr_dump && "\n\n[stderr]\n" + binResult.stderr_dump.trim()}
                      </div>
                    )}
                  </div>

                  {binRunHealthCheck && (
                    <div>
                      <button
                        type="button"
                        style={btnSecondary}
                        onClick={() => setShowCheckLog((v) => !v)}
                      >
                        {showCheckLog ? "收起健康检查日志" : "查看健康检查日志"}
                      </button>
                      {showCheckLog && (
                        <div className="mt-2" style={logBox}>
                          {(binResult.stdout_check || "").trim() || "<无标准输出>"}
                          {binResult.stderr_check &&
                            "\n\n[stderr]\n" + (binResult.stderr_check || "").trim()}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            )}
              </form>
            )}

            {/* 指数 bin 导出表单 */}
            {binTab === "index" && (
              <div className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium mb-1">bin Snapshot ID</label>
                    <input
                      value={binSnapshotId}
                      onChange={(e) => setBinSnapshotId(e.target.value)}
                      style={inputStyle}
                      placeholder="例如：qlib_bin_index_2025_all"
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      指数也会导出到同一个 Qlib bin 目录，便于与股票共用。
                    </p>
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">指数日期区间</label>
                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <span className="block text-xs text-gray-500 mb-1">开始日期</span>
                        <input
                          type="date"
                          value={indexStart}
                          onChange={(e) => setIndexStart(e.target.value)}
                          style={inputStyle}
                        />
                      </div>
                      <div>
                        <span className="block text-xs text-gray-500 mb-1">结束日期</span>
                        <input
                          type="date"
                          value={indexEnd}
                          onChange={(e) => setIndexEnd(e.target.value)}
                          style={inputStyle}
                        />
                      </div>
                    </div>

                    {/* 指数数据源 */}
                    <div className="mt-3">
                      <label className="block text-sm font-medium mb-1">指数数据源</label>
                      <select
                        value={indexDataSource}
                        onChange={(e) => setIndexDataSource(e.target.value as "tushare" | "tdx")}
                        style={inputStyle}
                      >
                        <option value="tushare">Tushare（market.index_daily）</option>
                        <option value="tdx">TDX（market.index_daily_tdx）</option>
                      </select>
                      <p className="text-xs text-gray-500 mt-1">
                        TDX 数据源将使用 <code>market.index_daily_tdx</code>（价格/金额为厘），导出时统一转换为元。
                      </p>
                    </div>
                  </div>
                </div>

                {/* 指数 market 选择 */}
                <div>
                  <label className="block text-sm font-medium mb-1">指数市场 (market)</label>
                  {indexDataSource === "tdx" ? (
                    <p className="text-xs text-gray-500">TDX 数据源当前不使用 market 过滤（固定提供可导出的指数列表）。</p>
                  ) : (
                    <>
                      {indexMarketsError && (
                        <div className="text-xs text-red-600 mb-1">{indexMarketsError}</div>
                      )}
                      {indexMarkets.length === 0 ? (
                        <p className="text-xs text-gray-500">暂无指数市场信息，请检查后端配置。</p>
                      ) : (
                        <div className="flex flex-wrap gap-2 text-sm">
                          {indexMarkets.map((m) => {
                            const active = selectedIndexMarkets.includes(m.market);
                            return (
                              <button
                                key={m.market}
                                type="button"
                                onClick={() => {
                                  setSelectedIndexMarkets((prev) =>
                                    prev.includes(m.market)
                                      ? prev.filter((x) => x !== m.market)
                                      : [...prev, m.market],
                                  );
                                }}
                                style={active ? tabButtonActive : tabButtonBase}
                              >
                                {m.market}
                              </button>
                            );
                          })}
                        </div>
                      )}
                      <p className="text-xs text-gray-500 mt-1">
                        先选择一个或多个 market，再从下方列表选择具体指数。
                      </p>
                    </>
                  )}
                </div>

                {/* 指数列表 */}
                <div>
                  <label className="block text-sm font-medium mb-1">指数列表</label>
                  {indicesError && (
                    <div className="text-xs text-red-600 mb-1">{indicesError}</div>
                  )}
                  {indicesLoading ? (
                    <p className="text-xs text-gray-500">加载指数列表中...</p>
                  ) : indices.length === 0 ? (
                    <p className="text-xs text-gray-500">
                      {indexDataSource === "tdx" ? "TDX 指数列表为空，请检查 market.index_daily_tdx 是否有数据。" : "请选择 market 以加载指数列表。"}
                    </p>
                  ) : (
                    <select
                      value={selectedIndexCode}
                      onChange={(e) => setSelectedIndexCode(e.target.value)}
                      style={inputStyle}
                    >
                      {indices.map((idx) => (
                        <option key={idx.ts_code} value={idx.ts_code}>
                          {idx.ts_code} {idx.name ? `- ${idx.name}` : ""}
                        </option>
                      ))}
                    </select>
                  )}
                </div>

                <div className="flex items-center justify-between flex-wrap gap-2">
                  <label className="flex items-center gap-2 text-sm">
                    <input
                      type="checkbox"
                      checked={indexRunHealthCheck}
                      onChange={(e) => setIndexRunHealthCheck(e.target.checked)}
                    />
                    <span>
                      导出后运行 <code>check_data_health.py</code>（针对整个日频 bin）
                    </span>
                  </label>

                  <button
                    type="button"
                    disabled={indexLoading || !selectedIndexCode}
                    style={{ ...btnPrimary, opacity: indexLoading || !selectedIndexCode ? 0.6 : 1 }}
                    onClick={async () => {
                      if (!selectedIndexCode) return;
                      setIndexLoading(true);
                      setIndexError(null);
                      setIndexResult(null);
                      setIndexShowDumpLog(false);
                      setIndexShowCheckLog(false);
                      try {
                        const resp = await backendRequest<IndexBinExportResponse>(
                          "POST",
                          "/api/v1/qlib/index/bin/export",
                          {
                            snapshot_id: binSnapshotId.trim(),
                            index_code: selectedIndexCode,
                            start: indexStart,
                            end: indexEnd,
                            data_source: indexDataSource,
                            run_health_check: indexRunHealthCheck,
                          },
                        );
                        setIndexResult(resp);
                        // 导出成功后刷新 bin 列表
                        loadBinExports();
                      } catch (e: any) {
                        setIndexError(e?.message || "导出指数 bin 失败");
                      } finally {
                        setIndexLoading(false);
                      }
                    }}
                  >
                    {indexLoading ? "导出指数中..." : "导出选中指数到 bin"}
                  </button>
                </div>

                {/* 指数 bin 健康检查（基于 snapshot_id） */}
                <div className="flex items-center justify-between flex-wrap gap-2">
                  <div className="text-xs text-gray-600">
                    可单独对当前 bin Snapshot 做一次指数健康检查（检查 instruments/index.txt + 整体数据健康）。
                  </div>
                  <button
                    type="button"
                    disabled={indexHealthLoading || !binSnapshotId.trim()}
                    style={{ ...btnSecondary, opacity: indexHealthLoading || !binSnapshotId.trim() ? 0.6 : 1 }}
                    onClick={async () => {
                      if (!binSnapshotId.trim()) return;
                      setIndexHealthLoading(true);
                      setIndexHealthError(null);
                      setIndexHealthResult(null);
                      try {
                        const resp = await backendRequest<IndexHealthCheckResponse>(
                          "POST",
                          "/api/v1/qlib/index/health_check",
                          { snapshot_id: binSnapshotId.trim() },
                        );
                        setIndexHealthResult(resp);
                      } catch (e: any) {
                        setIndexHealthError(e?.message || "指数健康检查失败");
                      } finally {
                        setIndexHealthLoading(false);
                      }
                    }}
                  >
                    {indexHealthLoading ? "检查中..." : "指数 bin 健康检查"}
                  </button>
                </div>

                {indexError && (
                  <div className="p-3 rounded-lg bg-red-50 text-red-700 text-sm mt-2">{indexError}</div>
                )}
                {indexHealthError && (
                  <div className="p-3 rounded-lg bg-red-50 text-red-700 text-sm mt-2">{indexHealthError}</div>
                )}

                {indexResult && (
                  <div className="mt-3 space-y-3 text-sm">
                    <div className="p-3 rounded-lg bg-green-50 text-green-700">
                      <div className="font-medium mb-1">指数 bin 导出完成</div>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-1 text-xs text-green-800">
                        <div>
                          <span className="font-semibold">Snapshot ID: </span>
                          <span className="font-mono">{indexResult.snapshot_id}</span>
                        </div>
                        <div>
                          <span className="font-semibold">指数代码: </span>
                          <span className="font-mono">{indexResult.index_code}</span>
                        </div>
                        <div>
                          <span className="font-semibold">CSV 目录: </span>
                          <span className="font-mono">{indexResult.csv_dir}</span>
                        </div>
                        <div>
                          <span className="font-semibold">bin 目录: </span>
                          <span className="font-mono">{indexResult.bin_dir}</span>
                        </div>
                        <div>
                          <span className="font-semibold">dump_bin: </span>
                          <span>{indexResult.dump_bin_ok ? "✅ 成功" : "❌ 失败"}</span>
                        </div>
                        {indexResult.check_ok !== null && (
                          <div>
                            <span className="font-semibold">健康检查: </span>
                            <span>{indexResult.check_ok ? "✅ 通过" : "❌ 存在问题"}</span>
                          </div>
                        )}
                      </div>
                    </div>

                    <div className="space-y-2">
                      <div>
                        <button
                          type="button"
                          style={btnSecondary}
                          onClick={() => setIndexShowDumpLog((v) => !v)}
                        >
                          {indexShowDumpLog ? "收起 dump_bin 日志" : "查看 dump_bin 日志"}
                        </button>
                        {indexShowDumpLog && (
                          <div className="mt-2" style={logBox}>
                            {(indexResult.stdout_dump || "").trim() || "<无标准输出>"}
                            {indexResult.stderr_dump &&
                              "\n\n[stderr]\n" + (indexResult.stderr_dump || "").trim()}
                          </div>
                        )}
                      </div>

                      {indexRunHealthCheck && (
                        <div>
                          <button
                            type="button"
                            style={btnSecondary}
                            onClick={() => setIndexShowCheckLog((v) => !v)}
                          >
                            {indexShowCheckLog ? "收起健康检查日志" : "查看健康检查日志"}
                          </button>
                          {indexShowCheckLog && (
                            <div className="mt-2" style={logBox}>
                              {(indexResult.stdout_check || "").trim() || "<无标准输出>"}
                              {indexResult.stderr_check &&
                                "\n\n[stderr]\n" + (indexResult.stderr_check || "").trim()}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {indexHealthResult && (
                  <div className="mt-3 space-y-2 text-sm">
                    <div className="p-3 rounded-lg bg-sky-50 text-sky-700">
                      <div className="font-medium mb-1">指数 bin 健康检查结果</div>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-1 text-xs text-sky-800">
                        <div>
                          <span className="font-semibold">Snapshot ID: </span>
                          <span className="font-mono">{indexHealthResult.snapshot_id}</span>
                        </div>
                        <div>
                          <span className="font-semibold">bin 目录: </span>
                          <span className="font-mono">{indexHealthResult.bin_dir}</span>
                        </div>
                        <div>
                          <span className="font-semibold">指数注册文件: </span>
                          <span>
                            {indexHealthResult.has_index_file
                              ? `✅ 存在，指数数目 ${indexHealthResult.index_count}`
                              : "❌ 未找到有效的 instruments/index.txt"}
                          </span>
                        </div>
                        {indexHealthResult.check_ok !== null && (
                          <div>
                            <span className="font-semibold">数据健康检查: </span>
                            <span>{indexHealthResult.check_ok ? "✅ 通过" : "❌ 存在问题"}</span>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </section>

      {/* Snapshot 列表：仅在 HDF5 Snapshot 导出标签下展示 */}
      {exportTab === "snapshot" && (
        <section style={cardStyle}>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold">现有 Snapshot</h2>
            <button
              onClick={loadSnapshots}
              disabled={loadingList}
              className="text-sm text-blue-600 hover:underline"
            >
              {loadingList ? "刷新中..." : "刷新"}
            </button>
          </div>

          {snapshots.length === 0 ? (
            <p className="text-gray-500 text-sm">暂无 Snapshot</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-2 px-2">Snapshot ID</th>
                    <th className="text-center py-2 px-2">日频</th>
                    <th className="text-center py-2 px-2">分钟</th>
                    <th className="text-center py-2 px-2">板块日线</th>
                    <th className="text-center py-2 px-2">板块索引</th>
                    <th className="text-center py-2 px-2">板块成员</th>
                    <th className="text-center py-2 px-2">资金流向</th>
                    <th className="text-center py-2 px-2">每日指标</th>
                    <th className="text-left py-2 px-2">创建时间</th>
                    <th className="text-center py-2 px-2">操作</th>
                  </tr>
                </thead>
                <tbody>
                  {snapshots.map((s) => (
                    <tr key={s.snapshot_id} className="border-b hover:bg-gray-50">
                      <td className="py-2 px-2 font-mono text-xs">{s.snapshot_id}</td>
                      <td className="py-2 px-2 text-center">{s.has_daily ? "✅" : "—"}</td>
                      <td className="py-2 px-2 text-center">{s.has_minute ? "✅" : "—"}</td>
                      <td className="py-2 px-2 text-center">{s.has_board ? "✅" : "—"}</td>
                      <td className="py-2 px-2 text-center">{s.has_board_index ? "✅" : "—"}</td>
                      <td className="py-2 px-2 text-center">{s.has_board_member ? "✅" : "—"}</td>
                      <td className="py-2 px-2 text-center">{s.has_moneyflow ? "✅" : "—"}</td>
                      <td className="py-2 px-2 text-center">{s.has_daily_basic ? "✅" : "—"}</td>
                      <td className="py-2 px-2 text-xs text-gray-500">
                        {s.created_at ? new Date(s.created_at).toLocaleString() : "—"}
                      </td>
                      <td className="py-2 px-2 text-center space-x-2">
                        <button onClick={() => setDetailSnapshot(s)} style={btnSecondary}>
                          详情
                        </button>
                        <button
                          type="button"
                          onClick={() => handleExportFieldMapForSnapshot(s.snapshot_id)}
                          style={btnSecondary}
                        >
                          生成CSV
                        </button>
                        <a
                          href={buildBackendUrl(
                            `/api/v1/qlib/snapshots/${encodeURIComponent(s.snapshot_id)}/export`,
                          )}
                          style={{ ...btnSecondary, display: "inline-block" }}
                        >
                          导出
                        </a>
                        <button onClick={() => handleDelete(s.snapshot_id)} style={btnDanger}>
                          删除
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {/* Qlib bin 导出情况：仅在 bin 标签下展示 */}
      {exportTab === "bin" && (
        <section style={cardStyle}>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold">已导出的 Qlib bin</h2>
            <button
              onClick={loadBinExports}
              disabled={binExportsLoading}
              className="text-sm text-blue-600 hover:underline"
            >
              {binExportsLoading ? "刷新中..." : "刷新"}
            </button>
          </div>

          {binExportsError && (
            <div className="p-3 rounded-lg bg-red-50 text-red-700 text-sm mb-3">
              {binExportsError}
            </div>
          )}

          {binExports.length === 0 ? (
            <p className="text-gray-500 text-sm">暂无 Qlib bin 导出记录。</p>
          ) : (
            <div className="overflow-x-auto text-xs">
              <table className="w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-2 px-2">Snapshot ID</th>
                    <th className="text-left py-2 px-2">时间范围</th>
                    <th className="text-left py-2 px-2">数据类型</th>
                    <th className="text-left py-2 px-2">样本过滤</th>
                    <th className="text-left py-2 px-2">bin 目录</th>
                    <th className="text-left py-2 px-2">创建时间 (上海)</th>
                    <th className="text-left py-2 px-2">最近修改 (上海)</th>
                  </tr>
                </thead>
                <tbody>
                  {binExports.map((b) => (
                    <tr key={b.snapshot_id} className="border-b hover:bg-gray-50 align-top">
                      <td className="py-2 px-2 font-mono">{b.snapshot_id}</td>
                      <td className="py-2 px-2">
                        {b.start && b.end ? `${b.start} ~ ${b.end}` : "—"}
                        {b.exchanges && b.exchanges.length > 0 && (
                          <div className="text-gray-500 mt-1">{b.exchanges.join(", ")}</div>
                        )}
                      </td>
                      <td className="py-2 px-2">
                        {b.freq_types && b.freq_types.length > 0 ? b.freq_types.join(", ") : "daily"}
                      </td>
                      <td className="py-2 px-2">
                        <div>
                          剔除 ST: {b.exclude_st === true ? "是" : b.exclude_st === false ? "否" : "未知"}
                        </div>
                        <div>
                          剔除退市/停牌: {b.exclude_delisted_or_paused === true ? "是" : b.exclude_delisted_or_paused === false ? "否" : "未知"}
                        </div>
                      </td>
                      <td className="py-2 px-2 text-gray-700 break-all">{b.bin_dir}</td>
                      <td className="py-2 px-2 text-gray-500">{formatDateTimeShanghai(b.created_at)}</td>
                      <td className="py-2 px-2 text-gray-500">{formatDateTimeShanghai(b.modified_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {/* 详情弹窗 */}
      {detailSnapshot && (
        <div style={modalOverlay} onClick={() => setDetailSnapshot(null)}>
          <div style={modalContent} onClick={(e) => e.stopPropagation()}>
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-semibold">Snapshot 详情</h3>
              <button
                onClick={() => setDetailSnapshot(null)}
                className="text-gray-400 hover:text-gray-600 text-xl"
              >
                ×
              </button>
            </div>

            <div className="space-y-4">
              {/* 基本信息 */}
              <div>
                <h4 className="text-sm font-medium text-gray-700 mb-2">基本信息</h4>
                <div className="bg-gray-50 rounded-lg p-3 text-sm space-y-1">
                  <div><span className="text-gray-500">Snapshot ID:</span> <span className="font-mono">{detailSnapshot.snapshot_id}</span></div>
                  <div><span className="text-gray-500">路径:</span> <span className="font-mono text-xs break-all">{detailSnapshot.path}</span></div>
                  <div><span className="text-gray-500">创建时间:</span> {detailSnapshot.created_at ? new Date(detailSnapshot.created_at).toLocaleString() : "—"}</div>
                </div>
              </div>

              {/* 包含的数据 */}
              <div>
                <h4 className="text-sm font-medium text-gray-700 mb-2">包含的数据</h4>
                <div className="bg-gray-50 rounded-lg p-3 text-sm">
                  <div className="grid grid-cols-2 gap-2">
                    <div className="flex items-center gap-2">
                      <span>{detailSnapshot.has_daily ? "✅" : "❌"}</span>
                      <span>日频行情 (daily_pv.h5)</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span>{detailSnapshot.has_minute ? "✅" : "❌"}</span>
                      <span>分钟线 (minute_1min.h5)</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span>{detailSnapshot.has_board ? "✅" : "❌"}</span>
                      <span>板块日线 (board_daily.h5)</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span>{detailSnapshot.has_board_index ? "✅" : "❌"}</span>
                      <span>板块索引 (board_index.h5)</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span>{detailSnapshot.has_board_member ? "✅" : "❌"}</span>
                      <span>板块成员 (board_member.h5)</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span>{detailSnapshot.has_moneyflow ? "✅" : "❌"}</span>
                      <span>资金流向 (moneyflow.h5)</span>
                    </div>
                  </div>
                </div>
              </div>

              {/* Meta 信息 */}
              {detailSnapshot.meta && (
                <div>
                  <h4 className="text-sm font-medium text-gray-700 mb-2">Meta 信息 (meta.json)</h4>
                  <div className="bg-gray-50 rounded-lg p-3 text-sm">
                    <pre className="text-xs overflow-auto max-h-60 whitespace-pre-wrap">
                      {JSON.stringify(detailSnapshot.meta, null, 2)}
                    </pre>
                  </div>
                </div>
              )}

              {/* 操作按钮 */}
              <div className="flex justify-end gap-2 pt-2">
                <button
                  onClick={() => {
                    setSnapshotId(detailSnapshot.snapshot_id);
                    setDetailSnapshot(null);
                  }}
                  style={btnSecondary}
                >
                  使用此 ID 导出
                </button>
                <button
                  onClick={() => setDetailSnapshot(null)}
                  style={btnPrimary}
                >
                  关闭
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
