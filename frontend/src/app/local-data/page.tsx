"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

const TDX_BASE =
  process.env.NEXT_PUBLIC_TDX_BACKEND_BASE || "http://127.0.0.1:8001";

interface BackendError {
  message: string;
  detail?: string;
}

interface PingResult {
  ok: boolean;
  error?: BackendError;
}

type LocalDataTab =
  | "init"
  | "incremental"
  | "adjust"
  | "jobs"
  | "stats"
  | "testing"
  | "schedules"
  | "logs";

type DataSource = "TDX" | "Tushare";

const FREQUENCY_CHOICES: { label: string; value: string }[] = [
  { label: "手动 (不调度)", value: "" },
  { label: "5 分钟", value: "5m" },
  { label: "10 分钟", value: "10m" },
  { label: "15 分钟", value: "15m" },
  { label: "30 分钟", value: "30m" },
  { label: "1 小时", value: "1h" },
  { label: "每日", value: "daily" },
];

const INGESTION_DATASETS: Record<string, string> = {
  kline_daily_qfq: "日线（前复权）",
  kline_minute_raw: "1 分钟原始",
  kline_weekly: "周线（由日线QFQ聚合）",
  tdx_board_all: "通达信板块（信息+成分+行情）",
  tdx_board_index: "通达信板块信息",
  tdx_board_member: "通达信板块成分",
  tdx_board_daily: "通达信板块行情",
  stock_moneyflow: "个股资金流（moneyflow_ind_dc）",
  trade_agg_5m: "高频聚合 5m",
};

interface IngestionJobCounters {
  total?: number;
  done?: number;
  running?: number;
  pending?: number;
  failed?: number;
  success?: number;
  inserted_rows?: number;
}

interface IngestionJobStatus {
  job_id?: string;
  job_type?: string;
  status?: string;
  created_at?: string;
  started_at?: string;
  finished_at?: string;
  progress?: number;
  counters?: IngestionJobCounters;
  logs?: string[];
  meta?: any;
}

interface IncrementalPrefill {
  dataSource?: DataSource;
  dataset?: string;
  targetDate?: string;
  startDate?: string | null;
  symbolsScope?: "watchlist" | "all";
}

function classNames(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

async function backendRequest<T = any>(
  method: string,
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const url = `${TDX_BASE.replace(/\/$/, "")}${path}`;
  const controller = new AbortController();
  // Increase timeout to 10 minutes (600000ms) as requested for long-running data checks
  const timeoutId = setTimeout(() => controller.abort(), 600000);
  try {
    let res: Response;
    try {
      res = await fetch(url, {
        ...options,
        method,
        signal: controller.signal,
      });
    } catch (err: any) {
      // 统一处理浏览器/Next.js 的 AbortError，避免出现“signal is aborted without reason”这类底层报错
      if (err && (err.name === "AbortError" || String(err.message || "").includes("aborted"))) {
        throw new Error("请求已超时或被中断，请稍后重试。");
      }
      throw err;
    }
    if (!res.ok) {
      let content: string | undefined;
      try {
        content = await res.text();
      } catch {
        content = undefined;
      }
      throw new Error(
        `后端请求失败: HTTP ${res.status} ${res.statusText}$${"{"}${
          content ? ` | ${content}` : ""
        }${"}"}`,
      );
    }
    if (!res.body) return {} as T;
    const text = await res.text();
    if (!text) return {} as T;
    return JSON.parse(text) as T;
  } finally {
    clearTimeout(timeoutId);
  }
}

function formatDateTime(value?: string | null): string {
  if (!value) return "—";
  try {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    const s = d.toLocaleString("zh-CN", {
      timeZone: "Asia/Shanghai",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
    return s.replace(/\//g, "-");
  } catch {
    return String(value);
  }
}

export default function LocalDataPage() {
  const [activeTab, setActiveTab] = useState<LocalDataTab>("init");
  const [pingResult, setPingResult] = useState<PingResult | null>(null);
  const [pingLoading, setPingLoading] = useState(false);
  const [incrementalPrefill, setIncrementalPrefill] =
    useState<IncrementalPrefill | null>(null);

  const backendBaseDisplay = useMemo(
    () => TDX_BASE.replace(/\/$/, ""),
    [],
  );

  const handlePing = useCallback(async () => {
    setPingLoading(true);
    setPingResult(null);
    try {
      await backendRequest("GET", "/api/testing/schedule", {
        // 只要能连通即可，不关心具体返回结构
      });
      setPingResult({ ok: true });
    } catch (e: any) {
      setPingResult({
        ok: false,
        error: {
          message: e?.message || "调度后端连接失败",
        },
      });
    } finally {
      setPingLoading(false);
    }
  }, []);

  const handleFillLatestFromStats = useCallback(
    (kind: string, latestTradingDay: string, minDate?: string | null) => {
      const lower = (kind || "").toLowerCase();
      let dataSource: DataSource = "TDX";
      let dataset: string | undefined;
      let symbolsScope: "watchlist" | "all" | undefined;

      if (lower === "kline_daily_qfq") {
        dataset = "kline_daily_qfq";
      } else if (lower === "kline_daily_raw") {
        dataset = "kline_daily_raw";
      } else if (lower === "kline_minute_raw") {
        dataset = "kline_minute_raw";
      } else if (lower === "trade_agg_5m") {
        dataset = "trade_agg_5m";
        symbolsScope = "all";
      } else if (
        lower === "tdx_board_index" ||
        lower === "tdx_board_member" ||
        lower === "tdx_board_daily"
      ) {
        dataSource = "Tushare";
        dataset = "tdx_board_all";
      } else {
        return;
      }
      setIncrementalPrefill({
        dataSource,
        dataset,
        targetDate: latestTradingDay,
        startDate: minDate || null,
        symbolsScope,
      });
      setActiveTab("incremental");
    },
    [],
  );

  useEffect(() => {
    // 首次进入页面时，不自动 ping，避免阻塞渲染；交给用户手动测试。
  }, []);

  const tabs: { key: LocalDataTab; label: string }[] = [
    { key: "init", label: "初始化" },
    { key: "incremental", label: "增量" },
    { key: "adjust", label: "复权生成" },
    { key: "jobs", label: "任务监视器" },
    { key: "stats", label: "数据看板" },
    { key: "testing", label: "数据源测试" },
    { key: "schedules", label: "数据入库调度" },
    { key: "logs", label: "运行日志" },
  ];

  return (
    <main style={{ padding: 24 }}>
      <section style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>🗄️ 本地数据管理</h1>
        <p style={{ marginTop: 4, fontSize: 13, color: "#666" }}>
          集中管理 TDX 接口测试与数据入库调度，支持手动与自动执行。
        </p>
      </section>

      <section
        style={{
          marginBottom: 16,
          padding: 12,
          borderRadius: 10,
          background: "#fff",
          boxShadow: "0 2px 8px rgba(0,0,0,0.06)",
          fontSize: 13,
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 12,
            flexWrap: "wrap",
          }}
        >
          <div>
            <div style={{ color: "#444" }}>
              当前调度后端地址：
              <code
                style={{
                  padding: "2px 6px",
                  borderRadius: 4,
                  background: "#f3f4f6",
                  fontSize: 12,
                }}
              >
                {backendBaseDisplay}
              </code>
            </div>
            <div style={{ marginTop: 4, color: "#888", fontSize: 12 }}>
              启动命令示例：
              <code
                style={{
                  padding: "2px 6px",
                  borderRadius: 4,
                  background: "#f3f4f6",
                  fontSize: 12,
                }}
              >
                uvicorn backend.main:app --host 0.0.0.0 --port 8001
              </code>
            </div>
          </div>

          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <button
              type="button"
              onClick={handlePing}
              disabled={pingLoading}
              style={{
                padding: "6px 10px",
                borderRadius: 8,
                border: "none",
                background: "#0ea5e9",
                color: "#fff",
                cursor: "pointer",
                fontSize: 13,
              }}
            >
              {pingLoading ? "测试连接中..." : "测试连接"}
            </button>
            {pingResult && (
              <span
                style={{
                  fontSize: 12,
                  color: pingResult.ok ? "#16a34a" : "#dc2626",
                }}
              >
                {pingResult.ok
                  ? "调度后端连接成功。"
                  : pingResult.error?.message || "调度后端连接失败"}
              </span>
            )}
          </div>
        </div>
      </section>

      {/* Tab 切换 */}
      <section style={{ marginBottom: 12 }}>
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 6,
            borderBottom: "1px solid #e5e7eb",
            paddingBottom: 4,
            marginBottom: 8,
          }}
        >
          {tabs.map((tab) => (
            <button
              key={tab.key}
              type="button"
              onClick={() => setActiveTab(tab.key)}
              style={{
                padding: "6px 10px",
                borderRadius: 999,
                border: "none",
                background:
                  activeTab === tab.key ? "#0f766e" : "transparent",
                color: activeTab === tab.key ? "#fff" : "#374151",
                fontSize: 13,
                cursor: "pointer",
              }}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </section>

      {/* 内容区域 */}
      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 14,
          boxShadow: "0 2px 10px rgba(0,0,0,0.04)",
          fontSize: 13,
        }}
      >
        {activeTab === "init" && <InitTab />}
        {activeTab === "incremental" && (
          <IncrementalTab
            prefill={incrementalPrefill}
            onPrefillConsumed={() => setIncrementalPrefill(null)}
          />
        )}
        {activeTab === "adjust" && <AdjustTab />}
        {activeTab === "jobs" && <JobsTab />}
        {activeTab === "stats" && (
          <DataStatsTab onFillLatest={handleFillLatestFromStats} />
        )}
        {activeTab === "testing" && <TestingTab />}
        {activeTab === "schedules" && <IngestionSchedulesTab />}
        {activeTab === "logs" && <LogsTab />}
      </section>
    </main>
  );
}

function InitTab() {
  const [dataSource, setDataSource] = useState<DataSource>("TDX");
  const [dataset, setDataset] = useState<string>("kline_daily_qfq");
  const [tradeAggScope, setTradeAggScope] = useState<"all" | "watchlist">("all");
  const [startDate, setStartDate] = useState<string>("1990-01-01");
  const [endDate, setEndDate] = useState<string>(() => {
    const d = new Date();
    return d.toISOString().slice(0, 10);
  });
  const [exchanges, setExchanges] = useState<string>("sh,sz,bj");
  const [calExchange, setCalExchange] = useState<string>("SSE");
  const [workers, setWorkers] = useState<number>(1);
  const [truncate, setTruncate] = useState<boolean>(true);
  const [confirmClear, setConfirmClear] = useState<boolean>(false);
  const [submitting, setSubmitting] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const [jobId, setJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<IngestionJobStatus | null>(null);
  const [jobLoading, setJobLoading] = useState<boolean>(false);
  const [autoRefresh, setAutoRefresh] = useState<boolean>(true);

  const [logJobId, setLogJobId] = useState<string | null>(null);
  const [logItems, setLogItems] = useState<any[]>([]);
  const [logLoading, setLogLoading] = useState<boolean>(false);

  const datasetOptionsTDX: { key: string; label: string }[] = [
    {
      key: "kline_daily_qfq",
      label: "kline_daily_qfq · 日线（前复权 QFQ）",
    },
    {
      key: "kline_daily_raw_go",
      label: "kline_daily_raw_go · 日线（未复权 RAW · Go 直连）",
    },
    {
      key: "kline_minute_raw",
      label: "kline_minute_raw · 1 分钟原始（TDX 全量）",
    },
    {
      key: "trade_agg_5m",
      label: "trade_agg_5m · 高频聚合 5m",
    },
    {
      key: "symbol_dim",
      label: "symbol_dim · 股票基础信息（TDX /api/codes）",
    },
  ];

  const datasetOptionsTushare: { key: string; label: string }[] = [
    { key: "tdx_board_all", label: "tdx_board_all · 通达信板块（信息+成分+行情）" },
    { key: "tdx_board_index", label: "tdx_board_index · 通达信板块信息" },
    { key: "tdx_board_member", label: "tdx_board_member · 通达信板块成分" },
    { key: "tdx_board_daily", label: "tdx_board_daily · 通达信板块行情" },
    { key: "kline_weekly", label: "kline_weekly · 周线（由本地日线QFQ聚合）" },
    {
      key: "stock_moneyflow",
      label: "stock_moneyflow · 个股资金流（moneyflow_ind_dc）",
    },
    {
      key: "tushare_trade_cal",
      label: "tushare_trade_cal · 交易日历（Tushare trade_cal 同步）",
    },
  ];

  // 根据数据源动态调整默认参数
  useEffect(() => {
    if (dataSource === "TDX") {
      setDataset("kline_daily_qfq");
      setStartDate("1990-01-01");
      setTruncate(true);
      setConfirmClear(false);
    } else {
      const d = new Date();
      const today = d.toISOString().slice(0, 10);
      const ago = new Date(d.getTime() - 365 * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10);
      setDataset("tdx_board_all");
      setStartDate(ago);
      setEndDate(today);
      setTruncate(false);
      setConfirmClear(true);
    }
  }, [dataSource]);

  const submitInit = async () => {
    setSubmitting(true);
    setError(null);
    try {
      if (dataSource === "TDX") {
        if (dataset === "symbol_dim") {
          const opts = {
            exchanges: exchanges
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean),
          };
          const payload = { dataset: "symbol_dim", mode: "init", options: opts };
          const resp: any = await backendRequest(
            "POST",
            "/api/ingestion/run",
            {
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          if (resp && resp.job_id) {
            setJobId(String(resp.job_id));
            setAutoRefresh(true);
          }
        } else if (dataset === "kline_daily_qfq") {
          const opts = {
            exchanges: exchanges
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean),
            start_date: startDate,
            end_date: endDate,
            batch_size: 100,
            workers: Number(workers) || 1,
          };
          const payload = { dataset: "kline_daily_qfq", mode: "init", options: opts };
          const resp: any = await backendRequest(
            "POST",
            "/api/ingestion/run",
            {
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          if (resp && resp.job_id) {
            setJobId(String(resp.job_id));
            setAutoRefresh(true);
          }
        } else if (dataset === "trade_agg_5m") {
          const opts = {
            start_date: startDate,
            end_date: endDate,
            freq_minutes: 5,
            symbols_scope: tradeAggScope,
            batch_size: 50,
            workers: Number(workers) || 1,
          };
          const payload = { dataset: "trade_agg_5m", mode: "init", options: opts };
          const resp: any = await backendRequest(
            "POST",
            "/api/ingestion/run",
            {
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          if (resp && resp.job_id) {
            setJobId(String(resp.job_id));
            setAutoRefresh(true);
          }
        } else {
          if (truncate && !confirmClear) {
            setError(
              "请先勾选确认或取消清空选项后再继续。显示方式同旧版：清空前必须二次确认。",
            );
            return;
          }
          const commonExchanges = exchanges
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);
          if (dataset === "kline_minute_raw" || dataset === "kline_daily_raw_go") {
            const opts = {
              exchanges: commonExchanges,
              start_date: startDate,
              // 对 Go 驱动的分钟/日线 RAW 初始化：不再传递 end_date，由 Go / TDX 自动拉取至最新有数据的交易日
              batch_size: 100,
              workers: Number(workers) || 1,
              truncate: Boolean(truncate),
            };
            const payload = { dataset, options: opts };
            const resp: any = await backendRequest(
              "POST",
              "/api/ingestion/init",
              {
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
              },
            );
            if (resp && resp.job_id) {
              setJobId(String(resp.job_id));
              setAutoRefresh(true);
            }
          } else {
            const opts = {
              exchanges: commonExchanges,
              start_date: startDate,
              end_date: endDate,
              batch_size: 100,
              workers: Number(workers) || 1,
              truncate: Boolean(truncate),
            };
            const payload = { dataset, options: opts };
            const resp: any = await backendRequest(
              "POST",
              "/api/ingestion/init",
              {
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
              },
            );
            if (resp && resp.job_id) {
              setJobId(String(resp.job_id));
              setAutoRefresh(true);
            }
          }
        }
      } else {
        if (dataset === "tushare_trade_cal") {
          const payload = {
            start_date: startDate,
            end_date: endDate,
            exchange: calExchange || "SSE",
          };
          const resp: any = await backendRequest(
            "POST",
            "/api/calendar/sync",
            {
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          const inserted = Number(resp?.inserted_or_updated || 0);
          setError(
            inserted > 0
              ? `已同步 ${inserted} 条交易日历记录。`
              : "交易日历同步完成。",
          );
        } else {
          const opts = {
            start_date: startDate,
            end_date: endDate,
            batch_size: 200,
          };
          const payload = { dataset, mode: "init", options: opts };
          const resp: any = await backendRequest(
            "POST",
            "/api/ingestion/run",
            {
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          if (resp && resp.job_id) {
            setJobId(String(resp.job_id));
            setAutoRefresh(true);
          }
        }
      }
    } catch (e: any) {
      setError(e?.message || "初始化任务提交失败");
    } finally {
      setSubmitting(false);
    }
  };
  const loadJobStatus = useCallback(
    async (id: string) => {
      setJobLoading(true);
      try {
        const data: any = await backendRequest(
          "GET",
          `/api/ingestion/job/${id}`,
        );
        setJobStatus(data as IngestionJobStatus);
        const status = String(data?.status || "").toLowerCase();
        if (["success", "failed", "canceled"].includes(status)) {
          // 任务结束，不再自动刷新
          setAutoRefresh(false);
        }
      } catch (e: any) {
        setError(e?.message || "加载任务状态失败");
      } finally {
        setJobLoading(false);
      }
    },
    [],
  );

  // 自动轮询任务状态
  useEffect(() => {
    if (!jobId || !autoRefresh) return;
    let cancelled = false;

    const tick = async () => {
      if (!jobId) return;
      await loadJobStatus(jobId);
      if (!cancelled && autoRefresh) {
        setTimeout(tick, 5000);
      }
    };

    tick();

    return () => {
      cancelled = true;
    };
  }, [jobId, autoRefresh, loadJobStatus]);

  const openJobLogs = async (jobIdValue: string) => {
    try {
      setLogLoading(true);
      setLogJobId(jobIdValue);
      const data: any = await backendRequest(
        "GET",
        `/api/ingestion/logs?job_id=${jobIdValue}&limit=500&offset=0`,
      );
      const items = Array.isArray(data?.items) ? data.items : [];
      setLogItems(items);
    } catch (e: any) {
      setError(e?.message || "加载运行日志失败");
    } finally {
      setLogLoading(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1.1fr 1.1fr",
          gap: 16,
          alignItems: "flex-start",
        }}
      >
        <div>
          <h3 style={{ marginTop: 0, fontSize: 15 }}>🚀 初始化同步</h3>
          <div style={{ marginBottom: 8 }}>
            <label style={{ fontSize: 13 }}>数据源</label>
            <select
              value={dataSource}
              onChange={(e) =>
                setDataSource(e.target.value as DataSource)
              }
              style={{
                display: "block",
                marginTop: 4,
                padding: "6px 8px",
                borderRadius: 8,
                border: "1px solid #d4d4d4",
                fontSize: 13,
              }}
            >
              <option value="TDX">TDX</option>
              <option value="Tushare">Tushare</option>
            </select>
          </div>

          <div style={{ marginBottom: 8 }}>
            <label style={{ fontSize: 13 }}>目标数据集</label>
            <select
              value={dataset}
              onChange={(e) => setDataset(e.target.value)}
              style={{
                display: "block",
                marginTop: 4,
                padding: "6px 8px",
                borderRadius: 8,
                border: "1px solid #d4d4d4",
                fontSize: 13,
                width: "100%",
              }}
            >
              {(dataSource === "TDX"
                ? datasetOptionsTDX
                : datasetOptionsTushare
              ).map((opt) => (
                <option key={opt.key} value={opt.key}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>

          {dataSource === "TDX" && dataset === "trade_agg_5m" && (
            <div style={{ marginBottom: 8 }}>
              <label style={{ fontSize: 13 }}>股票范围</label>
              <select
                value={tradeAggScope}
                onChange={(e) => setTradeAggScope(e.target.value as "all" | "watchlist")}
                style={{
                  display: "block",
                  marginTop: 4,
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                  fontSize: 13,
                  width: "100%",
                }}
              >
                <option value="all">全市场（TDX /api/codes 返回的全部股票）</option>
                <option value="watchlist">自选股（app.watchlist_items）</option>
              </select>
            </div>
          )}

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
              gap: 10,
              marginBottom: 8,
            }}
          >
            <div>
              <label style={{ fontSize: 13 }}>开始日期</label>
              <input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                }}
              />
            </div>
            {!(
              dataSource === "TDX" &&
              (dataset === "kline_minute_raw" || dataset === "kline_daily_raw_go")
            ) && (
              <div>
                <label style={{ fontSize: 13 }}>结束日期</label>
                <input
                  type="date"
                  value={endDate}
                  onChange={(e) => setEndDate(e.target.value)}
                  style={{
                    marginTop: 4,
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #d4d4d4",
                  }}
                />
              </div>
            )}
          </div>
          {dataSource === "TDX" && dataset === "kline_minute_raw" && (
            <p style={{ marginTop: 0, marginBottom: 8, fontSize: 12, color: "#6b7280" }}>
              不再指定截止日期，系统将从开始日期起自动拉取至最新有数据的交易日。
            </p>
          )}

          {dataSource === "TDX" && (
            <div style={{ marginBottom: 8 }}>
              <label style={{ fontSize: 13 }}>交易所(逗号分隔)</label>
              <input
                value={exchanges}
                onChange={(e) => setExchanges(e.target.value)}
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                }}
              />
            </div>
          )}

          {dataSource === "TDX" &&
            (dataset === "kline_minute_raw" ||
              dataset === "kline_daily_raw_go" ||
              dataset === "trade_agg_5m") && (
              <div style={{ marginBottom: 8 }}>
                <label style={{ fontSize: 13 }}>并行度 (Workers)</label>
                <select
                  value={workers}
                  onChange={(e) =>
                    setWorkers(Number(e.target.value) || 1)
                  }
                  style={{
                    display: "block",
                    marginTop: 4,
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #d4d4d4",
                    fontSize: 13,
                    width: "100%",
                  }}
                >
                  {[1, 2, 4, 8].map((w) => (
                    <option key={w} value={w}>
                      {w} 线程
                    </option>
                  ))}
                </select>
              </div>
            )}

          {dataSource === "Tushare" && dataset === "tushare_trade_cal" && (
            <div style={{ marginBottom: 8 }}>
              <label style={{ fontSize: 13 }}>交易所(用于Tushare日历)</label>
              <select
                value={calExchange}
                onChange={(e) => setCalExchange(e.target.value)}
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                  fontSize: 13,
                }}
              >
                <option value="SSE">SSE</option>
                <option value="SZSE">SZSE</option>
              </select>
            </div>
          )}

          {dataSource === "TDX" &&
            (dataset === "kline_minute_raw" || dataset === "kline_daily_raw_go") && (
            <div style={{ marginTop: 4, marginBottom: 8 }}>
              <label style={{ fontSize: 13 }}>
                <input
                  type="checkbox"
                  checked={truncate}
                  onChange={(e) => setTruncate(e.target.checked)}
                  style={{ marginRight: 4 }}
                />
                初始化前清空目标表(或目标范围)
              </label>
              {truncate && (
                <div style={{ marginTop: 4 }}>
                  <label style={{ fontSize: 13, color: "#b91c1c" }}>
                    <input
                      type="checkbox"
                      checked={confirmClear}
                      onChange={(e) => setConfirmClear(e.target.checked)}
                      style={{ marginRight: 4 }}
                    />
                    我已知晓清空数据的风险，并确认继续
                  </label>
                </div>
              )}
            </div>
          )}

          <button
            type="button"
            onClick={submitInit}
            disabled={submitting}
            style={{
              marginTop: 4,
              padding: "8px 12px",
              borderRadius: 8,
              border: "none",
              background: "#16a34a",
              color: "#fff",
              cursor: "pointer",
              fontSize: 13,
              minWidth: 120,
            }}
          >
            {submitting ? "正在提交..." : "开始初始化"}
          </button>

          {error && (
            <p style={{ marginTop: 8, fontSize: 12, color: "#b91c1c" }}>
              {error}
            </p>
          )}
        </div>

        <div>
          <h3 style={{ marginTop: 0, fontSize: 15 }}>当前初始化任务进度</h3>
          {jobId ? (
            <div style={{ fontSize: 12 }}>
              <p style={{ margin: 0 }}>当前作业ID：{jobId}</p>
              <div
                style={{
                  marginTop: 6,
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                }}
              >
                <button
                  type="button"
                  onClick={() => jobId && loadJobStatus(jobId)}
                  disabled={jobLoading}
                  style={{
                    padding: "4px 8px",
                    borderRadius: 6,
                    border: "1px solid #d4d4d4",
                    background: "#fff",
                    cursor: "pointer",
                    fontSize: 12,
                  }}
                >
                  {jobLoading ? "刷新中..." : "手动刷新"}
                </button>
                <label style={{ fontSize: 12 }}>
                  <input
                    type="checkbox"
                    checked={autoRefresh}
                    onChange={(e) => setAutoRefresh(e.target.checked)}
                    style={{ marginRight: 4 }}
                  />
                  自动刷新
                </label>
              </div>

              {jobStatus && (
                <div style={{ marginTop: 8 }}>
                  <p style={{ margin: 0 }}>
                    状态：{jobStatus.status || "未知"} · 进度：
                    {jobStatus.progress ?? 0}%
                  </p>
                  <div
                    style={{
                      marginTop: 4,
                      width: "100%",
                      background: "#e5e7eb",
                      borderRadius: 999,
                      overflow: "hidden",
                    }}
                  >
                    <div
                      style={{
                        width: `${Math.min(
                          100,
                          Math.max(0, jobStatus.progress ?? 0),
                        )}%`,
                        height: 8,
                        background: "#16a34a",
                      }}
                    />
                  </div>
                  {jobStatus.counters && (
                    <p
                      style={{
                        marginTop: 4,
                        fontSize: 12,
                        color: "#4b5563",
                      }}
                    >
                      总数 {jobStatus.counters.total ?? 0} · 已完成
                      {" "}
                      {jobStatus.counters.done ?? 0} · 运行中
                      {" "}
                      {jobStatus.counters.running ?? 0} · 排队
                      {" "}
                      {jobStatus.counters.pending ?? 0} · 成功
                      {" "}
                      {jobStatus.counters.success ?? 0} · 失败
                      {" "}
                      {jobStatus.counters.failed ?? 0} · 新增行数
                      {" "}
                      {jobStatus.counters.inserted_rows ?? 0}
                    </p>
                  )}
                  {jobStatus.logs && jobStatus.logs.length > 0 && (
                    <div style={{ marginTop: 8 }}>
                      <p
                        style={{
                          margin: 0,
                          fontSize: 12,
                          color: "#4b5563",
                        }}
                      >
                        最近日志：
                      </p>
                      <ul
                        style={{
                          marginTop: 4,
                          paddingLeft: 18,
                          maxHeight: 180,
                          overflowY: "auto",
                          fontSize: 12,
                        }}
                      >
                        {jobStatus.logs.map((m, idx) => (
                          <li key={idx} style={{ marginBottom: 2 }}>
                            <code>{String(m)}</code>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              )}
            </div>
          ) : (
            <p style={{ fontSize: 12, color: "#6b7280" }}>
              尚未提交初始化任务。请在左侧填写参数并点击“开始初始化”。
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

function IncrementalTab({
  prefill,
  onPrefillConsumed,
}: {
  prefill?: IncrementalPrefill | null;
  onPrefillConsumed?: () => void;
}) {
  const [dataSource, setDataSource] = useState<DataSource>("TDX");
  const [dataset, setDataset] = useState<string>("kline_daily_qfq");
  const [date, setDate] = useState<string>(() => {
    const d = new Date();
    return d.toISOString().slice(0, 10);
  });
  const [startDate, setStartDate] = useState<string>("");
  const [exchanges, setExchanges] = useState<string>("sh,sz,bj");
  const [batchSize, setBatchSize] = useState<number>(100);
  const [workers, setWorkers] = useState<number>(1);
  const [symbolsScope, setSymbolsScope] = useState<"watchlist" | "all">("watchlist");

  const [calStart, setCalStart] = useState<string>("");
  const [calEnd, setCalEnd] = useState<string>("");
  const [calExchange, setCalExchange] = useState<string>("SSE");

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [jobId, setJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<IngestionJobStatus | null>(null);
  const [jobLoading, setJobLoading] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);

  const datasetOptionsTDX = [
    {
      key: "kline_daily_qfq",
      label: "kline_daily_qfq · 日线（前复权 QFQ）",
    },
    {
      key: "kline_daily_raw",
      label: "kline_daily_raw · 日线（未复权 RAW）",
    },
    {
      key: "kline_minute_raw",
      label: "kline_minute_raw · 1 分钟（原始 RAW）",
    },
    {
      key: "trade_agg_5m",
      label: "trade_agg_5m · 高频聚合 5m",
    },
  ];

  const datasetOptionsTushare = [
    {
      key: "tdx_board_all",
      label: "tdx_board_all · 通达信板块（信息+成分+行情）",
    },
    { key: "tdx_board_index", label: "tdx_board_index · 通达信板块信息" },
    { key: "tdx_board_member", label: "tdx_board_member · 通达信板块成分" },
    { key: "tdx_board_daily", label: "tdx_board_daily · 通达信板块行情" },
    {
      key: "kline_weekly",
      label: "kline_weekly · 周线（由本地日线QFQ聚合）",
    },
    {
      key: "stock_moneyflow",
      label:
        "stock_moneyflow · 个股资金流（按交易日增量，默认最近3个自然日）",
    },
    {
      key: "tushare_trade_cal",
      label: "tushare_trade_cal · 交易日历（Tushare trade_cal 同步）",
    },
  ];

  // 处理来自“数据看板”的预填参数：数据源 / 数据集 / 日期范围
  useEffect(() => {
    if (!prefill) return;
    if (prefill.dataSource) {
      setDataSource(prefill.dataSource);
    }
    if (prefill.dataset) {
      setDataset(prefill.dataset);
    }
    if (prefill.targetDate) {
      setDate(prefill.targetDate);
    }
    if (prefill.startDate !== undefined) {
      setStartDate(prefill.startDate || "");
    }
    if (prefill.symbolsScope) {
      setSymbolsScope(prefill.symbolsScope);
    }
    if (onPrefillConsumed) {
      onPrefillConsumed();
    }
  }, [prefill, onPrefillConsumed]);

  useEffect(() => {
    if (dataSource === "TDX") {
      // 仅调整默认交易所，不强制覆盖当前 dataset，避免打断外部预填
      setExchanges("sh,sz,bj");
    } else {
      // 切换到 Tushare 时，如果当前 dataset 不在 Tushare 选项里，则默认选 tdx_board_all
      const isTushareDataset = datasetOptionsTushare.some(
        (opt) => opt.key === dataset,
      );
      if (!isTushareDataset) {
        setDataset("tdx_board_all");
      }
      const d = new Date();
      const today = d.toISOString().slice(0, 10);
      const ago = new Date(d.getTime() - 365 * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10);
      setCalStart(ago);
      setCalEnd(today);
    }
  }, [dataSource, dataset]);

  const loadJobStatus = useCallback(
    async (id: string) => {
      setJobLoading(true);
      try {
        const data: any = await backendRequest("GET", `/api/ingestion/job/${id}`);
        setJobStatus(data as IngestionJobStatus);
        const status = String(data?.status || "").toLowerCase();
        if (["success", "failed", "canceled"].includes(status)) {
          setAutoRefresh(false);
        }
      } catch (e: any) {
        setError(e?.message || "加载任务状态失败");
      } finally {
        setJobLoading(false);
      }
    },
    [],
  );

  useEffect(() => {
    if (!jobId || !autoRefresh) return;
    let cancelled = false;

    const tick = async () => {
      if (!jobId) return;
      await loadJobStatus(jobId);
      if (!cancelled && autoRefresh) {
        setTimeout(tick, 5000);
      }
    };

    tick();

    return () => {
      cancelled = true;
    };
  }, [jobId, autoRefresh, loadJobStatus]);

  const submitIncremental = async () => {
    setSubmitting(true);
    setError(null);
    try {
      if (dataSource === "TDX") {
        if (dataset === "trade_agg_5m") {
          if (!symbolsScope) {
            setError("错误：股票范围 (symbolsScope) 参数丢失。请重新选择股票范围（Watchlist 或 All）。");
            return;
          }
          const argsParts: string[] = ["--mode", "incremental"];
          if (startDate) {
            argsParts.push("--start-date", startDate);
          }
          if (date) {
            argsParts.push("--end-date", date);
          }
          argsParts.push("--freq-minutes", "5");
          argsParts.push("--symbols-scope", symbolsScope);
          argsParts.push("--batch-size", String(Number(batchSize) || 50));
          argsParts.push("--workers", String(Number(workers) || 1));

          console.log("[DEBUG] Submitting trade_agg_5m with args:", argsParts);
          const opts = {
            args: argsParts.join(" "),
          };
          const payload = {
            dataset: "trade_agg_5m",
            mode: "incremental",
            options: opts,
          };
          const resp: any = await backendRequest(
            "POST",
            "/api/ingestion/run",
            {
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          if (resp && resp.job_id) {
            setJobId(String(resp.job_id));
            setAutoRefresh(true);
          }
        } else {
          const isMinuteDataset = dataset === "kline_minute_raw";
          const opts: any = {
            date,
            start_date: startDate || null,
            exchanges: exchanges
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean),
            batch_size: Number(batchSize) || 100,
            // max_empty 仅在需要限制空天数时显式使用；当前分钟增量默认传 0，表示不根据空天数提前停止，完整扫完日期区间
            workers: Number(workers) || 1,
          };
          if (isMinuteDataset) {
            opts.max_empty = 0;
          }
          const payload = { dataset, mode: "incremental", options: opts };
          const resp: any = await backendRequest(
            "POST",
            "/api/ingestion/run",
            {
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          if (resp && resp.job_id) {
            setJobId(String(resp.job_id));
            setAutoRefresh(true);
          }
        }
      } else {
        if (dataset === "tushare_trade_cal") {
          const payload = {
            start_date: calStart,
            end_date: calEnd,
            exchange: calExchange || "SSE",
          };
          const resp: any = await backendRequest(
            "POST",
            "/api/calendar/sync",
            {
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          const inserted = Number(resp?.inserted_or_updated || 0);
          setError(
            inserted > 0
              ? `已同步 ${inserted} 条交易日历记录。`
              : "交易日历同步完成。",
          );
        } else {
          let effectiveStart: string | null = startDate || null;
          if (dataset === "stock_moneyflow" && !startDate) {
            try {
              const endDt = new Date(date);
              const defaultStart = new Date(
                endDt.getTime() - 2 * 24 * 60 * 60 * 1000,
              )
                .toISOString()
                .slice(0, 10);
              effectiveStart = defaultStart;
            } catch {
              effectiveStart = date;
            }
          }
          const opts = {
            start_date: effectiveStart,
            end_date: date,
            batch_size: Number(batchSize) || 100,
          };
          const payload = { dataset, mode: "incremental", options: opts };
          const resp: any = await backendRequest(
            "POST",
            "/api/ingestion/run",
            {
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          if (resp && resp.job_id) {
            setJobId(String(resp.job_id));
            setAutoRefresh(true);
          }
        }
      }
    } catch (e: any) {
      setError(e?.message || "增量任务提交失败");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1.1fr 1.1fr",
          gap: 16,
          alignItems: "flex-start",
        }}
      >
        <div>
          <h3 style={{ marginTop: 0, fontSize: 15 }}>🔄 增量更新</h3>
          <div style={{ marginBottom: 8 }}>
            <label style={{ fontSize: 13 }}>数据源</label>
            <select
              value={dataSource}
              onChange={(e) =>
                setDataSource(e.target.value as DataSource)
              }
              style={{
                display: "block",
                marginTop: 4,
                padding: "6px 8px",
                borderRadius: 8,
                border: "1px solid #d4d4d4",
                fontSize: 13,
              }}
            >
              <option value="TDX">TDX</option>
              <option value="Tushare">Tushare</option>
            </select>
          </div>

          <div style={{ marginBottom: 8 }}>
            <label style={{ fontSize: 13 }}>目标数据集</label>
            <select
              value={dataset}
              onChange={(e) => setDataset(e.target.value)}
              style={{
                display: "block",
                marginTop: 4,
                padding: "6px 8px",
                borderRadius: 8,
                border: "1px solid #d4d4d4",
                fontSize: 13,
                width: "100%",
              }}
            >
              {(dataSource === "TDX"
                ? datasetOptionsTDX
                : datasetOptionsTushare
              ).map((opt) => (
                <option key={opt.key} value={opt.key}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
              gap: 10,
              marginBottom: 8,
            }}
          >
            <div>
              <label style={{ fontSize: 13 }}>目标日期</label>
              <input
                type="date"
                value={date}
                onChange={(e) => setDate(e.target.value)}
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                }}
              />
            </div>
            <div>
              <label style={{ fontSize: 13 }}>覆盖起始日期(可选)</label>
              <input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                }}
              />
            </div>
          </div>

          {dataSource === "TDX" && (
            <div style={{ marginBottom: 8 }}>
              <label style={{ fontSize: 13 }}>交易所(逗号分隔)</label>
              <input
                value={exchanges}
                onChange={(e) => setExchanges(e.target.value)}
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                }}
              />
            </div>
          )}

          {dataSource === "Tushare" && dataset === "tushare_trade_cal" && (
            <div style={{ marginBottom: 8 }}>
              <label style={{ fontSize: 13 }}>交易日历同步窗口</label>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                  gap: 10,
                  marginTop: 4,
                }}
              >
                <input
                  type="date"
                  value={calStart}
                  onChange={(e) => setCalStart(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #d4d4d4",
                  }}
                />
                <input
                  type="date"
                  value={calEnd}
                  onChange={(e) => setCalEnd(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #d4d4d4",
                  }}
                />
              </div>
              <div style={{ marginTop: 4 }}>
                <select
                  value={calExchange}
                  onChange={(e) => setCalExchange(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #d4d4d4",
                    fontSize: 13,
                  }}
                >
                  <option value="SSE">SSE</option>
                  <option value="SZSE">SZSE</option>
                </select>
              </div>
            </div>
          )}

          {dataSource === "TDX" && dataset === "trade_agg_5m" && (
            <div style={{ marginBottom: 8 }}>
              <label style={{ fontSize: 13 }}>股票范围 (Scope)</label>
              <select
                value={symbolsScope}
                onChange={(e) => setSymbolsScope(e.target.value as any)}
                style={{
                  display: "block",
                  marginTop: 4,
                  padding: "6px 8px",
                  width: "100%",
                  boxSizing: "border-box",
                }}
              >
                <option value="watchlist">Watchlist (自选股)</option>
                <option value="all">All (全市场)</option>
              </select>
              <div style={{ fontSize: 11, color: "#666", marginTop: 2 }}>
                全市场更新耗时较长，请谨慎选择
              </div>
            </div>
          )}

          <div style={{ marginBottom: 8 }}>
            <label style={{ fontSize: 13 }}>批量大小 (Batch Size)</label>
            <input
              value={batchSize}
              onChange={(e) => setBatchSize(Number(e.target.value) || 100)}
              style={{
                marginTop: 4,
                width: "100%",
                padding: "6px 8px",
                borderRadius: 8,
                border: "1px solid #d4d4d4",
              }}
            />
          </div>

          {dataSource === "TDX" &&
            (dataset === "kline_daily_qfq" ||
              dataset === "kline_daily_raw" ||
              dataset === "kline_minute_raw" ||
              dataset === "trade_agg_5m") && (
              <div style={{ marginBottom: 8 }}>
                <label style={{ fontSize: 13 }}>并行度 (Workers)</label>
                <select
                  value={workers}
                  onChange={(e) =>
                    setWorkers(Number(e.target.value) || 1)
                  }
                  style={{
                    marginTop: 4,
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #d4d4d4",
                    fontSize: 13,
                  }}
                >
                  {[1, 2, 4, 8].map((w) => (
                    <option key={w} value={w}>
                      {w} 线程
                    </option>
                  ))}
                </select>
              </div>
            )}

          <button
            type="button"
            onClick={submitIncremental}
            disabled={submitting}
            style={{
              marginTop: 4,
              padding: "8px 12px",
              borderRadius: 8,
              border: "none",
              background: "#0f766e",
              color: "#fff",
              cursor: "pointer",
              fontSize: 13,
              minWidth: 120,
            }}
          >
            {submitting ? "正在提交..." : "开始增量"}
          </button>

          {error && (
            <p style={{ marginTop: 8, fontSize: 12, color: "#b91c1c" }}>
              {error}
            </p>
          )}
        </div>

        <div>
          <h3 style={{ marginTop: 0, fontSize: 15 }}>当前增量任务进度</h3>
          {jobId ? (
            <div style={{ fontSize: 12 }}>
              <p style={{ margin: 0 }}>当前作业ID：{jobId}</p>
              <div
                style={{
                  marginTop: 6,
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                }}
              >
                <button
                  type="button"
                  onClick={() => jobId && loadJobStatus(jobId)}
                  disabled={jobLoading}
                  style={{
                    padding: "4px 8px",
                    borderRadius: 6,
                    border: "1px solid #d4d4d4",
                    background: "#fff",
                    cursor: "pointer",
                    fontSize: 12,
                  }}
                >
                  {jobLoading ? "刷新中..." : "手动刷新"}
                </button>
                <label style={{ fontSize: 12 }}>
                  <input
                    type="checkbox"
                    checked={autoRefresh}
                    onChange={(e) => setAutoRefresh(e.target.checked)}
                    style={{ marginRight: 4 }}
                  />
                  自动刷新
                </label>
              </div>

              {jobStatus && (
                <div style={{ marginTop: 8 }}>
                  <p style={{ margin: 0 }}>
                    状态：{jobStatus.status || "未知"} · 进度：
                    {jobStatus.progress ?? 0}%
                  </p>
                  <div
                    style={{
                      marginTop: 4,
                      width: "100%",
                      background: "#e5e7eb",
                      borderRadius: 999,
                      overflow: "hidden",
                    }}
                  >
                    <div
                      style={{
                        width: `${Math.min(
                          100,
                          Math.max(0, jobStatus.progress ?? 0),
                        )}%`,
                        height: 8,
                        background: "#0f766e",
                      }}
                    />
                  </div>
                  {jobStatus.counters && (
                    <p
                      style={{
                        marginTop: 4,
                        fontSize: 12,
                        color: "#4b5563",
                      }}
                    >
                      总数 {jobStatus.counters.total ?? 0} · 已完成
                      {" "}
                      {jobStatus.counters.done ?? 0} · 运行中
                      {" "}
                      {jobStatus.counters.running ?? 0} · 排队
                      {" "}
                      {jobStatus.counters.pending ?? 0} · 成功
                      {" "}
                      {jobStatus.counters.success ?? 0} · 失败
                      {" "}
                      {jobStatus.counters.failed ?? 0} · 新增行数
                      {" "}
                      {jobStatus.counters.inserted_rows ?? 0}
                    </p>
                  )}
                  {jobStatus.logs && jobStatus.logs.length > 0 && (
                    <div style={{ marginTop: 8 }}>
                      <p
                        style={{
                          margin: 0,
                          fontSize: 12,
                          color: "#4b5563",
                        }}
                      >
                        最近日志：
                      </p>
                      <ul
                        style={{
                          marginTop: 4,
                          paddingLeft: 18,
                          maxHeight: 180,
                          overflowY: "auto",
                          fontSize: 12,
                        }}
                      >
                        {jobStatus.logs.map((m, idx) => (
                          <li key={idx} style={{ marginBottom: 2 }}>
                            <code>{String(m)}</code>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              )}
            </div>
          ) : (
            <p style={{ fontSize: 12, color: "#6b7280" }}>
              尚未提交增量任务。请在左侧填写参数并点击“开始增量”。
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

function AdjustTab() {
  const [which, setWhich] = useState<"both" | "qfq" | "hfq">("both");
  const [startDate, setStartDate] = useState<string>("1990-01-01");
  const [endDate, setEndDate] = useState<string>(() => {
    const d = new Date();
    return d.toISOString().slice(0, 10);
  });
  const [exchanges, setExchanges] = useState<string>("sh,sz,bj");
  const [workers, setWorkers] = useState<number>(1);
  const [truncate, setTruncate] = useState(false);
  const [confirmClear, setConfirmClear] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [jobId, setJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<IngestionJobStatus | null>(null);
  const [jobLoading, setJobLoading] = useState<boolean>(false);
  const [autoRefresh, setAutoRefresh] = useState<boolean>(true);

  const loadJobStatus = useCallback(
    async (id: string) => {
      setJobLoading(true);
      try {
        const data: any = await backendRequest("GET", `/api/ingestion/job/${id}`);
        setJobStatus(data as IngestionJobStatus);
        const status = String(data?.status || "").toLowerCase();
        if (["success", "failed", "canceled"].includes(status)) {
          setAutoRefresh(false);
        }
      } catch (e: any) {
        setError(e?.message || "加载任务状态失败");
      } finally {
        setJobLoading(false);
      }
    },
    [],
  );

  useEffect(() => {
    if (!jobId || !autoRefresh) return;
    let cancelled = false;

    const tick = async () => {
      if (!jobId) return;
      await loadJobStatus(jobId);
      if (!cancelled && autoRefresh) {
        setTimeout(tick, 5000);
      }
    };

    tick();

    return () => {
      cancelled = true;
    };
  }, [jobId, autoRefresh, loadJobStatus]);

  const submitAdjust = async () => {
    setSubmitting(true);
    setError(null);
    try {
      if (truncate && !confirmClear) {
        setError("请先勾选确认或取消清理选项后再继续");
        return;
      }
      const opts = {
        which,
        start_date: startDate,
        end_date: endDate,
        exchanges: exchanges
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
        workers: Number(workers) || 1,
        truncate: Boolean(truncate),
      };
      const resp: any = await backendRequest("POST", "/api/adjust/rebuild", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ options: opts }),
      });
      if (resp && resp.job_id) {
        setJobId(String(resp.job_id));
        setAutoRefresh(true);
      }
    } catch (e: any) {
      setError(e?.message || "复权生成任务提交失败");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1.1fr 1.1fr",
          gap: 16,
          alignItems: "flex-start",
        }}
      >
        <div>
          <h3 style={{ marginTop: 0, fontSize: 15 }}>🛠️ 复权生成（RAW → QFQ/HFQ）</h3>
          <div style={{ marginBottom: 8 }}>
            <label style={{ fontSize: 13 }}>生成类型</label>
            <select
              value={which}
              onChange={(e) =>
                setWhich(e.target.value as "both" | "qfq" | "hfq")
              }
              style={{
                display: "block",
                marginTop: 4,
                padding: "6px 8px",
                borderRadius: 8,
                border: "1px solid #d4d4d4",
                fontSize: 13,
              }}
            >
              <option value="both">QFQ+HFQ</option>
              <option value="qfq">仅QFQ</option>
              <option value="hfq">仅HFQ</option>
            </select>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
              gap: 10,
              marginBottom: 8,
            }}
          >
            <div>
              <label style={{ fontSize: 13 }}>开始日期</label>
              <input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                }}
              />
            </div>
            <div>
              <label style={{ fontSize: 13 }}>结束日期</label>
              <input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                }}
              />
            </div>
          </div>

          <div style={{ marginBottom: 8 }}>
            <label style={{ fontSize: 13 }}>交易所(逗号分隔)</label>
            <input
              value={exchanges}
              onChange={(e) => setExchanges(e.target.value)}
              style={{
                marginTop: 4,
                width: "100%",
                padding: "6px 8px",
                borderRadius: 8,
                border: "1px solid #d4d4d4",
              }}
            />
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
              gap: 10,
              marginBottom: 8,
            }}
          >
            <div>
              <label style={{ fontSize: 13 }}>并行度</label>
              <select
                value={workers}
                onChange={(e) => setWorkers(Number(e.target.value) || 1)}
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                  fontSize: 13,
                }}
              >
                {[1, 2, 4, 8].map((w) => (
                  <option key={w} value={w}>
                    {w} 线程
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div style={{ marginBottom: 8 }}>
            <label style={{ fontSize: 13 }}>
              <input
                type="checkbox"
                checked={truncate}
                onChange={(e) => setTruncate(e.target.checked)}
                style={{ marginRight: 4 }}
              />
              生成前清理目标表/范围
            </label>
            {truncate && (
              <div style={{ marginTop: 4 }}>
                <label style={{ fontSize: 13, color: "#b91c1c" }}>
                  <input
                    type="checkbox"
                    checked={confirmClear}
                    onChange={(e) => setConfirmClear(e.target.checked)}
                    style={{ marginRight: 4 }}
                  />
                  我已知晓清理数据的风险，并确认继续
                </label>
              </div>
            )}
          </div>

          <button
            type="button"
            onClick={submitAdjust}
            disabled={submitting}
            style={{
              marginTop: 4,
              padding: "8px 12px",
              borderRadius: 8,
              border: "none",
              background: "#7c3aed",
              color: "#fff",
              cursor: "pointer",
              fontSize: 13,
              minWidth: 120,
            }}
          >
            {submitting ? "正在提交..." : "开始生成"}
          </button>

          {error && (
            <p style={{ marginTop: 8, fontSize: 12, color: "#b91c1c" }}>
              {error}
            </p>
          )}
        </div>

        <div>
          <h3 style={{ marginTop: 0, fontSize: 15 }}>当前复权任务进度</h3>
          {jobId ? (
            <div style={{ fontSize: 12 }}>
              <p style={{ margin: 0 }}>当前作业ID：{jobId}</p>
              <div
                style={{
                  marginTop: 6,
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                }}
              >
                <button
                  type="button"
                  onClick={() => jobId && loadJobStatus(jobId)}
                  disabled={jobLoading}
                  style={{
                    padding: "4px 8px",
                    borderRadius: 6,
                    border: "1px solid #d4d4d4",
                    background: "#fff",
                    cursor: "pointer",
                    fontSize: 12,
                  }}
                >
                  {jobLoading ? "刷新中..." : "手动刷新"}
                </button>
                <label style={{ fontSize: 12 }}>
                  <input
                    type="checkbox"
                    checked={autoRefresh}
                    onChange={(e) => setAutoRefresh(e.target.checked)}
                    style={{ marginRight: 4 }}
                  />
                  自动刷新
                </label>
              </div>

              {jobStatus && (
                <div style={{ marginTop: 8 }}>
                  <p style={{ margin: 0 }}>
                    状态：{jobStatus.status || "未知"} · 进度：
                    {jobStatus.progress ?? 0}%
                  </p>
                  <div
                    style={{
                      marginTop: 4,
                      width: "100%",
                      background: "#e5e7eb",
                      borderRadius: 999,
                      overflow: "hidden",
                    }}
                  >
                    <div
                      style={{
                        width: `${Math.min(
                          100,
                          Math.max(0, jobStatus.progress ?? 0),
                        )}%`,
                        height: 8,
                        background: "#7c3aed",
                      }}
                    />
                  </div>
                  {jobStatus.counters && (
                    <p
                      style={{
                        marginTop: 4,
                        fontSize: 12,
                        color: "#4b5563",
                      }}
                    >
                      总数 {jobStatus.counters.total ?? 0} · 已完成{" "}
                      {jobStatus.counters.done ?? 0} · 运行中{" "}
                      {jobStatus.counters.running ?? 0} · 排队{" "}
                      {jobStatus.counters.pending ?? 0} · 成功{" "}
                      {jobStatus.counters.success ?? 0} · 失败{" "}
                      {jobStatus.counters.failed ?? 0} · 新增行数{" "}
                      {jobStatus.counters.inserted_rows ?? 0}
                    </p>
                  )}
                  {jobStatus.logs && jobStatus.logs.length > 0 && (
                    <div style={{ marginTop: 8 }}>
                      <p
                        style={{
                          margin: 0,
                          fontSize: 12,
                          color: "#4b5563",
                        }}
                      >
                        最近日志：
                      </p>
                      <ul
                        style={{
                          marginTop: 4,
                          paddingLeft: 18,
                          maxHeight: 180,
                          overflowY: "auto",
                          fontSize: 12,
                        }}
                      >
                        {jobStatus.logs.map((m, idx) => (
                          <li key={idx} style={{ marginBottom: 2 }}>
                            <code>{String(m)}</code>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              )}
            </div>
          ) : (
            <p style={{ fontSize: 12, color: "#6b7280" }}>
              尚未提交复权任务。请在左侧填写参数并点击“开始生成”。
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

function JobsTab() {
  const [items, setItems] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeOnly, setActiveOnly] = useState(true);
  const [limit, setLimit] = useState<number>(50);
  const [autoRefresh, setAutoRefresh] = useState(true);

  const [logJobId, setLogJobId] = useState<string | null>(null);
  const [logItems, setLogItems] = useState<any[]>([]);
  const [logLoading, setLogLoading] = useState(false);

  const loadJobs = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const qs = new URLSearchParams();
      qs.set("limit", String(limit));
      qs.set("active_only", String(activeOnly));
      const data: any = await backendRequest(
        "GET",
        `/api/ingestion/jobs?${qs.toString()}`,
      );
      setItems(Array.isArray(data?.items) ? data.items : []);
    } catch (e: any) {
      setError(e?.message || "加载任务失败");
    } finally {
      setLoading(false);
    }
  }, [activeOnly, limit]);

  useEffect(() => {
    loadJobs();
  }, [loadJobs]);

  useEffect(() => {
    if (!autoRefresh) return;
    const anyActive = items.some((job: any) => {
      const st = String(job?.status || "").toLowerCase();
      return ["running", "queued", "pending"].includes(st);
    });
    if (!anyActive) return;
    const id = setTimeout(() => {
      loadJobs();
    }, 5000);
    return () => clearTimeout(id);
  }, [autoRefresh, items, loadJobs]);

  const handleDelete = useCallback(
    async (jobId: string, status: string) => {
      if (typeof window !== "undefined") {
        const ok = window.confirm(
          "确定要删除该任务及其相关历史记录吗？此操作不可恢复。",
        );
        if (!ok) return;
      }
      try {
        await backendRequest("DELETE", `/api/ingestion/job/${jobId}`);
        await loadJobs();
      } catch (e: any) {
        setError(e?.message || "删除任务失败");
      }
    },
    [loadJobs],
  );

  const handleCancel = useCallback(
    async (jobId: string, status: string) => {
      if (typeof window !== "undefined") {
        const ok = window.confirm("确定要停止该任务吗？正在执行的同步任务会被强制取消。");
        if (!ok) return;
      }
      try {
        await backendRequest("POST", `/api/ingestion/job/${jobId}/cancel`);
        await loadJobs();
      } catch (e: any) {
        setError(e?.message || "停止任务失败");
      }
    },
    [loadJobs],
  );

  const openJobLogs = async (jobIdValue: string) => {
    try {
      setLogLoading(true);
      setLogJobId(jobIdValue);
      const data: any = await backendRequest(
        "GET",
        `/api/ingestion/logs?job_id=${jobIdValue}&limit=500&offset=0`,
      );
      const items = Array.isArray(data?.items) ? data.items : [];
      setLogItems(items);
    } catch (e: any) {
      setError(e?.message || "加载运行日志失败");
    } finally {
      setLogLoading(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <h3 style={{ marginTop: 0, fontSize: 15 }}>📊 任务监视器</h3>
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 12,
          marginBottom: 8,
          fontSize: 13,
        }}
      >
        <label>
          <input
            type="checkbox"
            checked={activeOnly}
            onChange={(e) => setActiveOnly(e.target.checked)}
            style={{ marginRight: 4 }}
          />
          仅显示运行中/排队
        </label>
        <label>
          最多显示
          <input
            type="number"
            min={10}
            max={200}
            step={10}
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value) || 50)}
            style={{
              marginLeft: 4,
              width: 80,
              padding: "4px 6px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
            }}
          />
        </label>
        <label>
          <input
            type="checkbox"
            checked={autoRefresh}
            onChange={(e) => setAutoRefresh(e.target.checked)}
            style={{ marginRight: 4 }}
          />
          自动刷新
        </label>
        <button
          type="button"
          onClick={loadJobs}
          disabled={loading}
          style={{
            padding: "4px 10px",
            borderRadius: 6,
            border: "1px solid #d4d4d4",
            background: "#fff",
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          {loading ? "刷新中..." : "手动刷新"}
        </button>
      </div>

      {error && (
        <p style={{ fontSize: 12, color: "#b91c1c" }}>{error}</p>
      )}

      {items.length === 0 && !loading ? (
        <p style={{ fontSize: 13, color: "#6b7280" }}>暂无任务。</p>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {items.map((job: any, idx: number) => {
            const summary = job?.summary || {};
            const meta = job?.meta || {};
            const dataset =
              meta.dataset || summary.dataset || (summary.datasets && summary.datasets[0]);
            const mode = (meta.mode || summary.mode || job.job_type || "").toLowerCase();
            const status = (job.status || "").toLowerCase();
            const counters = job.counters || {};
            const percent = Number(job.progress || 0);
            const errorSamples = job.error_samples || [];
            const jobId: string | undefined = job.job_id;

            let cat = "其他";
            const dsLower = String(dataset || "").toLowerCase();
            if (
              ["kline_daily_qfq", "kline_daily", "kline_daily_raw"].includes(dsLower) &&
              mode === "init"
            ) {
              cat = "日线初始化";
            } else if (
              ["kline_daily_qfq", "kline_daily", "kline_daily_raw"].includes(dsLower) &&
              mode === "incremental"
            ) {
              cat = "日线增量";
            } else if (dsLower === "adjust_daily" && ["rebuild", "init"].includes(mode)) {
              cat = "复权计算";
            } else if (dsLower.startsWith("tdx_board_")) {
              cat = "板块数据";
            } else if (["kline_weekly", "kline_weekly_qfq"].includes(dsLower)) {
              cat = "周线聚合";
            } else if (dsLower === "stock_moneyflow") {
              cat = "资金流数据";
            }

            const createdAt = job.created_at || job.started_at;

            const startDate =
              meta.start_date ||
              summary.start_date ||
              summary.start ||
              summary.date_from ||
              null;
            const endDate =
              meta.end_date ||
              summary.end_date ||
              summary.end ||
              summary.date_to ||
              null;
            const targetDate = summary.date || summary.target_date || null;

            let dateRangeText: string;
            if (startDate || endDate) {
              dateRangeText = `${startDate || "—"} .. ${endDate || "—"}`;
            } else if (targetDate) {
              dateRangeText = String(targetDate);
            } else {
              dateRangeText = "—";
            }

            let exchangesText: string | null = null;
            const exVal = (meta.exchanges ?? summary.exchanges) as any;
            if (Array.isArray(exVal)) {
              exchangesText = exVal.join(",");
            } else if (typeof exVal === "string") {
              exchangesText = exVal;
            }

            const extraParts: string[] = [];
            if (exchangesText) extraParts.push(`交易所：${exchangesText}`);
            if (dateRangeText && dateRangeText !== "—") {
              extraParts.push(`日期：${dateRangeText}`);
            }
            if (summary.which) extraParts.push(`复权类型：${summary.which}`);
            if (summary.workers) extraParts.push(`并行度：${summary.workers}`);
            if (meta.freq_minutes)
              extraParts.push(`频率：${meta.freq_minutes} 分钟`);
            if (meta.symbols_scope)
              extraParts.push(`代码范围：${meta.symbols_scope}`);

            const rangeText =
              extraParts.length > 0 ? extraParts.join(" · ") : "—";

            const datasetLabel =
              dataset && INGESTION_DATASETS[String(dataset)]
                ? `${dataset} · ${INGESTION_DATASETS[String(dataset)]}`
                : dataset || "—";

            const typeText =
              mode === "init"
                ? "全量"
                : mode === "incremental"
                  ? "增量"
                  : meta.type || job.job_type || "—";

            const sourceText =
              meta.source === "tdx_api"
                ? "TDX 接口"
                : meta.source === "tushare"
                  ? "Tushare"
                  : meta.source === "derived_from_kline_daily_qfq"
                    ? "本地日线聚合"
                    : meta.source === "tdx_api_minute_trade_all"
                      ? "TDX 分钟成交聚合"
                      : meta.source || "—";

            const canDelete = !!jobId;
            // 仅当任务由 Go 驱动（即 summary/meta 中存在 go_task_id）且仍在运行/排队时，才允许前端发起停止请求。
            const hasGoTaskId =
              !!(meta as any)?.go_task_id ||
              !!(summary && (summary as any).go_task_id);
            const canCancel =
              !!jobId &&
              hasGoTaskId &&
              ["running", "queued", "pending"].includes(status);

            return (
              <div
                key={jobId || idx}
                style={{
                  borderRadius: 10,
                  border: "1px solid #e5e7eb",
                  padding: 10,
                  background: "#fafafa",
                }}
              >
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    gap: 8,
                    marginBottom: 4,
                    fontSize: 13,
                  }}
                >
                  <div>
                    <div>
                      {cat} · 数据集: {datasetLabel} · 类型: {typeText} · 来源:{" "}
                      {sourceText}
                    </div>
                    <div
                      style={{
                        fontSize: 11,
                        color: "#6b7280",
                        marginTop: 2,
                      }}
                    >
                      开始时间：{formatDateTime(createdAt)}
                    </div>
                  </div>
                  <div
                    style={{
                      fontSize: 11,
                      color:
                        status === "success"
                          ? "#16a34a"
                          : status === "failed"
                            ? "#b91c1c"
                            : "#374151",
                      display: "flex",
                      alignItems: "center",
                      gap: 6,
                    }}
                  >
                    状态：{job.status || "—"}

                    {jobId && (
                      <button
                        type="button"
                        onClick={() => openJobLogs(jobId)}
                        style={{
                          padding: "2px 6px",
                          borderRadius: 6,
                          border: "1px solid #d4d4d4",
                          background: "#fff",
                          cursor: "pointer",
                          fontSize: 11,
                        }}
                      >
                        详情
                      </button>
                    )}
                    {canCancel && jobId && (
                      <button
                        type="button"
                        onClick={() => handleCancel(jobId, status)}
                        style={{
                          padding: "2px 6px",
                          borderRadius: 6,
                          border: "1px solid #fed7aa",
                          background: "#ffedd5",
                          color: "#c2410c",
                          cursor: "pointer",
                          fontSize: 11,
                        }}
                      >
                        停止
                      </button>
                    )}
                    {canDelete && jobId && (
                      <button
                        type="button"
                        onClick={() => handleDelete(jobId, status)}
                        style={{
                          padding: "2px 6px",
                          borderRadius: 6,
                          border: "1px solid #fecaca",
                          background: "#fee2e2",
                          color: "#b91c1c",
                          cursor: "pointer",
                          fontSize: 11,
                        }}
                      >
                        删除
                      </button>
                    )}
                  </div>
                </div>

                <div
                  style={{
                    width: "100%",
                    background: "#e5e7eb",
                    borderRadius: 999,
                    overflow: "hidden",
                    marginTop: 4,
                  }}
                >
                  <div
                    style={{
                      width: `${Math.min(100, Math.max(0, percent))}%`,
                      height: 8,
                      background: "#0ea5e9",
                    }}
                  />
                </div>
                <div
                  style={{
                    fontSize: 12,
                    color: "#4b5563",
                    marginTop: 4,
                  }}
                >
                  进度 {percent}% · 完成 {counters.done ?? 0}/
                  {counters.total ?? 0} · 新增 {counters.inserted_rows ?? 0} 条
                </div>
                <div
                  style={{
                    fontSize: 12,
                    color: "#6b7280",
                    marginTop: 2,
                  }}
                >
                  总数 {counters.total ?? 0} · 已完成 {counters.done ?? 0} ·
                  运行中 {counters.running ?? 0} · 排队 {" "}
                  {counters.pending ?? 0} · 成功 {counters.success ?? 0} · 失败 {" "}
                  {counters.failed ?? 0}
                </div>
                <div
                  style={{
                    fontSize: 12,
                    color: "#4b5563",
                    marginTop: 4,
                  }}
                >
                  范围：{rangeText}
                </div>
                {counters.failed > 0 && errorSamples?.length > 0 && (
                  <details style={{ marginTop: 6 }}>
                    <summary style={{ cursor: "pointer", fontSize: 12 }}>
                      查看失败明细（样本）
                    </summary>
                    <ul
                      style={{
                        marginTop: 4,
                        paddingLeft: 18,
                        fontSize: 12,
                      }}
                    >
                      {errorSamples.map((err: any, i: number) => {
                        const tsCode = err.ts_code || "—";
                        const detail = err.detail || {};
                        const tradeDate =
                          detail.trade_date ||
                          detail.date ||
                          detail.start_date ||
                          null;
                        let msg = String(err.message || "").trim();
                        if (msg.length > 200) {
                          msg = `${msg.slice(0, 200)}...`;
                        }
                        return (
                          <li key={i} style={{ marginBottom: 2 }}>
                            <span>
                              代码：{tsCode} · 日期/范围：
                              {tradeDate || "未知"}
                            </span>
                            <br />
                            <span>错误：{msg}</span>
                          </li>
                        );
                      })}
                    </ul>
                  </details>
                )}
              </div>
            );
          })}
        </div>
      )}

      {logJobId && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.35)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            style={{
              width: "80%",
              maxWidth: 900,
              maxHeight: "80vh",
              background: "#fff",
              borderRadius: 10,
              padding: 12,
              boxShadow: "0 10px 30px rgba(0,0,0,0.25)",
              display: "flex",
              flexDirection: "column",
              gap: 8,
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
              }}
            >
              <h4 style={{ margin: 0, fontSize: 14 }}>
                运行日志详情 · Job {logJobId}
              </h4>
              <button
                type="button"
                onClick={() => {
                  setLogJobId(null);
                  setLogItems([]);
                }}
                style={{
                  padding: "2px 8px",
                  borderRadius: 6,
                  border: "1px solid #d4d4d4",
                  background: "#fff",
                  cursor: "pointer",
                  fontSize: 12,
                }}
              >
                关闭
              </button>
            </div>
            <div
              style={{
                flex: 1,
                overflowY: "auto",
                border: "1px solid #e5e7eb",
                borderRadius: 8,
                padding: 8,
                fontFamily:
                  "SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
                fontSize: 12,
                background: "#fafafa",
              }}
            >
              {logLoading ? (
                <p style={{ fontSize: 12 }}>日志加载中...</p>
              ) : logItems.length === 0 ? (
                <p style={{ fontSize: 12, color: "#6b7280" }}>
                  暂无日志记录。
                </p>
              ) : (
                logItems.map((it: any, idx: number) => {
                  const ts = it?.timestamp || "";
                  const level = it?.level || "";
                  const datasetLabel = it?.dataset || "";
                  const modeLabel = it?.mode || "";
                  const payload = it?.payload ?? {};
                  const text = JSON.stringify(payload, null, 2);
                  return (
                    <div
                      key={`${ts}-${idx}`}
                      style={{
                        marginBottom: 8,
                        paddingBottom: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      <div
                        style={{
                          fontSize: 11,
                          color: "#4b5563",
                          marginBottom: 2,
                        }}
                      >
                        [{ts}] [{level}] {datasetLabel} {modeLabel}
                      </div>
                      <pre
                        style={{
                          margin: 0,
                          whiteSpace: "pre-wrap",
                          wordBreak: "break-all",
                        }}
                      >
                        {text}
                      </pre>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ...

function DataStatsTab({
  onFillLatest,
}: {
  onFillLatest?: (
    kind: string,
    latestTradingDay: string,
    minDate?: string | null,
  ) => void;
}) {
  const [items, setItems] = useState<any[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [gapLoadingKind, setGapLoadingKind] = useState<string | null>(null);
  const [gapResult, setGapResult] = useState<any | null>(null);

  const loadExistingStats = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data: any = await backendRequest("GET", "/api/data-stats");
      const nextItems = Array.isArray(data?.items) ? data.items : [];
      setItems(nextItems);
    } catch (e: any) {
      setError(e?.message || "加载统计数据失败");
    } finally {
      setLoading(false);
    }
  }, []);

  const triggerRefresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    setGapResult(null);
    try {
      await backendRequest("POST", "/api/data-stats/refresh");
      const data: any = await backendRequest("GET", "/api/data-stats");
      const nextItems = Array.isArray(data?.items) ? data.items : [];
      setItems(nextItems);
    } catch (e: any) {
      setError(e?.message || "刷新统计数据失败");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    // 初次进入数据看板时仅加载上次刷新结果，不主动触发后端 refresh
    loadExistingStats();
  }, [loadExistingStats]);

  const handleCheckGapsClick = useCallback(
    async (kind: string, refresh: boolean = false) => {
      setGapLoadingKind(kind);
      if (!refresh) setGapResult(null);
      setError(null);
      try {
        const params = new URLSearchParams({ data_kind: kind });
        if (refresh) {
          params.append("refresh", "true");
        }
        const data: any = await backendRequest(
          "GET",
          `/api/data-stats/gaps?${params.toString()}`,
        );
        setGapResult(data);
      } catch (e: any) {
        setError(e?.message || "数据检查失败");
      } finally {
        setGapLoadingKind(null);
      }
    },
    [],
  );

  const handleFillLatestClick = useCallback(
    async (kind: string, minDate?: string | null) => {
      if (!onFillLatest) return;
      try {
        const data: any = await backendRequest(
          "GET",
          "/api/trading/latest-day",
        );
        const latest = data?.latest_trading_day;
        if (!latest) {
          setError("无法获取最新交易日，请先同步交易日历。");
          return;
        }
        onFillLatest(kind, String(latest), minDate ?? null);
      } catch (e: any) {
        setError(e?.message || "获取最新交易日失败");
      }
    },
    [onFillLatest],
  );

  // ...

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <h3 style={{ marginTop: 0, fontSize: 15 }}>📊 数据看板（统计总览）</h3>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          marginBottom: 6,
        }}
      >
        <button
          type="button"
          onClick={triggerRefresh}
          disabled={loading}
          style={{
            padding: "6px 10px",
            borderRadius: 8,
            border: "none",
            background: "#0ea5e9",
            color: "#fff",
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          {loading ? "刷新中..." : "刷新统计数据"}
        </button>
        <span style={{ fontSize: 12, color: "#6b7280" }}>
          统计数据来自后台预计算表 market.data_stats，适合快速查看各类数据的时间范围、条数和更新时间。
        </span>
      </div>

      {error && (
        <p style={{ fontSize: 12, color: "#b91c1c", margin: 0 }}>
          {error}
        </p>
      )}

      {items.length === 0 && !loading ? (
        <p style={{ fontSize: 13, color: "#6b7280" }}>
          当前没有统计数据，请先执行一次刷新。
        </p>
      ) : (
        <div
          style={{
            width: "100%",
            overflowX: "auto",
            marginTop: 4,
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
              <tr style={{ background: "#f3f4f6" }}>
                <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                  类别
                </th>
                <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                  描述
                </th>
                <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                  记录数
                </th>
                <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                  起始日期
                </th>
                <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                  结束日期
                </th>
                <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                  最后更新时间
                </th>
                <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                  最近检查
                </th>
                <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                  表名
                </th>
                <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                  操作
                </th>
              </tr>
            </thead>
            <tbody>
              {items.map((it: any, idx: number) => {
                const extra =
                  (it?.extra_info &&
                    (typeof it.extra_info === "object"
                      ? it.extra_info
                      : {})) ||
                  {};
                const lastRaw = it.last_updated_at;
                const lastDisp =
                  lastRaw != null ? formatDateTime(String(lastRaw)) : "—";
                
                // 处理最近检查信息
                const lastCheckAt = it.last_check_at ? formatDateTime(String(it.last_check_at)) : "—";
                let checkSummary = "";
                if (it.last_check_result) {
                    try {
                        const res = typeof it.last_check_result === 'string' ? JSON.parse(it.last_check_result) : it.last_check_result;
                        if (typeof res.missing_days === 'number') {
                            checkSummary = ` (缺失 ${res.missing_days} 天)`;
                        }
                    } catch (e) {
                        // ignore
                    }
                }

                const kind = String(
                  it.data_kind || it.kind || "",
                );
                
                // 处理描述文本：去掉 trade_agg_5m 后面的括号内容
                let description = extra.desc || it.label || it.description || "—";
                if (kind === "trade_agg_5m") {
                    description = description.replace(/（.*?）|\(.*?\)/g, "").trim();
                }

                const minDateStr =
                  it.min_date || it.date_min || it.start_date || null;
                const canFillLatest = [
                  "kline_daily_qfq",
                  "kline_daily_raw",
                  "kline_minute_raw",
                  "tdx_board_index",
                  "tdx_board_member",
                  "tdx_board_daily",
                  "trade_agg_5m",
                ].includes(kind);
                return (
                  <tr key={idx}>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                      }}
                    >
                      {it.data_kind || it.kind || "—"}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                      }}
                    >
                      {description}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                      }}
                    >
                      {it.row_count || it.rows || 0}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                      }}
                    >
                      {it.min_date || it.date_min || it.start_date || "—"}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                      }}
                    >
                      {it.max_date || it.date_max || it.end_date || "—"}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                      }}
                    >
                      {lastDisp}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                      }}
                    >
                      <div style={{ fontSize: 12 }}>{lastCheckAt}</div>
                      {checkSummary && <div style={{ fontSize: 11, color: "#666" }}>{checkSummary}</div>}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                      }}
                    >
                      {it.table_name || it.table || "—"}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                      }}
                    >
                      <div style={{ display: "flex", gap: 6 }}>
                        {canFillLatest && kind && (
                          <button
                            type="button"
                            onClick={() => handleFillLatestClick(kind, minDateStr)}
                            style={{
                              padding: "4px 8px",
                              borderRadius: 6,
                              border: "1px solid #d4d4d4",
                              background: "#fff",
                              cursor: "pointer",
                              fontSize: 12,
                            }}
                          >
                            补齐到最新交易日
                          </button>
                        )}
                        {kind && (
                          <button
                            type="button"
                            onClick={() => handleCheckGapsClick(kind, false)}
                            disabled={gapLoadingKind === kind}
                            style={{
                              padding: "4px 8px",
                              borderRadius: 6,
                              border: "1px solid #d4d4d4",
                              background: "#fff",
                              cursor: "pointer",
                              fontSize: 12,
                            }}
                          >
                            {gapLoadingKind === kind ? "检查中..." : "数据检查"}
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {gapResult && (
            <div
              style={{
                marginTop: 8,
                fontSize: 12,
                background: "#f9fafb",
                padding: 8,
                borderRadius: 8,
                border: "1px solid #e5e7eb",
              }}
            >
              <div style={{ marginBottom: 4, fontWeight: "bold", display: "flex", justifyContent: "space-between" }}>
                <span>检查结果 (Kind: {gapResult.data_kind})</span>
                <button
                  type="button"
                  onClick={() => handleCheckGapsClick(gapResult.data_kind, true)}
                  disabled={gapLoadingKind === gapResult.data_kind}
                  style={{
                    padding: "2px 8px",
                    borderRadius: 4,
                    border: "1px solid #d4d4d4",
                    background: "#fff",
                    cursor: "pointer",
                    fontSize: 11,
                  }}
                >
                  {gapLoadingKind === gapResult.data_kind ? "重新检查中..." : "立即重新检查(强制刷新)"}
                </button>
              </div>
              <div>
                表: {gapResult.table_name} · 区间: {gapResult.start_date} ~ {gapResult.end_date}
              </div>
              {gapResult.last_check_at && (
                <div style={{ color: "#6b7280" }}>
                  结果生成于: {formatDateTime(gapResult.last_check_at)} (缓存)
                </div>
              )}
              {typeof gapResult.symbol_count === "number" && (
                <div>
                  覆盖股票数量: {gapResult.symbol_count}
                </div>
              )}
              <div>
                交易日总数: {gapResult.total_trading_days}，有数据天数:
                {" "}
                {gapResult.covered_days}，缺失交易日:
                {" "}
                {gapResult.missing_days}
              </div>
              {Array.isArray(gapResult.missing_ranges) &&
                gapResult.missing_ranges.length > 0 && (
                  <details style={{ marginTop: 4 }}>
                    <summary>
                      缺失日期段 ({gapResult.missing_ranges.length})
                    </summary>
                    <ul
                      style={{
                        marginTop: 4,
                        paddingLeft: 18,
                      }}
                    >
                      {gapResult.missing_ranges.map((r: any, idx: number) => (
                        <li key={idx}>
                          {r.start === r.end
                            ? r.start
                            : `${r.start} ~ ${r.end}`}（{r.days} 天）
                        </li>
                      ))}
                    </ul>
                  </details>
                )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function TestingRunsTable({ runs }: { runs: any[] }) {
  if (!runs || runs.length === 0) {
    return (
      <p style={{ fontSize: 13, color: "#6b7280" }}>暂无测试执行记录</p>
    );
  }
  return (
    <div
      style={{
        width: "100%",
        overflowX: "auto",
        marginTop: 4,
        marginBottom: 8,
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
          <tr style={{ background: "#f3f4f6" }}>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              执行ID
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              调度
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              发起者
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              状态
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              开始时间
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              结束时间
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              成功数
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              失败数
            </th>
          </tr>
        </thead>
        <tbody>
          {runs.map((item: any, idx: number) => {
            const summary = item.summary || {};
            return (
              <tr key={idx}>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {item.run_id}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {item.schedule_id || "手动"}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {item.triggered_by}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {item.status}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {formatDateTime(item.started_at)}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {formatDateTime(item.finished_at)}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {summary.success ?? "—"}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {summary.failed ?? "—"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function IngestionLogsTable({
  logs,
  selectedKeys,
  onToggleItem,
}: {
  logs: any[];
  selectedKeys: string[];
  onToggleItem: (item: any, checked: boolean, key: string) => void;
}) {
  const rows: Array<{
    key: string;
    raw: any;
    task: string;
    run_id: string | null;
    ts: string | null;
    level: string | null;
    dataset: string;
    mode?: string;
    status?: string;
    note?: string | null;
  }> = [];

  for (const item of logs || []) {
    const payload = item.payload || {};
    const summary = payload.summary || {};
    let dataset: string | undefined =
      item.dataset ||
      summary.dataset ||
      (Array.isArray(summary.datasets) && summary.datasets.length > 0
        ? summary.datasets[0]
        : undefined);
    if (!dataset && typeof payload.raw === "string" && payload.raw.trim()) {
      dataset = payload.raw.split(" ")[0];
    }
    const mode: string | undefined =
      item.mode || summary.mode || payload.status;
    let note: string | null = null;
    if (payload.error != null) {
      note = String(payload.error);
    } else if (summary && Object.keys(summary).length > 0) {
      try {
        note = JSON.stringify(summary);
      } catch {
        note = String(summary);
      }
    } else if (typeof payload.raw === "string" && payload.raw.trim()) {
      note = payload.raw;
    }
    if (!note && typeof payload.logs === "string" && payload.logs.trim()) {
      let snippet = payload.logs.trim();
      if (snippet.length > 300) snippet = "..." + snippet.slice(-300);
      note = snippet;
    }

    const key = `${item.run_id || ""}||${item.timestamp || ""}`;
    rows.push({
      key,
      raw: item,
      task: mode ? `${dataset || "—"} · ${mode}` : dataset || "—",
      run_id: item.run_id || null,
      ts: item.timestamp || null,
      level: item.level || null,
      dataset: dataset || "—",
      mode,
      status: payload.status,
      note,
    });
  }

  if (rows.length === 0) {
    return (
      <p style={{ fontSize: 13, color: "#6b7280" }}>暂无入库日志</p>
    );
  }

  return (
    <div
      style={{
        width: "100%",
        overflowX: "auto",
        marginTop: 4,
        marginBottom: 8,
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
          <tr style={{ background: "#f3f4f6" }}>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              选择
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              任务内容
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              运行ID
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              日志时间
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              级别
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              数据集
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              模式
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              状态
            </th>
            <th style={{ padding: 6, borderBottom: "1px solid #e5e7eb" }}>
              备注
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const checked = selectedKeys.includes(r.key);
            return (
              <tr key={r.key}>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) =>
                      onToggleItem(r.raw, e.target.checked, r.key)
                    }
                  />
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {r.task}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {r.run_id}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {formatDateTime(r.ts || undefined)}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {r.level}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {r.dataset}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {r.mode || "—"}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {r.status || "—"}
                </td>
                <td
                  style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}
                >
                  {r.note || "—"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function TestingTab() {
  const [schedules, setSchedules] = useState<any[]>([]);
  const [runs, setRuns] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadAll = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [s, r] = await Promise.all([
        backendRequest("GET", "/api/testing/schedule"),
        backendRequest("GET", "/api/testing/runs?limit=50"),
      ]);
      setSchedules(Array.isArray(s?.items) ? s.items : []);
      setRuns(Array.isArray(r?.items) ? r.items : []);
    } catch (e: any) {
      setError(e?.message || "加载测试调度与历史失败");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadAll();
  }, [loadAll]);

  const triggerRunNow = async () => {
    try {
      await backendRequest("POST", "/api/testing/run", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ triggered_by: "ui" }),
      });
      await loadAll();
    } catch (e: any) {
      setError(e?.message || "测试任务提交失败");
    }
  };

  const createSchedule = async (
    frequency: string,
    enabled: boolean,
  ): Promise<void> => {
    try {
      await backendRequest("POST", "/api/testing/schedule", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ frequency: frequency || "5m", enabled }),
      });
      await loadAll();
    } catch (e: any) {
      setError(e?.message || "测试调度创建失败");
    }
  };

  const updateSchedule = async (
    schedId: string,
    frequency: string,
    enabled: boolean,
  ) => {
    try {
      await backendRequest("POST", "/api/testing/schedule", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          schedule_id: schedId,
          frequency,
          enabled,
        }),
      });
      await loadAll();
    } catch (e: any) {
      setError(e?.message || "测试调度更新失败");
    }
  };

  const toggleSchedule = async (schedId: string, enabled: boolean) => {
    try {
      await backendRequest(
        "POST",
        `/api/testing/schedule/${schedId}/toggle`,
        {
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled }),
        },
      );
      await loadAll();
    } catch (e: any) {
      setError(e?.message || "切换测试调度状态失败");
    }
  };

  const runSchedule = async (schedId: string) => {
    try {
      await backendRequest(
        "POST",
        `/api/testing/schedule/${schedId}/run`,
        {},
      );
      await loadAll();
    } catch (e: any) {
      setError(e?.message || "触发调度运行失败");
    }
  };

  const [newFreq, setNewFreq] = useState<string>("5m");
  const [newEnabled, setNewEnabled] = useState<boolean>(true);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <h3 style={{ marginTop: 0, fontSize: 15 }}>🧪 TDX 接口自动化测试</h3>
      <div style={{ display: "flex", gap: 8, marginBottom: 4 }}>
        <button
          type="button"
          onClick={triggerRunNow}
          disabled={loading}
          style={{
            padding: "6px 10px",
            borderRadius: 8,
            border: "none",
            background: "#22c55e",
            color: "#fff",
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          立即执行测试
        </button>
        <button
          type="button"
          onClick={loadAll}
          disabled={loading}
          style={{
            padding: "6px 10px",
            borderRadius: 8,
            border: "1px solid #d4d4d4",
            background: "#fff",
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          刷新状态
        </button>
      </div>

      {error && (
        <p style={{ fontSize: 12, color: "#b91c1c" }}>{error}</p>
      )}

      <div style={{ marginTop: 4 }}>
        <h4 style={{ fontSize: 14, margin: "4px 0" }}>测试调度</h4>
        {schedules.length === 0 && !loading ? (
          <p style={{ fontSize: 13, color: "#6b7280" }}>
            尚未配置测试调度，使用下方表单新建。
          </p>
        ) : (
          <div
            style={{ display: "flex", flexDirection: "column", gap: 8 }}
          >
            {schedules.map((item: any) => {
              const schedId = item.schedule_id;
              const enabled = item.enabled ?? true;
              const freqValue = item.frequency || "";
              const freqLabel =
                FREQUENCY_CHOICES.find((f) => f.value === freqValue)?.label ||
                (freqValue || "手动");

              return (
                <div
                  key={schedId}
                  style={{
                    borderRadius: 10,
                    border: "1px solid #e5e7eb",
                    padding: 10,
                    background: "#fafafa",
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                      marginBottom: 4,
                      fontSize: 13,
                    }}
                  >
                    <div>调度 {schedId}</div>
                    <div style={{ fontSize: 12, color: "#6b7280" }}>
                      {enabled ? "🟢 启用" : "⚪️ 停用"}
                    </div>
                  </div>
                  <div
                    style={{
                      fontSize: 12,
                      color: "#4b5563",
                      marginBottom: 4,
                    }}
                  >
                    <div>调度频率：{freqLabel}</div>
                    <div>
                      上次运行：{item.last_run_at || "—"} · 下次运行：
                      {item.next_run_at || "—"}
                    </div>
                    <div>
                      上次状态：{item.last_status || "—"} · 错误信息：
                      {item.last_error || "—"}
                    </div>
                  </div>
                  <div
                    style={{
                      display: "flex",
                      flexWrap: "wrap",
                      gap: 8,
                      alignItems: "center",
                      fontSize: 12,
                    }}
                  >
                    <select
                      value={freqValue}
                      onChange={async (e) => {
                        await updateSchedule(
                          schedId,
                          e.target.value,
                          enabled,
                        );
                      }}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 6,
                        border: "1px solid #d4d4d4",
                        fontSize: 12,
                      }}
                    >
                      {FREQUENCY_CHOICES.map((f) => (
                        <option key={f.value} value={f.value}>
                          {f.label}
                        </option>
                      ))}
                    </select>
                    <label>
                      <input
                        type="checkbox"
                        checked={enabled}
                        onChange={async (e) => {
                          await updateSchedule(
                            schedId,
                            freqValue,
                            e.target.checked,
                          );
                        }}
                        style={{ marginRight: 4 }}
                      />
                      启用调度
                    </label>
                    <button
                      type="button"
                      onClick={() => toggleSchedule(schedId, !enabled)}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 6,
                        border: "1px solid #d4d4d4",
                        background: "#fff",
                        cursor: "pointer",
                      }}
                    >
                      切换启用
                    </button>
                    <button
                      type="button"
                      onClick={() => runSchedule(schedId)}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 6,
                        border: "1px solid #d4d4d4",
                        background: "#fff",
                        cursor: "pointer",
                      }}
                    >
                      立即运行
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      <div
        style={{
          marginTop: 8,
          paddingTop: 8,
          borderTop: "1px dashed #e5e7eb",
          fontSize: 13,
        }}
      >
        <h4 style={{ fontSize: 14, margin: "4px 0" }}>新建测试调度</h4>
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 8,
            alignItems: "center",
          }}
        >
          <select
            value={newFreq}
            onChange={(e) => setNewFreq(e.target.value)}
            style={{
              padding: "4px 8px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
              fontSize: 12,
            }}
          >
            {FREQUENCY_CHOICES.map((f) => (
              <option key={f.value} value={f.value}>
                {f.label}
              </option>
            ))}
          </select>
          <label style={{ fontSize: 12 }}>
            <input
              type="checkbox"
              checked={newEnabled}
              onChange={(e) => setNewEnabled(e.target.checked)}
              style={{ marginRight: 4 }}
            />
            启用调度
          </label>
          <button
            type="button"
            onClick={() => createSchedule(newFreq, newEnabled)}
            disabled={loading}
            style={{
              padding: "6px 10px",
              borderRadius: 8,
              border: "none",
              background: "#6366f1",
              color: "#fff",
              cursor: "pointer",
              fontSize: 13,
            }}
          >
            创建调度
          </button>
        </div>
      </div>

      <div
        style={{
          marginTop: 10,
          borderTop: "1px dashed #e5e7eb",
          paddingTop: 8,
        }}
      >
        <h4 style={{ fontSize: 14, margin: "4px 0" }}>最近测试执行</h4>
        <TestingRunsTable runs={runs} />
      </div>
    </div>
  );
}

function IngestionSchedulesTab() {
  const [schedules, setSchedules] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [newDataset, setNewDataset] = useState<string>("kline_daily_qfq");
  const [newMode, setNewMode] = useState<"incremental" | "init">(
    "incremental",
  );
  const [newFreq, setNewFreq] = useState<string>("5m");
  const [newEnabled, setNewEnabled] = useState<boolean>(true);

  const loadSchedules = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data: any = await backendRequest("GET", "/api/ingestion/schedule");
      setSchedules(Array.isArray(data?.items) ? data.items : []);
    } catch (e: any) {
      setError(e?.message || "加载入库调度失败");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadSchedules();
  }, [loadSchedules]);

  const createDefaults = async () => {
    try {
      await backendRequest("POST", "/api/ingestion/schedule/defaults", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "创建默认调度失败");
    }
  };

  const runManual = async (dataset: string, mode: string) => {
    try {
      await backendRequest("POST", "/api/ingestion/run", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataset,
          mode,
          triggered_by: "ui",
        }),
      });
    } catch (e: any) {
      setError(e?.message || "入库任务提交失败");
    }
  };

  const updateSchedule = async (
    schedId: string,
    dataset: string,
    mode: string,
    frequency: string,
    enabled: boolean,
  ) => {
    try {
      await backendRequest("POST", "/api/ingestion/schedule", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          schedule_id: schedId,
          dataset,
          mode,
          frequency,
          enabled,
        }),
      });
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "入库调度更新失败");
    }
  };

  const toggleSchedule = async (schedId: string, enabled: boolean) => {
    try {
      await backendRequest(
        "POST",
        `/api/ingestion/schedule/${schedId}/toggle`,
        {
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled }),
        },
      );
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "切换入库调度状态失败");
    }
  };

  const runSchedule = async (schedId: string) => {
    try {
      await backendRequest("POST", `/api/ingestion/schedule/${schedId}/run`, {});
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "触发入库调度运行失败");
    }
  };

  const createSchedule = async () => {
    try {
      await backendRequest("POST", "/api/ingestion/schedule", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataset: newDataset,
          mode: newMode,
          frequency: newFreq || "5m",
          enabled: newEnabled,
        }),
      });
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "入库调度创建失败");
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <h3 style={{ marginTop: 0, fontSize: 15 }}>📥 数据入库调度</h3>

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 10,
          alignItems: "center",
          marginBottom: 6,
        }}
      >
        <button
          type="button"
          onClick={createDefaults}
          disabled={loading}
          style={{
            padding: "6px 10px",
            borderRadius: 8,
            border: "none",
            background: "#0ea5e9",
            color: "#fff",
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          创建默认调度
        </button>
      </div>

      <div
        style={{
          padding: 10,
          borderRadius: 10,
          border: "1px solid #e5e7eb",
          background: "#fafafa",
          marginBottom: 8,
        }}
      >
        <h4 style={{ fontSize: 14, margin: "0 0 6px" }}>手动执行入库任务</h4>
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 8,
            alignItems: "center",
          }}
        >
          <select
            value={newDataset}
            onChange={(e) => setNewDataset(e.target.value)}
            style={{
              padding: "4px 8px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
              fontSize: 12,
            }}
          >
            {Object.entries(INGESTION_DATASETS).map(([key, label]) => (
              <option key={key} value={key}>{`${key} · ${label}`}</option>
            ))}
          </select>
          <label style={{ fontSize: 12 }}>
            <input
              type="radio"
              checked={newMode === "incremental"}
              onChange={() => setNewMode("incremental")}
              style={{ marginRight: 4 }}
            />
            增量
          </label>
          <label style={{ fontSize: 12 }}>
            <input
              type="radio"
              checked={newMode === "init"}
              onChange={() => setNewMode("init")}
              style={{ marginRight: 4 }}
            />
            初始化
          </label>
          <button
            type="button"
            onClick={() => runManual(newDataset, newMode)}
            disabled={loading}
            style={{
              padding: "6px 10px",
              borderRadius: 8,
              border: "none",
              background: "#22c55e",
              color: "#fff",
              cursor: "pointer",
              fontSize: 13,
            }}
          >
            立即执行
          </button>
        </div>
      </div>

      {error && (
        <p style={{ fontSize: 12, color: "#b91c1c" }}>{error}</p>
      )}

      <div>
        <h4 style={{ fontSize: 14, margin: "0 0 6px" }}>已配置的入库调度</h4>
        {schedules.length === 0 && !loading ? (
          <p style={{ fontSize: 13, color: "#6b7280" }}>
            尚未配置入库调度，使用下方表单新建。
          </p>
        ) : (
          <div
            style={{ display: "flex", flexDirection: "column", gap: 8 }}
          >
            {schedules.map((item: any) => {
              const schedId = item.schedule_id;
              const dataset = item.dataset;
              const mode = item.mode;
              const enabled = item.enabled ?? true;
              const freqValue = item.frequency || "";
              const freqLabel =
                FREQUENCY_CHOICES.find((f) => f.value === freqValue)?.label ||
                (freqValue || "手动");

              return (
                <div
                  key={schedId}
                  style={{
                    borderRadius: 10,
                    border: "1px solid #e5e7eb",
                    padding: 10,
                    background: "#fafafa",
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                      marginBottom: 4,
                      fontSize: 13,
                    }}
                  >
                    <div>
                      调度 {schedId} · {dataset} · {mode}
                    </div>
                    <div style={{ fontSize: 12, color: "#6b7280" }}>
                      {enabled ? "🟢 启用" : "⚪️ 停用"}
                    </div>
                  </div>
                  <div
                    style={{
                      fontSize: 12,
                      color: "#4b5563",
                      marginBottom: 4,
                    }}
                  >
                    <div>调度频率：{freqLabel}</div>
                    <div>
                      上次运行：{item.last_run_at || "—"} · 下次运行：
                      {item.next_run_at || "—"}
                    </div>
                    <div>
                      上次状态：{item.last_status || "—"} · 错误信息：
                      {item.last_error || "—"}
                    </div>
                  </div>
                  <div
                    style={{
                      display: "flex",
                      flexWrap: "wrap",
                      gap: 8,
                      alignItems: "center",
                      fontSize: 12,
                    }}
                  >
                    <select
                      value={freqValue}
                      onChange={async (e) => {
                        await updateSchedule(
                          schedId,
                          dataset,
                          mode,
                          e.target.value,
                          enabled,
                        );
                      }}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 6,
                        border: "1px solid #d4d4d4",
                        fontSize: 12,
                      }}
                    >
                      {FREQUENCY_CHOICES.map((f) => (
                        <option key={f.value} value={f.value}>
                          {f.label}
                        </option>
                      ))}
                    </select>
                    <label>
                      <input
                        type="checkbox"
                        checked={enabled}
                        onChange={async (e) => {
                          await updateSchedule(
                            schedId,
                            dataset,
                            mode,
                            freqValue,
                            e.target.checked,
                          );
                        }}
                        style={{ marginRight: 4 }}
                      />
                      启用调度
                    </label>
                    <button
                      type="button"
                      onClick={() => toggleSchedule(schedId, !enabled)}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 6,
                        border: "1px solid #d4d4d4",
                        background: "#fff",
                        cursor: "pointer",
                      }}
                    >
                      切换启用
                    </button>
                    <button
                      type="button"
                      onClick={() => runSchedule(schedId)}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 6,
                        border: "1px solid #d4d4d4",
                        background: "#fff",
                        cursor: "pointer",
                      }}
                    >
                      立即运行
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      <div
        style={{
          marginTop: 10,
          paddingTop: 8,
          borderTop: "1px dashed #e5e7eb",
        }}
      >
        <h4 style={{ fontSize: 14, margin: "0 0 6px" }}>新建入库调度</h4>
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 8,
            alignItems: "center",
            fontSize: 12,
          }}
        >
          <select
            value={newDataset}
            onChange={(e) => setNewDataset(e.target.value)}
            style={{
              padding: "4px 8px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
              fontSize: 12,
            }}
          >
            {Object.entries(INGESTION_DATASETS).map(([key, label]) => (
              <option key={key} value={key}>{`${key} · ${label}`}</option>
            ))}
          </select>
          <label>
            <input
              type="radio"
              checked={newMode === "incremental"}
              onChange={() => setNewMode("incremental")}
              style={{ marginRight: 4 }}
            />
            增量
          </label>
          <label>
            <input
              type="radio"
              checked={newMode === "init"}
              onChange={() => setNewMode("init")}
              style={{ marginRight: 4 }}
            />
            初始化
          </label>
          <select
            value={newFreq}
            onChange={(e) => setNewFreq(e.target.value)}
            style={{
              padding: "4px 8px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
              fontSize: 12,
            }}
          >
            {FREQUENCY_CHOICES.map((f) => (
              <option key={f.value} value={f.value}>
                {f.label}
              </option>
            ))}
          </select>
          <label>
            <input
              type="checkbox"
              checked={newEnabled}
              onChange={(e) => setNewEnabled(e.target.checked)}
              style={{ marginRight: 4 }}
            />
            启用调度
          </label>
          <button
            type="button"
            onClick={createSchedule}
            disabled={loading}
            style={{
              padding: "6px 10px",
              borderRadius: 8,
              border: "none",
              background: "#6366f1",
              color: "#fff",
              cursor: "pointer",
              fontSize: 13,
            }}
          >
            创建调度
          </button>
        </div>
      </div>
    </div>
  );
}

function LogsTab() {
  const [logsLimit, setLogsLimit] = useState<number>(50);
  const [testingRuns, setTestingRuns] = useState<any[]>([]);
  const [ingestionLogs, setIngestionLogs] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [testingOffset, setTestingOffset] = useState<number>(0);
  const [logsOffset, setLogsOffset] = useState<number>(0);
  const [selectedLogKeys, setSelectedLogKeys] = useState<string[]>([]);
  const [testingTotal, setTestingTotal] = useState<number>(0);
  const [logsTotal, setLogsTotal] = useState<number>(0);

  const loadLogs = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [runs, logs] = await Promise.all([
        backendRequest(
          "GET",
          `/api/testing/runs?limit=30&offset=${testingOffset}`,
        ),
        backendRequest(
          "GET",
          `/api/ingestion/logs?limit=${logsLimit}&offset=${logsOffset}`,
        ),
      ]);
      setTestingRuns(Array.isArray(runs?.items) ? runs.items : []);
      setIngestionLogs(Array.isArray(logs?.items) ? logs.items : []);
      setTestingTotal(Number(runs?.total || 0));
      setLogsTotal(Number(logs?.total || 0));
    } catch (e: any) {
      setError(e?.message || "加载日志失败");
    } finally {
      setLoading(false);
    }
  }, [logsLimit, testingOffset, logsOffset]);

  useEffect(() => {
    loadLogs();
  }, [loadLogs]);

  // 当翻页或修改每页条数时，自动清空当前所有选择，实现“每页独立选择”语义
  useEffect(() => {
    setSelectedLogKeys([]);
  }, [logsOffset, logsLimit]);

  const makeLogKey = (item: any): string => {
    return `${item.run_id || ""}||${item.timestamp || ""}`;
  };

  const handleToggleLogItem = (
    item: any,
    checked: boolean,
    key: string,
  ) => {
    setSelectedLogKeys((prev) => {
      if (checked) {
        if (prev.includes(key)) return prev;
        return [...prev, key];
      }
      return prev.filter((k) => k !== key);
    });
  };

  const handleSelectAllLogsOnPage = () => {
    const keysOnPage = (ingestionLogs || []).map((it: any) => makeLogKey(it));
    setSelectedLogKeys((prev) => {
      const set = new Set(prev);
      for (const k of keysOnPage) {
        set.add(k);
      }
      return Array.from(set);
    });
  };

  const handleClearLogSelection = () => {
    setSelectedLogKeys([]);
  };

  const handleDeleteSelectedLogs = async () => {
    if (!selectedLogKeys.length) return;
    if (!window.confirm("确认删除当前页选中的入库日志记录？")) return;
    try {
      const items = (ingestionLogs || [])
        .filter((it: any) => selectedLogKeys.includes(makeLogKey(it)))
        .map((it: any) => ({ job_id: it.run_id, ts: it.timestamp }));
      await backendRequest("DELETE", "/api/ingestion/logs", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items, delete_all: false }),
      });
      setSelectedLogKeys([]);
      await loadLogs();
    } catch (e: any) {
      setError(e?.message || "删除入库日志失败");
    }
  };

  const handleDeleteAllLogs = async () => {
    if (!window.confirm("确认删除全部入库运行日志？该操作不可恢复！")) return;
    try {
      await backendRequest("DELETE", "/api/ingestion/logs", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ delete_all: true }),
      });
      setSelectedLogKeys([]);
      await loadLogs();
    } catch (e: any) {
      setError(e?.message || "删除全部入库日志失败");
    }
  };

  const handleDeleteAllTestingRuns = async () => {
    if (!window.confirm("确认删除全部测试执行记录？该操作不可恢复！")) return;
    try {
      await backendRequest("DELETE", "/api/testing/runs", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ delete_all: true }),
      });
      await loadLogs();
    } catch (e: any) {
      setError(e?.message || "删除测试执行记录失败");
    }
  };

  // 计算分页信息（测试执行记录与入库日志各自独立）
  const testingPageSize = 30;
  const testingTotalPages = Math.max(
    1,
    Math.ceil((testingTotal || 0) / testingPageSize),
  );
  const testingCurrentPage = Math.min(
    testingTotalPages,
    Math.floor(testingOffset / testingPageSize) + 1,
  );

  const logsTotalPages = Math.max(
    1,
    Math.ceil((logsTotal || 0) / Math.max(1, logsLimit)),
  );
  const logsCurrentPage = Math.min(
    logsTotalPages,
    Math.floor(logsOffset / Math.max(1, logsLimit)) + 1,
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <h3 style={{ marginTop: 0, fontSize: 15 }}>📝 执行日志</h3>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          marginBottom: 4,
        }}
      >
        <label style={{ fontSize: 13 }}>
          日志条数
          <input
            type="number"
            min={10}
            max={200}
            step={10}
            value={logsLimit}
            onChange={(e) => {
              const v = Number(e.target.value) || 50;
              const clamped = Math.min(200, Math.max(10, v));
              setLogsLimit(clamped);
              setLogsOffset(0);
            }}
            style={{
              marginLeft: 4,
              width: 80,
              padding: "4px 6px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
            }}
          />
        </label>
        <button
          type="button"
          onClick={loadLogs}
          disabled={loading}
          style={{
            padding: "6px 10px",
            borderRadius: 8,
            border: "1px solid #d4d4d4",
            background: "#fff",
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          {loading ? "刷新中..." : "刷新日志"}
        </button>
        <button
          type="button"
          onClick={handleDeleteAllTestingRuns}
          disabled={loading}
          style={{
            padding: "6px 10px",
            borderRadius: 8,
            border: "1px solid #d4d4d4",
            background: "#fff7ed",
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          清空测试执行记录
        </button>
      </div>

      {error && (
        <p style={{ fontSize: 12, color: "#b91c1c" }}>{error}</p>
      )}

      <div
        style={{
          marginTop: 4,
          borderTop: "1px dashed #e5e7eb",
          paddingTop: 6,
        }}
      >
        <h4 style={{ fontSize: 14, margin: "0 0 4px" }}>测试执行记录</h4>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            marginBottom: 4,
          }}
        >
          <button
            type="button"
            onClick={() =>
              setTestingOffset((prev) =>
                Math.max(0, prev - testingPageSize),
              )
            }
            disabled={loading || testingOffset <= 0}
            style={{
              padding: "4px 8px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
              background: "#fff",
              cursor: "pointer",
              fontSize: 12,
            }}
          >
            上一页
          </button>
          <button
            type="button"
            onClick={() =>
              setTestingOffset((prev) => prev + testingPageSize)
            }
            disabled={
              loading || testingOffset + testingPageSize >= testingTotal
            }
            style={{
              padding: "4px 8px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
              background: "#fff",
              cursor: "pointer",
              fontSize: 12,
            }}
          >
            下一页
          </button>
          <span style={{ fontSize: 12, color: "#6b7280" }}>
            第 {testingCurrentPage} / {testingTotalPages} 页
          </span>
        </div>
        <TestingRunsTable runs={testingRuns} />
      </div>

      <div
        style={{
          marginTop: 8,
          borderTop: "1px dashed #e5e7eb",
          paddingTop: 6,
        }}
      >
        <h4 style={{ fontSize: 14, margin: "0 0 4px" }}>入库运行日志</h4>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            marginBottom: 4,
          }}
        >
          <button
            type="button"
            onClick={() =>
              setLogsOffset((prev) => Math.max(0, prev - logsLimit))
            }
            disabled={loading || logsOffset <= 0}
            style={{
              padding: "4px 8px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
              background: "#fff",
              cursor: "pointer",
              fontSize: 12,
            }}
          >
            上一页
          </button>
          <button
            type="button"
            onClick={() => setLogsOffset((prev) => prev + logsLimit)}
            disabled={
              loading || logsOffset + logsLimit >= logsTotal || logsTotal === 0
            }
            style={{
              padding: "4px 8px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
              background: "#fff",
              cursor: "pointer",
              fontSize: 12,
            }}
          >
            下一页
          </button>
          <span style={{ fontSize: 12, color: "#6b7280" }}>
            第 {logsCurrentPage} / {logsTotalPages} 页
          </span>
          <button
            type="button"
            onClick={handleSelectAllLogsOnPage}
            disabled={ingestionLogs.length === 0}
            style={{
              padding: "4px 8px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
              background: "#f9fafb",
              cursor: "pointer",
              fontSize: 12,
            }}
          >
            本页全选
          </button>
          <button
            type="button"
            onClick={handleClearLogSelection}
            disabled={!selectedLogKeys.length}
            style={{
              padding: "4px 8px",
              borderRadius: 6,
              border: "1px solid #d4d4d4",
              background: "#fff",
              cursor: "pointer",
              fontSize: 12,
            }}
          >
            清除选择
          </button>
          <button
            type="button"
            onClick={handleDeleteSelectedLogs}
            disabled={!selectedLogKeys.length || loading}
            style={{
              padding: "6px 10px",
              borderRadius: 8,
              border: "1px solid #f97316",
              background: "#fffbeb",
              cursor: "pointer",
              fontSize: 13,
              color: "#c2410c",
            }}
          >
            删除选中日志
          </button>
          <button
            type="button"
            onClick={handleDeleteAllLogs}
            disabled={loading}
            style={{
              padding: "6px 10px",
              borderRadius: 8,
              border: "1px solid #dc2626",
              background: "#fef2f2",
              cursor: "pointer",
              fontSize: 13,
              color: "#b91c1c",
            }}
          >
            清空全部入库日志
          </button>
        </div>
        <IngestionLogsTable
          logs={ingestionLogs}
          selectedKeys={selectedLogKeys}
          onToggleItem={handleToggleLogItem}
        />
      </div>
    </div>
  );
}
