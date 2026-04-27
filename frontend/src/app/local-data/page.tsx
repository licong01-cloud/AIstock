"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import styles from "./localData.module.css";

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
  | "miniqmt"
  | "jobs"
  | "stats"
  | "testing"
  | "schedules"
  | "factor_metrics_schedules"
  | "logs";

type DataSource = "TDX" | "Tushare" | "xtquant" | "News";

const FREQUENCY_CHOICES: { label: string; value: string }[] = [
  { label: "手动 (不调度)", value: "" },
  { label: "10 秒", value: "10s" },
  { label: "15 秒", value: "15s" },
  { label: "30 秒", value: "30s" },
  { label: "5 分钟", value: "5m" },
  { label: "10 分钟", value: "10m" },
  { label: "15 分钟", value: "15m" },
  { label: "30 分钟", value: "30m" },
  { label: "1 小时", value: "1h" },
  { label: "每日", value: "daily" },
];

const INGESTION_DATASETS_BY_SOURCE: Record<DataSource, Record<string, string>> = {
  TDX: {
    kline_daily_raw: "日线（未复权 RAW）",
    kline_daily_raw_go: "日线（未复权 RAW · Go 直连）",
  },
  Tushare: {
    stock_moneyflow_ts: "个股资金流（moneyflow · Tushare）",
    daily_basic: "股票每日指标（Tushare daily_basic）",
    index_daily: "指数日线行情（Tushare index_daily）",
    cyq_perf: "筹码分布绩效（Tushare cyq_perf）",
    cyq_chips: "每日筹码分布（Tushare cyq_chips）",
    sw_sector: "申万行业板块（分类+成分股+日线）",
    sw_index_classify: "申万行业分类（Tushare index_classify）",
    sw_index_member: "申万行业成分股PIT映射（Tushare index_member_all）",
    sw_daily: "申万行业指数日线（Tushare sw_daily）",
    sector_data: "申万L2行业展开到个股（22列，后处理）",
    suspend_d: "Tushare suspend_d daily suspend/resume info",
    stk_limit: "每日涨跌停价格（Tushare stk_limit）",
    margin_detail: "融资融券明细（Tushare margin_detail）",
  },
  xtquant: {
    xtquant_pershare_index: "每股主要指标（PershareIndex）",
  },
  News: {
    news_realtime: "新闻实时入库（多源快讯）",
  },
};

// 合并所有数据集为平面映射（用于任务列表显示）
const INGESTION_DATASETS: Record<string, string> = Object.values(
  INGESTION_DATASETS_BY_SOURCE
).reduce((acc, sourceMap) => ({ ...acc, ...sourceMap }), {});

// 支持truncate操作的数据集列表（初始化时可清空表）
const TRUNCABLE_DATASETS: string[] = [
  // Tushare数据集
  "adj_factor",
  "stock_moneyflow_ts",
  "stock_st",
  "bak_basic",
  "stk_limit",
  "suspend_d",
  "margin_detail",
  "daily_basic",
  "anns_d",
  "cyq_perf",
  "cyq_chips",
  "stock_basic",
  "sw_index_classify",
  "sw_index_member",
  "sw_daily",
  "sw_sector",
  "sector_data",
  "index_daily",
  "index_basic",
  // TDX数据集
  "kline_daily_raw",
  "kline_minute_raw",
  "minute_1m",
  // xtquant数据集
  "xtquant_pershare_index",
];

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
  latestTradingDate?: string | null;
  currentMaxDate?: string | null;
  hasData?: boolean;
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
        `后端请求失败: HTTP ${res.status} ${res.statusText}${
          content ? ` | ${content}` : ""
        }`,
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
    (
      kind: string,
      startDate: string,
      latestTradingDay: string,
      currentMaxDate?: string | null,
    ) => {
      const lower = (kind || "").toLowerCase();
      let dataSource: DataSource = "TDX";
      let dataset: string | undefined;
      let symbolsScope: "watchlist" | "all" | undefined;

      if (lower === "kline_daily_raw_go" || lower === "kline_daily_raw") {
        dataset = "kline_daily_raw_go";
      } else if (lower === "kline_minute_raw") {
        dataset = "kline_minute_raw";
      } else if (lower === "stock_moneyflow_ts") {
        dataSource = "Tushare";
        dataset = "stock_moneyflow_ts";
      } else if (lower === "stock_st") {
        dataSource = "Tushare";
        dataset = "stock_st";
      } else if (lower === "bak_basic") {
        dataSource = "Tushare";
        dataset = "bak_basic";
      } else if (lower === "anns_d") {
        dataSource = "Tushare";
        dataset = "anns_d";
      } else if (lower === "index_daily") {
        dataSource = "Tushare";
        dataset = "index_daily";
      } else if (lower === "adj_factor") {
        dataSource = "Tushare";
        dataset = "adj_factor";
      } else if (lower === "cyq_perf") {
        dataSource = "Tushare";
        dataset = "cyq_perf";
      } else if (lower === "cyq_chips") {
        dataSource = "Tushare";
        dataset = "cyq_chips";
      } else if (lower === "daily_basic") {
        dataSource = "Tushare";
        dataset = "daily_basic";
      } else if (lower === "stk_limit") {
        dataSource = "Tushare";
        dataset = "stk_limit";
      } else if (lower === "suspend_d") {
        dataSource = "Tushare";
        dataset = "suspend_d";
      } else if (lower === "stock_basic") {
        dataSource = "Tushare";
        dataset = "stock_basic";
      } else if (lower === "margin_detail") {
        dataSource = "Tushare";
        dataset = "margin_detail";
      } else if (lower === "xtquant_pershare_index") {
        dataSource = "xtquant";
        dataset = "xtquant_pershare_index";
      } else {
        return;
      }
      setIncrementalPrefill({
        dataSource,
        dataset,
        targetDate: latestTradingDay,
        startDate: startDate || null,
        symbolsScope,
        latestTradingDate: latestTradingDay || null,
        currentMaxDate: currentMaxDate ?? null,
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
    { key: "miniqmt", label: "miniQMT 数据" },
    { key: "jobs", label: "任务监视器" },
    { key: "stats", label: "数据看板" },
    { key: "testing", label: "数据源测试" },
    { key: "schedules", label: "数据入库调度" },
    { key: "factor_metrics_schedules", label: "因子指标调度" },
    { key: "logs", label: "运行日志" },
  ];

  return (
    <main className={styles.page}>
      <section className={styles.sectionBlock}>
        <h1 className={styles.sectionHeading}>🗄️ 本地数据管理</h1>
        <p className={styles.sectionSubtext}>
          集中管理 TDX 接口测试与数据入库调度，支持手动与自动执行。
        </p>
      </section>

      <section className={`${styles.heroCard} ${styles.sectionBlock}`}>
        <div className={styles.rowBetweenWrap}>
          <div>
            <div className={styles.textMain}>
              当前调度后端地址：
              <code className={styles.codeChip}>{backendBaseDisplay}</code>
            </div>
            <div className={styles.textMuted}>
              启动命令示例：
              <code className={styles.codeChip}>
                uvicorn backend.main:app --host 0.0.0.0 --port 8001
              </code>
            </div>
          </div>

          <div className={styles.rowWrap}>
            <button
              type="button"
              onClick={handlePing}
              disabled={pingLoading}
              className={styles.btnPrimary}
              aria-label="测试调度后端连接"
            >
              {pingLoading ? "测试连接中..." : "测试连接"}
            </button>
            {pingResult && (
              <span
                className={styles.textSmall}
                style={{ color: pingResult.ok ? "#16a34a" : "#dc2626" }}
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
      <section className={styles.sectionBlock}>
        <div className={styles.tabBar}>
          {tabs.map((tab) => (
            <button
              key={tab.key}
              type="button"
              onClick={() => setActiveTab(tab.key)}
              data-testid={`local-data-tab-${tab.key}`}
              className={classNames(
                styles.tabBtn,
                activeTab === tab.key && styles.tabBtnActive,
              )}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </section>

      {/* 内容区域 */}
      <section className={styles.contentCard}>
        {activeTab === "init" && <InitTab />}
        {activeTab === "incremental" && (
          <IncrementalTab
            prefill={incrementalPrefill}
            onPrefillConsumed={() => setIncrementalPrefill(null)}
          />
        )}
        {activeTab === "adjust" && <AdjustTab />}
        {activeTab === "miniqmt" && <MiniQMTTab onFillLatest={handleFillLatestFromStats} />}
        {activeTab === "jobs" && <JobsTab />}
        {activeTab === "stats" && (
          <DataStatsTab onFillLatest={handleFillLatestFromStats} />
        )}
        {activeTab === "testing" && <TestingTab />}
        {activeTab === "schedules" && <IngestionSchedulesTab />}
        {activeTab === "factor_metrics_schedules" && <FactorMetricsSchedulesTab />}
        {activeTab === "logs" && <LogsTab />}
      </section>
    </main>
  );
}

function InitTab() {
  const [dataSource, setDataSource] = useState<DataSource>("TDX");
  const [dataset, setDataset] = useState<string>("kline_daily_raw_go");
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

  const jobIdRef = useRef<string | null>(jobId);
  const autoRefreshRef = useRef<boolean>(autoRefresh);

  useEffect(() => {
    jobIdRef.current = jobId;
  }, [jobId]);

  useEffect(() => {
    autoRefreshRef.current = autoRefresh;
  }, [autoRefresh]);

  // 指数日线 index_daily 的 market 多选，默认勾选 CSI/SSE/SZSE
  const [indexMarkets, setIndexMarkets] = useState<string[]>(["CSI", "SSE", "SZSE"]);

  const datasetOptionsTDX: { key: string; label: string }[] = [
    {
      key: "kline_daily_raw_go",
      label: "kline_daily_raw_go · 日线（未复权 RAW · Go 直连）",
    },
    {
      key: "kline_minute_raw",
      label: "kline_minute_raw · 1 分钟原始（TDX 全量）",
    },
    {
      key: "symbol_dim",
      label: "symbol_dim · 股票基础信息（TDX /api/codes）",
    },
  ];

  const datasetOptionsTushare: { key: string; label: string }[] = [
    { key: "kline_weekly", label: "kline_weekly · 周线（由本地日线QFQ聚合）" },
    {
      key: "stock_moneyflow_ts",
      label: "stock_moneyflow_ts · 个股资金流 (Tushare moneyflow_ts)",
    },
    { key: "stock_basic", label: "stock_basic · 最新股票列表" },
    {
      key: "index_basic",
      label: "index_basic · 指数基础信息（Tushare index_basic）",
    },
    {
      key: "index_daily",
      label: "index_daily · 指数日线行情（Tushare index_daily）",
    },
    { key: "stock_st", label: "stock_st · ST 股票列表" },
    { key: "bak_basic", label: "bak_basic · 历史股票列表" },
    { key: "daily_basic", label: "daily_basic · 股票每日指标（Tushare）" },
    { key: "stk_limit", label: "stk_limit · 每日涨跌停价格（Tushare）" },
    { key: "suspend_d", label: "suspend_d - Daily suspend/resume info (Tushare)" },
    {
      key: "anns_d",
      label: "anns_d · 上市公司公告（Tushare anns_d）",
    },
    {
      key: "tushare_trade_cal",
      label: "tushare_trade_cal · 交易日历（Tushare trade_cal 同步）",
    },
    {
      key: "cyq_perf",
      label: "cyq_perf · 每日筹码及胜率（Tushare）",
    },
    {
      key: "cyq_chips",
      label: "cyq_chips · 每日筹码分布（Tushare）",
    },
    {
      key: "sw_sector",
      label: "sw_sector · 申万行业板块（分类+成分股+日线，统一同步）",
    },
    {
      key: "sector_data",
      label: "sector_data · 申万L2行业展开到个股（22列，后处理）",
    },
    {
      key: "margin_detail",
      label: "margin_detail · 融资融券交易明细（Tushare）",
    },
  ];

  const datasetOptionsXtquant: { key: string; label: string }[] = [
    {
      key: "xtquant_pershare_index",
      label: "xtquant_pershare_index · 每股主要指标（PershareIndex）",
    },
  ];

  // 根据数据源动态调整默认参数
  useEffect(() => {
    if (dataSource === "TDX") {
      setDataset("kline_daily_raw_go");
      setStartDate("1990-01-01");
      setTruncate(true);
      setConfirmClear(false);
    } else if (dataSource === "xtquant") {
      setDataset("xtquant_pershare_index");
      setStartDate("2010-01-01");
      setEndDate(new Date().toISOString().slice(0, 10));
      setTruncate(false);
      setConfirmClear(false);
    } else {
      const d = new Date();
      const today = d.toISOString().slice(0, 10);
      const ago = new Date(d.getTime() - 365 * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10);
      setDataset("kline_weekly");
      setStartDate(ago);
      setEndDate(today);
      setTruncate(false);
      setConfirmClear(true);
    }
  }, [dataSource]);

  // 针对 stock_basic：默认全量仅需当前日期且强制 truncate 前置
  useEffect(() => {
    if (dataset === "stock_basic") {
      const today = new Date().toISOString().slice(0, 10);
      setStartDate(today);
      setEndDate(today);
      setTruncate(true);
      setConfirmClear(true);
    }
  }, [dataset]);

  const loadJobStatus = useCallback(
    async (id: string) => {
      setJobLoading(true);
      try {
        const data = await backendRequest<IngestionJobStatus>(
          "GET",
          `/api/ingestion/job/${id}`,
        );
        setJobStatus(data);
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

    let timer: ReturnType<typeof setTimeout> | null = null;

    const tick = async () => {
      const currentJobId = jobIdRef.current;
      if (!currentJobId || cancelled) return;
      await loadJobStatus(currentJobId);
      if (!cancelled && autoRefreshRef.current && jobIdRef.current) {
        timer = setTimeout(tick, 5000);
      }
    };

    tick();

    return () => {
      cancelled = true;
      if (timer) {
        clearTimeout(timer);
      }
    };
  }, [jobId, autoRefresh, loadJobStatus]);

  const submitInit = async () => {
    setSubmitting(true);
    setError(null);
    try {
      if (dataSource === "TDX") {
        // TDX 分支
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
          return;
        }

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
        return;
      }

      // xtquant 分支
      if (dataSource === "xtquant") {
        const opts: any = {
          start_date: startDate,
          end_date: endDate,
          workers: Number(workers) || 1,
          truncate: Boolean(truncate),
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
        return;
      }

      // Tushare 分支
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
        return;
      }

      if (
        [
          "adj_factor",
          "stock_basic",
          "index_basic",
          "index_daily",
          "stock_st",
          "bak_basic",
          "stk_limit",
          "suspend_d",
          "margin_detail",
          "anns_d",
          "cyq_perf",
          "cyq_chips",
        ].includes(dataset) &&
        truncate &&
        !confirmClear
      ) {
        setError(
          "请先勾选确认或取消清空选项后再继续。显示方式同旧版：清空前必须二次确认。",
        );
        return;
      }

      const opts: any = {
        batch_size: 200,
      };
      if (dataset !== "index_basic") {
        opts.start_date = startDate;
        opts.end_date = endDate;
      }
      if (dataset === "adj_factor") {
        opts.truncate = Boolean(truncate);
      }
      if (
        [
          "stock_basic",
          "index_basic",
          "index_daily",
          "stock_st",
          "bak_basic",
          "suspend_d",
          "anns_d",
          "cyq_perf",
          "cyq_chips",
          "margin_detail",
        ].includes(dataset)
      ) {
        opts.truncate = Boolean(truncate);
      }
      if (dataset === "index_daily") {
        if (indexMarkets && indexMarkets.length > 0) {
          opts.index_markets = indexMarkets;
        }
      }
      if (dataset === "stock_st" || dataset === "bak_basic" || dataset === "stk_limit" || dataset === "suspend_d" || dataset === "margin_detail" || dataset === "anns_d" || dataset === "cyq_perf" || dataset === "cyq_chips") {
        if (!opts.start_date || !opts.end_date) {
          setError("请填写起止日期再执行初始化。");
          return;
        }
        opts.batch_sleep = 0.2;
      }
      if (dataset === "stock_basic" || dataset === "index_basic") {
        delete opts.start_date;
        delete opts.end_date;
      }

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
    } catch (e: any) {
      setError(e?.message || "初始化任务提交失败");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className={styles.twoColumnLayout}>
      <div>
        <h3 className={styles.headingSmall}>初始化参数</h3>

        <div className={styles.formGroupRow}>
          <label className={styles.label}>数据源</label>
          <div className={styles.segmentedControl}>
            <button
              type="button"
              className={classNames(
                styles.segmentedItem,
                dataSource === "TDX" && styles.segmentedItemActive,
              )}
              onClick={() => setDataSource("TDX")}
            >
              TDX
            </button>
            <button
              type="button"
              className={classNames(
                styles.segmentedItem,
                dataSource === "Tushare" && styles.segmentedItemActive,
              )}
              onClick={() => setDataSource("Tushare")}
            >
              Tushare
            </button>
            <button
              type="button"
              className={classNames(
                styles.segmentedItem,
                dataSource === "xtquant" && styles.segmentedItemActive,
              )}
              onClick={() => setDataSource("xtquant")}
            >
              xtquant
            </button>
          </div>
        </div>

        <div className={styles.formGroup}>
          <label className={styles.label} htmlFor="dataset-select">
            目标数据集
          </label>
          <select
            id="dataset-select"
            className={styles.select}
            aria-label="目标数据集"
            value={dataset}
            onChange={(e) => setDataset(e.target.value)}
          >
            {dataSource === "TDX"
              ? datasetOptionsTDX.map((opt) => (
                  <option key={opt.key} value={opt.key}>
                    {opt.label}
                  </option>
                ))
              : dataSource === "xtquant"
                ? datasetOptionsXtquant.map((opt) => (
                    <option key={opt.key} value={opt.key}>
                      {opt.label}
                    </option>
                  ))
                : datasetOptionsTushare.map((opt) => (
                    <option key={opt.key} value={opt.key}>
                      {opt.label}
                    </option>
                  ))}
          </select>
        </div>

        {dataSource === "Tushare" && dataset === "index_daily" && (
          <div className={styles.formGroup}>
            <label className={styles.label}>按市场筛选指数（可多选）</label>
            <div className={styles.row} style={{ marginBottom: 4 }}>
              <button
                type="button"
                className={styles.btnSecondary}
                style={{ padding: "2px 8px", fontSize: 12 }}
                onClick={() => {
                  const all = ["MSCI", "CSI", "SSE", "SZSE", "CICC", "SW", "OTH"];
                  setIndexMarkets((prev) =>
                    prev.length === all.length ? [] : all,
                  );
                }}
              >
                {indexMarkets.length === 7 ? "取消全选" : "全选"}
              </button>
            </div>
            <div className={styles.multipleCheckboxRow}>
              {["MSCI", "CSI", "SSE", "SZSE", "CICC", "SW", "OTH"].map(
                (mkt) => (
                  <label key={mkt} className={styles.checkboxInlineLabel}>
                    <input
                      type="checkbox"
                      className={styles.inputCheckbox}
                      checked={indexMarkets.includes(mkt)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setIndexMarkets((prev) =>
                            prev.includes(mkt) ? prev : [...prev, mkt],
                          );
                        } else {
                          setIndexMarkets((prev) =>
                            prev.filter((x) => x !== mkt),
                          );
                        }
                      }}
                    />
                    {mkt}
                  </label>
                ),
              )}
            </div>
          </div>
        )}

        {/* 日期范围选择：起始日期 / 结束日期 */}
        <div className={styles.formGroupRow}>
          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="init-start-date">
              起始日期
            </label>
            <input
              id="init-start-date"
              type="date"
              className={styles.input}
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              aria-label="起始日期"
            />
          </div>
          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="init-end-date">
              结束日期
            </label>
            <input
              id="init-end-date"
              type="date"
              className={styles.input}
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              aria-label="结束日期"
            />
          </div>
        </div>

        {/* 并行度选择 */}
        <div className={styles.formGroup}>
          <label className={styles.label} htmlFor="init-workers">
            并行度
          </label>
          <select
            id="init-workers"
            className={styles.select}
            aria-label="并行度"
            value={workers}
            onChange={(e) => setWorkers(Number(e.target.value) || 1)}
          >
            {[1, 2, 4, 8].map((w) => (
              <option key={w} value={w}>
                {w} 线程
              </option>
            ))}
          </select>
        </div>

        {((dataSource === "TDX" &&
          (dataset === "kline_minute_raw" ||
            dataset === "kline_daily_raw_go")) ||
          (dataSource === "Tushare" &&
            [
              "adj_factor",
              "stock_basic",
              "index_basic",
              "index_daily",
              "stock_st",
              "bak_basic",
              "stk_limit",
              "suspend_d",
              "margin_detail",
              "anns_d",
            ].includes(dataset))) && (
          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="init-truncate">
              <input
                id="init-truncate"
                type="checkbox"
                checked={truncate}
                onChange={(e) => {
                  setTruncate(e.target.checked);
                  if (!e.target.checked) {
                    setConfirmClear(false);
                  }
                }}
                className={styles.inputCheckbox}
              />
              初始化前清空目标表(或目标范围)
            </label>
            {truncate && (
              <div className={styles.textDangerSmall}>
                <label>
                  <input
                    type="checkbox"
                    checked={confirmClear}
                    onChange={(e) => setConfirmClear(e.target.checked)}
                    className={styles.inputCheckbox}
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
          className={styles.btnSuccess}
        >
          {submitting ? "正在提交..." : "开始初始化"}
        </button>

        {error && (
          <p className={styles.textDangerSmall} style={{ marginTop: 8 }}>
            {error}
          </p>
        )}
      </div>

      <div>
        <h3 className={styles.headingSmall}>当前初始化任务进度</h3>
        {jobId ? (
          <div className={styles.textSmall}>
            <p className={styles.textSmall}>当前作业ID：{jobId}</p>
            <div className={styles.row} style={{ marginTop: 6 }}>
              <button
                type="button"
                onClick={() => jobId && loadJobStatus(jobId)}
                disabled={jobLoading}
                className={styles.btnSecondary}
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
  const [dataset, setDataset] = useState<string>("kline_daily_raw");
  const [date, setDate] = useState<string>(() => {
    const d = new Date();
    return d.toISOString().slice(0, 10);
  });
  const [startDate, setStartDate] = useState<string>("");
  const [endDate, setEndDate] = useState<string>(() => {
    const d = new Date();
    return d.toISOString().slice(0, 10);
  });
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

  const jobIdRef = useRef<string | null>(jobId);
  const autoRefreshRef = useRef<boolean>(autoRefresh);

  useEffect(() => {
    jobIdRef.current = jobId;
  }, [jobId]);

  useEffect(() => {
    autoRefreshRef.current = autoRefresh;
  }, [autoRefresh]);

  const [autoInfo, setAutoInfo] = useState<{
    latestTradingDate?: string | null;
    currentMaxDate?: string | null;
  } | null>(null);

  const datasetOptionsTDX = [
    {
      key: "kline_daily_raw_go",
      label: "kline_daily_raw_go · 日线（未复权 RAW · Go 直连）",
    },
    {
      key: "kline_minute_raw",
      label: "kline_minute_raw · 1 分钟（原始 RAW）",
    },
  ];

  const datasetOptionsTushare = [
    {
      key: "kline_weekly",
      label: "kline_weekly · 周线（由本地日线QFQ聚合）",
    },
    {
      key: "adj_factor",
      label: "adj_factor · 复权因子（Tushare adj_factor）",
    },
    {
      key: "stock_moneyflow_ts",
      label:
        "stock_moneyflow_ts · 个股资金流（按交易日增量，默认最近3个自然日）",
    },
    {
      key: "index_daily",
      label: "index_daily · 指数日线行情（Tushare index_daily）",
    },
    {
      key: "index_basic",
      label: "index_basic · 指数基础信息（Tushare index_basic）",
    },
    { key: "stock_st", label: "stock_st · ST 股票列表（按公告日增量）" },
    { key: "bak_basic", label: "bak_basic · 历史股票列表（按交易日增量）" },
    { key: "daily_basic", label: "daily_basic · 股票每日指标（Tushare）" },
    { key: "stk_limit", label: "stk_limit · 每日涨跌停价格（Tushare）" },
    { key: "suspend_d", label: "suspend_d - Daily suspend/resume info (Tushare)" },
    {
      key: "anns_d",
      label: "anns_d · 上市公司公告（Tushare anns_d）",
    },
    {
      key: "cyq_perf",
      label: "cyq_perf · 每日筹码及胜率（Tushare）",
    },
    {
      key: "cyq_chips",
      label: "cyq_chips · 每日筹码分布（Tushare）",
    },
    {
      key: "tushare_trade_cal",
      label: "tushare_trade_cal · 交易日历（Tushare trade_cal 同步）",
    },
    {
      key: "sw_sector",
      label: "sw_sector · 申万行业板块（分类+成分股全量+日线增量）",
    },
    {
      key: "sector_data",
      label: "sector_data · 申万L2行业展开到个股（增量后处理）",
    },
    {
      key: "margin_detail",
      label: "margin_detail · 融资融券交易明细（Tushare）",
    },
    {
      key: "stock_basic",
      label: "stock_basic · 最新股票列表（全量刷新，init 模式）",
    },
  ];

  const datasetOptionsXtquant = [
    {
      key: "xtquant_pershare_index",
      label: "xtquant_pershare_index · 每股主要指标（PershareIndex）",
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
      setEndDate(prefill.targetDate);
    }
    if (prefill.startDate !== undefined) {
      setStartDate(prefill.startDate || "");
    }
    if (
      prefill.latestTradingDate !== undefined ||
      prefill.currentMaxDate !== undefined
    ) {
      setAutoInfo({
        latestTradingDate: prefill.latestTradingDate ?? prefill.targetDate ?? null,
        currentMaxDate: prefill.currentMaxDate ?? null,
      });
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
    } else if (dataSource === "xtquant") {
      // 切换到 xtquant 时，如果当前 dataset 不在 xtquant 选项里，则默认选 xtquant_pershare_index
      const isXtquantDataset = datasetOptionsXtquant.some(
        (opt) => opt.key === dataset,
      );
      if (!isXtquantDataset) {
        setDataset("xtquant_pershare_index");
      }
    } else {
      // 切换到 Tushare 时，如果当前 dataset 不在 Tushare 选项里，则默认选 kline_weekly
      const isTushareDataset = datasetOptionsTushare.some(
        (opt) => opt.key === dataset,
      );
      if (!isTushareDataset) {
        setDataset("kline_weekly");
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

    let timer: ReturnType<typeof setTimeout> | null = null;

    const tick = async () => {
      const currentJobId = jobIdRef.current;
      if (!currentJobId || cancelled) return;
      await loadJobStatus(currentJobId);
      if (!cancelled && autoRefreshRef.current && jobIdRef.current) {
        timer = setTimeout(tick, 5000);
      }
    };

    tick();

    return () => {
      cancelled = true;
      if (timer) {
        clearTimeout(timer);
      }
    };
  }, [jobId, autoRefresh, loadJobStatus]);

  const submitIncremental = async () => {
    setSubmitting(true);
    setError(null);
    try {
      if (dataSource === "TDX") {
          const isGoTdxSpecial =
            dataset === "kline_daily_raw_go" ||
            dataset === "kline_minute_raw";
          if (isGoTdxSpecial) {
            if (!startDate) {
              setError("请先选择起始日期");
              return;
            }
            const payload = {
              data_kind: dataset,
              start_date: startDate,
              workers: Number(workers) || 1,
            };
            const resp: any = await backendRequest(
              "POST",
              "/api/ingestion/incremental",
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
      } else if (dataSource === "xtquant") {
        // xtquant 增量分支
        const opts = {
          start_date: startDate || null,
          end_date: endDate || date || null,
          workers: Number(workers) || 1,
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
          const effectiveEnd: string | null = endDate || date || null;
          if (dataset === "stock_moneyflow_ts" && !startDate) {
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
            end_date: effectiveEnd,
            batch_size: Number(batchSize) || 100,
          };
          const isInitOnlyDataset = dataset === "stock_basic" || dataset === "index_basic";
          const payload = { dataset, mode: isInitOnlyDataset ? "init" : "incremental", options: opts };
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
    <div className={styles.section}>
      <div className={styles.initGrid}>
        <div>
          <h3 className={styles.headingSmall}>🔄 增量更新</h3>

        <div className={styles.mb8}>
          <label className={styles.label} htmlFor="incr-datasource">
            数据源
          </label>
          <select
            id="incr-datasource"
            value={dataSource}
            onChange={(e) =>
              setDataSource(e.target.value as DataSource)
            }
            className={styles.select}
            title="选择增量数据源"
          >
            <option value="TDX">TDX</option>
            <option value="Tushare">Tushare</option>
            <option value="xtquant">xtquant</option>
          </select>
        </div>

        <div className={styles.mb8}>
          <label className={styles.label} htmlFor="incr-dataset">
            目标数据集
          </label>
          <select
            id="incr-dataset"
            value={dataset}
            onChange={(e) => setDataset(e.target.value)}
            className={styles.select}
            title="选择增量数据集"
          >
            {(dataSource === "TDX"
              ? datasetOptionsTDX
              : dataSource === "xtquant"
                ? datasetOptionsXtquant
                : datasetOptionsTushare
            ).map((opt) => (
              <option key={opt.key} value={opt.key}>
                {opt.label}
              </option>
            ))}
          </select>
        </div>

        <div className={`${styles.gridTwo} ${styles.mb8}`}>
          {!(
            dataSource === "TDX" &&
            (dataset === "kline_daily_raw_go" ||
              dataset === "kline_minute_raw")
          ) && (
            <div>
              <label className={styles.label} htmlFor="incr-target-date">
                {dataSource === "Tushare" ? "增量截止日期" : "目标日期"}
              </label>
              <input
                id="incr-target-date"
                type="date"
                value={dataSource === "Tushare" ? endDate : date}
                onChange={(e) =>
                  dataSource === "Tushare"
                    ? setEndDate(e.target.value)
                    : setDate(e.target.value)
                }
                className={styles.input}
              />
            </div>
          )}
          <div>
            <label className={styles.label} htmlFor="incr-start-date">
              {dataSource === "TDX" &&
              (dataset === "kline_daily_raw_go" ||
                dataset === "kline_minute_raw")
                ? "增量起始日期"
                : "覆盖起始日期(可选)"}
            </label>
            <input
              id="incr-start-date"
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className={styles.input}
            />
          </div>
        </div>

        {dataSource === "TDX" &&
          (dataset === "kline_daily_raw_go" ||
            dataset === "kline_minute_raw") && (
            <div className={styles.mb8}>
              <div className={styles.textMutedSmall}>
                将从
                {startDate || "（请先选择起始日期）"}
                自动补齐到当前最新交易日。
              </div>
              {autoInfo && (
                <div className={styles.textMutedSmall} style={{ marginTop: 2 }}>
                  <div className={styles.textMutedSmall}>
                    当前数据集已有最晚日期：
                    {autoInfo.currentMaxDate || "无"}
                  </div>
                  <div className={styles.textMutedSmall}>
                    本次将从
                    {startDate || "（请先选择起始日期）"}
                    自动补齐到当前最新交易日：
                    {autoInfo.latestTradingDate || "未知"}
                  </div>
                </div>
              )}
            </div>
          )}

        {dataSource === "TDX" && (
          <div className={styles.mb8}>
            <label className={styles.label} htmlFor="incr-exchanges">
              交易所(逗号分隔)
            </label>
            <input
              id="incr-exchanges"
              value={exchanges}
              onChange={(e) => setExchanges(e.target.value)}
              className={styles.inputText}
              title="输入交易所列表，逗号分隔"
            />
          </div>
        )}

        {dataSource === "Tushare" && dataset === "tushare_trade_cal" && (
          <div className={styles.mb8}>
            <label className={styles.label}>交易日历同步窗口</label>
            <div className={`${styles.gridTwo} ${styles.mb8}`}>
              <input
                type="date"
                value={calStart}
                onChange={(e) => setCalStart(e.target.value)}
                className={styles.input}
                aria-label="交易日历开始日期"
              />
              <input
                type="date"
                value={calEnd}
                onChange={(e) => setCalEnd(e.target.value)}
                className={styles.input}
                aria-label="交易日历结束日期"
              />
            </div>
            <div className={styles.mb8}>
              <select
                aria-label="交易所(用于Tushare日历)"
                value={calExchange}
                onChange={(e) => setCalExchange(e.target.value)}
                className={styles.select}
                title="选择交易所（用于Tushare日历）"
              >
                <option value="SSE">SSE</option>
                <option value="SZSE">SZSE</option>
              </select>
            </div>
          </div>
        )}

        {dataSource === "TDX" && (
          <div className={styles.mb8}>
            <label className={styles.label} htmlFor="incr-workers">
              并行度
            </label>
            <select
              id="incr-workers"
              className={styles.select}
              aria-label="并行度"
              value={workers}
              onChange={(e) => setWorkers(Number(e.target.value) || 1)}
            >
              {[1, 2, 4, 8].map((w) => (
                <option key={w} value={w}>
                  {w}
                </option>
              ))}
            </select>
          </div>
        )}

        <button
          type="button"
          onClick={submitIncremental}
          disabled={submitting}
          className={styles.btnSuccess}
        >
          {submitting ? "正在提交..." : "开始增量"}
        </button>

        {error && (
          <p className={styles.textDangerSmall} style={{ marginTop: 8 }}>
            {error}
          </p>
        )}
      </div>

      <div>
        <h3 className={styles.headingSmall}>当前增量任务进度</h3>
        {jobId ? (
          <div className={styles.textSmall}>
            <p className={styles.textSmall}>当前作业ID：{jobId}</p>
            <div className={styles.row} style={{ marginTop: 6 }}>
              <button
                type="button"
                onClick={() => jobId && loadJobStatus(jobId)}
                disabled={jobLoading}
                className={styles.btnSecondary}
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
  const [truncate, setTruncate] = useState<boolean>(true);
  const [confirmClear, setConfirmClear] = useState<boolean>(false);
  const [submitting, setSubmitting] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const [jobId, setJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<IngestionJobStatus | null>(null);
  const [jobLoading, setJobLoading] = useState<boolean>(false);
  const [autoRefresh, setAutoRefresh] = useState<boolean>(true);

  const jobIdRef = useRef<string | null>(jobId);
  const autoRefreshRef = useRef<boolean>(autoRefresh);

  useEffect(() => {
    jobIdRef.current = jobId;
  }, [jobId]);

  useEffect(() => {
    autoRefreshRef.current = autoRefresh;
  }, [autoRefresh]);

  const loadJobStatus = useCallback(
    async (id: string) => {
      setJobLoading(true);
      try {
        const data = await backendRequest<IngestionJobStatus>(
          "GET",
          `/api/ingestion/job/${id}`,
        );
        setJobStatus(data);
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

    let timer: ReturnType<typeof setTimeout> | null = null;

    const tick = async () => {
      const currentJobId = jobIdRef.current;
      if (!currentJobId || cancelled) return;
      await loadJobStatus(currentJobId);
      if (!cancelled && autoRefreshRef.current && jobIdRef.current) {
        timer = setTimeout(tick, 5000);
      }
    };

    tick();

    return () => {
      cancelled = true;
      if (timer) {
        clearTimeout(timer);
      }
    };
  }, [jobId, autoRefresh, loadJobStatus]);

  const submitAdjust = async () => {
    setSubmitting(true);
    setError(null);
    try {
      if (truncate && !confirmClear) {
        setError(
          "请先勾选确认或取消清空选项后再继续。显示方式同旧版：清空前必须二次确认。",
        );
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

      const payload = {
        which,
        options: opts,
      } as any;

      const resp: any = await backendRequest(
        "POST",
        "/api/adjust/rebuild",
        {
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      if (resp && resp.job_id) {
        setJobId(String(resp.job_id));
        setAutoRefresh(true);
      }
    } catch (e: any) {
      setError(e?.message || "复权任务提交失败");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className={styles.section}>
      <div className={styles.gridTwoTight}>
        <div>
          <h3 className={styles.headingSmall}>🛠️ 复权生成（RAW → QFQ/HFQ）</h3>

          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="adjust-which">
              生成类型
            </label>
            <select
              id="adjust-which"
              value={which}
              onChange={(e) =>
                setWhich(e.target.value as "both" | "qfq" | "hfq")
              }
              className={styles.select}
            >
              <option value="both">QFQ+HFQ</option>
              <option value="qfq">仅QFQ</option>
              <option value="hfq">仅HFQ</option>
            </select>
          </div>

          <div className={styles.gridTwo}>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="init-start-date">
                起始日期
              </label>
              <input
                id="init-start-date"
                type="date"
                className={styles.input}
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                aria-label="起始日期"
              />
            </div>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="init-end-date">
                结束日期
              </label>
              <input
                id="init-end-date"
                type="date"
                className={styles.input}
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                aria-label="结束日期"
              />
            </div>
          </div>

          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="tushare-basic-truncate">
              <input
                id="tushare-basic-truncate"
                type="checkbox"
                checked={truncate}
                onChange={(e) => {
                  setTruncate(e.target.checked);
                  if (!e.target.checked) {
                    setConfirmClear(false);
                  }
                }}
                className={styles.inputCheckbox}
              />
              初始化前清空目标表（TRUNCATE）
            </label>
            {truncate && (
              <div className={styles.textDangerSmall}>
                <label>
                  <input
                    type="checkbox"
                    checked={confirmClear}
                    onChange={(e) => setConfirmClear(e.target.checked)}
                    className={styles.inputCheckbox}
                  />
                  我已知晓清空数据风险，并确认继续
                </label>
              </div>
            )}
          </div>

          <div className={styles.formGroup}>
            <label className={styles.label} htmlFor="init-exchanges">
              交易所(逗号分隔)
            </label>
            <input
              id="init-exchanges"
              className={styles.input}
              placeholder="如 SSE,SZSE"
              aria-label="交易所(逗号分隔)"
              value={exchanges}
              onChange={(e) => setExchanges(e.target.value)}
            />
          </div>

          <div className={styles.gridTwo}>
            <div className={styles.formGroup}>
              <label className={styles.label} htmlFor="init-workers">
                并行度
              </label>
              <select
                id="init-workers"
                className={styles.select}
                aria-label="并行度"
                value={workers}
                onChange={(e) => setWorkers(Number(e.target.value) || 1)}
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

function MiniQMTTab({ onFillLatest }: { onFillLatest?: (kind: string, startDate: string, latestTradingDay: string, currentMaxDate?: string | null) => void }) {
  const [stockCode, setStockCode] = useState<string>("000001.SZ");
  const [ranges, setRanges] = useState<Record<string, any>>({});
  const [latestDay, setLatestDay] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [submitting, setSubmitting] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [periods, setPeriods] = useState<string[]>(["1d", "1m", "5m", "1h"]);
  const [scope, setScope] = useState<string>("all");
  const [startTime, setStartTime] = useState<string>(""); // YYYYMMDD
  const [activeTaskId, setTaskId] = useState<string | null>(null);
  const [taskProgress, setTaskProgress] = useState<any>(null);
  const [progressLoading, setProgressLoading] = useState<boolean>(false);
  
  // 数据集概览相关状态
  const [datasets, setDatasets] = useState<any[]>([]);
  const [datasetsLoading, setDatasetsLoading] = useState<boolean>(false);
  const [showDatasetDetail, setShowDatasetDetail] = useState<boolean>(false);
  const [selectedDataset, setSelectedDataset] = useState<any>(null);

  const loadRanges = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const newRanges: Record<string, any> = {};
      const periodsToCheck = ["1d", "1m", "5m", "1h"];
      
      for (const p of periodsToCheck) {
        try {
          const data = await backendRequest(
            "GET",
            `/api/v1/qmt/data/range?stock_code=${stockCode}&period=${p}`
          );
          newRanges[p] = data;
        } catch (e) {
          console.warn(`Failed to load range for ${p}:`, e);
          newRanges[p] = { start: null, end: null, count: 0 };
        }
      }
      setRanges(newRanges);

      try {
        const latestData = await backendRequest("GET", "/api/v1/qmt/data/latest-day");
        setLatestDay(latestData?.latest_day || null);
      } catch (e) {
        console.warn("Failed to load latest day:", e);
      }
    } catch (e: any) {
      setError(e?.message || "加载 miniQMT 数据范围失败");
    } finally {
      setLoading(false);
    }
  }, [stockCode]);

  const loadProgress = useCallback(async (tid: string) => {
    setProgressLoading(true);
    try {
      const data = await backendRequest("GET", `/api/v1/qmt/data/task/${tid}/progress`);
      setTaskProgress(data);
      if (data.status === "success" || data.status === "failed") {
        setTaskId(null);
        if (data.status === "success") {
          loadRanges();
        }
      }
    } catch (e: any) {
      const message = String(e?.message || "");
      if (message.includes("HTTP 404")) {
        setTaskId(null);
        setTaskProgress({
          status: "failed",
          progress: 0,
          error: "任务不存在或已过期（可能后端已重启）",
        });
      }
      console.error("Failed to load task progress:", e);
    } finally {
      setProgressLoading(false);
    }
  }, [loadRanges]);

  useEffect(() => {
    loadRanges();
  }, [loadRanges]);

  // 加载数据集概览
  const loadDatasets = useCallback(async () => {
    setDatasetsLoading(true);
    try {
      const data = await backendRequest("GET", "/api/v1/qmt/data/datasets");
      setDatasets(data.datasets || []);
    } catch (e: any) {
      console.error("加载数据集概览失败:", e);
      setError(e?.message || "加载数据集概览失败");
    } finally {
      setDatasetsLoading(false);
    }
  }, []);

  useEffect(() => {
    loadDatasets();
  }, [loadDatasets]);

  // 一键补齐
  const handleCatchUp = async (datasetId: string) => {
    try {
      setError(null);
      const resp = await backendRequest("POST", `/api/v1/qmt/data/datasets/${datasetId}/catch-up`, {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      if (resp.success) {
        if (resp.action === "redirect_to_incremental" && onFillLatest) {
          // 非 QMT 数据集：跳转到增量页面，自动填入起止日期
          onFillLatest(
            datasetId,
            resp.gap_start || "",
            resp.gap_end || resp.latest_trading_date || "",
            resp.current_max_date || null,
          );
        } else if (resp.action === "no_gap") {
          // 数据已是最新
          setError(null);
          alert(resp.message || `${datasetId} 数据已是最新`);
        } else if (resp.task_id) {
          // QMT 数据集：后台任务已启动
          setTaskId(resp.task_id);
        }
      }
    } catch (e: any) {
      setError(e?.message || "一键补齐失败");
    }
  };

  // 查看数据集详情
  const handleViewDetail = async (datasetId: string) => {
    try {
      const data = await backendRequest("GET", `/api/v1/qmt/data/datasets/${datasetId}`);
      setSelectedDataset(data);
      setShowDatasetDetail(true);
    } catch (e: any) {
      setError(e?.message || "加载数据集详情失败");
    }
  };

  const handleOneClickUpdate = async () => {
    setSubmitting(true);
    setError(null);
    setTaskProgress(null);
    try {
      const resp = await backendRequest("POST", "/api/v1/qmt/data/one-click-update", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ 
          periods, 
          scope,
          start_time: startTime.replace(/[^0-9]/g, '') // 确保只传数字
        }),
      });
      if (resp.success && resp.task_id) {
        setTaskId(resp.task_id);
      }
    } catch (e: any) {
      setError(e?.message || "一键更新失败");
    } finally {
      setSubmitting(false);
    }
  };

  const handleDownloadFinancial = async () => {
    setSubmitting(true);
    setError(null);
    setTaskProgress(null);
    try {
      const resp = await backendRequest("POST", "/api/v1/qmt/data/download-financial", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          stock_list: [stockCode],
          table_list: ["Capital", "Balance", "Income", "CashFlow"],
        }),
      });
      if (resp.success && resp.task_id) {
        setTaskId(resp.task_id);
      }
    } catch (e: any) {
      setError(e?.message || "下载财务数据失败");
    } finally {
      setSubmitting(false);
    }
  };

  const togglePeriod = (p: string) => {
    setPeriods(prev => 
      prev.includes(p) ? prev.filter(x => x !== p) : [...prev, p]
    );
  };

  return (
    <div className={styles.section}>
      {/* 数据集概览 */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
          <h3 className={styles.headingSmall} style={{ margin: 0 }}>📊 数据集概览</h3>
          <button
            type="button"
            onClick={loadDatasets}
            disabled={datasetsLoading}
            className={styles.btnSecondary}
            style={{ padding: "2px 8px", fontSize: 11 }}
          >
            {datasetsLoading ? "刷新中..." : "手动刷新"}
          </button>
        </div>
        
        {datasetsLoading ? (
          <p className={styles.textMutedSmall}>加载数据集概览中...</p>
        ) : datasets.length > 0 ? (
          <div style={{ border: "1px solid #e5e7eb", borderRadius: 8, overflow: "hidden" }}>
            <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
              <thead style={{ background: "#f9fafb" }}>
                <tr>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>数据集</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>状态</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>数据范围</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>覆盖率</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>更新状态</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>操作</th>
                </tr>
              </thead>
              <tbody>
                {datasets.map((ds: any) => {
                  const statusEmoji = ds.status === "complete" ? "✅" : ds.status === "partial" ? "⚠️" : ds.status === "empty" ? "❌" : "❓";
                  const statusText = ds.status === "complete" ? "完整" : ds.status === "partial" ? "部分" : ds.status === "empty" ? "空" : "未知";
                  const updateStatusText = ds.update_status === "up_to_date" ? "最新" : ds.update_status.startsWith("outdated") ? `过期 ${ds.update_status.replace("outdated_", "")} 天` : "未知";
                  const dateRange = ds.date_range?.start && ds.date_range?.end 
                    ? `${ds.date_range.start} ~ ${ds.date_range.end}`
                    : "-";
                  
                  return (
                    <tr key={ds.id} style={{ borderBottom: "1px solid #f3f4f6" }}>
                      <td style={{ padding: 8 }}>
                        <div style={{ fontWeight: 500 }}>{ds.name}</div>
                        <div style={{ fontSize: 10, color: "#6b7280" }}>{ds.period}</div>
                      </td>
                      <td style={{ padding: 8 }}>{statusEmoji} {statusText}</td>
                      <td style={{ padding: 8 }}>{dateRange}</td>
                      <td style={{ padding: 8 }}>{ds.stock_range?.coverage_rate ? `${(ds.stock_range.coverage_rate * 100).toFixed(1)}%` : "-"}</td>
                      <td style={{ padding: 8 }}>{updateStatusText}</td>
                      <td style={{ padding: 8 }}>
                        <button
                          type="button"
                          onClick={() => handleCatchUp(ds.id)}
                          disabled={!!activeTaskId}
                          style={{
                            padding: "2px 6px",
                            borderRadius: 4,
                            border: "1px solid #16a34a",
                            background: !!activeTaskId ? "#dcfce7" : "#fff",
                            color: "#16a34a",
                            cursor: !!activeTaskId ? "not-allowed" : "pointer",
                            fontSize: 11,
                            marginRight: 4
                          }}
                        >
                          补齐
                        </button>
                        <button
                          type="button"
                          onClick={() => handleViewDetail(ds.id)}
                          style={{
                            padding: "2px 6px",
                            borderRadius: 4,
                            border: "1px solid #d4d4d4",
                            background: "#fff",
                            cursor: "pointer",
                            fontSize: 11
                          }}
                        >
                          详情
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <p className={styles.textMutedSmall}>暂无数据集信息</p>
        )}
      </div>

      <div className={styles.gridTwoTight}>
        <div>
          <h3 className={styles.headingSmall}>📊 miniQMT 历史数据管理</h3>
          <p className={styles.textMutedSmall} style={{ marginBottom: 12 }}>
            通过 miniQMT (xtquant) 下载 K 线和财务数据到本地缓存，提升执行效率。
          </p>

          <div className={styles.formGroup}>
            <label className={styles.label}>参考股票代码 (用于查询范围)</label>
            <div className={styles.rowWrapSmall}>
              <input
                type="text"
                className={styles.input}
                value={stockCode}
                onChange={(e) => setStockCode(e.target.value.toUpperCase())}
                placeholder="如 000001.SZ"
                style={{ width: 140 }}
              />
              <button
                type="button"
                onClick={loadRanges}
                disabled={loading}
                className={styles.btnSecondary}
              >
                刷新范围
              </button>
            </div>
          </div>

          <div className={styles.formGroup}>
            <label className={styles.label}>回补起始日期 (YYYYMMDD)</label>
            <div className={styles.rowWrapSmall}>
              <input
                type="text"
                className={styles.input}
                value={startTime}
                onChange={(e) => setStartTime(e.target.value)}
                placeholder="可选，如 20240101"
                style={{ width: 140 }}
              />
              <span className={styles.textMutedSmall} style={{ marginLeft: 8 }}>
                如果不填，miniQMT 将自动进行增量更新。
              </span>
            </div>
          </div>

          <div className={styles.formGroup}>
            <label className={styles.label}>更新范围与周期</label>
            <div className={styles.multipleCheckboxRow}>
              {["1d", "1m", "5m", "1h"].map(p => (
                <label key={p} className={styles.checkboxInlineLabel}>
                  <input
                    type="checkbox"
                    checked={periods.includes(p)}
                    onChange={() => togglePeriod(p)}
                  />
                  {p === "1d" ? "日线" : p}
                </label>
              ))}
            </div>
            <div className={styles.segmentedControl} style={{ marginTop: 8 }}>
              <button
                type="button"
                className={classNames(styles.segmentedItem, scope === "all" && styles.segmentedItemActive)}
                onClick={() => setScope("all")}
              >
                沪深A股 (全市场)
              </button>
              <button
                type="button"
                className={classNames(styles.segmentedItem, scope === "watchlist" && styles.segmentedItemActive)}
                onClick={() => setScope("watchlist")}
              >
                自选股 (Watchlist)
              </button>
            </div>
          </div>

          <div className={styles.rowWrap} style={{ marginTop: 16 }}>
            <button
              type="button"
              onClick={handleOneClickUpdate}
              disabled={submitting || loading || !!activeTaskId}
              className={styles.btnSuccess}
              style={{ minWidth: 160 }}
            >
              {submitting ? "正在提交..." : activeTaskId ? "正在后台更新..." : "🚀 一键更新到最新"}
            </button>
            <button
              type="button"
              onClick={handleDownloadFinancial}
              disabled={submitting || loading || !!activeTaskId}
              className={styles.btnPrimary}
            >
              📥 同步复权因子/财务数据
            </button>
          </div>

          {(activeTaskId || taskProgress) && (
            <div style={{ marginTop: 20, padding: 12, background: "#f0f9ff", borderRadius: 8, border: "1px solid #bae6fd" }}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6, alignItems: "center" }}>
                <span style={{ fontSize: 13, fontWeight: 600, color: "#0369a1" }}>
                  {taskProgress?.status === "success" ? "✅ 更新已完成" : 
                   taskProgress?.status === "failed" ? "❌ 更新失败" : "⏳ 正在更新数据..."}
                </span>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ fontSize: 12, color: "#0369a1" }}>{taskProgress?.progress ?? 0}%</span>
                  <button
                    type="button"
                    onClick={() => activeTaskId && loadProgress(activeTaskId)}
                    disabled={!activeTaskId || progressLoading}
                    className={styles.btnSecondary}
                    style={{ padding: "2px 8px", fontSize: 11 }}
                  >
                    {progressLoading ? "刷新中..." : "手动刷新进度"}
                  </button>
                </div>
              </div>
              <div style={{ width: "100%", background: "#e0f2fe", height: 8, borderRadius: 4, overflow: "hidden" }}>
                <div 
                  style={{ 
                    width: `${taskProgress?.progress ?? 0}%`, 
                    height: "100%", 
                    background: taskProgress?.status === "failed" ? "#ef4444" : "#0ea5e9",
                    transition: "width 0.3s ease" 
                  }} 
                />
              </div>
              {taskProgress?.message && (
                <div style={{ marginTop: 6, fontSize: 12, color: "#0369a1" }}>{taskProgress.message}</div>
              )}
              {taskProgress?.last_stock && (
                <div style={{ marginTop: 4, fontSize: 11, color: "#64748b" }}>
                  当前进度: {taskProgress.finished}/{taskProgress.total} (最近: {taskProgress.last_stock})
                </div>
              )}
              {taskProgress?.error && (
                <div style={{ marginTop: 6, fontSize: 12, color: "#b91c1c" }}>错误原因: {taskProgress.error}</div>
              )}
            </div>
          )}

          {error && (
            <p className={styles.textDangerSmall} style={{ marginTop: 12 }}>
              {error}
            </p>
          )}
        </div>

        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
            <h3 className={styles.headingSmall} style={{ margin: 0 }}>当前本地数据状态</h3>
            <button 
              type="button" 
              onClick={loadRanges} 
              disabled={loading} 
              className={styles.btnSecondary}
              style={{ padding: "2px 8px", fontSize: 11 }}
            >
              {loading ? "刷新中..." : "手动刷新状态"}
            </button>
          </div>
          <div className={styles.cardSoft}>
            <div className={styles.textSmall} style={{ marginBottom: 8 }}>
              最新交易日 (miniQMT): <strong style={{ color: latestDay ? "#111827" : "#ef4444" }}>{latestDay || (loading ? "查询中..." : "查询失败，请检查 miniQMT 连接")}</strong>
            </div>
            <table className={styles.statsTable} style={{ fontSize: 12 }}>
              <thead>
                <tr>
                  <th className={styles.statsHeaderCell}>周期</th>
                  <th className={styles.statsHeaderCell}>起始日期</th>
                  <th className={styles.statsHeaderCell}>结束日期</th>
                  <th className={styles.statsHeaderCell}>条数</th>
                </tr>
              </thead>
              <tbody>
                {["1d", "1m", "5m", "1h"].map(p => {
                  const r = ranges[p] || {};
                  return (
                    <tr key={p}>
                      <td className={styles.statsCell}>{p === "1d" ? "日线" : p}</td>
                      <td className={styles.statsCell}>{r.start || "—"}</td>
                      <td className={styles.statsCell}>{r.end || "—"}</td>
                      <td className={styles.statsCell}>{r.count || 0}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            <p className={styles.textMutedSmall} style={{ marginTop: 8 }}>
              * 以上范围基于参考代码 <strong>{stockCode}</strong> 的本地缓存情况。
            </p>
          </div>
        </div>
      </div>

      {/* 数据集详情弹窗 */}
      {showDatasetDetail && selectedDataset && (
        <div style={{
          position: "fixed",
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background: "rgba(0, 0, 0, 0.5)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          zIndex: 1000
        }}>
          <div style={{
            background: "#fff",
            borderRadius: 12,
            maxWidth: 600,
            maxHeight: "80vh",
            overflow: "auto",
            padding: 24,
            boxShadow: "0 4px 20px rgba(0, 0, 0, 0.15)"
          }}>
            <h3 style={{ margin: 0, fontSize: 16, marginBottom: 16 }}>
              📊 {selectedDataset.name} - 详细统计
            </h3>
            
            <div style={{ marginBottom: 16 }}>
              <h4 style={{ margin: 0, fontSize: 13, color: "#6b7280", marginBottom: 8 }}>基本信息</h4>
              <div style={{ fontSize: 12 }}>
                <div>• 数据集 ID: {selectedDataset.id}</div>
                <div>• 周期: {selectedDataset.period}</div>
                <div>• 状态: {selectedDataset.status === "complete" ? "✅ 完整" : selectedDataset.status === "partial" ? "⚠️ 部分" : selectedDataset.status === "empty" ? "❌ 空" : "❓ 未知"}</div>
                <div>• 最后更新: {selectedDataset.last_updated || "—"}</div>
              </div>
            </div>

            {selectedDataset.date_range && (
              <div style={{ marginBottom: 16 }}>
                <h4 style={{ margin: 0, fontSize: 13, color: "#6b7280", marginBottom: 8 }}>📅 日期范围</h4>
                <div style={{ fontSize: 12 }}>
                  <div>• 起始日期: {selectedDataset.date_range.start || "—"}</div>
                  <div>• 结束日期: {selectedDataset.date_range.end || "—"}</div>
                  <div>• 最新可用: {selectedDataset.date_range.latest_available || "—"}</div>
                  <div>• 最新交易日: {selectedDataset.date_range.latest_trading_day || "—"}</div>
                  <div>• 缺失交易日: {selectedDataset.date_range.gap_days?.length || 0} 天</div>
                  <div>• 覆盖交易日: {selectedDataset.date_range.covered_trading_days}/{selectedDataset.date_range.total_trading_days} ({selectedDataset.date_range.total_trading_days > 0 ? ((selectedDataset.date_range.covered_trading_days / selectedDataset.date_range.total_trading_days) * 100).toFixed(1) : 0}%)</div>
                </div>
              </div>
            )}

            {selectedDataset.stock_range && (
              <div style={{ marginBottom: 16 }}>
                <h4 style={{ margin: 0, fontSize: 13, color: "#6b7280", marginBottom: 8 }}>📈 股票范围</h4>
                <div style={{ fontSize: 12 }}>
                  <div>• 总股票数: {selectedDataset.stock_range.total_count || 0}</div>
                  <div>• 已覆盖: {selectedDataset.stock_range.covered_count || 0} ({selectedDataset.stock_range.coverage_rate ? (selectedDataset.stock_range.coverage_rate * 100).toFixed(1) : 0}%)</div>
                </div>
              </div>
            )}

            {selectedDataset.data_size && (
              <div style={{ marginBottom: 16 }}>
                <h4 style={{ margin: 0, fontSize: 13, color: "#6b7280", marginBottom: 8 }}>💾 数据规模</h4>
                <div style={{ fontSize: 12 }}>
                  <div>• 记录数: {(selectedDataset.data_size.record_count || 0).toLocaleString()}</div>
                  <div>• 存储大小: {selectedDataset.data_size.size_mb?.toFixed(2) || 0} MB</div>
                </div>
              </div>
            )}

            {selectedDataset.quality_metrics && (
              <div style={{ marginBottom: 16 }}>
                <h4 style={{ margin: 0, fontSize: 13, color: "#6b7280", marginBottom: 8 }}>📊 质量指标</h4>
                <div style={{ fontSize: 12 }}>
                  <div>• 完整性: {(selectedDataset.quality_metrics.completeness * 100).toFixed(1)}%</div>
                  <div>• 一致性: {(selectedDataset.quality_metrics.consistency * 100).toFixed(1)}%</div>
                  <div>• 及时性: {(selectedDataset.quality_metrics.timeliness * 100).toFixed(1)}%</div>
                </div>
              </div>
            )}

            <div style={{ display: "flex", gap: 8, marginTop: 24 }}>
              <button
                type="button"
                onClick={() => setShowDatasetDetail(false)}
                style={{
                  padding: "6px 12px",
                  borderRadius: 6,
                  border: "1px solid #d4d4d4",
                  background: "#fff",
                  cursor: "pointer",
                  fontSize: 13
                }}
              >
                关闭
              </button>
              <button
                type="button"
                onClick={() => {
                  handleCatchUp(selectedDataset.id);
                  setShowDatasetDetail(false);
                }}
                disabled={!!activeTaskId}
                style={{
                  padding: "6px 12px",
                  borderRadius: 6,
                  border: "1px solid #16a34a",
                  background: !!activeTaskId ? "#dcfce7" : "#22c55e",
                  color: "#fff",
                  cursor: !!activeTaskId ? "not-allowed" : "pointer",
                  fontSize: 13
                }}
              >
                {!!activeTaskId ? "正在补齐..." : "一键补齐"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 进度监控组件 */}
      {(activeTaskId || taskProgress) && (
        <div style={{ marginTop: 20, padding: 12, background: "#f0f9ff", borderRadius: 8, border: "1px solid #bae6fd" }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
            <span style={{ fontSize: 13, fontWeight: 600, color: "#0369a1" }}>
              {taskProgress?.status === "success" ? "✅ 更新已完成" : 
               taskProgress?.status === "failed" ? "❌ 更新失败" : "⏳ 正在更新数据..."}
            </span>
            <span style={{ fontSize: 12, color: "#0369a1" }}>{taskProgress?.progress ?? 0}%</span>
          </div>
          <div style={{ width: "100%", background: "#e0f2fe", height: 8, borderRadius: 4, overflow: "hidden" }}>
            <div 
              style={{ 
                width: `${taskProgress?.progress ?? 0}%`, 
                height: "100%", 
                background: taskProgress?.status === "failed" ? "#ef4444" : "#0ea5e9",
                transition: "width 0.3s ease" 
              }} 
            />
          </div>
          {taskProgress?.message && (
            <div style={{ marginTop: 6, fontSize: 12, color: "#0369a1" }}>{taskProgress.message}</div>
          )}
          {taskProgress?.last_stock && (
            <div style={{ marginTop: 4, fontSize: 11, color: "#64748b" }}>
              当前进度: {taskProgress.finished}/{taskProgress.total} (最近: {taskProgress.last_stock})
            </div>
          )}
          {taskProgress?.error && (
            <div style={{ marginTop: 6, fontSize: 12, color: "#b91c1c" }}>错误原因: {taskProgress.error}</div>
          )}
        </div>
      )}
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
  const [lastUserActionAt, setLastUserActionAt] = useState<number>(0);

  // QMT 任务相关状态
  const [qmtTasks, setQmtTasks] = useState<any[]>([]);
  const [qmtLoading, setQmtLoading] = useState(false);
  const [selectedQmtTask, setSelectedQmtTask] = useState<any>(null);
  const [showQmtTaskLogs, setShowQmtTaskLogs] = useState(false);
  const [qmtTaskLogs, setQmtTaskLogs] = useState<any[]>([]);

  const [annsPdfLimit, setAnnsPdfLimit] = useState<number>(200);
  const [annsPdfSleep, setAnnsPdfSleep] = useState<number>(0);
  const [annsPdfTimeout, setAnnsPdfTimeout] = useState<number>(25);
  const [annsPdfRetryFailed, setAnnsPdfRetryFailed] = useState<boolean>(false);
  const [annsPdfSubmitting, setAnnsPdfSubmitting] = useState<boolean>(false);

  const [logJobId, setLogJobId] = useState<string | null>(null);
  const [logItems, setLogItems] = useState<any[]>([]);
  const [logLoading, setLogLoading] = useState(false);

  // 加载 QMT 任务列表
  const loadQmtTasks = useCallback(async () => {
    setQmtLoading(true);
    try {
      const data = await backendRequest("GET", `/api/v1/qmt/data/tasks?active_only=${activeOnly}&limit=${limit}`);
      setQmtTasks(data.tasks || []);
    } catch (e: any) {
      console.error("加载 QMT 任务失败:", e);
    } finally {
      setQmtLoading(false);
    }
  }, [activeOnly, limit]);

  // 加载 QMT 任务日志
  const loadQmtTaskLogs = useCallback(async (taskId: string) => {
    try {
      const data = await backendRequest("GET", `/api/v1/qmt/data/tasks/${taskId}/progress`);
      setQmtTaskLogs(data.logs || []);
    } catch (e: any) {
      console.error("加载 QMT 任务日志失败:", e);
    }
  }, []);

  useEffect(() => {
    loadQmtTasks();
  }, [loadQmtTasks]);

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
    const recentActionWindowMs = 60_000;
    const recentlyTriggered = lastUserActionAt > 0 && Date.now() - lastUserActionAt <= recentActionWindowMs;
    if (!anyActive && !recentlyTriggered) return;
    const id = setTimeout(() => {
      loadJobs();
    }, 5000);
    return () => clearTimeout(id);
  }, [autoRefresh, items, loadJobs, lastUserActionAt]);

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
        setLastUserActionAt(Date.now());
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
        setLastUserActionAt(Date.now());
        await loadJobs();
      } catch (e: any) {
        setError(e?.message || "停止任务失败");
      }
    },
    [loadJobs],
  );

  const handleClearQueued = useCallback(async () => {
    if (typeof window !== "undefined") {
      const ok = window.confirm("确定要清除所有排队/待执行的任务吗？此操作不可恢复。");
      if (!ok) return;
    }
    try {
      await backendRequest("DELETE", "/api/ingestion/jobs/queued");
      setLastUserActionAt(Date.now());
      await loadJobs();
    } catch (e: any) {
      setError(e?.message || "清除排队任务失败");
    }
  }, [loadJobs]);

  const handleRunAnnsPdf = useCallback(async () => {
    setError(null);
    setAnnsPdfSubmitting(true);
    try {
      const payload = {
        dataset: "anns_pdf",
        mode: "init",
        options: {
          limit: annsPdfLimit,
          sleep: annsPdfSleep,
          timeout: annsPdfTimeout,
          retry_failed: annsPdfRetryFailed,
        },
        triggered_by: "manual",
      };
      await backendRequest("POST", "/api/ingestion/run", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setLastUserActionAt(Date.now());
      await loadJobs();
    } catch (e: any) {
      setError(e?.message || "触发公告 PDF 下载任务失败");
    } finally {
      setAnnsPdfSubmitting(false);
    }
  }, [annsPdfLimit, annsPdfSleep, annsPdfTimeout, loadJobs]);

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
      
      {/* QMT 数据同步任务 */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8, color: "#6b7280" }}>
          miniQMT 数据同步任务
        </div>
        {qmtLoading ? (
          <p style={{ fontSize: 12, color: "#6b7280" }}>加载 QMT 任务中...</p>
        ) : qmtTasks.length > 0 ? (
          <div style={{ border: "1px solid #e5e7eb", borderRadius: 8, overflow: "hidden" }}>
            <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
              <thead style={{ background: "#f9fafb" }}>
                <tr>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>任务 ID</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>数据集</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>类型</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>状态</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>进度</th>
                  <th style={{ padding: 8, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>操作</th>
                </tr>
              </thead>
              <tbody>
                {qmtTasks.map((task: any, idx: number) => (
                  <tr key={task.task_id || task.job_id || idx} style={{ borderBottom: "1px solid #f3f4f6" }}>
                    <td style={{ padding: 8, fontSize: 11 }}>{(task.task_id || task.job_id || "—").slice(0, 8)}...</td>
                    <td style={{ padding: 8 }}>{task.dataset_name || task.dataset_id || task.summary?.dataset || "—"}</td>
                    <td style={{ padding: 8 }}>{task.type || task.job_type || "—"}</td>
                    <td style={{ padding: 8 }}>
                      <span style={{
                        color: task.status === "success" ? "#16a34a" : task.status === "failed" ? "#b91c1c" : "#374151",
                        fontWeight: 500
                      }}>
                        {task.status === "running" ? "运行中" : 
                         task.status === "success" ? "成功" : 
                         task.status === "failed" ? "失败" : 
                         task.status === "queued" ? "排队" : task.status}
                      </span>
                    </td>
                    <td style={{ padding: 8 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                        <div style={{ width: 60, background: "#e5e7eb", height: 6, borderRadius: 3, overflow: "hidden" }}>
                          <div 
                            style={{ 
                              width: `${task.progress || 0}%`, 
                              height: "100%", 
                              background: task.status === "failed" ? "#ef4444" : "#0ea5e9" 
                            }} 
                          />
                        </div>
                        <span style={{ fontSize: 11 }}>{task.progress || 0}%</span>
                      </div>
                    </td>
                    <td style={{ padding: 8 }}>
                      <button
                        type="button"
                        onClick={() => {
                          setSelectedQmtTask(task);
                          loadQmtTaskLogs(task.task_id);
                          setShowQmtTaskLogs(true);
                        }}
                        style={{
                          padding: "2px 6px",
                          borderRadius: 4,
                          border: "1px solid #d4d4d4",
                          background: "#fff",
                          cursor: "pointer",
                          fontSize: 11
                        }}
                      >
                        日志
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p style={{ fontSize: 12, color: "#6b7280" }}>暂无 QMT 数据同步任务</p>
        )}
      </div>

      {/* QMT 任务日志弹窗 */}
      {showQmtTaskLogs && selectedQmtTask && (
        <div style={{
          position: "fixed",
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background: "rgba(0, 0, 0, 0.5)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          zIndex: 1000
        }}>
          <div style={{
            background: "#fff",
            borderRadius: 12,
            maxWidth: 700,
            maxHeight: "80vh",
            overflow: "auto",
            padding: 24,
            boxShadow: "0 4px 20px rgba(0, 0, 0, 0.15)"
          }}>
            <h3 style={{ margin: 0, fontSize: 16, marginBottom: 16 }}>
              📝 任务日志 - {selectedQmtTask.dataset_name || selectedQmtTask.dataset_id}
            </h3>
            
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 12, marginBottom: 8 }}>
                <div>• 任务 ID: {selectedQmtTask.task_id}</div>
                <div>• 状态: {selectedQmtTask.status}</div>
                <div>• 进度: {selectedQmtTask.progress || 0}%</div>
                {selectedQmtTask.counters && (
                  <>
                    <div>• 总数: {selectedQmtTask.counters.total || 0}</div>
                    <div>• 完成: {selectedQmtTask.counters.finished || 0}</div>
                    <div>• 成功: {selectedQmtTask.counters.success || 0}</div>
                    <div>• 失败: {selectedQmtTask.counters.failed || 0}</div>
                  </>
                )}
              </div>
            </div>

            <div style={{ marginBottom: 16 }}>
              <h4 style={{ margin: 0, fontSize: 13, color: "#6b7280", marginBottom: 8 }}>日志记录</h4>
              <div style={{
                background: "#f9fafb",
                borderRadius: 8,
                padding: 12,
                maxHeight: 400,
                overflow: "auto",
                fontSize: 11
              }}>
                {qmtTaskLogs.length > 0 ? (
                  qmtTaskLogs.map((log: any, idx: number) => (
                    <div key={idx} style={{ marginBottom: 4, fontFamily: "monospace" }}>
                      <span style={{ color: "#6b7280" }}>[{log.timestamp}]</span>
                      <span style={{ color: log.level === "error" ? "#b91c1c" : log.level === "success" ? "#16a34a" : "#374151" }}>
                        [{log.level?.toUpperCase() || "INFO"}]
                      </span>
                      <span>{log.message}</span>
                      {log.stock_code && <span style={{ color: "#6b7280" }}> ({log.stock_code})</span>}
                    </div>
                  ))
                ) : (
                  <p style={{ color: "#6b7280" }}>暂无日志记录</p>
                )}
              </div>
            </div>

            <button
              type="button"
              onClick={() => setShowQmtTaskLogs(false)}
              style={{
                padding: "6px 12px",
                borderRadius: 6,
                border: "1px solid #d4d4d4",
                background: "#fff",
                cursor: "pointer",
                fontSize: 13
              }}
            >
              关闭
            </button>
          </div>
        </div>
      )}

      {/* 数据入库任务 */}
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
        <label style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <input
            type="checkbox"
            checked={annsPdfRetryFailed}
            onChange={(e) => setAnnsPdfRetryFailed(e.target.checked)}
          />
          同时重试上次失败记录
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
        <button
          type="button"
          onClick={handleClearQueued}
          disabled={loading}
          style={{
            padding: "4px 10px",
            borderRadius: 6,
            border: "1px solid #d4d4d4",
            background: "#fff6f6",
            cursor: "pointer",
            fontSize: 13,
            color: "#b91c1c",
          }}
        >
          清除排队任务
        </button>
      </div>

      <div
        style={{
          marginBottom: 8,
          padding: 8,
          borderRadius: 8,
          border: "1px dashed #e5e7eb",
          background: "#f9fafb",
          fontSize: 13,
          display: "flex",
          flexWrap: "wrap",
          gap: 8,
          alignItems: "center",
        }}
      >
        <div style={{ fontWeight: 500 }}>📄 公告 PDF 下载（dataset = anns_pdf）</div>
        <label>
          每次处理条数
          <input
            type="number"
            min={1}
            max={5000}
            value={annsPdfLimit}
            onChange={(e) =>
              setAnnsPdfLimit(Math.min(5000, Math.max(1, Number(e.target.value) || 200)))
            }
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
          每条间隔秒数
          <input
            type="number"
            min={0}
            step={0.1}
            value={annsPdfSleep}
            onChange={(e) => setAnnsPdfSleep(Number(e.target.value) || 0)}
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
          请求超时
          <input
            type="number"
            min={5}
            max={120}
            value={annsPdfTimeout}
            onChange={(e) =>
              setAnnsPdfTimeout(Math.min(120, Math.max(5, Number(e.target.value) || 25)))
            }
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
          onClick={handleRunAnnsPdf}
          disabled={annsPdfSubmitting}
          style={{
            padding: "4px 10px",
            borderRadius: 6,
            border: "1px solid #16a34a",
            background: annsPdfSubmitting ? "#dcfce7" : "#22c55e",
            color: "#ffffff",
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          {annsPdfSubmitting ? "正在提交..." : "开始下载公告 PDF"}
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
              ["kline_daily", "kline_daily_raw"].includes(dsLower) &&
              mode === "init"
            ) {
              cat = "日线初始化";
            } else if (
              ["kline_daily", "kline_daily_raw"].includes(dsLower) &&
              mode === "incremental"
            ) {
              cat = "日线增量";
            } else if (dsLower === "adjust_daily" && ["rebuild", "init"].includes(mode)) {
              cat = "复权计算";
            } else if (["kline_weekly", "kline_weekly_qfq"].includes(dsLower)) {
              cat = "周线聚合";
            } else if (dsLower === "stock_moneyflow_ts") {
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
                  : meta.source === "derived_from_kline_daily_raw"
                    ? "本地日线聚合"
                    : meta.source === "tdx_api_minute_trade_all"
                      ? "TDX 分钟成交聚合"
                      : meta.source || "—";

            const canDelete = !!jobId;
            // 仅当任务由 Go 驱动（即 summary/meta 中存在 go_task_id）且仍在运行/排队时，才允许前端发起停止请求，
            // 否则后端 /api/ingestion/job/{job_id}/cancel 会返回 400（go_task_id not found for this job）。
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
    startDate: string,
    latestTradingDay: string,
    currentMaxDate?: string | null,
  ) => void;
}) {
  const [items, setItems] = useState<any[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [gapLoadingKind, setGapLoadingKind] = useState<string | null>(null);
  const [gapResult, setGapResult] = useState<any | null>(null);
  const [fillLoadingKind, setFillLoadingKind] = useState<string | null>(null);
  const [newsStats, setNewsStats] = useState<any | null>(null);
  const [newsLoading, setNewsLoading] = useState<boolean>(false);

  const [collapsedCategories, setCollapsedCategories] = useState<
    Record<"market" | "basic" | "xtquant" | "sector" | "other", boolean>
  >({ market: false, basic: false, xtquant: false, sector: false, other: false });

  const CATEGORY_LABELS: Record<"market" | "basic" | "xtquant" | "sector" | "other", string> = {
    market: "行情数据",
    basic: "基础信息",
    xtquant: "xtquant 财务数据",
    sector: "申万行业板块",
    other: "其他",
  };

  const getCategoryKey = (kind: string): "market" | "basic" | "xtquant" | "sector" | "other" => {
    const k = (kind || "").toLowerCase();
    if (
      k.startsWith("kline_") ||
      k === "kline_minute_raw" ||
      k === "stock_moneyflow_ts" ||
      k === "margin_detail" ||
      k === "suspend_d" ||
      k === "index_daily"
    ) {
      return "market";
    }
    if (
      k === "stock_basic" ||
      k === "stock_st" ||
      k === "bak_basic" ||
      k === "daily_basic" ||
      k === "adj_factor" ||
      k === "symbol_dim" ||
      k === "index_basic" ||
      k === "stk_limit"
    ) {
      return "basic";
    }
    if (k.startsWith("xtquant_")) {
      return "xtquant";
    }
    // 申万行业板块数据
    if (k.startsWith("sw_") || k === "sector_data") {
      return "sector";
    }
    // cyq数据集放到其他分类
    if (k === "cyq_perf" || k === "cyq_chips") {
      return "other";
    }
    return "other";
  };

  // 普通函数声明，避免 const 声明带来的 TDZ 问题
  async function loadNewsStats() {
    setNewsLoading(true);
    try {
      const data: any = await backendRequest("GET", "/api/v1/news/stats");
      setNewsStats(data || null);
    } catch {
      // 静默失败：新闻统计只是附加信息，不影响主统计功能
    } finally {
      setNewsLoading(false);
    }
  }

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
    setNewsLoading(true);
    setNewsStats(null);
    try {
      await backendRequest("POST", "/api/data-stats/refresh");
      const data: any = await backendRequest("GET", "/api/data-stats");
      const nextItems = Array.isArray(data?.items) ? data.items : [];
      setItems(nextItems);
      await loadNewsStats();
    } catch (e: any) {
      setError(e?.message || "刷新统计数据失败");
    } finally {
      setLoading(false);
      setNewsLoading(false);
    }
  }, []);

  useEffect(() => {
    // 初次进入数据看板时仅加载上次刷新结果，不主动触发后端 refresh
    loadExistingStats();
    loadNewsStats();
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
      setFillLoadingKind(kind);
      const lower = (kind || "").toLowerCase();
      try {
        if (
          lower === "kline_daily_raw_go" ||
          lower === "kline_daily_raw" ||
          lower === "kline_minute_raw" ||
          lower === "adj_factor" ||
          lower === "daily_basic" ||
          lower === "stock_basic" ||
          lower === "stock_moneyflow_ts" ||
          lower === "stock_st" ||
          lower === "bak_basic" ||
          lower === "stk_limit" ||
          lower === "suspend_d" ||
          lower === "margin_detail" ||
          lower === "anns_d" ||
          lower === "index_daily" ||
          lower === "xtquant_pershare_index" ||
          lower === "cyq_perf" ||
          lower === "cyq_chips" ||
          lower === "sw_daily" ||
          lower === "sector_data"
        ) {
          const params = new URLSearchParams({ data_kind: kind });
          const data: any = await backendRequest(
            "GET",
            `/api/ingestion/auto-range?${params.toString()}`,
          );
          const startDate = data?.start_date;
          const latestTradingDate = data?.latest_trading_date;
          const currentMaxDate = data?.current_max_date ?? null;
          if (!startDate || !latestTradingDate) {
            setError("无法自动计算补齐区间，请检查数据统计和交易日历。");
            return;
          }
          onFillLatest(kind, String(startDate), String(latestTradingDate), currentMaxDate);
        } else {
          setError(`数据集 “${kind}” 未注册 auto-range 补齐逻辑，请在 handleFillLatestClick 中添加支持。`);
          return;
        }
      } catch (e: any) {
        setError(e?.message || "自动补齐区间计算失败");
      } finally {
        setFillLoadingKind(null);
      }
    },
    [onFillLatest],
  );

  // ...

  return (
    <div className={styles.section}>
      <h3 className={styles.headingSmall}>📊 数据看板（统计总览）</h3>
      <div className={styles.rowWrapSmall}>
        <button
          type="button"
          onClick={triggerRefresh}
          disabled={loading}
          className={styles.btnPrimary}
          aria-label="刷新统计数据"
        >
          {loading ? "刷新中..." : "刷新统计数据"}
        </button>
        <span className={styles.textMuted}>
          统计数据来自后台预计算表 market.data_stats，适合快速查看各类数据的时间范围、条数和更新时间。
        </span>
      </div>

      {/* 新闻统计摘要（仅展示数量与时间范围，不展示新闻内容） */}
      {newsStats && (
        <div className={styles.cardInfo}>
          <div className={styles.rowBetween}>
            <span style={{ fontWeight: 500 }}>📰 新闻数据概览</span>
            {newsLoading && <span className={styles.textMuted}>加载中...</span>}
          </div>
          <div className={styles.rowWrapSmall}>
            <span>
              总条数：<strong>{newsStats.total_count ?? 0}</strong>
            </span>
            <span>
              最早发布时间：
              {newsStats.earliest_time
                ? formatDateTime(String(newsStats.earliest_time))
                : "—"}
            </span>
            <span>
              最新发布时间：
              {newsStats.latest_time
                ? formatDateTime(String(newsStats.latest_time))
                : "—"}
            </span>
          </div>
          {Array.isArray(newsStats.sources) && newsStats.sources.length > 0 && (
            <div className={styles.newsSources}>
              <span>按来源统计：</span>
              <span style={{ marginLeft: 4 }}>
                {newsStats.sources
                  .map((s: any) => {
                    const raw = s.source || "未知";
                    let label = raw;
                    if (raw === "cls_telegraph") label = "财联社";
                    else if (raw === "sina_finance") label = "新浪财经";
                    else if (raw === "tradingview") label = "TradingView 外媒";
                    return `${label}: ${s.count ?? 0}`;
                  })
                  .join(" · ")}
              </span>
            </div>
          )}
        </div>
      )}

      {/* 指数基础信息 index_basic 摘要：从通用 data-stats 中挑出 index_basic 记录 */}
      {items && items.length > 0 && (() => {
        const indexItem = items.find((it: any) => {
          const kind = String(it.data_kind || it.kind || "").toLowerCase();
          return kind === "index_basic";
        });
        if (!indexItem) return null;

        const rowCount = indexItem.row_count || indexItem.rows || 0;
        const minDate =
          indexItem.min_date || indexItem.date_min || indexItem.start_date || "—";
        const maxDate =
          indexItem.max_date || indexItem.date_max || indexItem.end_date || "—";

        return (
          <div className={styles.cardInfo}>
            <div className={styles.rowBetween}>
              <span style={{ fontWeight: 500 }}>指数数据（index_basic）</span>
            </div>
            <div className={styles.rowWrapSmall}>
              <span>
                指数数量：<strong>{rowCount}</strong>
              </span>
              <span>
                覆盖日期区间：{minDate} ~ {maxDate}
              </span>
            </div>
          </div>
        );
      })()}

      {error && (
        <p className={styles.textDangerSmall}>
          {error}
        </p>
      )}

      {items.length === 0 && !loading ? (
        <p className={styles.textMuted}>当前没有统计数据，请先执行一次刷新。</p>
      ) : (
        <div className={styles.tableWrapper}>
          <table className={styles.statsTable}>
            <thead>
              <tr>
                <th className={styles.statsHeaderCell}>数据集</th>
                <th className={styles.statsHeaderCell}>描述</th>
                <th className={styles.statsHeaderCell}>行数</th>
                <th className={styles.statsHeaderCell}>开始日期</th>
                <th className={styles.statsHeaderCell}>结束日期</th>
                <th className={styles.statsHeaderCell}>最后更新时间</th>
                <th className={styles.statsHeaderCell}>最近检查</th>
                <th className={styles.statsHeaderCell}>表名</th>
                <th className={styles.statsHeaderCell}>操作</th>
              </tr>
            </thead>
            <tbody>
              {(() => {
                const grouped: Record<
                  "market" | "basic" | "xtquant" | "sector" | "other",
                  any[]
                > = { market: [], basic: [], xtquant: [], sector: [], other: [] };
                items.forEach((it: any) => {
                  const kindKey = String(it.data_kind || it.kind || "");
                  const cat = getCategoryKey(kindKey);
                  grouped[cat].push(it);
                });

                const order: Array<"market" | "basic" | "xtquant" | "sector" | "other"> = [
                  "market",
                  "basic",
                  "xtquant",
                  "sector",
                  "other",
                ];

                const rows: JSX.Element[] = [];

                order.forEach((catKey) => {
                  const catItems = grouped[catKey];
                  if (!catItems.length) return;

                  const isCollapsed = collapsedCategories[catKey];
                  rows.push(
                    <tr key={`cat-${catKey}`} className={styles.statsCategoryRow}>
                      <td
                        className={styles.statsCell}
                        colSpan={9}
                        style={{ fontWeight: 500, background: "#f3f4f6" }}
                      >
                        <button
                          type="button"
                          onClick={() =>
                            setCollapsedCategories((prev) => ({
                              ...prev,
                              [catKey]: !prev[catKey],
                            }))
                          }
                          className={styles.btnGhost}
                          style={{
                            padding: "2px 8px",
                            fontSize: 12,
                            marginRight: 6,
                          }}
                        >
                          {isCollapsed ? "▶" : "▼"}
                        </button>
                        {CATEGORY_LABELS[catKey]}（{catItems.length} 个数据集）
                      </td>
                    </tr>,
                  );

                  if (isCollapsed) return;

                  catItems.forEach((it: any, idx: number) => {
                    const extra =
                      (it?.extra_info &&
                        (typeof it.extra_info === "object"
                          ? it.extra_info
                          : {})) ||
                      {};
                    const lastRaw = it.last_updated_at;
                    const lastDisp =
                      lastRaw != null ? formatDateTime(String(lastRaw)) : "—";

                    const lastCheckAt = it.last_check_at
                      ? formatDateTime(String(it.last_check_at))
                      : "—";
                    let checkSummary = "";
                    if (it.last_check_result) {
                      try {
                        const res =
                          typeof it.last_check_result === "string"
                            ? JSON.parse(it.last_check_result)
                            : it.last_check_result;
                        if (typeof res.missing_days === "number") {
                          checkSummary = ` (缺失 ${res.missing_days} 天)`;
                        }
                      } catch {
                        // ignore
                      }
                    }

                    const kind = String(it.data_kind || it.kind || "");

                    let description =
                      extra.desc || it.label || it.description || "—";
                    if (
                      kind === "stock_moneyflow_ts" &&
                      (!description || description === "—")
                    ) {
                      description = "个股资金流（moneyflow · Tushare）";
                    }

                    const minDateStr =
                      it.min_date || it.date_min || it.start_date || null;
                    const canFillLatest = [
                      "kline_daily_raw",
                      "kline_daily_raw_go",
                      "kline_minute_raw",
                      "adj_factor",
                      "daily_basic",
                      "stock_basic",
                      "stock_moneyflow_ts",
                      "stock_st",
                      "bak_basic",
                      "anns_d",
                      "index_daily",
                      "xtquant_pershare_index",
                      "cyq_perf",
                      "cyq_chips",
                      "stk_limit",
                      "suspend_d",
                      "margin_detail",
                    ].includes(kind);

                    rows.push(
                      <tr key={`${catKey}-${idx}`}>
                        <td className={styles.statsCell}>
                          {it.data_kind || it.kind || "—"}
                        </td>
                        <td className={styles.statsCell}>{description}</td>
                        <td className={styles.statsCell}>
                          {it.row_count || it.rows || 0}
                        </td>
                        <td className={styles.statsCell}>
                          {it.min_date || it.date_min || it.start_date || "—"}
                        </td>
                        <td className={styles.statsCell}>
                          {it.max_date || it.date_max || it.end_date || "—"}
                        </td>
                        <td className={styles.statsCell}>{lastDisp}</td>
                        <td className={styles.statsCell}>
                          <div className={styles.statsCheckInfo}>{lastCheckAt}</div>
                          {checkSummary && (
                            <div
                              className={styles.statsCheckInfo}
                              style={{ color: "#666" }}
                            >
                              {checkSummary}
                            </div>
                          )}
                        </td>
                        <td className={styles.statsCell}>
                          {it.table_name || it.table || "—"}
                        </td>
                        <td className={styles.statsCell}>
                          <div className={styles.rowWrapSmall}>
                            {canFillLatest && kind && (
                              <button
                                type="button"
                                onClick={() =>
                                  handleFillLatestClick(kind, minDateStr)
                                }
                                disabled={fillLoadingKind === kind}
                                className={styles.btnSecondary}
                              >
                                {fillLoadingKind === kind
                                  ? "补齐中..."
                                  : "补齐到最新交易日"}
                              </button>
                            )}
                            {kind && (
                              <button
                                type="button"
                                onClick={() =>
                                  handleCheckGapsClick(kind, false)
                                }
                                disabled={gapLoadingKind === kind}
                                className={styles.btnGhost}
                              >
                                {gapLoadingKind === kind
                                  ? "检查中..."
                                  : "数据检查"}
                              </button>
                            )}
                          </div>
                        </td>
                      </tr>,
                    );
                  });
                });

                return rows;
              })()}
            </tbody>
          </table>
          {gapResult && (
            <div className={`${styles.cardSoft} ${styles.note}`}>
              <div className={styles.rowBetween}>
                <span>检查结果 (Kind: {gapResult.data_kind})</span>
                <button
                  type="button"
                  onClick={() => handleCheckGapsClick(gapResult.data_kind, true)}
                  disabled={gapLoadingKind === gapResult.data_kind}
                  className={styles.btnGhost}
                >
                  {gapLoadingKind === gapResult.data_kind ? "重新检查中..." : "立即重新检查(强制刷新)"}
                </button>
              </div>
              <div className={styles.textSmall}>
                表: {gapResult.table_name} · 区间: {gapResult.start_date} ~ {gapResult.end_date}
              </div>
              {gapResult.last_check_at && (
                <div className={styles.textMuted}>
                  结果生成于: {formatDateTime(gapResult.last_check_at)} (缓存)
                </div>
              )}
              {typeof gapResult.symbol_count === "number" && (
                <div className={styles.textSmall}>
                  覆盖股票数量: {gapResult.symbol_count}
                </div>
              )}
              <div className={styles.textSmall}>
                交易日总数: {gapResult.total_trading_days}，有数据天数:
                {" "}
                {gapResult.covered_days}，缺失交易日:
                {" "}
                {gapResult.missing_days}
              </div>
              {Array.isArray(gapResult.missing_ranges) &&
                gapResult.missing_ranges.length > 0 && (
                  <details style={{ marginTop: 4 }}>
                    <summary className={styles.textSmall}>
                      缺失日期段 ({gapResult.missing_ranges.length})
                    </summary>
                    <ul className={styles.gapSummary}>
                      {gapResult.missing_ranges.map((r: any, idx: number) => (
                        <li key={idx}>
                          {r.start === r.end ? r.start : `${r.start} ~ ${r.end}`}（{r.days} 天）
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
  const [confirmDeleteIdTest, setConfirmDeleteIdTest] = useState<string | null>(
    null,
  );

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

  const deleteTestSchedule = async (schedId: string) => {
    try {
      await backendRequest("DELETE", `/api/testing/schedule/${schedId}`);
      setConfirmDeleteIdTest(null);
      await loadAll();
    } catch (e: any) {
      setError(e?.message || "删除测试调度失败");
    }
  };

  const [newFreq, setNewFreq] = useState<string>("5m");
  const [newEnabled, setNewEnabled] = useState<boolean>(true);

  return (
    <div className={styles.column}>
      <h3 style={{ marginTop: 0, fontSize: 15 }}>🧪 TDX 接口自动化测试</h3>
      <div className={styles.rowWrap} style={{ marginBottom: 4 }}>
        <button
          type="button"
          onClick={triggerRunNow}
          disabled={loading}
          className={styles.btnPrimary}
          aria-label="立即执行测试"
          style={{ background: "#22c55e" }}
        >
          立即执行测试
        </button>
        <button
          type="button"
          onClick={loadAll}
          disabled={loading}
          className={styles.btnGhost}
          aria-label="刷新测试调度状态"
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
          <div className={styles.column}>
            {schedules.map((item: any) => {
              const schedId = item.schedule_id;
              const enabled = item.enabled ?? true;
              const freqValue = item.frequency || "";
              const freqLabel =
                FREQUENCY_CHOICES.find((f) => f.value === freqValue)?.label ||
                (freqValue || "手动");

              return (
                <div key={schedId} className={styles.card}>
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
                      上次运行：{formatDateTime(item.last_run_at)} · 下次运行：
                      {formatDateTime(item.next_run_at)}
                    </div>
                    <div>
                      上次状态：{item.last_status || "—"}
                      {item.last_inserted_rows != null && ` · 写入 ${Number(item.last_inserted_rows).toLocaleString()} 条`}
                      {" · "}错误信息：{item.last_error || "—"}
                    </div>
                  </div>
                  <div className={styles.rowWrap} style={{ fontSize: 12 }}>
                    <select
                      value={freqValue}
                      onChange={async (e) => {
                        await updateSchedule(
                          schedId,
                          e.target.value,
                          enabled,
                        );
                      }}
                      className={styles.btnSecondary}
                      aria-label="选择测试调度频率"
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
                      className={styles.btnGhost}
                      aria-label="切换测试调度启用状态"
                    >
                      切换启用
                    </button>
                    <button
                      type="button"
                      onClick={() => runSchedule(schedId)}
                      className={styles.btnGhost}
                      aria-label="立即运行测试调度"
                    >
                      立即运行
                    </button>
                    {confirmDeleteIdTest === schedId ? (
                      <>
                        <button
                          type="button"
                          onClick={() => deleteTestSchedule(schedId)}
                          style={{
                            padding: "4px 8px",
                            borderRadius: 6,
                            border: "1px solid #dc2626",
                            background: "#fee2e2",
                            color: "#b91c1c",
                            cursor: "pointer",
                          }}
                        >
                          确认删除
                        </button>
                        <button
                          type="button"
                          onClick={() => setConfirmDeleteIdTest(null)}
                          style={{
                            padding: "4px 8px",
                            borderRadius: 6,
                            border: "1px solid #d4d4d4",
                            background: "#fff",
                            cursor: "pointer",
                          }}
                        >
                          取消
                        </button>
                      </>
                    ) : (
                      <button
                        type="button"
                        onClick={() => setConfirmDeleteIdTest(schedId)}
                        style={{
                          padding: "4px 8px",
                          borderRadius: 6,
                          border: "1px solid #f97316",
                          background: "#fff7ed",
                          color: "#ea580c",
                          cursor: "pointer",
                        }}
                      >
                        删除
                      </button>
                    )}
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
        }}
      >
        <h4 style={{ fontSize: 14, margin: "4px 0" }}>新建测试调度</h4>
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

// 支持并行度（workers）设置的数据集
const WORKER_SUPPORTED_DATASETS = new Set([
  "kline_daily_raw", "kline_minute_raw", "kline_daily",
  "kline_daily_raw_go",
  "xtquant_pershare_index", "adjust_daily", "kline_adjust_daily",
]);

// 每日定时调度快捷创建：12 个目标数据集及默认执行时间
const DAILY_SCHEDULE_PRESETS: { dataset: string; label: string; source: string; defaultAt: string; frequency?: string }[] = [
  { dataset: "kline_daily_raw", label: "日线（未复权 RAW）", source: "TDX", defaultAt: "17:00" },
  { dataset: "kline_minute_raw", label: "分钟线（RAW）", source: "TDX", defaultAt: "17:00" },
  // Tushare 盘后数据（按执行时间排序）
  { dataset: "daily_basic", label: "股票每日指标", source: "Tushare", defaultAt: "16:30" },
  { dataset: "stock_moneyflow_ts", label: "个股资金流（TS）", source: "Tushare", defaultAt: "16:30" },
  { dataset: "adj_factor", label: "复权因子", source: "Tushare", defaultAt: "16:30" },
  { dataset: "index_daily", label: "指数日线行情", source: "Tushare", defaultAt: "16:30" },
  { dataset: "sw_sector", label: "申万行业板块（统一同步）", source: "Tushare", defaultAt: "16:45" },
  { dataset: "sector_data", label: "申万行业展开数据（后处理）", source: "Tushare", defaultAt: "17:00" },
  { dataset: "margin_detail", label: "融资融券明细", source: "Tushare", defaultAt: "17:07" },
  { dataset: "cyq_perf", label: "每日筹码及胜率", source: "Tushare", defaultAt: "17:15" },
  // Tushare 低频/不定期数据
  { dataset: "stock_basic", label: "股票基础信息", source: "Tushare", defaultAt: "17:30" },
  { dataset: "stock_st", label: "ST 标记", source: "Tushare", defaultAt: "17:33" },
  { dataset: "bak_basic", label: "备用基础信息", source: "Tushare", defaultAt: "17:36" },
  { dataset: "stk_limit", label: "每日涨跌停价格", source: "Tushare", defaultAt: "09:10" },
  { dataset: "suspend_d", label: "Daily suspend/resume info", source: "Tushare", defaultAt: "", frequency: "1h" },
];

/** 数据健康检查报告组件 — 在 _auto_retry_stale 调度卡片内展开 */
function HealthCheckReport({ summary }: { summary: any }) {
  if (!summary || typeof summary !== "object") return null;

  const ds: any[] = Array.isArray(summary.datasets) ? summary.datasets : [];
  const overall = summary.overall || "unknown";
  const latestDay = summary.latest_trading_day || "-";
  const retried: string[] = Array.isArray(summary.retried_datasets) ? summary.retried_datasets : [];
  const errors: any[] = Array.isArray(summary.retry_errors) ? summary.retry_errors : [];
  const checkTime = summary.check_time
    ? new Date(summary.check_time).toLocaleString("zh-CN", { hour12: false })
    : "-";

  const overallColor =
    overall === "ok" ? "#16a34a" : overall === "partial" ? "#d97706" : overall === "failed" ? "#dc2626" : "#6b7280";
  const overallBg =
    overall === "ok" ? "#dcfce7" : overall === "partial" ? "#fef3c7" : overall === "failed" ? "#fee2e2" : "#f3f4f6";
  const overallText =
    overall === "ok" ? "全部正常" : overall === "partial" ? "部分异常" : overall === "failed" ? "执行失败" : "未知状态";

  const freshCount = ds.filter((d) => d?.is_fresh === true).length;
  const staleCount = ds.filter((d) => d?.is_fresh === false).length;
  const unknownCount = ds.length - freshCount - staleCount;

  const thStyle: React.CSSProperties = {
    padding: "4px 8px", fontSize: 11, textAlign: "left", fontWeight: 600,
    borderBottom: "1px solid #e5e7eb", background: "#f9fafb", whiteSpace: "nowrap",
  };
  const tdStyle: React.CSSProperties = {
    padding: "3px 8px", fontSize: 11, borderBottom: "1px solid #f3f4f6", whiteSpace: "nowrap",
  };

  const actionLabel = (a: string | undefined) => {
    if (!a || a === "none") return { text: "-", color: "#9ca3af" };
    if (a === "retry") return { text: "重试", color: "#d97706" };
    if (a === "skip_no_schedule") return { text: "无调度", color: "#9ca3af" };
    return { text: a, color: "#6b7280" };
  };

  const jobStatusLabel = (s: string | undefined) => {
    if (!s) return { text: "-", color: "#6b7280" };
    if (s === "success") return { text: "成功", color: "#16a34a" };
    if (s === "failed") return { text: "失败", color: "#dc2626" };
    if (s === "running") return { text: "运行中", color: "#2563eb" };
    if (s === "no_job_today") return { text: "未执行", color: "#9ca3af" };
    return { text: s, color: "#6b7280" };
  };

  if (ds.length === 0) {
    return (
      <div style={{ marginTop: 8, borderTop: "1px dashed #d1d5db", paddingTop: 8, fontSize: 12, color: "#6b7280" }}>
        暂无检查报告数据
      </div>
    );
  }

  return (
    <div style={{ marginTop: 8, borderTop: "1px dashed #d1d5db", paddingTop: 8 }}>
      {/* 摘要行 */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap", fontSize: 12, marginBottom: 6 }}>
        <span style={{
          padding: "1px 8px", borderRadius: 4, fontWeight: 600, fontSize: 11,
          color: overallColor, background: overallBg,
        }}>
          {overallText}
        </span>
        <span style={{ color: "#6b7280" }}>
          检查时间: {checkTime}
        </span>
        <span style={{ color: "#6b7280" }}>
          最新交易日: {latestDay}
        </span>
        <span style={{ color: "#16a34a" }}>
          新鲜: {freshCount}
        </span>
        <span style={{ color: staleCount > 0 ? "#dc2626" : "#6b7280" }}>
          过期: {staleCount}
        </span>
        {unknownCount > 0 && (
          <span style={{ color: "#9ca3af" }}>
            未知: {unknownCount}
          </span>
        )}
        {retried.length > 0 && (
          <span style={{ color: "#d97706" }}>
            已重试: {retried.length}
          </span>
        )}
        {errors.length > 0 && (
          <span style={{ color: "#dc2626" }}>
            重试失败: {errors.length}
          </span>
        )}
      </div>

      {/* 可展开的数据集详情 */}
      <details style={{ marginTop: 4 }}>
        <summary style={{ cursor: "pointer", fontSize: 12, color: "#4b5563", marginBottom: 4 }}>
          查看各数据集检查结果（{ds.length} 个）
        </summary>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr>
                <th style={thStyle}>数据集</th>
                <th style={thStyle}>数据最新日期</th>
                <th style={thStyle}>新鲜度</th>
                <th style={thStyle}>今日状态</th>
                <th style={thStyle}>动作</th>
                <th style={thStyle}>写入行数</th>
              </tr>
            </thead>
            <tbody>
              {ds.map((d, idx) => {
                if (!d || !d.dataset) return null;
                const act = actionLabel(d.action);
                const job = jobStatusLabel(d.today_job_status);
                const isFresh = d.is_fresh === true;
                const isStale = d.is_fresh === false;
                const rowBg = isFresh
                  ? undefined
                  : d.action === "retry"
                    ? "#fffbeb"
                    : "#fef2f2";
                return (
                  <tr key={d.dataset} style={{ background: rowBg }}>
                    <td style={tdStyle}>
                      <span style={{ fontWeight: 500 }}>{d.dataset}</span>
                    </td>
                    <td style={tdStyle}>{d.data_max_date || "-"}</td>
                    <td style={tdStyle}>
                      <span style={{
                        display: "inline-block", width: 8, height: 8, borderRadius: "50%",
                        background: isFresh ? "#22c55e" : isStale ? "#ef4444" : "#d4d4d4", marginRight: 4,
                      }} />
                      {isFresh ? "新鲜" : isStale ? "过期" : "未知"}
                    </td>
                    <td style={{ ...tdStyle, color: job.color }}>{job.text}</td>
                    <td style={{ ...tdStyle, color: act.color, fontWeight: d.action === "retry" ? 600 : 400 }}>
                      {act.text}
                      {d.retry_status && (
                        <span style={{ marginLeft: 4, color: d.retry_status === "submitted" ? "#2563eb" : "#6b7280", fontWeight: 400 }}>
                          {d.retry_status}
                        </span>
                      )}
                    </td>
                    <td style={tdStyle}>
                      {d.inserted_rows != null ? Number(d.inserted_rows).toLocaleString() : "-"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {/* 重试详情 */}
        {retried.length > 0 && (
          <div style={{ marginTop: 8, fontSize: 11, color: "#6b7280" }}>
            <span style={{ fontWeight: 600, color: "#d97706" }}>重试数据集:</span>{" "}
            {retried.join(", ")}
          </div>
        )}
        {errors.length > 0 && (
          <div style={{ marginTop: 4, fontSize: 11 }}>
            <span style={{ fontWeight: 600, color: "#dc2626" }}>重试错误:</span>
            <ul style={{ margin: "2px 0", paddingLeft: 16 }}>
              {errors.map((e, i) => (
                <li key={`${e?.dataset || e?.name || ""}-${i}`} style={{ color: "#dc2626" }}>
                  {e?.dataset || e?.name || "unknown"}: {e?.error || e?.message || JSON.stringify(e)}
                </li>
              ))}
            </ul>
          </div>
        )}
      </details>
    </div>
  );
}

function IngestionSchedulesTab() {
  const [schedules, setSchedules] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [dataSource, setDataSource] = useState<DataSource>("TDX");
  const [newDataset, setNewDataset] = useState<string>("kline_daily_raw");
  const [newMode, setNewMode] = useState<"incremental" | "init">(
    "incremental",
  );
  const [newFreq, setNewFreq] = useState<string>("5m");
  const [newDailyAt, setNewDailyAt] = useState<string>("16:30");
  const [newEnabled, setNewEnabled] = useState<boolean>(true);
  const [newWorkers, setNewWorkers] = useState<number>(0); // 0 = 默认
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null);

  // 编辑执行时间
  const [editingAtTime, setEditingAtTime] = useState<Record<string, string>>({});

  // 快捷创建：每个数据集的执行时间
  const [presetTimes, setPresetTimes] = useState<Record<string, string>>(
    () => Object.fromEntries(DAILY_SCHEDULE_PRESETS.map((p) => [p.dataset, p.defaultAt])),
  );
  const [batchCreating, setBatchCreating] = useState(false);
  const [runningAll, setRunningAll] = useState(false);
  const [runningDataset, setRunningDataset] = useState<string | null>(null);
  const [presetWorkers, setPresetWorkers] = useState<Record<string, number>>({ kline_daily_raw: 4, kline_minute_raw: 4 });
  const [presetStats, setPresetStats] = useState<Record<string, string | null>>({});
  const [statsLoading, setStatsLoading] = useState(false);

  // 每日执行状态
  const [dailyStatus, setDailyStatus] = useState<Record<string, { status: string; created_at?: string; finished_at?: string } | undefined>>({});
  const [dailyStatusLoading, setDailyStatusLoading] = useState(false);

  // truncate相关状态
  const [initTruncate, setInitTruncate] = useState<boolean>(false);
  const [showTruncateConfirm, setShowTruncateConfirm] = useState<boolean>(false);

  // 当数据源切换时，更新默认数据集
  useEffect(() => {
    const datasets = INGESTION_DATASETS_BY_SOURCE[dataSource];
    const firstKey = Object.keys(datasets)[0];
    setNewDataset(firstKey);
  }, [dataSource]);

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

  const runManual = async (dataset: string, mode: string, truncate?: boolean) => {
    try {
      const payload: any = {
        dataset,
        mode,
        triggered_by: "ui",
      };
      // 仅在初始化模式且启用truncate时传递参数
      if (mode === "init" && truncate) {
        payload.truncate = true;
      }
      await backendRequest("POST", "/api/ingestion/run", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
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
    options?: Record<string, any>,
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
          options: options || {},
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

  const deleteSchedule = async (schedId: string) => {
    try {
      await backendRequest("DELETE", `/api/ingestion/schedule/${schedId}`);
      setConfirmDeleteId(null);
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "删除入库调度失败");
    }
  };

  const createSchedule = async () => {
    try {
      const freq = newFreq || "5m";
      const options: Record<string, any> = {};
      if (freq === "daily" && newDailyAt) {
        options.at = newDailyAt;
      }
      if (newWorkers > 0 && WORKER_SUPPORTED_DATASETS.has(newDataset)) {
        options.workers = newWorkers;
      }
      await backendRequest("POST", "/api/ingestion/schedule", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataset: newDataset,
          mode: newMode,
          frequency: freq,
          enabled: newEnabled,
          options,
        }),
      });
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "入库调度创建失败");
    }
  };

  const batchCreateSchedules = async (datasets?: string[]) => {
    setBatchCreating(true);
    setError(null);
    try {
      const targets = datasets
        ? DAILY_SCHEDULE_PRESETS.filter((p) => datasets.includes(p.dataset))
        : DAILY_SCHEDULE_PRESETS;
      const items = targets.map((p) => ({
        dataset: p.dataset,
        mode: "incremental" as const,
        frequency: p.frequency || "daily",
        enabled: true,
        at: p.frequency ? undefined : presetTimes[p.dataset] || p.defaultAt,
        workers: presetWorkers[p.dataset] || undefined,
      }));
      await backendRequest("POST", "/api/ingestion/schedule/batch-create", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items }),
      });
      await loadSchedules();
    } catch (e: any) {
      setError(e?.message || "批量创建调度失败");
    } finally {
      setBatchCreating(false);
    }
  };

  const runAllPresets = async () => {
    setRunningAll(true);
    setError(null);
    try {
      await backendRequest("POST", "/api/ingestion/schedule/run-all-presets", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
    } catch (e: any) {
      setError(e?.message || "立即执行全部失败");
    } finally {
      setRunningAll(false);
    }
  };

  const runSinglePreset = async (dataset: string) => {
    setRunningDataset(dataset);
    setError(null);
    try {
      const body: any = { dataset };
      if (presetWorkers[dataset]) body.workers = presetWorkers[dataset];
      await backendRequest("POST", "/api/ingestion/schedule/run-single-preset", {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      loadDailyStatus();
    } catch (e: any) {
      setError(e?.message || `执行 ${dataset} 失败`);
    } finally {
      setRunningDataset(null);
    }
  };

  const loadPresetStats = async () => {
    setStatsLoading(true);
    setError(null);
    try {
      const data: any = await backendRequest("GET", "/api/ingestion/schedule/preset-stats");
      const map: Record<string, string | null> = {};
      for (const item of data?.items || []) {
        map[item.dataset] = item.current_max_date || null;
      }
      setPresetStats(map);
    } catch (e: any) {
      setError(e?.message || "刷新数据集统计失败");
    } finally {
      setStatsLoading(false);
    }
  };

  const loadDailyStatus = useCallback(async () => {
    setDailyStatusLoading(true);
    try {
      const data: any = await backendRequest("GET", "/api/ingestion/schedule/preset-daily-status");
      setDailyStatus(data?.items || {});
    } catch {
      // 静默失败，不影响主流程
    } finally {
      setDailyStatusLoading(false);
    }
  }, []);

  // 组件加载时获取每日状态
  useEffect(() => {
    loadDailyStatus();
  }, [loadDailyStatus]);

  // 自动刷新：有 running/queued 状态时每 5s 刷新
  useEffect(() => {
    const hasActive = Object.values(dailyStatus).some(
      (ds) => ds && (ds.status === "running" || ds.status === "queued"),
    );
    if (!hasActive) return;
    const timer = setInterval(() => {
      loadDailyStatus();
    }, 5000);
    return () => clearInterval(timer);
  }, [dailyStatus, loadDailyStatus]);

  return (
    <div className={styles.section}>
      <style>{`@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }`}</style>
      <h3 className={styles.headingSmall}>📥 数据入库调度</h3>

      <div className={styles.rowWrap} style={{ marginBottom: 6 }}>
        <button
          type="button"
          onClick={createDefaults}
          disabled={loading}
          className={styles.btnPrimary}
          aria-label="创建默认调度"
        >
          创建默认调度
        </button>
      </div>

      {/* 每日定时调度快捷创建 */}
      <div className={styles.card} style={{ marginBottom: 12 }}>
        <h4 className={styles.headingSmall}>⏰ 每日定时调度 · 快捷创建</h4>
        <p className={styles.textMutedSmall} style={{ marginBottom: 8 }}>
          为 {DAILY_SCHEDULE_PRESETS.length} 个核心数据集创建每日增量调度，自动检测缺口并补齐到最新交易日。
        </p>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
          {DAILY_SCHEDULE_PRESETS.map((p) => {
            const ds = dailyStatus[p.dataset];
            const statusDot = (s: typeof ds) => {
              if (!s) return { bg: "#d4d4d4", color: "#9ca3af", text: "未执行" };
              if (s.status === "success") return { bg: "#22c55e", color: "#16a34a", text: "成功" };
              if (s.status === "failed") return { bg: "#ef4444", color: "#dc2626", text: "失败" };
              if (s.status === "running") return { bg: "#3b82f6", color: "#2563eb", text: "执行中" };
              return { bg: "#eab308", color: "#ca8a04", text: "排队中" };
            };
            const mainSt = statusDot(ds);
            return (
            <React.Fragment key={p.dataset}>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                fontSize: 12,
                padding: "4px 0",
              }}
            >
              <span
                style={{
                  background: p.source === "TDX" ? "#dbeafe" : "#fef3c7",
                  color: p.source === "TDX" ? "#1d4ed8" : "#92400e",
                  borderRadius: 4,
                  padding: "1px 6px",
                  fontSize: 11,
                  fontWeight: 500,
                }}
              >
                {p.source}
              </span>
              <span style={{ flex: 1, minWidth: 0 }}>
                {p.dataset} · {p.label}
                {presetStats[p.dataset] !== undefined && (
                  <span style={{ marginLeft: 4, fontSize: 11, color: presetStats[p.dataset] ? "#16a34a" : "#dc2626" }}>
                    [{presetStats[p.dataset] || "无数据"}]
                  </span>
                )}
              </span>
              {/* 每日执行状态 */}
              <span style={{ fontSize: 11, display: "inline-flex", alignItems: "center", gap: 3, minWidth: 56 }}>
                <span style={{
                  width: 8, height: 8, borderRadius: "50%", display: "inline-block",
                  background: mainSt.bg,
                  animation: ds?.status === "running" ? "pulse 1.5s ease-in-out infinite" : undefined,
                }} />
                <span style={{ color: mainSt.color }}>{mainSt.text}</span>
              </span>
              <input
                type="time"
                value={presetTimes[p.dataset] || p.defaultAt}
                onChange={(e) =>
                  setPresetTimes((prev) => ({ ...prev, [p.dataset]: e.target.value }))
                }
                style={{ width: 90, fontSize: 12, padding: "2px 4px", borderRadius: 4, border: "1px solid #d4d4d4" }}
                aria-label={`${p.dataset} 执行时间`}
              />
              {p.source === "TDX" && (
                <select
                  value={presetWorkers[p.dataset] || 4}
                  onChange={(e) => setPresetWorkers((prev) => ({ ...prev, [p.dataset]: Number(e.target.value) }))}
                  style={{ width: 58, fontSize: 11, padding: "2px 2px", borderRadius: 4, border: "1px solid #d4d4d4" }}
                  aria-label={`${p.dataset} 并发度`}
                >
                  {[1, 2, 4, 8, 16].map((n) => (
                    <option key={n} value={n}>{n}线程</option>
                  ))}
                </select>
              )}
              <button
                type="button"
                onClick={() => runSinglePreset(p.dataset)}
                disabled={runningDataset === p.dataset}
                className={styles.btnPrimary}
                style={{ fontSize: 11, padding: "2px 8px", background: "#22c55e" }}
              >
                {runningDataset === p.dataset ? "执行中" : "执行"}
              </button>
              <button
                type="button"
                onClick={() => batchCreateSchedules([p.dataset])}
                disabled={batchCreating}
                className={styles.btnToggle}
                style={{ fontSize: 11, padding: "2px 8px" }}
              >
                创建
              </button>
            </div>
            {/* sw_sector 子任务展示 */}
            {p.dataset === "sw_sector" && (
              <div style={{ gridColumn: "1 / -1", paddingLeft: 40, fontSize: 11, color: "#6b7280", lineHeight: 1.8 }}>
                {([
                  ["sw_index_classify", "行业分类"],
                  ["sw_index_member", "成分股映射"],
                  ["sw_daily", "行业日线"],
                ] as const).map(([subDs, subLabel], idx, arr) => {
                  const subSt = statusDot(dailyStatus[subDs]);
                  const prefix = idx < arr.length - 1 ? "├" : "└";
                  return (
                    <div key={subDs}>
                      {prefix} {subDs} {subLabel}
                      <span style={{ marginLeft: 8, display: "inline-flex", alignItems: "center", gap: 3 }}>
                        <span style={{
                          width: 6, height: 6, borderRadius: "50%", display: "inline-block",
                          background: subSt.bg,
                          animation: dailyStatus[subDs]?.status === "running" ? "pulse 1.5s ease-in-out infinite" : undefined,
                        }} />
                        <span style={{ color: subSt.color }}>{subSt.text}</span>
                      </span>
                    </div>
                  );
                })}
              </div>
            )}
            </React.Fragment>
            );
          })}
        </div>
        <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button
            type="button"
            onClick={() => batchCreateSchedules()}
            disabled={batchCreating}
            className={styles.btnPrimary}
            style={{ fontSize: 13 }}
          >
            {batchCreating ? "创建中…" : "一键创建全部调度"}
          </button>
          <button
            type="button"
            onClick={runAllPresets}
            disabled={runningAll}
            className={styles.btnPrimary}
            style={{ fontSize: 13, background: "#22c55e" }}
          >
            {runningAll ? "执行中…" : "立即执行全部"}
          </button>
          <button
            type="button"
            onClick={() => { loadPresetStats(); loadDailyStatus(); }}
            disabled={statsLoading}
            className={styles.btnToggle}
            style={{ fontSize: 13 }}
          >
            {statsLoading ? "刷新中…" : "刷新数据集统计"}
          </button>
        </div>
      </div>

      <div className={styles.card}>
        <h4 className={styles.headingSmall}>手动执行入库任务</h4>
        <div className={styles.rowWrap}>
          <select
            value={dataSource}
            onChange={(e) => setDataSource(e.target.value as DataSource)}
            className={styles.select}
            aria-label="选择数据源"
          >
            <option value="TDX">TDX</option>
            <option value="Tushare">Tushare</option>
            <option value="xtquant">xtquant</option>
          </select>
          <select
            value={newDataset}
            onChange={(e) => setNewDataset(e.target.value)}
            className={styles.select}
            aria-label="选择手动执行数据集"
          >
            {Object.entries(INGESTION_DATASETS_BY_SOURCE[dataSource]).map(([key, label]) => (
              <option key={key} value={key}>{`${key} · ${label}`}</option>
            ))}
          </select>
          <label className={styles.labelSmall}>
            <input
              type="radio"
              checked={newMode === "incremental"}
              onChange={() => setNewMode("incremental")}
              className={styles.inputCheckbox}
            />
            增量
          </label>
          <label className={styles.labelSmall}>
            <input
              type="radio"
              checked={newMode === "init"}
              onChange={() => setNewMode("init")}
              className={styles.inputCheckbox}
            />
            初始化
          </label>
          {newMode === "init" && TRUNCABLE_DATASETS.includes(newDataset) && (
            <label className={styles.labelSmall} style={{ color: "#dc2626" }}>
              <input
                type="checkbox"
                checked={initTruncate}
                onChange={(e) => setInitTruncate(e.target.checked)}
                className={styles.inputCheckbox}
              />
              清空现有数据
            </label>
          )}
          <button
            type="button"
            onClick={() => {
              if (newMode === "init" && initTruncate && TRUNCABLE_DATASETS.includes(newDataset)) {
                setShowTruncateConfirm(true);
              } else {
                runManual(newDataset, newMode, initTruncate);
              }
            }}
            disabled={loading}
            className={styles.btnPrimary}
            aria-label="立即执行入库任务"
            style={{ background: "#22c55e" }}
          >
            立即执行
          </button>
        </div>
      </div>

      {error && (
        <p className={styles.textDangerSmall}>{error}</p>
      )}

      {/* truncate二次确认弹窗 */}
      {showTruncateConfirm && (
        <div
          style={{
            position: "fixed",
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: "rgba(0, 0, 0, 0.5)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              maxWidth: 450,
              padding: 24,
              boxShadow: "0 4px 20px rgba(0, 0, 0, 0.15)",
            }}
          >
            <h3 style={{ margin: "0 0 16px", fontSize: 16, color: "#dc2626" }}>
              ⚠️ 危险操作确认
            </h3>
            <p style={{ fontSize: 14, marginBottom: 12, lineHeight: 1.6 }}>
              您选择了<strong>清空现有数据</strong>选项，这将永久删除数据集
              <strong>「{newDataset} · {INGESTION_DATASETS[newDataset] || newDataset}」</strong>
              中的所有现有数据，然后重新导入。
            </p>
            <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 20 }}>
              此操作不可恢复，请确认是否继续？
            </p>
            <div style={{ display: "flex", gap: 12, justifyContent: "flex-end" }}>
              <button
                type="button"
                onClick={() => setShowTruncateConfirm(false)}
                style={{
                  padding: "8px 16px",
                  borderRadius: 6,
                  border: "1px solid #d4d4d4",
                  background: "#fff",
                  cursor: "pointer",
                  fontSize: 13,
                }}
              >
                取消
              </button>
              <button
                type="button"
                onClick={() => {
                  setShowTruncateConfirm(false);
                  runManual(newDataset, newMode, true);
                }}
                style={{
                  padding: "8px 16px",
                  borderRadius: 6,
                  border: "1px solid #dc2626",
                  background: "#fee2e2",
                  color: "#b91c1c",
                  cursor: "pointer",
                  fontSize: 13,
                  fontWeight: 500,
                }}
              >
                确认清空并初始化
              </button>
            </div>
          </div>
        </div>
      )}

      <div>
        <h4 className={styles.headingSmall}>已配置的入库调度</h4>
        {schedules.length === 0 && !loading ? (
          <p className={styles.textMuted}>尚未配置入库调度，使用下方表单新建。</p>
        ) : (
          <div className={styles.section}>
            {[...schedules]
              .sort((a, b) => {
                // 启用的排前面
                const aEnabled = a.enabled ?? true;
                const bEnabled = b.enabled ?? true;
                if (aEnabled !== bEnabled) return aEnabled ? -1 : 1;
                // 按执行时间排序
                const aAt = (a.options?.at || "99:99") as string;
                const bAt = (b.options?.at || "99:99") as string;
                return aAt.localeCompare(bAt);
              })
              .map((item: any) => {
              const schedId = item.schedule_id;
              const schedDataset = item.dataset;
              const mode = item.mode;
              const enabled = item.enabled ?? true;
              const freqValue = item.frequency || "";
              const opts = item.options || {};
              const atTime = opts.at || "";
              const baseLabel =
                FREQUENCY_CHOICES.find((f) => f.value === freqValue)?.label ||
                (freqValue || "手动");
              const freqLabel =
                freqValue === "daily" && atTime
                  ? `每日 ${atTime}`
                  : baseLabel;

              const isHealthCheck = schedDataset === "_auto_retry_stale";
              const displayName = isHealthCheck ? "数据健康检查" : schedDataset;
              const displayMode = isHealthCheck ? "check_and_retry" : mode;

              return (
                <div key={schedId} className={styles.cardSoft}>
                  <div className={styles.rowBetween} style={{ fontSize: 13 }}>
                    <div>
                      {displayName} · {displayMode}
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      {freqValue === "daily" && (() => {
                        const isEditing = schedId in editingAtTime;
                        return isEditing ? (
                          <>
                            <input
                              type="time"
                              value={editingAtTime[schedId] || atTime}
                              onChange={(e) => setEditingAtTime(prev => ({ ...prev, [schedId]: e.target.value }))}
                              style={{ fontSize: 11, padding: "1px 4px", border: "1px solid #93c5fd", borderRadius: 4, width: 80 }}
                            />
                            <button
                              type="button"
                              onClick={async () => {
                                const newAt = editingAtTime[schedId];
                                if (newAt && newAt !== atTime) {
                                  await updateSchedule(schedId, schedDataset, mode, freqValue, enabled, { ...opts, at: newAt });
                                }
                                setEditingAtTime(prev => { const next = { ...prev }; delete next[schedId]; return next; });
                              }}
                              style={{ fontSize: 10, padding: "1px 6px", background: "#2563eb", color: "#fff", border: "none", borderRadius: 3, cursor: "pointer" }}
                            >
                              确认
                            </button>
                            <button
                              type="button"
                              onClick={() => setEditingAtTime(prev => { const next = { ...prev }; delete next[schedId]; return next; })}
                              style={{ fontSize: 10, padding: "1px 6px", background: "#e5e7eb", border: "none", borderRadius: 3, cursor: "pointer" }}
                            >
                              取消
                            </button>
                          </>
                        ) : (
                          <span
                            style={{ fontSize: 11, background: "#e0f2fe", color: "#0369a1", borderRadius: 4, padding: "1px 6px", cursor: "pointer" }}
                            title="点击编辑执行时间"
                            onClick={() => setEditingAtTime(prev => ({ ...prev, [schedId]: atTime || "16:30" }))}
                          >
                            {atTime || "未设置"}
                          </span>
                        );
                      })()}
                      <span className={styles.textMuted}>
                        {enabled ? "🟢 启用" : "⚪️ 停用"}
                      </span>
                    </div>
                  </div>
                  <div className={styles.textMutedSmall}>
                    <div>调度频率：{freqLabel}</div>
                    <div>
                      上次运行：{formatDateTime(item.last_run_at)} · 下次运行：
                      {formatDateTime(item.next_run_at)}
                    </div>
                    <div>
                      上次状态：{item.last_status || "—"}
                      {item.last_inserted_rows != null && ` · 写入 ${Number(item.last_inserted_rows).toLocaleString()} 条`}
                      {" · "}错误信息：{item.last_error || "—"}
                    </div>
                    {WORKER_SUPPORTED_DATASETS.has(schedDataset) && (
                      <div>并行度：{opts.workers || "默认"}</div>
                    )}
                  </div>
                  <div className={styles.rowWrapSmall}>
                    <select
                      value={freqValue}
                      onChange={async (e) => {
                        await updateSchedule(
                          schedId,
                          schedDataset,
                          mode,
                          e.target.value,
                          enabled,
                          opts,
                        );
                      }}
                      className={styles.selectSmall}
                    >
                      {FREQUENCY_CHOICES.map((f) => (
                        <option key={f.value} value={f.value}>
                          {f.label}
                        </option>
                      ))}
                    </select>
                    {WORKER_SUPPORTED_DATASETS.has(schedDataset) && (
                      <select
                        value={opts.workers || ""}
                        onChange={async (e) => {
                          const newOpts = { ...opts };
                          const val = Number(e.target.value);
                          if (val > 0) {
                            newOpts.workers = val;
                          } else {
                            delete newOpts.workers;
                          }
                          await updateSchedule(
                            schedId,
                            schedDataset,
                            mode,
                            freqValue,
                            enabled,
                            newOpts,
                          );
                        }}
                        className={styles.selectSmall}
                        style={{ width: 80 }}
                        aria-label={`${schedDataset} 并行度`}
                      >
                        <option value="">默认</option>
                        {[1, 2, 4, 8, 16].map((n) => (
                          <option key={n} value={n}>{n} 线程</option>
                        ))}
                      </select>
                    )}
                    <label>
                      <input
                        type="checkbox"
                        checked={enabled}
                        onChange={async (e) => {
                          await updateSchedule(
                            schedId,
                            schedDataset,
                            mode,
                            freqValue,
                            e.target.checked,
                            opts,
                          );
                        }}
                        className={styles.inputCheckbox}
                      />
                      启用调度
                    </label>
                    <button
                      type="button"
                      onClick={() => toggleSchedule(schedId, !enabled)}
                      className={styles.btnToggle}
                    >
                      切换启用
                    </button>
                    <button
                      type="button"
                      onClick={() => runSchedule(schedId)}
                      className={styles.btnToggle}
                    >
                      立即运行
                    </button>
                    {confirmDeleteId === schedId ? (
                      <>
                        <button
                          type="button"
                          onClick={() => deleteSchedule(schedId)}
                          className={styles.btnDelete}
                        >
                          确认删除
                        </button>
                        <button
                          type="button"
                          onClick={() => setConfirmDeleteId(null)}
                          className={styles.btnToggle}
                        >
                          取消
                        </button>
                      </>
                    ) : (
                      <button
                        type="button"
                        onClick={() => setConfirmDeleteId(schedId)}
                        className={styles.btnWarnSoft}
                      >
                        删除
                      </button>
                    )}
                  </div>
                  {/* 数据健康检查报告 */}
                  {schedDataset === "_auto_retry_stale" && item.last_job_summary?.datasets && (
                    <HealthCheckReport summary={item.last_job_summary} />
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      <div className={`${styles.card} ${styles.cardDividerTop}`}>
        <h4 className={styles.headingSmall}>新建入库调度</h4>
        <div className={styles.rowWrapSmall}>
          <select
            value={dataSource}
            onChange={(e) => setDataSource(e.target.value as DataSource)}
            className={styles.selectSmall}
          >
            <option value="TDX">TDX</option>
            <option value="Tushare">Tushare</option>
            <option value="xtquant">xtquant</option>
          </select>
          <select
            value={newDataset}
            onChange={(e) => setNewDataset(e.target.value)}
            className={styles.selectSmall}
          >
            {Object.entries(INGESTION_DATASETS_BY_SOURCE[dataSource]).map(([key, label]) => (
              <option key={key} value={key}>{`${key} · ${label}`}</option>
            ))}
          </select>
          <label className={styles.labelSmall}>
            <input
              type="radio"
              checked={newMode === "incremental"}
              onChange={() => setNewMode("incremental")}
              className={styles.inputCheckbox}
            />
            增量
          </label>
          <label className={styles.labelSmall}>
            <input
              type="radio"
              checked={newMode === "init"}
              onChange={() => setNewMode("init")}
              className={styles.inputCheckbox}
            />
            初始化
          </label>
          <select
            value={newFreq}
            onChange={(e) => setNewFreq(e.target.value)}
            className={styles.selectSmall}
          >
            {FREQUENCY_CHOICES.map((f) => (
              <option key={f.value} value={f.value}>
                {f.label}
              </option>
            ))}
          </select>
          {newFreq === "daily" && (
            <input
              type="time"
              value={newDailyAt}
              onChange={(e) => setNewDailyAt(e.target.value)}
              style={{ width: 90, fontSize: 12, padding: "2px 4px", borderRadius: 4, border: "1px solid #d4d4d4" }}
              aria-label="每日执行时间"
            />
          )}
          {WORKER_SUPPORTED_DATASETS.has(newDataset) && (
            <select
              value={newWorkers}
              onChange={(e) => setNewWorkers(Number(e.target.value))}
              className={styles.selectSmall}
              style={{ width: 80 }}
              aria-label="并行度"
            >
              <option value={0}>默认</option>
              {[1, 2, 4, 8, 16].map((n) => (
                <option key={n} value={n}>{n} 线程</option>
              ))}
            </select>
          )}
          <label className={styles.labelSmall}>
            <input
              type="checkbox"
              checked={newEnabled}
              onChange={(e) => setNewEnabled(e.target.checked)}
              className={styles.inputCheckbox}
            />
            启用调度
          </label>
          <button
            type="button"
            onClick={createSchedule}
            disabled={loading}
            className={styles.btnSuccessSmall}
            aria-label="创建调度"
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


// ================================================================
// 因子指标调度 Tab
// ================================================================

const FM_SCHED_BASE = `${TDX_BASE}/api/v1/factor-metrics`;

interface FmSchedule {
  schedule_id: string;
  dataset: string;
  mode: string;
  enabled: boolean;
  frequency: string;
  options: {
    factor_names?: string[];
    include_disabled?: boolean;
    data_date?: string;
    workers?: number;
    one_shot?: boolean;
    at?: string;
    day_of_week?: string;
  };
  last_run_at?: string;
  next_run_at?: string;
  last_status?: string;
  last_error?: string;
  created_at?: string;
  updated_at?: string;
}

function parseFactorNamesInput(value: string): string[] | undefined {
  const names = value
    .split(/[\n,，;；\s]+/)
    .map((item) => item.trim())
    .filter(Boolean);
  return names.length ? Array.from(new Set(names)) : undefined;
}

function FactorMetricsSchedulesTab() {
  const [schedules, setSchedules] = useState<FmSchedule[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [newFreq, setNewFreq] = useState("weekly");
  const [newAt, setNewAt] = useState("18:30");
  const [newDayOfWeek, setNewDayOfWeek] = useState("sunday");
  const [newDataDate, setNewDataDate] = useState("");
  const [newFactorNames, setNewFactorNames] = useState("");
  const [newWorkers, setNewWorkers] = useState(2);
  const [newIncludeDisabled, setNewIncludeDisabled] = useState(false);
  const [newOneShot, setNewOneShot] = useState(false);
  const [newEnabled, setNewEnabled] = useState(true);
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null);

  const fetchSchedules = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const res = await fetch(`${FM_SCHED_BASE}/schedules`);
      const data = await res.json();
      setSchedules(data.items || []);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchSchedules(); }, [fetchSchedules]);

  const handleCreate = async () => {
    try {
      const res = await fetch(`${FM_SCHED_BASE}/schedules`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          frequency: newFreq, at: newAt, day_of_week: newDayOfWeek,
          data_date: newDataDate || null, include_disabled: newIncludeDisabled,
          factor_names: parseFactorNamesInput(newFactorNames),
          workers: Math.max(1, Math.min(8, Number(newWorkers) || 1)),
          one_shot: newOneShot, enabled: newEnabled,
        }),
      });
      if (!res.ok) {
        const err = await res.json();
        alert(`创建失败: ${err.detail || JSON.stringify(err)}`);
        return;
      }
      await fetchSchedules();
    } catch (e: any) { alert(`创建失败: ${e.message}`); }
  };

  const handleToggle = async (id: string, enabled: boolean) => {
    try {
      await fetch(`${FM_SCHED_BASE}/schedules/${id}/toggle?enabled=${enabled}`, { method: "POST" });
      await fetchSchedules();
    } catch (e: any) { alert(`切换失败: ${e.message}`); }
  };

  const handleRunNow = async (id: string) => {
    try {
      const res = await fetch(`${FM_SCHED_BASE}/schedules/${id}/run`, { method: "POST" });
      const data = await res.json();
      alert(`已提交: job_id=${data.job_id}`);
    } catch (e: any) { alert(`执行失败: ${e.message}`); }
  };

  const handleDelete = async (id: string) => {
    try {
      await fetch(`${FM_SCHED_BASE}/schedules/${id}`, { method: "DELETE" });
      setConfirmDeleteId(null);
      await fetchSchedules();
    } catch (e: any) { alert(`删除失败: ${e.message}`); }
  };

  const statusColor = (s?: string) => {
    if (s === "success") return "#4caf50";
    if (s === "failed") return "#f44336";
    if (s === "running" || s === "queued") return "#ff9800";
    return "#999";
  };

  const DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"];
  const DAY_LABELS: Record<string, string> = {
    monday: "周一", tuesday: "周二", wednesday: "周三", thursday: "周四",
    friday: "周五", saturday: "周六", sunday: "周日",
  };

  return (
    <div style={{ padding: 16 }}>
      <h3 style={{ marginBottom: 12 }}>因子独立指标定时计算</h3>

      {/* 创建表单 */}
      <div className={styles.card} style={{ marginBottom: 16, padding: 16 }}>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "flex-end" }}>
          <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            任务类型
            <select
              value={newOneShot ? "one_shot" : "recurring"}
              onChange={(e) => setNewOneShot(e.target.value === "one_shot")}
              data-testid="factor-metrics-task-type"
              className={styles.select}
            >
              <option value="recurring">周期任务</option>
              <option value="one_shot">单次任务</option>
            </select>
          </label>

          {!newOneShot && (
            <>
              <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                频率
                <select value={newFreq} onChange={(e) => setNewFreq(e.target.value)} className={styles.select}>
                  <option value="weekly">每周</option>
                  <option value="daily">每日</option>
                  <option value="">手动</option>
                </select>
              </label>
              {newFreq === "weekly" && (
                <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  周几
                  <select value={newDayOfWeek} onChange={(e) => setNewDayOfWeek(e.target.value)} className={styles.select}>
                    {DAYS.map((d) => (
                      <option key={d} value={d}>{DAY_LABELS[d]}</option>
                    ))}
                  </select>
                </label>
              )}
              <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                时间
                <input type="time" value={newAt} onChange={(e) => setNewAt(e.target.value)} className={styles.input} />
              </label>
            </>
          )}

          <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            快照日期
            <input
              type="text"
              value={newDataDate}
              onChange={(e) => setNewDataDate(e.target.value)}
              placeholder="YYYYMMDD 留空用最新"
              data-testid="factor-metrics-data-date"
              className={styles.input} style={{ width: 140  }}
            />
          </label>

          <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            指定因子（可选）
            <textarea
              value={newFactorNames}
              onChange={(e) => setNewFactorNames(e.target.value)}
              placeholder="留空=全部；支持逗号/空格/换行分隔"
              data-testid="factor-metrics-factor-names"
              className={styles.input}
              style={{ width: 280, minHeight: 56, resize: "vertical" }}
            />
          </label>

          <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            并行度
            <input
              type="number"
              min={1}
              max={8}
              value={newWorkers}
              onChange={(e) => setNewWorkers(Number(e.target.value))}
              data-testid="factor-metrics-workers"
              className={styles.input}
              style={{ width: 80 }}
            />
          </label>

          <label style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <input type="checkbox" checked={newIncludeDisabled} onChange={(e) => setNewIncludeDisabled(e.target.checked)} data-testid="factor-metrics-include-disabled" />
            包含禁用因子
          </label>

          <label style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <input type="checkbox" checked={newEnabled} onChange={(e) => setNewEnabled(e.target.checked)} data-testid="factor-metrics-enabled" />
            创建后启用
          </label>

          <button onClick={handleCreate} data-testid="factor-metrics-create" className={styles.btn} style={{ padding: "6px 16px"  }}>
            创建调度
          </button>
        </div>
      </div>

      {error && <div style={{ color: "red", marginBottom: 8 }}>错误: {error}</div>}

      {/* 调度列表 */}
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ background: "#f5f5f5" }}>
            <th className={styles.th}>类型</th>
            <th className={styles.th}>范围</th>
            <th className={styles.th}>因子</th>
            <th className={styles.th}>并行度</th>
            <th className={styles.th}>频率</th>
            <th className={styles.th}>快照</th>
            <th className={styles.th}>上次运行</th>
            <th className={styles.th}>状态</th>
            <th className={styles.th}>下次运行</th>
            <th className={styles.th}>启用</th>
            <th className={styles.th}>操作</th>
          </tr>
        </thead>
        <tbody>
          {schedules.length === 0 && !loading && (
            <tr><td colSpan={11} style={{ textAlign: "center", padding: 24, color: "#999" }}>
              暂无调度，请创建
            </td></tr>
          )}
          {schedules.map((s) => {
            const opts = s.options || {};
            const factorNames = Array.isArray(opts.factor_names) ? opts.factor_names : [];
            return (
              <tr key={s.schedule_id} style={{ borderBottom: "1px solid #eee" }}>
                <td className={styles.td}>{opts.one_shot ? "单次" : "周期"}</td>
                <td className={styles.td}>{opts.include_disabled ? "全部(含禁用)" : "仅可用"}</td>
                <td className={styles.td} style={{ maxWidth: 260, wordBreak: "break-all" }}>
                  {factorNames.length ? factorNames.join(", ") : "全部可计算因子"}
                </td>
                <td className={styles.td}>{opts.workers || "-"}</td>
                <td className={styles.td}>
                  {s.frequency === "weekly"
                    ? `每周${DAY_LABELS[opts.day_of_week || "sunday"] || ""} ${opts.at || ""}`
                    : s.frequency === "daily"
                    ? `每日 ${opts.at || ""}`
                    : s.frequency || "手动"}
                </td>
                <td className={styles.td}>{opts.data_date || "最新"}</td>
                <td className={styles.td}>{s.last_run_at ? new Date(s.last_run_at).toLocaleString("zh-CN") : "-"}</td>
                <td className={styles.td} style={{ color: statusColor(s.last_status)  }}>
                  {s.last_status || "-"}
                </td>
                <td className={styles.td}>{s.next_run_at ? new Date(s.next_run_at).toLocaleString("zh-CN") : "-"}</td>
                <td className={styles.td}>
                  <input
                    type="checkbox"
                    checked={s.enabled}
                    onChange={(e) => handleToggle(s.schedule_id, e.target.checked)}
                  />
                </td>
                <td className={styles.td}>
                  <button onClick={() => handleRunNow(s.schedule_id)} data-testid={`factor-metrics-run-${s.schedule_id}`} className={styles.btn} style={{ marginRight: 6, fontSize: 12, padding: "2px 8px"  }}>
                    立即运行
                  </button>
                  {confirmDeleteId === s.schedule_id ? (
                    <>
                      <button onClick={() => handleDelete(s.schedule_id)} className={styles.btn} style={{ color: "red", fontSize: 12, padding: "2px 8px"  }}>确认删除</button>
                      <button onClick={() => setConfirmDeleteId(null)} className={styles.btn} style={{ fontSize: 12, padding: "2px 8px"  }}>取消</button>
                    </>
                  ) : (
                    <button onClick={() => setConfirmDeleteId(s.schedule_id)} className={styles.btn} style={{ fontSize: 12, padding: "2px 8px"  }}>
                      删除
                    </button>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

      {loading && <div style={{ textAlign: "center", padding: 16, color: "#666" }}>加载中...</div>}
    </div>
  );
}
