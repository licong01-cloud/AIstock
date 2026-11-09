"use client";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { path, programPath, type DailyItem, type Decision, type Episode, type Page, type Params, type Readiness, type Summary } from "@/lib/api/advisoryWorkbench";
import { Badge, bps, day, Empty, ErrorNotice, gain, Metric, number, Pager, percent, readinessReason, text, time, useRead } from "./shared";
import { QuoteCell, QuoteProvider } from "../LiveQuotes";
import DailyList from "./DailyList";
import HistoricalLists from "./HistoricalLists";
import EpisodeTable from "./EpisodeTable";
import StockDrawer from "./StockDrawer";
import Performance from "./Performance";
import ModelObservation from "./ModelObservation";
import styles from "./workbench.module.css";

const tabs = ["当日推荐", "持续跟踪", "历史名单", "跟踪历史", "调整记录", "效果统计", "模型观察"];

function Adjustments({ summary, refresh }: { summary: Summary; refresh: number }) {
  const [page, setPage] = useState(1), [limit, setLimit] = useState(20);
  const [start, setStart] = useState(""), [end, setEnd] = useState("");
  const decisions = useRead<Page<Decision>>(path(programPath(summary.program_id, "/decisions"), { target_date: summary.target_date, snapshot_id: summary.snapshot_id, start, end, limit, offset: (page - 1) * limit }), refresh, summary.program_id);
  return <section className={styles.panel}><header className={styles.panelHeader}><h2>逐日调整记录</h2></header>
    <div className={styles.toolbar}><label>开始 <input type="date" aria-label="调整开始日期" value={start} onChange={e => { setStart(e.target.value); setPage(1); }} /></label><label>结束 <input type="date" aria-label="调整结束日期" value={end} min={start} onChange={e => { setEnd(e.target.value); setPage(1); }} /></label></div>
    <ErrorNotice error={decisions.error} stale={!!decisions.data} />{decisions.data && <><div className={styles.tableWrap}><table><thead><tr><th>目标日</th><th>股票</th><th>当前最新价</th><th>动作</th><th>原因</th><th>复评状态</th></tr></thead><tbody>{decisions.data.items.map((row, i) => <tr key={`${row.trade_date}-${row.symbol}-${i}`}><td>{row.trade_date}</td><td>{row.stock_name || row.symbol}<small>{row.symbol}</small></td><td><QuoteCell symbol={row.symbol} /></td><td><Badge value={row.action} /></td><td>{text(row.reason_code, "原因待核验")}</td><td>{text(row.review_status)}</td></tr>)}</tbody></table></div><Pager total={decisions.data.total_count} page={page} limit={limit} onChange={setPage} onLimit={n => { setLimit(n); setPage(1); }} /></>}
  </section>;
}

function Runtime({ summary, refresh }: { summary: Summary; refresh: number }) {
  const input = summary.selected?.date_context?.selection_as_of_trade_date || summary.selected?.date_context?.decision_as_of_trade_date || summary.runtime.data_date;
  const readiness = useRead<Readiness>(input ? path(programPath(summary.program_id, "/readiness"), { data_date: input }) : null, refresh, summary.program_id);
  return <details className={styles.panel}><summary>运行与数据状态 · {text(summary.runtime.state)} · {text(summary.runtime.settlement, "结算未记录")}</summary><div className={styles.runtime}>
    <div><h3>运行状态</h3><p>任务配置：{text(summary.program.status)}</p><p>所选日发布：{text(summary.runtime.state)}</p><p>目标日结算：{text(summary.runtime.settlement, "未记录")}</p><p>最近运行记录：{time(summary.runtime.updated_at)}</p>{summary.runtime.state === "ASSET_BLOCKED" && <p className={styles.inlineWarning}>冻结模型或逐 seed 资产绑定缺失，需要资产 owner 恢复；页面刷新不会替换模型。</p>}</div>
    <div><h3>共享数据就绪 · {day(input)}</h3><ErrorNotice error={readiness.error} />{!input ? <p>目标日输入日期未能确认。</p> : readiness.loading && !readiness.data ? <p>正在读取共享台账…</p> : readiness.data?.status === "UNAVAILABLE" ? <p>共享就绪合同暂不可读，请核对数据管理；不按钟点推定就绪。</p> : readiness.data?.items.map((row, i) => <div key={i}>{row.reason && <p>{readinessReason(row.reason)}</p>}<p>策略依赖：{row.consumer_ready === true || row.prediction_ready === true ? "已就绪" : "尚未确认就绪"}</p><p>当日全部同步：{row.all_sync_complete === true ? "已完成" : row.all_sync_complete === false ? "未全部完成" : "未能确认"}</p></div>)}<Link href="/local-data">查看本地数据管理 →</Link></div>
    <div><h3>完整复评覆盖</h3><p>{day(summary.coverage.first_published_target_date)} 至 {summary.coverage.through}</p><p>{text(summary.coverage.status)} · 缺口 {number(summary.coverage.missing_count)}</p><div className={styles.dateChips}>{summary.coverage.missing_dates.map(d => <button key={d} onClick={() => window.location.assign(`/paper-v2/advisory?program_id=${encodeURIComponent(summary.program_id)}&target_date=${d}`)}>{d}</button>)}</div></div>
  </div></details>;
}

export default function ProgramDetail({ programId, initialDate, initialVersion }: { programId: string; initialDate: string; initialVersion: string }) {
  const router = useRouter();
  const [target, setTarget] = useState(initialDate), [tab, setTab] = useState(initialVersion ? "历史名单" : "当日推荐"), [refresh, setRefresh] = useState(0);
  const [source, setSource] = useState("ALL"), [binding, setBinding] = useState("");
  const [start, setStart] = useState(""), [end, setEnd] = useState("");
  const [stock, setStock] = useState<DailyItem | Episode | null>(null);
  const filters: Params = { source, binding, start, end };
  const result = useRead<Summary>(path(programPath(programId), { target_date: target, ...filters }), refresh, programId);
  const summary = result.data;
  const clearPinned = (date?: string) => {
    const params = new URLSearchParams(window.location.search);
    params.delete("list_version_id");
    if (date) params.set("target_date", date);
    router.replace(`/paper-v2/advisory?${params}`, { scroll: false });
  };
  const changeDate = (date: string) => { clearPinned(date); setTarget(date); setStock(null); setTab("当日推荐"); };
  return <>
    <Link className={styles.back} href="/paper-v2/advisory">← 返回荐股总览</Link>
    <header className={styles.header}><div><h1>{summary?.program.program_name || "策略荐股详情"}</h1>{summary && <p><Badge value={summary.runtime.state} /> <span>首次发布目标日 {day(summary.coverage.first_published_target_date)} · 启用 {day(summary.program.enabled_since)}</span></p>}</div><div className={styles.actions}><button disabled={result.loading} onClick={() => { setRefresh(v => v + 1); setStock(null); }}>刷新</button><Link href="/paper-v2/advisory/manage">任务管理</Link></div></header>
    <ErrorNotice error={result.error} stale={!!summary} />{result.loading && !summary && <Empty>正在读取策略数据…</Empty>}
    {summary && <QuoteProvider key={`${summary.program_id}:${summary.list_version_id || "no-list"}`} programId={summary.program_id} listVersionId={summary.selected?.list_version_id}>
      {tab !== "历史名单" && <div className={styles.toolbar}><div className={styles.filters}><label>推荐目标日 <input aria-label="推荐目标日" type="date" value={target || summary.target_date} onChange={e => changeDate(e.target.value)} /></label><span>输入截止 {day(summary.selected?.date_context?.selection_as_of_trade_date || summary.selected?.date_context?.decision_as_of_trade_date)}</span>{summary.target_date !== summary.default_target_date && <Badge>历史或指定日期</Badge>}</div><span>最近读取 {time(summary.generated_at)}</span></div>}
      <nav className={styles.tabs} aria-label="荐股详情分类">{tabs.map(name => <button key={name} aria-current={tab === name ? "page" : undefined} onClick={() => { if (initialVersion && name !== "历史名单") clearPinned(); setTab(name); setStock(null); }}>{name}</button>)}</nav>
      {["持续跟踪", "跟踪历史", "效果统计"].includes(tab) && <><div className={styles.metrics}><Metric label="跟踪中" value={number(summary.metrics.active_count)} /><Metric label="已结束有效样本" value={number(summary.metrics.metric_evaluable_count)} /><Metric label="已结束胜率" value={percent(summary.metrics.win_rate)} /><Metric label="平均参考收益" value={bps(summary.metrics.avg_return_bps)} className={gain(summary.metrics.avg_return_bps)} note="费用前参考收益" /></div><div className={styles.toolbar}><div className={styles.filters}><label>来源 <select aria-label="记录来源" value={source} onChange={e => setSource(e.target.value)}>{["ALL", "FORWARD", "REPLAY", "UNKNOWN"].map(s => <option key={s} value={s}>{text(s)}</option>)}</select></label><label>绑定 <select aria-label="绑定范围" value={binding} onChange={e => setBinding(e.target.value)}><option value="">全部绑定</option><option value="CURRENT">当前绑定</option><option value="UNKNOWN">绑定待核验</option>{summary.bindings.map((b, i) => <option key={b.binding_version_id} value={b.binding_version_id}>版本 {b.program_version} · {day(b.effective_from_trade_date)}{i === 0 ? "（最近）" : ""}</option>)}</select></label><label>入选日 <input aria-label="入选开始日期" type="date" value={start} onChange={e => setStart(e.target.value)} /></label><span>至</span><input aria-label="入选结束日期" type="date" value={end} min={start} onChange={e => setEnd(e.target.value)} /></div></div></>}
      <div key={`${tab}:${summary.snapshot_id}:${source}:${binding}:${start}:${end}`}>
        {tab === "当日推荐" && <DailyList summary={summary} refresh={refresh} onStock={setStock} onDate={changeDate} />}
        {(tab === "持续跟踪" || tab === "跟踪历史") && <EpisodeTable summary={summary} filters={filters} refresh={refresh} tracking={tab === "持续跟踪"} onStock={setStock} />}
        {tab === "历史名单" && <HistoricalLists summary={summary} refresh={refresh} initialVersion={initialVersion} />}
        {tab === "调整记录" && <Adjustments summary={summary} refresh={refresh} />}
        {tab === "效果统计" && <Performance summary={summary} />}
        {tab === "模型观察" && <ModelObservation programId={programId} target={summary.target_date} refresh={refresh} />}
      </div>
      <Runtime summary={summary} refresh={refresh} />
      {stock && <StockDrawer key={stock.episode_id || stock.symbol} summary={summary} item={stock} onClose={() => setStock(null)} />}
    </QuoteProvider>}
  </>;
}
