"use client";
import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import type { HistoricalVersion, HistoryPage } from "@/lib/api/advisoryComparison";
import { path, programPath, type DailyItem, type DailyPage } from "@/lib/api/advisoryWorkbench";
import { QuoteProvider } from "../LiveQuotes";
import DailyList from "./DailyList";
import StockDrawer from "./StockDrawer";
import { Badge, day, Empty, ErrorNotice, Pager, text, time, useRead } from "./shared";
import type { ProgramSelection } from "@/lib/api/advisoryComparison";
import styles from "./workbench.module.css";

const statusText: Record<string, string> = { ALL: "全部状态", PUBLISHED: "已发布", REPLAY: "历史回放", SUPERSEDED: "已被替代", DRAFT: "草稿" };

export default function HistoricalLists({ summary, refresh, initialVersion }: { summary: ProgramSelection; refresh: number; initialVersion: string }) {
  const router = useRouter();
  const [pendingPin, setPendingPin] = useState(initialVersion);
  const [selected, setSelected] = useState<HistoricalVersion | null>(null);
  const [stock, setStock] = useState<DailyItem | null>(null);
  const [page, setPage] = useState(1), [limit, setLimit] = useState(20);
  const [start, setStart] = useState(""), [end, setEnd] = useState("");
  const [source, setSource] = useState("ALL"), [status, setStatus] = useState("ALL");
  const history = useRead<HistoryPage>(selected || pendingPin ? null : path(programPath(summary.program_id, "/history"), { start, end, source, status, limit, offset: (page - 1) * limit }), refresh, summary.program_id);
  const pinned = useRead<DailyPage & { selected: HistoricalVersion }>(pendingPin && !selected ? path(programPath(summary.program_id, `/history/${encodeURIComponent(pendingPin)}`)) : null, refresh, summary.program_id);
  useEffect(() => { if (pinned.data?.selected?.list_version_id === pendingPin) { setSelected(pinned.data.selected); setPendingPin(""); } }, [pinned.data, pendingPin]);
  const choose = (version: HistoricalVersion | null) => {
    setPendingPin(""); setSelected(version); setStock(null);
    const params = new URLSearchParams(window.location.search);
    if (version) params.set("list_version_id", version.list_version_id); else params.delete("list_version_id");
    router.replace(`/paper-v2/advisory?${params}`, { scroll: false });
  };
  if (selected) {
    const view: ProgramSelection = { ...summary, selected, list_version_id: selected.list_version_id, target_date: selected.trade_date,
      binding_version_id: selected.binding_version_id, snapshot_id: "" };
    return <QuoteProvider key={selected.list_version_id} programId={summary.program_id} listVersionId={selected.list_version_id}>
      <div className={styles.historyHeading}><button onClick={() => choose(null)}>← 返回历史名单目录</button><div><h2>{selected.trade_date} · 历史推荐名单</h2><p><Badge value={selected.version_status}>{statusText[selected.version_status]}</Badge> {text(selected.source)} · 生成于 {time(selected.created_at)}</p><small>版本 {selected.list_version_id} · 绑定 {selected.binding_version_id}</small></div></div>
      <DailyList key={selected.list_version_id} summary={view} versionId={selected.list_version_id} refresh={refresh} onStock={setStock} onDate={() => choose(null)} />
      {stock && <StockDrawer summary={view} item={stock} onClose={() => setStock(null)} />}
    </QuoteProvider>;
  }
  if (pendingPin) return <section className={styles.panel}><button onClick={() => choose(null)}>← 返回历史名单目录</button><ErrorNotice error={pinned.error} /><Empty>{pinned.error ? "指定版本暂不可读，请返回目录重试。" : "正在读取指定历史名单…"}</Empty></section>;
  return <section className={styles.panel}><header className={styles.panelHeader}><div><h2>历史名单</h2><p>查看此策略全部已保存版本 · 同日不同版本与历史回放分别保留</p></div><span>{history.data?.total_count ?? "—"} 个版本</span></header>
    <div className={styles.toolbar}><div className={styles.filters}><label>目标日起 <input aria-label="历史名单开始日期" type="date" value={start} onChange={e => { setStart(e.target.value); setPage(1); }} /></label><label>至 <input aria-label="历史名单结束日期" type="date" min={start} value={end} onChange={e => { setEnd(e.target.value); setPage(1); }} /></label><label>来源 <select aria-label="历史名单来源" value={source} onChange={e => { setSource(e.target.value); setPage(1); }}>{["ALL", "FORWARD", "REPLAY", "UNKNOWN"].map(s => <option key={s} value={s}>{text(s)}</option>)}</select></label><label>状态 <select aria-label="历史名单状态" value={status} onChange={e => { setStatus(e.target.value); setPage(1); }}>{["ALL", "PUBLISHED", "REPLAY", "SUPERSEDED", "DRAFT"].map(s => <option key={s} value={s}>{statusText[s]}</option>)}</select></label></div></div>
    <ErrorNotice error={history.error} stale={!!history.data} />{history.loading && !history.data && <Empty>正在读取历史版本目录…</Empty>}
    {history.data && <><div className={styles.tableWrap}><table><thead><tr><th>目标日</th><th>输入截止</th><th>来源 / 状态</th><th>当时跟踪数</th><th>生成时间</th><th>名单版本</th><th>操作</th></tr></thead><tbody>{history.data.items.map(row => <tr key={row.list_version_id}><td><strong>{row.trade_date}</strong></td><td>{day(row.date_context?.selection_as_of_trade_date || row.date_context?.decision_as_of_trade_date)}</td><td>{text(row.source)}<small><Badge value={row.version_status}>{statusText[row.version_status]}</Badge></small></td><td>{row.active_count}</td><td>{time(row.created_at)}</td><td title={`${row.list_version_id} · ${row.binding_version_id}`}>{row.list_version_id.slice(-12)}<small>绑定 {row.binding_version_id.slice(-10)}</small></td><td><button aria-label={`查看${row.trade_date}名单版本${row.list_version_id}`} onClick={() => choose(row)}>查看名单 →</button></td></tr>)}</tbody></table></div>{!history.data.items.length && <Empty>所选条件下没有已保存名单。</Empty>}<Pager total={history.data.total_count} page={page} limit={limit} onChange={setPage} onLimit={v => { setLimit(v); setPage(1); }} /></>}
    <p className={styles.explanation}>累计涨跌只计算该次连续荐股期间：继续推荐不重置，移出后冻结，再次入选另起一段。连续性或价格无法确认时显示待核验。当前最新价与今日涨跌仍显示现在的行情。</p>
  </section>;
}
