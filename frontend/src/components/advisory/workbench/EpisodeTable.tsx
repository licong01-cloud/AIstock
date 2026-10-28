"use client";
import { useState } from "react";
import { path, programPath, type Episode, type Page, type Params, type Summary } from "@/lib/api/advisoryWorkbench";
import { QuoteCell, EpisodeReturn } from "../LiveQuotes";
import { Badge, day, Empty, ErrorNotice, number, Pager, text, useRead } from "./shared";
import styles from "./workbench.module.css";

export default function EpisodeTable({ summary, filters, refresh, tracking, onStock }: { summary: Summary; filters: Params; refresh: number; tracking: boolean; onStock: (episode: Episode) => void }) {
  const [page, setPage] = useState(1), [limit, setLimit] = useState(20), [query, setQuery] = useState("");
  const [status, setStatus] = useState(tracking ? "UNSETTLED" : "EXITED"), [reason, setReason] = useState("");
  const result = useRead<Page<Episode>>(path(programPath(summary.program_id, "/episodes"), { ...filters, target_date: summary.target_date, snapshot_id: summary.snapshot_id, status, q: query, exit_reason: reason, limit, offset: (page - 1) * limit }), refresh, summary.program_id);
  return <section className={styles.panel}><header className={styles.panelHeader}><h2>{tracking ? "持续跟踪" : "历史荐股记录"}</h2><span>按荐股过程分别记录</span></header>
    <div className={styles.toolbar}><div className={styles.filters}><input aria-label="搜索历史股票" placeholder="股票名称或代码" value={query} onChange={e => { setQuery(e.target.value); setPage(1); }} /><select aria-label="荐股状态" value={status} onChange={e => { setStatus(e.target.value); setPage(1); }}><option value="ALL">全部状态</option><option value="ACTIVE">跟踪中</option><option value="EXITED">已产生退出</option><option value="UNSETTLED">未完成结算</option></select><select aria-label="退出原因" value={reason} onChange={e => { setReason(e.target.value); setPage(1); }}><option value="">全部退出原因</option>{["TAKE_PROFIT", "TRAILING_TAKE_PROFIT", "STOP_LOSS", "RANK_EXIT", "MAX_HOLDING_DAYS"].map(value => <option key={value} value={value}>{text(value)}</option>)}</select></div></div>
    <ErrorNotice error={result.error} stale={!!result.data} />
    {result.loading && !result.data && <Empty>正在读取荐股记录…</Empty>}
    {result.data && <><div className={styles.tableWrap}><table><thead><tr><th>股票</th><th>当前最新价</th><th>推荐 / 进入日</th><th>当前进度</th><th>跟踪天数</th><th>退出原因</th><th>参考收益</th><th>来源</th></tr></thead><tbody>{result.data.items.map(row => <tr key={row.episode_id}><td><button className={styles.stockLink} onClick={() => onStock(row)}>{row.stock_name || row.symbol}</button><small>{row.symbol}</small></td><td><QuoteCell symbol={row.symbol} /></td><td>{day(row.signal_date)}<small>进入 {day(row.effective_entry_date)}</small></td><td><Badge value={row.progress} /><small><Badge value={row.selected_membership} /></small><small>{row.performance_reason_code ? text(row.performance_reason_code, "参考价格尚待核验") : ""}</small></td><td>{number(row.holding_trading_days)}</td><td>{row.exit_reason ? <Badge value={row.exit_reason} /> : "—"}</td><td><EpisodeReturn episode={row} /></td><td>{text(row.source)}</td></tr>)}</tbody></table></div>
      {!result.data.items.length && <Empty>所选范围没有记录。</Empty>}<Pager total={result.data.total_count} limit={limit} page={page} onChange={setPage} onLimit={n => { setLimit(n); setPage(1); }} /></>}
    <p className={styles.explanation}>同一股票重复入选分次展示；当前价不覆盖已结束收益；浮动收益沿用已验证的收盘估值，不计入已结束胜率。数据截至当前读取，历史记录不代表当时已知的后续结果。</p>
  </section>;
}
