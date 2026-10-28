"use client";
import { useEffect, useRef, useState } from "react";
import { path, programPath, type DailyItem, type Episode, type EpisodeDetail, type Summary } from "@/lib/api/advisoryWorkbench";
import { QuoteCell, ReferenceChange, EpisodeReturn } from "../LiveQuotes";
import { Badge, day, ErrorNotice, Metric, number, Pager, text, useRead } from "./shared";
import styles from "./workbench.module.css";

export default function StockDrawer({ summary, item, onClose }: { summary: Summary; item: DailyItem | Episode; onClose: () => void }) {
  const [page, setPage] = useState(1), [limit, setLimit] = useState(20);
  const close = useRef<HTMLButtonElement>(null);
  const result = useRead<EpisodeDetail>(item.episode_id ? path(programPath(summary.program_id, `/episodes/${encodeURIComponent(item.episode_id)}`), { target_date: summary.target_date, snapshot_id: summary.snapshot_id, limit, offset: (page - 1) * limit }) : null, 0, summary.program_id);
  useEffect(() => { const previous = document.activeElement as HTMLElement | null; close.current?.focus(); return () => previous?.focus(); }, []);
  const episode = result.data?.episode;
  return <div className={styles.drawerBackdrop} onMouseDown={e => { if (e.target === e.currentTarget) onClose(); }}><aside className={styles.drawer} role="dialog" aria-modal="true" aria-label="股票荐股详情" onKeyDown={e => {
    if (e.key === "Escape") onClose();
    if (e.key === "Tab") { const nodes = [...e.currentTarget.querySelectorAll<HTMLElement>('button:not(:disabled),a[href],select,input,[tabindex="0"]')]; const first = nodes[0], last = nodes[nodes.length - 1]; if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first?.focus(); } else if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last?.focus(); } }
  }}>
    <header className={styles.panelHeader}><div><h2>{item.stock_name || item.symbol}</h2><small>{item.symbol} · 一次荐股过程</small></div><button ref={close} aria-label="关闭股票详情" onClick={onClose}>×</button></header>
    {"membership" in item && <div className={styles.changeTags}><Badge value={item.membership} /><Badge value={item.change} /></div>}
    <ErrorNotice error={result.error} stale={!!result.data} />
    <div className={styles.metrics}><Metric label="当前最新价" value={<QuoteCell symbol={item.symbol} />} /><Metric label="跟踪进度" value={<Badge value={episode?.progress || item.progress} />} /><Metric label="参考进入价" value={episode?.entry_price == null ? "待确认" : `¥${number(episode.entry_price, 2)}`} /><Metric label="参考退出价" value={episode?.exit_price == null ? "待确认" : `¥${number(episode.exit_price, 2)}`} /><Metric label="过程参考收益" value={<EpisodeReturn episode={episode} />} note="费用前，按已验证价格计算" />{"reference_price" in item && <Metric label="较推荐参考价涨跌" value={<ReferenceChange symbol={item.symbol} price={item.reference_price} date={item.reference_date} />} note="未复权价格变化，不计入过程胜率" />}</div>
    {!item.episode_id && <div className={styles.notice}>所选日名单已记录，尚未建立有效进入的荐股过程。{("reference_price" in item && item.reference_price != null) && <span>输入日 {day(item.reference_date)} 收盘参考价 ¥{number(item.reference_price, 2)}。</span>}</div>}
    {episode && <><ol className={styles.timeline}><li><strong>{day(episode.signal_date)} 推荐产生</strong></li><li><strong>{day(episode.effective_entry_date)} 进入参考日期</strong><small>是否确认以跟踪进度及有效价格为准。</small></li>{episode.exit_signal_date && <li><strong>{day(episode.exit_signal_date)} 退出信号</strong><Badge value={episode.exit_reason} /></li>}{episode.effective_exit_date && <li><strong>{day(episode.effective_exit_date)} 退出参考日期</strong><Badge value={episode.progress} /></li>}</ol>
      <h3 className={styles.subheading}>逐日调整记录</h3><div className={styles.tableWrap}><table><thead><tr><th>目标日</th><th>动作</th><th>原因</th></tr></thead><tbody>{result.data?.items.map((event, i) => <tr key={`${event.trade_date}-${i}`}><td>{event.trade_date}</td><td><Badge value={event.action} /></td><td>{text(event.reason_code, "详见运行与数据状态")}</td></tr>)}</tbody></table></div><Pager total={result.data?.total_count || 0} limit={limit} page={page} onChange={setPage} onLimit={n => { setLimit(n); setPage(1); }} />
      <p className={styles.explanation}>来源：{text(episode.source)}。过程和收益截至当前读取；不代表历史名单发布时已知，也不代表券商成交。</p></>}
    <div className={styles.notice}>退出信号、退出确认与收益结算分别记录。触发止损不等于已完成退出。</div>
  </aside></div>;
}
