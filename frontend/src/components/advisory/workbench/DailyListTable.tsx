"use client";
import { useContext, useEffect, useMemo, useState } from "react";
import type { DailyItem } from "@/lib/api/advisoryWorkbench";
import { canonicalQuoteSymbol, validQuoteSymbol, QuoteCell, RecommendationValue, TodayChange } from "../LiveQuotes";
import { QuoteContext } from "../QuoteSnapshot";
import { Badge, Empty, number, Pager } from "./shared";
import { dailyColumns, dailyStatus, sortDailyItems, type DailySort } from "./dailyListSort";
import styles from "./workbench.module.css";

export default function DailyListTable({ items, total, complete, loading, onStock, emptyMessage = "当前没有此类记录。" }: {
  items: DailyItem[]; total: number; complete: boolean; loading: boolean; onStock: (row: DailyItem) => void; emptyMessage?: string;
}) {
  const [sort, setSort] = useState<DailySort>({ key: "rank", direction: "asc" });
  const [page, setPage] = useState(1), [limit, setLimit] = useState(20);
  const { items: quotes, register, snapshot } = useContext(QuoteContext);
  // A group is the snapshot scope for sorting. Page changes and reordered DOM
  // never change this set or ask the provider to fetch the same securities again.
  const symbols = useMemo(() => [...new Set(items.map(row => canonicalQuoteSymbol(row.symbol)).filter(validQuoteSymbol))].sort().join(","), [items]);
  useEffect(() => {
    const release = symbols ? symbols.split(",").map(symbol => register(symbol)) : [];
    return () => release.forEach(stop => stop());
  }, [symbols, register]);
  const column = dailyColumns.find(c => c.key === sort.key)!;
  const values = column.quote ? quotes : undefined;
  const ordered = useMemo(() => sortDailyItems(items, sort, values), [items, sort, values]);
  const currentPage = Math.min(page, Math.max(1, Math.ceil(items.length / limit)));
  const visible = ordered.slice((currentPage - 1) * limit, currentPage * limit);
  const choose = (key: DailySort["key"]) => {
    setSort(previous => ({ key, direction: previous.key === key && previous.direction === "asc" ? "desc" : "asc" }));
    setPage(1);
  };
  return <>
    <div className={styles.tableWrap}><table data-testid="advisory-sortable-list"><thead><tr>{dailyColumns.map(c => {
      const active = c.key === sort.key;
      const next = active && sort.direction === "asc" ? "降序" : "升序";
      return <th key={c.key} scope="col" aria-label={c.label} aria-sort={active ? sort.direction === "asc" ? "ascending" : "descending" : "none"}>
        <button type="button" className={styles.sortHeader} data-active={active} disabled={!complete}
          aria-label={`按${c.label}${next}排序`} title={`点击按${c.label}${next}排序`} onClick={() => choose(c.key)}>
          {c.label}<span className={styles.sortDirection} aria-hidden="true">{active ? sort.direction === "asc" ? "↑" : "↓" : "↕"}</span>
        </button></th>;
    })}</tr></thead><tbody>{visible.map(row => <tr key={`${row.symbol}:${row.episode_id || ""}`} className={row.change === "NEW" ? styles.newRow : ""}>
      <td>{number(row.rank)}</td><td><button className={styles.stockLink} onClick={() => onStock(row)}>{row.stock_name || row.symbol}</button><small>{row.symbol}</small></td>
      <td><Badge value={dailyStatus(row)} /></td><td>{row.consecutive_days == null ? <RecommendationValue symbol={row.symbol} field="days" /> : row.consecutive_days === 1 ? "首日" : `${row.consecutive_days} 日`}</td>
      <td><RecommendationValue symbol={row.symbol} field="start" /></td><td><RecommendationValue symbol={row.symbol} field="base" /></td><td><QuoteCell symbol={row.symbol} /></td>
      <td><RecommendationValue symbol={row.symbol} field="return" /></td><td><TodayChange symbol={row.symbol} /></td>
    </tr>)}</tbody></table></div>
    {complete && !items.length && <Empty>{emptyMessage}</Empty>}
    {complete ? <><p className={styles.sortNote} aria-live="polite">按{column.label}{sort.direction === "asc" ? "升序" : "降序"} · 点击表头切换 · 缺失值置后{column.quote && snapshot?.loading ? " · 行情快照读取中" : ""}</p>
      <Pager total={total} limit={limit} page={currentPage} onChange={setPage} onLimit={value => { setLimit(value); setPage(1); }} /></>
      : <p className={styles.sortNote} role="status">{loading ? `正在读取完整名单（${number(items.length)} / ${number(total)}），完整读取后可排序。` : "完整名单未能读取，排序暂不可用，请刷新重试。"}</p>}
  </>;
}
