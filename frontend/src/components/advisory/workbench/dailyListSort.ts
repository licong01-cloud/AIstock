import type { DailyItem } from "@/lib/api/advisoryWorkbench";
import type { Quote, Quotes } from "../QuoteSnapshot";
import { canonicalQuoteSymbol } from "../LiveQuotes";
import { labels } from "./shared";

type Value = string | number | null;
const finite = (value: number | null | undefined): Value => value != null && Number.isFinite(value) ? value : null;
const date = (value?: string | null): Value => value && Number.isFinite(Date.parse(value)) ? Date.parse(value) : null;
export const dailyStatus = (row: DailyItem) => row.change === "UNKNOWN" && row.membership !== "IN_SCOPE" ? row.membership : row.change;

export const dailyColumns = [
  { key: "rank", label: "排名", quote: false, value: (row: DailyItem) => finite(row.rank) },
  { key: "stock", label: "股票", quote: false, value: (row: DailyItem) => row.stock_name || row.symbol || null },
  { key: "status", label: "推荐状态", quote: false, value: (row: DailyItem) => dailyStatus(row) === "UNKNOWN" ? null : labels[dailyStatus(row)] || null },
  { key: "days", label: "连续推荐", quote: true, value: (row: DailyItem, q?: Quote) => finite(row.consecutive_days ?? q?.recommendation?.consecutive_days) },
  { key: "start", label: "首次生效日", quote: true, value: (_row: DailyItem, q?: Quote) => date(q?.recommendation?.start_date) },
  { key: "base", label: "起始基准价", quote: true, value: (_row: DailyItem, q?: Quote) => finite(q?.recommendation?.base_price) },
  { key: "price", label: "行情快照价", quote: true, value: (_row: DailyItem, q?: Quote) => finite(q?.price) },
  { key: "return", label: "荐股期间累计涨跌", quote: true, value: (_row: DailyItem, q?: Quote) => finite(q?.recommendation?.return_bps) },
  { key: "today", label: "今日涨跌", quote: true, value: (_row: DailyItem, q?: Quote) => finite(q?.today_return_bps) },
] as const;
export type DailySort = { key: typeof dailyColumns[number]["key"]; direction: "asc" | "desc" };
const collator = new Intl.Collator("zh-CN", { numeric: true });
const compare = (a: Value, b: Value, direction = 1) => a == null ? b == null ? 0 : 1 : b == null ? -1
  : direction * (typeof a === "number" && typeof b === "number" ? a - b : collator.compare(String(a), String(b)));

export function sortDailyItems(items: DailyItem[], sort: DailySort, quotes?: Quotes): DailyItem[] {
  const column = dailyColumns.find(c => c.key === sort.key)!;
  return items.map((row, index) => ({ row, index, value: column.value(row, quotes?.[canonicalQuoteSymbol(row.symbol)]) }))
    .sort((a, b) => compare(a.value, b.value, sort.direction === "asc" ? 1 : -1)
      || compare(finite(a.row.rank), finite(b.row.rank)) || a.index - b.index)
    .map(item => item.row);
}
