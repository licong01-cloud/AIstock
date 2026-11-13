import { request } from "./api";

export type ViewKind = "positions" | "trades" | "orders" | "pnl" | "cash-flows" | "events";
export type Tab = ViewKind | "monitor";
export interface Summary {
  name: string; strategy: string; stage: string; status: string; initial_capital: string;
  created_at: string; economic_as_of: string | null; valuation_kind: string;
  cash: string | null; available_cash: string | null; frozen_cash: string | null;
  value: string | null; nav: string | null; opening_nav: string | null; closing_nav: string | null;
  pnl: string | null; daily_pnl: string | null; return_pct: string | null;
  daily_date: string | null; comparable_live: boolean;
  realized: string | null; unrealized: string | null; unrealized_change: string | null;
  fees: string | null; net_flows: string | null; position_count: number | null;
  position_ratio: string | null;
  completed_date: string | null; next_date: string | null; start_date: string | null; end_date: string | null;
  source: string; issue: string; cost_basis: string;
  cumulative_pnl: string | null; cumulative_return_pct: string | null; cumulative_fees: string | null; cumulative_note: string;
  valuation_day: string | null; valuation_day_pnl: string | null; valuation_day_note: string;
  today_pnl: string | null; today_note: string;
  valuation_change: string | null; previous_valuation_at: string | null; change_note: string;
}
export interface Row {
  key: string; security?: string | null; order_key?: string | null;
  name: string; market: string; date: string; time: string; status: string; side: string; description: string;
  quantity?: number | null; available?: number | null; frozen?: number | null; filled?: number | null;
  opening_quantity?: number | null; closing_quantity?: number | null;
  remaining?: number | null; buy_quantity?: number | null; sell_quantity?: number | null;
  price?: string | null; cost?: string | null; value?: string | null; opening_value?: string | null;
  cash_delta?: string | null; balance?: string | null; amount?: string | null; fees?: string | null;
  commission?: string | null; tax?: string | null; realized?: string | null;
  unrealized?: string | null; unrealized_change?: string | null; pnl?: string | null; return_pct?: string | null;
  weight?: string | null; buy_amount?: string | null; sell_amount?: string | null; known: boolean;
}
export interface Result {
  schema_version: "localsim_workbench_v1"; account_id: string; snapshot_set: string; generated_at: string;
  status: "complete" | "partial" | "unavailable"; summary: Summary; items: Row[]; total_count: number;
  next_cursor: string | null; missing_dates: string[]; warnings: string[];
  requested_start?: string | null; requested_end?: string | null; actual_start?: string | null; actual_end?: string | null;
}
export interface Overview { items: { key: string; summary: Summary; curve: Row[] }[]; generated_at: string; complete: boolean;
  nav: string|null; pnl: string|null; daily_pnl:string|null; return_pct:string|null; economic_as_of:string|null; included_count:number; excluded_count:number; explanation:string; curve:Row[] }
export interface Filters { start?: string; end?: string; as_of?: string; security?: string; side?: string; status?: string; group_by?: "date" | "security"; order_key?:string; search?:string; sort?:string; descending?:boolean }
export const base = "/simulation-runtime/localsim";
export interface DualModeStudy {key:string;name:string;start_date:string;end_date:string;account_count:number;status:string;complete:boolean;purpose:string}
export interface DualModeAccount {key:string;mode:string;rule:string;name:string;delay_seconds:number|null;initial_cash:string;nav:string;pnl:string;cash?:string;holdings_value?:string;last_day_pnl?:string;last_trade_date?:string;return_pct:string;max_drawdown_pct:string;fees:string;completion_pct:string|null;curve:{date:string;nav_cny:string}[];operations:Record<string,number>;statuses?:{name:string;count:number}[];constraints?:{name:string;count:number}[]}
export interface DualModeDetail {schema_version:"localsim_dual_mode_detail_v1";key:string;name:string;complete:boolean;start_date:string;end_date:string;accounts:DualModeAccount[];explanation:string;comparisons:{family:string;account_id:string;baseline_account_id:string;net_increment_cny:string;mdd_reduction:string}[]}
export interface DualModeView {schema_version:"localsim_dual_mode_account_view_v1";view:string;items:Record<string,string|number|null>[];total_count:number;next_cursor:string|null;snapshot_set:string}
export async function readDualModeStudies(signal?:AbortSignal):Promise<DualModeStudy[]>{
 const r=await request<{schema_version:string;items:DualModeStudy[]}>(`${base}/comparisons`,{signal});
 if(r.schema_version!=="localsim_dual_mode_overview_v1"||!Array.isArray(r.items))throw Error(READ_ERROR);return r.items;
}
export async function readDualModeDetail(key:string,signal?:AbortSignal):Promise<DualModeDetail>{
 const r=await request<DualModeDetail>(`${base}/comparisons/${encodeURIComponent(key)}`,{signal});
 if(r.schema_version!=="localsim_dual_mode_detail_v1"||r.key!==key||!Array.isArray(r.accounts))throw Error(READ_ERROR);return r;
}
export async function readDualModeView(key:string,account:string,filters:{view:string;start?:string;end?:string;cursor?:string},signal?:AbortSignal):Promise<DualModeView>{
 const r=await request<DualModeView>(`${base}/comparisons/${encodeURIComponent(key)}/accounts/${encodeURIComponent(account)}?${query(filters)}`,{signal});
 if(r.schema_version!=="localsim_dual_mode_account_view_v1"||!Array.isArray(r.items))throw Error(READ_ERROR);return r;
}
export const accountPath = (id: string) => `${base}/accounts/${encodeURIComponent(id)}`;
export function query(filters: object): string {
  const result = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => { if (value !== undefined && value !== null && value !== "") result.set(key, String(value)); });
  return result.toString();
}
export async function readView(id: string, view: string, filters: Filters & { cursor?: string; snapshot_set?: string; page_size?: number }, signal?: AbortSignal): Promise<Result> {
  const result = await request<Result>(`${accountPath(id)}/${view}?${query(filters)}`, { signal });
  if (result.schema_version !== "localsim_workbench_v1" || result.account_id !== id || !Array.isArray(result.items)) throw Error("读取结果需要重新核验");
  return result;
}
export async function readAllView(id:string,view:string,filters:Filters&{snapshot_set?:string},signal?:AbortSignal):Promise<Result>{
  let result=await readView(id,view,{...filters,page_size:200},signal);const first=result,items=[...result.items];
  while(result.next_cursor){if(items.length>200000||signal?.aborted)throw Error();result=await readView(id,view,{...filters,snapshot_set:first.snapshot_set,cursor:result.next_cursor,page_size:200},signal);if(result.snapshot_set!==first.snapshot_set)throw Error();items.push(...result.items);}
  if(items.length!==first.total_count)throw Error();return {...first,items,next_cursor:null};
}
const opaque = /(?:\b[0-9a-f]{24,}\b|[0-9a-f]{8}-[0-9a-f-]{27,}|[A-Za-z0-9]+_[A-Za-z0-9_\-]+|\b\d{6}\.(?:SH|SZ|BJ)\b|[A-Za-z]:[\\/]|https?:\/\/|[{}\[\]]|[\x00-\x1f]|\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b)/i;
export function human(value: unknown, fallback = "名称待完善"): string {
  const s = typeof value === "string" ? value.trim() : "";
  return s && s.length <= 120 && /[\u3400-\u9fff]/.test(s) && !opaque.test(s) ? s : fallback;
}
export function amount(value: string | number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !/^-?\d+(\.\d+)?$/.test(String(value))) return "暂不可计算";
  const s = String(value), negative = s.startsWith("-"), parts = s.replace(/^-/, "").split(".");
  const fraction = (parts[1] || "").padEnd(digits + 1, "0");
  let scaled = BigInt(parts[0] + fraction.slice(0, digits));
  if (Number(fraction[digits]) >= 5) scaled += BigInt(1);
  const raw = scaled.toString().padStart(digits+1,"0"), integer = digits ? raw.slice(0,-digits) : raw;
  return (negative && scaled !== BigInt(0) ? "−" : "") + integer.replace(/\B(?=(\d{3})+(?!\d))/g, ",") + (digits ? "." + raw.slice(-digits) : "");
}
export function chinaTime(value: string | number | null | undefined): string {
  if (value==null || (typeof value==="string"&&!/^\d{4}-\d{2}-\d{2}/.test(value)) || !Number.isFinite(new Date(value).getTime())) return "尚未提供";
  return new Date(value).toLocaleString("zh-CN", { timeZone:"Asia/Shanghai", hour12:false });
}
export function profitAmount(value: string | number | null | undefined): string {
  const formatted = amount(value);
  return formatted !== "暂不可计算" && !String(value).startsWith("-") && /[1-9]/.test(formatted) ? "+" + formatted : formatted;
}
export function safeDate(value: string | null | undefined): string {
  return value && /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : "尚未提供";
}
export const READ_ERROR = "本次读取失败，保留上次成功结果；请按账户与发生时间核查。";
export const TAB_NAMES: Record<Tab,string> = { positions:"持仓",trades:"历史成交",orders:"委托记录",pnl:"区间盈亏","cash-flows":"资金流水",events:"回放与运行",monitor:"交易监控" };

export const selectionKey="localsim-workbench-selection";
export function rememberAccount(accountId:string,tab:Tab="positions",filters:Filters={}){sessionStorage.setItem(selectionKey,JSON.stringify({accountId,tab,filters}));}
export function rememberReplay(id:string){sessionStorage.setItem("localsim-replay-selection",id);}
