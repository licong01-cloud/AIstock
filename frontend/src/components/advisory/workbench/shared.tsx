"use client";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { read } from "@/lib/api/advisoryWorkbench";
import styles from "./workbench.module.css";

export const labels: Record<string, string> = {
  OUTSIDE_LIST: "不在所选日名单", ALPHA_RANK_DROP_EXIT: "排名下降退出", NOT_IN_CURRENT_TOPK: "未在当前候选范围",
  REPLACEMENT_BUDGET_LIMIT: "已达本次调整上限", NONE: "继续跟踪", ENTER_RANK: "达到进入排名",
  ENABLED: "已启用", PAUSED: "已暂停", DRAFT: "草稿", ARCHIVED: "已归档", ACTIVE: "跟踪中", EXITED: "已退出",
  PUBLISHED: "名单已发布", ASSET_BLOCKED: "资产阻断", WAITING_DATA: "等待数据", FAILED: "运行失败", NO_CURRENT_LIST: "无当前名单",
  SETTLED: "已结算", NOT_DUE: "未到结算时间", NOT_ENTERED: "未进入", NEW: "新加入", CONTINUING: "继续推荐", REMOVED: "移出名单",
  IN_SCOPE: "荐股范围内", OBSERVATION: "观察候选", OUTSIDE_PENDING: "范围外待复评", OUTSIDE_EXIT: "退出建议", UNKNOWN: "待核验",
  TRACKING: "跟踪中", PENDING_ENTRY: "待进入", PENDING_EXIT: "待退出确认", WAITING_REVIEW: "待复评", ENDED: "已结束",
  ENTER: "进入", HOLD: "继续跟踪", EXIT: "退出", WATCH: "观察", WAITING: "等待确认", SUCCEEDED: "复评完成",
  TAKE_PROFIT: "止盈", TRAILING_TAKE_PROFIT: "移动止盈", STOP_LOSS: "止损", RANK_EXIT: "排名退出", RANK_WEAK: "排名转弱",
  TIME_EXIT: "期限退出", MAX_HOLDING_DAYS: "期限退出", FORWARD: "日常前向", REPLAY: "历史回放", ALL: "全部记录",
  READY: "有效样本可统计", UNREALIZED_ONLY: "暂无已结束样本", NO_EPISODES: "暂无荐股过程", WAITING_MARKET_DATA: "等待参考价格",
  SHADOW_ONLY: "影子观察", NOT_CONFIGURED: "尚未配置观察模型", NOT_APPLICABLE: "此策略不适用", IMMATURE_OR_PENDING: "未成熟或待评价",
  COMPLETE: "完整", GAPS: "有复评缺口", CALENDAR_INCOMPLETE: "日历覆盖不足", NO_LISTS: "尚无发布记录",
  PENDING_TARGET_OPEN_ENTRY: "等待目标日进入确认", PENDING_TARGET_OPEN_REVIEW: "等待目标日复评", OUTSIDE_ENTRY_THRESHOLD: "未达到进入排名",
  REMOVAL_WITHOUT_EXIT_DECISION: "已移出名单，尚无退出确认", MARK_DATA_MISSING: "缺少估值价格", SETTLEMENT_MARKET_DATA_MISSING: "结算价格未齐",
  MARK_SOURCE_UNAVAILABLE: "估值来源不可读", ENTRY_NOT_FINALIZED: "进入尚未确认", EXIT_NOT_FINALIZED: "退出尚未结算",
};
export const readinessReason = (reason?: string | null) => !reason ? "" :
  reason.includes("factor dependencies") ? "因子输入依赖尚未完整解析，不能确认就绪。" :
  reason.includes("sequence window") ? "模型所需历史窗口尚未完整声明，不能确认就绪。" :
  reason.includes("asset") || reason.includes("frozen factor") ? "冻结资产身份或输入定义尚未确认。" :
  reason.includes("pit") ? "历史身份或依赖窗口尚未确认。" : "依赖或源数据尚未通过就绪确认，请查看数据管理。";
export const text = (value: string | null | undefined, fallback = "待核验") => value ? labels[value] || fallback : fallback;
export const number = (value: number | null | undefined, digits = 0) => value == null || !Number.isFinite(value) ? "—" : value.toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
export const bps = (value: number | null | undefined) => value == null ? "—" : `${value > 0 ? "+" : ""}${number(value / 100, 2)}%`;
export const percent = (value: number | null | undefined) => value == null ? "—" : `${number(value * 100, 2)}%`;
export const day = (value: string | null | undefined) => value?.slice(0, 10) || "未记录";
export const time = (value: string | null | undefined) => value && !Number.isNaN(Date.parse(value)) ? new Date(value).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false }) : "未记录";
export const gain = (value: number | null | undefined) => value == null || value === 0 ? "" : value > 0 ? styles.gain : styles.loss;

export function Badge({ value, children }: { value?: string | null; children?: ReactNode }) {
  const color = ["ASSET_BLOCKED", "FAILED", "STOP_LOSS"].includes(value || "") ? styles.bad
    : ["WAITING_DATA", "PENDING_ENTRY", "PENDING_EXIT", "OUTSIDE_PENDING", "REMOVED", "UNKNOWN", "NO_CURRENT_LIST", "WAITING_REVIEW"].includes(value || "") ? styles.wait
    : ["TAKE_PROFIT", "TRAILING_TAKE_PROFIT"].includes(value || "") ? styles.profit
    : value === "CONTINUING" ? styles.continuing : ["ENDED", "ARCHIVED", "OBSERVATION", "EXITED", "EXIT", "OUTSIDE_LIST", "ALPHA_RANK_DROP_EXIT"].includes(value || "") ? styles.mutedBadge : styles.blue;
  const icon = value === "STOP_LOSS" ? "▼ " : ["TAKE_PROFIT", "TRAILING_TAKE_PROFIT"].includes(value || "") ? "▲ " : value === "ALPHA_RANK_DROP_EXIT" ? "↘ " : "";
  return <span className={`${styles.badge} ${color}`}>{icon && <span aria-hidden="true">{icon}</span>}{children || text(value)}</span>;
}
export function Metric({ label, value, note, className = "" }: { label: string; value: ReactNode; note?: string; className?: string }) {
  return <div className={styles.metric}><span>{label}</span><strong className={className}>{value}</strong>{note && <small>{note}</small>}</div>;
}
export function ErrorNotice({ error, stale = false }: { error: string | null; stale?: boolean }) {
  return error ? <div role="alert" className={styles.notice}>{error}{stale && <small>以下保留最后一次成功读取的内容，请刷新后确认。</small>}</div> : null;
}
export function Empty({ children }: { children: ReactNode }) { return <div className={styles.empty}>{children}</div>; }
export function Pager({ total, limit, page, onChange, onLimit }: { total: number; limit: number; page: number; onChange: (page: number) => void; onLimit: (limit: number) => void }) {
  return <footer className={styles.pager}><span>共 {number(total)} 条 · 每页 <select aria-label="每页条数" value={limit} onChange={e => onLimit(Number(e.target.value))}>{[20, 50, 100].map(n => <option key={n}>{n}</option>)}</select> 条</span><div><button disabled={page <= 1} onClick={() => onChange(page - 1)}>上一页</button><span> {page} / {Math.max(1, Math.ceil(total / limit))} </span><button disabled={page * limit >= total} onClick={() => onChange(page + 1)}>下一页</button></div></footer>;
}

export function useRead<T>(url: string | null, refresh = 0, expectedProgram?: string) {
  const [state, setState] = useState<{ url: string | null; data: T | null; error: string | null; loading: boolean }>({ url, data: null, error: null, loading: !!url });
  const generation = useRef(0);
  useEffect(() => {
    const seq = ++generation.current;
    if (!url) { setState({ url, data: null, error: null, loading: false }); return; }
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort("timeout"), 45000);
    setState(previous => ({ url, data: previous.url === url ? previous.data : null, error: null, loading: true }));
    read<T>(url, controller.signal).then(data => {
      if (seq !== generation.current || controller.signal.aborted) return;
      if (expectedProgram && (!data || typeof data !== "object" || !("program_id" in data) || data.program_id !== expectedProgram)) throw new Error("返回数据不属于所选策略，请刷新重试。");
      const expectedSnapshot = new URLSearchParams(url.split("?")[1] || "").get("snapshot_id");
      if (expectedSnapshot && (!data || typeof data !== "object" || !("snapshot_id" in data) || data.snapshot_id !== expectedSnapshot)) throw new Error("返回的数据版本已变化，请刷新后继续查看。");
      setState({ url, data, error: null, loading: false });
    }).catch(error => {
      if (seq !== generation.current || (controller.signal.aborted && controller.signal.reason !== "timeout")) return;
      setState(previous => ({ ...previous, loading: false, error: controller.signal.reason === "timeout" ? "读取超时，请刷新重试。" : error instanceof Error ? error.message : "读取失败。" }));
    }).finally(() => clearTimeout(timeout));
    return () => { controller.abort(); clearTimeout(timeout); };
  }, [url, refresh, expectedProgram]);
  return state.url === url ? state : { url, data: null, error: null, loading: !!url };
}
