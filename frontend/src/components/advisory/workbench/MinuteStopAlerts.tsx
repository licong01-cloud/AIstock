"use client";

import { useEffect, useState } from "react";
import { path, programPath, type MinuteStopAlerts as AlertView } from "@/lib/api/advisoryWorkbench";
import { ErrorNotice, number, time, useRead } from "./shared";
import styles from "./workbench.module.css";

const statusText: Record<string, string> = {
  NOT_CURRENT_DAY: "所选日不是当前交易日。",
  PROGRAM_DISABLED: "该荐股任务未启用。",
  CALENDAR_UNVERIFIED: "交易日历待核验。",
  NON_TRADING_DAY: "当前为非交易日。",
  WAITING_LIST: "等待当日名单发布。",
  BINDING_UNVERIFIED: "名单的策略绑定待核验。",
  RULE_UNVERIFIED: "现行止损规则待核验。",
  RULE_DISABLED: "该方案未启用止损规则。",
  NO_ELIGIBLE_EPISODES: "当前没有已确认进入且尚无退出决定的跟踪记录。",
  WAITING_SESSION: "等待首根已闭合的连续竞价分钟线。",
};

const reasonText: Record<string, string> = {
  ENTRY_PRICE_UNVERIFIED: "进入参考价待核验",
  ENTRY_TIME_UNVERIFIED: "当日进入确认时间待核验",
  NO_CLOSED_MINUTES: "等待进入后的首根闭合分钟",
  MINUTE_COVERAGE_INCOMPLETE: "闭合分钟缺失或未到齐",
  NO_TRADED_MINUTES: "已读取分钟暂无成交",
  MINUTE_SOURCE_UNAVAILABLE: "分钟行情暂不可用",
  MINUTE_DATA_INVALID: "分钟行情身份或数值待核验",
  SCAN_BUSY: "行情检查正忙",
  SCAN_NOT_COMPLETED: "本次未读取该股分钟行情",
};

const sellabilityText = {
  T1_RESTRICTED: "本次进入份额受 T+1 限制",
  SUSPENSION_RECORDED: "有停牌记录，可卖时段待核验",
  UNVERIFIED: "账户可卖数量待核验",
};

export default function MinuteStopAlerts({
  programId, targetDate, listVersionId,
}: { programId: string; targetDate: string; listVersionId: string }) {
  const [enabled, setEnabled] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const url = enabled ? path(programPath(programId, "/minute-stop-alerts"), {
    target_date: targetDate, list_version_id: listVersionId,
  }) : null;
  const result = useRead<AlertView>(url, refresh, programId);
  useEffect(() => {
    if (!enabled || result.loading) return;
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") setRefresh(value => value + 1);
    }, 60000);
    return () => window.clearInterval(timer);
  }, [enabled, result.loading]);
  const bound = result.data?.target_date === targetDate && result.data?.list_version_id === listVersionId;
  const view = enabled && !result.loading && !result.error && bound ? result.data : null;
  const triggered = view?.items.filter(row => row.status === "TRIGGERED") || [];
  const remaining = view?.items.filter(row => row.status !== "TRIGGERED") || [];
  return <section className={styles.panel} aria-label="盘中止损提醒">
    <header className={styles.panelHeader}>
      <h3>盘中止损提醒</h3>
      <div className={styles.actions}>
        {enabled && <button disabled={result.loading} onClick={() => setRefresh(value => value + 1)}>刷新提醒</button>}
        <button aria-pressed={enabled} onClick={() => setEnabled(value => !value)}>
          {enabled ? "暂停本页提醒" : "开启本页提醒"}
        </button>
      </div>
    </header>
    <p className={styles.explanation}>按现行止损线检查已确认进入的跟踪记录。本页可见时每分钟刷新。触发与账户可卖分开显示。</p>
    <ErrorNotice error={result.error} />
    {result.data && !bound && <ErrorNotice error="提醒与所选名单的身份不一致，请刷新资料。" />}
    {enabled && result.loading && <p role="status">正在检查已闭合分钟线…</p>}
    {view && view.status !== "READY" && <p role="status">{statusText[view.status] || "提醒状态待核验。"}</p>}
    {view?.status === "READY" && <>
      <p role="status">止损阈值 {number(view.stop_loss_bps == null ? null : view.stop_loss_bps / 100)}% ·
        已触发 {triggered.length} 只 · 可判断 {view.evaluated_count}/{view.eligible_count} 只 · 检查时间 {time(view.checked_at)}</p>
      <div className={styles.tableWrap}><table><thead><tr>
        <th>股票</th><th>名单状态</th><th>止损检查</th><th>策略参考价</th><th>可卖状态</th>
      </tr></thead><tbody>{[...triggered, ...remaining].map(row => <tr key={row.alert_id}>
        <td>{row.stock_name || row.symbol}<small>{row.symbol}</small></td>
        <td>{row.membership === "IN_SCOPE" ? "荐股范围内" : "范围外待复评"}</td>
        <td>{row.status === "TRIGGERED"
          ? <strong>已触发 · {time(row.triggered_at)}</strong>
          : row.status === "NOT_TRIGGERED" ? "未触发" : "待核验"}
          {row.status === "UNVERIFIED" && <small>{reasonText[row.reason_code || ""] || "数据待核验"}</small>}
          {row.status === "TRIGGERED" && row.first_trigger_verified === false && <small>更早分钟缺失</small>}
          {row.observed_through && <small>行情截至 {time(row.observed_through)}{row.coverage === "INCOMPLETE" ? " · 覆盖不全" : ""}</small>}
        </td>
        <td>进入 {row.entry_price == null ? "待核验" : `¥${number(row.entry_price)}`}
          {row.trigger_price != null && <small>触发 ¥{number(row.trigger_price)}</small>}</td>
        <td>{sellabilityText[row.sellability]}</td>
      </tr>)}</tbody></table></div>
    </>}
  </section>;
}
