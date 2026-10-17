"use client";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { chinaTime, cutoverBlockerText, onlyInFlightBlocker } from "@/lib/simulation/localsim/presentation";
import type { LocalSimCutoverReadiness, LocalSimSchedulerVerification } from "@/lib/simulation/localsim/types";
import type { LocalSimReadback } from "@/lib/simulation/localsim/use-readback";
import styles from "../localsim.module.css";

type Props = {
  readiness: LocalSimReadback<LocalSimCutoverReadiness>;
  scheduler: LocalSimReadback<LocalSimSchedulerVerification>;
  onRefresh: () => void;
};

export function LocalSimReadbackStatus({ readiness, scheduler, onRefresh }: Props) {
  const fresh = readiness.data !== null && !readiness.error && !readiness.stale;
  const waiting = fresh && onlyInFlightBlocker(readiness.data!);
  const ready = fresh && readiness.data!.ready;
  const cutoverTitle = readiness.error ? "切流状态读取失败"
    : readiness.stale && readiness.data ? "切流状态已过期，等待重新核验"
      : !readiness.data ? "正在读取切流条件"
        : waiting ? `有 ${readiness.data.in_flight_economic_run_count} 个交易日正在执行，切流暂时等待`
          : ready ? "切流前置检查通过" : "切流存在阻塞";

  const health = scheduler.data;
  const healthFresh = health !== null && !scheduler.error && !scheduler.stale;
  const current = health?.current_trade_date_blockers;
  const healthy = healthFresh && health?.running && health.thread_alive
    && health.scheduler_loop_health?.status === "HEALTHY"
    && health.effective_runtime_health === "NO_CURRENT_DAY_BLOCKER"
    && current?.status === "CLEAR" && current.blocker_count === 0;
  const healthTitle = scheduler.error ? "当日调度核验读取失败"
    : !health ? "正在读取当日调度核验"
      : scheduler.stale ? "当日调度核验已过期"
        : healthy ? "未发现当日调度阻塞" : "当日调度状态需要检查";

  return <section aria-label="LocalSIM 运行状态" className={styles.statusSection}>
    <div className={styles.statusToolbar}>
      <p className={styles.statusMeta}>页面可见时每 5 秒刷新；返回页面时重新核验。</p>
      <Button className={styles.statusRefresh} variant="outline" size="sm" disabled={readiness.refreshing && scheduler.refreshing} onClick={onRefresh}>刷新运行状态</Button>
    </div>
    <div className={styles.statusGrid}>
      <Alert className={`${styles.statusCard} ${readiness.error || (fresh && !ready && !waiting) ? styles.statusError : waiting ? styles.statusWaiting : ready ? styles.statusSuccess : ""}`} variant={readiness.error || (fresh && !ready && !waiting) ? "destructive" : "default"}>
        <AlertTitle className={styles.statusTitle}>切流条件 · {cutoverTitle}</AlertTitle>
        <AlertDescription className={styles.statusContent}>
          {readiness.error ? <p>{readiness.error}。请刷新重试，控制操作暂不可用。</p>
            : !fresh ? <p>尚未获得有效的新状态，控制操作暂不可用。</p>
              : waiting ? <p>这是在途交易保护；现有回放继续执行，切流及受保护的控制操作等待交易日结束。</p>
                : ready ? <p>前置条件已满足；接续实时模拟盘仍取决于各账户回放进度和安全切换时点。</p>
                  : <><p>请先核对以下阻塞，保持后端保护：</p><ul>{readiness.data!.blockers.map((reason) => <li key={reason}>{cutoverBlockerText(reason)}</li>)}</ul></>}
          <p>后端核验时间：{chinaTime(readiness.data?.checked_at)}</p>
          <p>页面最近读取：{chinaTime(readiness.updatedAt)}{readiness.refreshing ? " · 正在刷新" : ""}</p>
          {readiness.data && <details className={styles.statusDetails}><summary>切流诊断详情</summary><pre>{JSON.stringify(readiness.data, null, 2)}</pre></details>}
        </AlertDescription>
      </Alert>
      <Alert className={`${styles.statusCard} ${scheduler.error || (healthFresh && !healthy) ? styles.statusError : healthy ? styles.statusSuccess : ""}`} variant={scheduler.error || (healthFresh && !healthy) ? "destructive" : "default"}>
        <AlertTitle className={styles.statusTitle}>当日调度核验 · {healthTitle}</AlertTitle>
        <AlertDescription className={styles.statusContent}>
          {scheduler.error && <p>{scheduler.error}。请刷新重试。</p>}
          {health && <p>核验日期：{current?.trade_date || "未返回"}；调度器：{health.running ? "运行" : "停止"}；线程：{health.thread_alive ? "存活" : "不可用"}；调度循环：{health.scheduler_loop_health?.status === "HEALTHY" ? "健康" : "需要检查"}；当日阻塞：{current?.blocker_count ?? "未返回"}。</p>}
          <p>此项仅核验当日调度。历史回放的进度和失败原因需查看各任务，切流就绪也不代表历史回放已验收。</p>
          <p>页面最近读取：{chinaTime(scheduler.updatedAt)}{scheduler.refreshing ? " · 正在刷新" : ""}</p>
          {health && <details className={styles.statusDetails}><summary>调度诊断详情</summary><pre>{JSON.stringify(health, null, 2)}</pre></details>}
        </AlertDescription>
      </Alert>
    </div>
  </section>;
}
