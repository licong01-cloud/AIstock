"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowLeft, RefreshCw, StopCircle } from "lucide-react";
import { cancelBatch, getBatch, HMMApiError, retryFailedBatch } from "@/lib/hmm-evolution/api";
import type { BatchDetail, BatchItem } from "@/lib/hmm-research/contracts";
import { TERMINAL_BATCH_STATUSES } from "@/lib/hmm-research/contracts";
import EvidencePanel from "@/components/hmm-research/EvidencePanel";
import HMMResearchShell from "@/components/hmm-research/HMMResearchShell";
import StatusBadge from "@/components/hmm-research/StatusBadge";
import VisibleErrorState from "@/components/hmm-research/VisibleErrorState";
import styles from "@/components/hmm-research/hmm-research.module.css";

const POLL_FAST_MS = 3_000;
const POLL_SLOW_MS = 10_000;
const POLL_BACKOFF_AFTER_MS = 60_000;
const POLL_TIMEOUT_MS = 15 * 60_000;

export default function BatchDetailView({ batchId }: { batchId: string }) {
  const [batch, setBatch] = useState<BatchDetail | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(true);
  const [stale, setStale] = useState(false);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<string | null>(null);
  const pollStartedAt = useRef<number | null>(null);

  const load = useCallback(async (background = false) => {
    setError(null);
    if (!background) setLoading(true);
    try {
      setBatch(await getBatch(batchId));
      setStale(false);
      setLastUpdatedAt(new Date().toISOString());
    } catch (nextError) {
      setError(nextError);
      setStale(background);
    } finally {
      if (!background) setLoading(false);
    }
  }, [batchId]);

  useEffect(() => { void load(); }, [load]);

  useEffect(() => {
    if (!batch || TERMINAL_BATCH_STATUSES.has(batch.status)) {
      pollStartedAt.current = null;
      return;
    }
    if (pollStartedAt.current === null) pollStartedAt.current = Date.now();
    const elapsed = Date.now() - pollStartedAt.current;
    if (elapsed >= POLL_TIMEOUT_MS) {
      setStale(true);
      setError(new HMMApiError({
        error_code: "HMM_EVOLUTION_CLIENT_TIMEOUT",
        reason_code: "hmm_evolution_client_polling_timeout",
        message: "批次自动轮询已达到 15 分钟上限，页面保留最后一次数据并停止自动请求。",
        context: { retry_condition: "核对 worker 与 durable state 后手动刷新。" },
      }, 504));
      return;
    }
    const delay = elapsed >= POLL_BACKOFF_AFTER_MS ? POLL_SLOW_MS : POLL_FAST_MS;
    const timer = setTimeout(() => void load(true), delay);
    return () => clearTimeout(timer);
  }, [batch, load]);

  const orderedItems = useMemo(
    () => [...(batch?.items || [])].sort((a, b) => a.ordinal - b.ordinal),
    [batch],
  );

  const onCancel = async () => {
    try {
      await cancelBatch(batchId);
      await load();
    } catch (nextError) {
      setError(nextError);
    }
  };

  const onRetry = async () => {
    try {
      const retry = await retryFailedBatch(batchId);
      window.location.assign(`/hmm-evolution/batches/${encodeURIComponent(retry.batch_id)}`);
    } catch (nextError) {
      setError(nextError);
    }
  };

  return (
    <HMMResearchShell>
      <main className={styles.page}>
        <div className={styles.detailHeader}>
          <div>
            <Link href="/hmm-evolution" className={styles.backLink}><ArrowLeft size={13} /> 返回演进实验室</Link>
            <div className={styles.eyebrow}>Batch Evidence</div>
            <h1 className={styles.title}>批次 {batchId}</h1>
            <p className={styles.heroCopy}>逐项查看状态、研究推荐和失败证据；详情固定在页面中，不使用抽屉或 raw JSON。</p>
          </div>
          <div className={styles.panelActions}>
            {batch ? <StatusBadge status={batch.status} /> : null}
            <button type="button" className={styles.button} onClick={() => void load(false)}><RefreshCw size={14} />刷新</button>
            {batch && !TERMINAL_BATCH_STATUSES.has(batch.status) ? <button type="button" className={`${styles.button} ${styles.buttonDanger}`} onClick={() => void onCancel()}><StopCircle size={14} />取消</button> : null}
            {batch && ["partial_failed", "failed", "timed_out"].includes(batch.status) ? <button type="button" className={`${styles.button} ${styles.buttonSoft}`} onClick={() => void onRetry()}>仅重试失败项</button> : null}
          </div>
        </div>

        {loading ? <div className={styles.loadingState}>正在加载批次证据；失败会显式终止。</div> : null}
        {error ? <VisibleErrorState error={error} onRetry={() => void load(false)} /> : null}
        {stale && batch ? <div className={`${styles.notice} ${styles.noticeWarning}`}>当前显示最后一次成功数据（{formatDateTime(lastUpdatedAt)}），自动轮询已暂停；手动刷新成功前不得视为最新状态。</div> : null}
        {batch ? (
          <div className={styles.stack}>
            <section className={styles.metricsGrid}>
              <Metric label="候选数" value={String(batch.candidate_count)} note={`retry generation ${batch.retry_generation}`} className={styles.metricGreen} />
              <Metric label="成功" value={String(batch.succeeded_count)} note="包含复用与本轮成功结果" className={styles.metricBlue} />
              <Metric label="失败 / 超时" value={`${batch.failed_count} / ${batch.timed_out_count}`} note="失败不会被空集合掩盖" className={styles.metricAmber} />
              <Metric label="Top-3" value={String(batch.items.filter((item) => item.is_top3).length)} note="研究推荐，需 QE 终审" className={styles.metricSlate} />
            </section>

            {batch.items.some((item) => item.evidence_quality === "degraded") ? (
              <div className={`${styles.notice} ${styles.noticeWarning}`}>
                <strong>批次包含降级证据</strong>
                <span>{batch.items.filter((item) => item.evidence_quality === "degraded").length} 个候选的日期覆盖或行情证据不完整；请进入对应 evaluation 查看受影响范围与 warning。</span>
              </div>
            ) : null}

            <section className={styles.panel}>
              <div className={styles.panelHeader}><div><h2 className={styles.panelTitle}>候选项目</h2><div className={styles.panelSubtitle}>所有候选均保留，未排名不等于淘汰</div></div></div>
              <div className={styles.panelBodyTable}><BatchItemsTable items={orderedItems} /></div>
            </section>

            <section className={styles.detailGrid}>
              <div className={styles.panel}>
                <div className={styles.panelHeader}><h2 className={styles.panelTitle}>批次身份</h2></div>
                <div className={styles.panelBody}>
                  <EvidencePanel sections={[{ title: "Durable state", rows: [
                    { label: "Batch ID", value: batch.batch_id },
                    { label: "状态", value: batch.status },
                    { label: "创建时间", value: formatDateTime(batch.created_at) },
                    { label: "最近心跳", value: formatDateTime(batch.heartbeat_at) },
                    { label: "完成时间", value: formatDateTime(batch.completed_at) },
                  ] }]} />
                </div>
              </div>
              <div className={styles.panel}>
                <div className={styles.panelHeader}><h2 className={styles.panelTitle}>推荐证据</h2></div>
                <div className={styles.panelBody}>
                  <EvidencePanel sections={[{ title: "Recommendation", rows: [
                    { label: "版本", value: batch.recommendation_version },
                    { label: "已排名", value: String(batch.items.filter((item) => item.recommendation_rank !== null).length) },
                    { label: "证据降级", value: String(batch.items.filter((item) => item.evidence_quality === "degraded").length) },
                    { label: "业务语义", value: "仅研究推荐；无生产替换或交易动作" },
                  ] }]} />
                </div>
              </div>
            </section>
          </div>
        ) : null}
      </main>
    </HMMResearchShell>
  );
}

function BatchItemsTable({ items }: { items: BatchItem[] }) {
  if (items.length === 0) return <div className={styles.emptyState}>批次没有 item；这不是成功结果。</div>;
  return (
    <table className={styles.table}>
      <thead><tr><th>序号</th><th>候选</th><th>评估状态</th><th>推荐排名</th><th>推荐分</th><th>证据质量</th><th>失败原因</th></tr></thead>
      <tbody>{items.map((item) => <tr key={item.candidate_id}>
        <td>{item.ordinal + 1}</td>
        <td><Link className={styles.tableLink} href={`/hmm-evolution/evaluations/${encodeURIComponent(item.eval_id)}`}>{item.candidate_display_name}</Link><div className={styles.candidateMeta}>{item.candidate_id}</div></td>
        <td><StatusBadge status={item.evaluation_status} /></td>
        <td>{item.recommendation_rank ?? "未排名"}</td>
        <td>{item.recommendation_score === null ? "未计算" : item.recommendation_score.toFixed(2)}</td>
        <td>{item.evidence_quality || "未生成"}</td>
        <td>{item.evaluation_reason_code || item.reason_code || "无"}</td>
      </tr>)}</tbody>
    </table>
  );
}

function Metric({ label, value, note, className }: { label: string; value: string; note: string; className: string }) {
  return <article className={`${styles.metricCard} ${className}`}><div className={styles.metricLabel}>{label}</div><div className={styles.metricValue}>{value}</div><div className={styles.metricNote}>{note}</div></article>;
}

function formatDateTime(value: string | null): string {
  return value ? new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : "未记录";
}
