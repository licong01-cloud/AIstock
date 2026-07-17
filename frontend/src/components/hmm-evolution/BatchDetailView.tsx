"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { ArrowLeft, RefreshCw, StopCircle } from "lucide-react";
import { cancelBatch, getBatch, retryFailedBatch } from "@/lib/hmm-evolution/api";
import type { BatchDetail, BatchItem } from "@/lib/hmm-research/contracts";
import { TERMINAL_BATCH_STATUSES } from "@/lib/hmm-research/contracts";
import EvidencePanel from "@/components/hmm-research/EvidencePanel";
import HMMResearchShell from "@/components/hmm-research/HMMResearchShell";
import StatusBadge from "@/components/hmm-research/StatusBadge";
import VisibleErrorState from "@/components/hmm-research/VisibleErrorState";
import styles from "@/components/hmm-research/hmm-research.module.css";

export default function BatchDetailView({ batchId }: { batchId: string }) {
  const [batch, setBatch] = useState<BatchDetail | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      setBatch(await getBatch(batchId));
    } catch (nextError) {
      setError(nextError);
    } finally {
      setLoading(false);
    }
  }, [batchId]);

  useEffect(() => { void load(); }, [load]);

  useEffect(() => {
    if (!batch || TERMINAL_BATCH_STATUSES.has(batch.status)) return;
    const timer = setTimeout(() => void load(), 3_000);
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
            <button type="button" className={styles.button} onClick={() => void load()}><RefreshCw size={14} />刷新</button>
            {batch && !TERMINAL_BATCH_STATUSES.has(batch.status) ? <button type="button" className={`${styles.button} ${styles.buttonDanger}`} onClick={() => void onCancel()}><StopCircle size={14} />取消</button> : null}
            {batch && ["partial_failed", "failed", "timed_out"].includes(batch.status) ? <button type="button" className={`${styles.button} ${styles.buttonSoft}`} onClick={() => void onRetry()}>仅重试失败项</button> : null}
          </div>
        </div>

        {loading ? <div className={styles.loadingState}>正在加载批次证据；失败会显式终止。</div> : null}
        {error ? <VisibleErrorState error={error} onRetry={() => void load()} /> : null}
        {batch ? (
          <div className={styles.stack}>
            <section className={styles.metricsGrid}>
              <Metric label="候选数" value={String(batch.candidate_count)} note={`retry generation ${batch.retry_generation}`} className={styles.metricGreen} />
              <Metric label="成功" value={String(batch.succeeded_count)} note="包含复用与本轮成功结果" className={styles.metricBlue} />
              <Metric label="失败 / 超时" value={`${batch.failed_count} / ${batch.timed_out_count}`} note="失败不会被空集合掩盖" className={styles.metricAmber} />
              <Metric label="Top-3" value={String(batch.items.filter((item) => item.is_top3).length)} note="研究推荐，需 QE 终审" className={styles.metricSlate} />
            </section>

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
