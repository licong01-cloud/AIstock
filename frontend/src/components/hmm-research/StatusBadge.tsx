import type { BatchStatus, CandidateLifecycle, EvaluationStatus } from "@/lib/hmm-research/contracts";
import styles from "./hmm-research.module.css";

const LABELS: Record<string, string> = {
  research_only: "研究候选",
  retired: "已退役",
  invalid: "无效",
  preparation_queued: "等待输入冻结",
  preparing: "正在冻结输入",
  queued: "排队中",
  running: "运行中",
  cancel_requested: "取消中",
  completed: "已完成",
  succeeded: "已成功",
  partial_failed: "部分失败",
  failed: "失败",
  cancelled: "已取消",
  timed_out: "已超时",
  reused: "复用结果",
  waiting_shared: "等待共享结果",
};

export default function StatusBadge({
  status,
}: {
  status: BatchStatus | EvaluationStatus | CandidateLifecycle | string;
}) {
  const className = ["completed", "succeeded", "research_only", "reused"].includes(status)
    ? styles.tagGood
    : ["preparation_queued", "preparing", "running", "queued", "waiting_shared", "cancel_requested"].includes(status)
      ? styles.tagWarn
      : ["failed", "partial_failed", "timed_out", "invalid"].includes(status)
        ? styles.tagDanger
        : "";
  return (
    <span className={`${styles.tag} ${className}`} aria-label={`状态：${LABELS[status] || status}`}>
      {LABELS[status] || status}
    </span>
  );
}
