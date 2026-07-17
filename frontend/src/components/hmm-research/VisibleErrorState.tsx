"use client";

import { AlertTriangle, RotateCcw } from "lucide-react";
import { HMMApiError } from "@/lib/hmm-evolution/api";
import styles from "./hmm-research.module.css";

const REASON_EXPLANATIONS: Record<string, string> = {
  hmm_evolution_invalid_spec: "请求参数或评估身份不符合 HMM 研究契约。",
  hmm_evolution_unsafe_asset_path: "资产路径不是安全的 QE 相对路径。",
  hmm_evolution_unsupported_source: "当前候选来源不在已批准的三类只读来源内。",
  hmm_evolution_runtime_disabled: "HMM 研究运行时尚未启用，页面不会用旧数据或静态样例替代。",
  hmm_evolution_schema_unavailable: "独立 hmm_evolution schema 当前不可用。",
  hmm_evolution_qe_asset_unavailable: "QE 只读资产当前不可访问，请核对节点和资产水位。",
  hmm_evolution_qe_asset_catalog_incomplete: "QE 资产目录不完整，本次操作要求完整目录。",
  hmm_evolution_qe_asset_too_large: "资产超过页面检查上限，需要使用有界 Range 读取。",
  hmm_evolution_artifact_manifest_invalid: "候选产物 manifest 或 coefficient 结构不符合研究契约。",
  hmm_evolution_artifact_hash_mismatch: "候选产物与登记时的 hash 不一致，未继续计算。",
  hmm_evolution_candidate_not_found: "候选不存在或已不在独立研究候选库中。",
  hmm_evolution_batch_not_found: "评估批次不存在。",
  hmm_evolution_evaluation_not_found: "离线评估记录不存在。",
  hmm_evolution_invalid_state_transition: "当前 durable 状态不允许执行该操作。",
  hmm_evolution_idempotency_conflict: "相同幂等键已用于不同请求，未创建重复任务。",
  "hmm_evolution_stale_fencing_token": "worker lease 已失效，旧执行者的写入被拒绝。",
  hmm_evolution_market_data_unavailable: "共同完成行情数据不足，未生成中性结果。",
  hmm_evolution_source_unavailable: "Phase 0 预测或标签输入当前不可用。",
  hmm_evolution_no_common_dates: "预测、标签、系数和行情之间没有共同可评估日期。",
  hmm_evolution_label_horizon_mismatch: "标签 horizon 与评估请求不一致。",
  hmm_evolution_coefficient_date_coverage_empty: "候选系数未覆盖请求的评估窗口。",
  hmm_evolution_evaluation_cancelled: "评估已在 durable checkpoint 响应取消。",
  hmm_evolution_client_timeout: "客户端已停止等待，不会永久显示加载状态。",
  hmm_evolution_client_polling_timeout: "批次轮询达到客户端上限，自动请求已停止。",
  hmm_evolution_client_request_failed: "客户端无法访问 HMM 研究接口。",
};

function normalizeError(error: unknown): HMMApiError {
  if (error instanceof HMMApiError) return error;
  return new HMMApiError(
    {
      error_code: "HMM_EVOLUTION_UI_ERROR",
      reason_code: "hmm_evolution_ui_render_failed",
      message: error instanceof Error ? error.message : "HMM 页面渲染失败。",
      context: {},
    },
    0,
  );
}

export default function VisibleErrorState({
  error,
  title = "HMM 研究数据加载失败",
  onRetry,
}: {
  error: unknown;
  title?: string;
  onRetry?: () => void;
}) {
  const normalized = normalizeError(error);
  const retryCondition =
    typeof normalized.context.retry_condition === "string"
      ? normalized.context.retry_condition
      : "修复上游数据或运行态后可重试；当前不会自动回退到旧结果。";
  return (
    <div className={styles.errorState} role="alert" aria-live="assertive">
      <h3 className={styles.errorTitle}>
        <AlertTriangle size={17} aria-hidden="true" />
        {title}
      </h3>
      <div className={styles.errorGrid}>
        <span className={styles.errorLabel}>Reason Code</span>
        <span className={styles.errorValue}>{normalized.reasonCode}</span>
        <span className={styles.errorLabel}>说明</span>
        <span className={styles.errorValue}>
          {REASON_EXPLANATIONS[normalized.reasonCode] || normalized.message}
        </span>
        <span className={styles.errorLabel}>重试条件</span>
        <span className={styles.errorValue}>{retryCondition}</span>
        <span className={styles.errorLabel}>Trace ID</span>
        <span className={styles.errorValue}>{normalized.traceId || "未返回"}</span>
      </div>
      {onRetry ? (
        <div className={styles.errorActions}>
          <button type="button" className={`${styles.button} ${styles.buttonDanger}`} onClick={onRetry}>
            <RotateCcw size={14} aria-hidden="true" />
            重试
          </button>
        </div>
      ) : null}
    </div>
  );
}
