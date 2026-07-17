"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Archive,
  Ban,
  ChevronRight,
  FileSearch,
  FlaskConical,
  Plus,
  RefreshCw,
  ShieldCheck,
  StopCircle,
} from "lucide-react";
import {
  cancelBatch,
  createBatch,
  getBatch,
  HMMApiError,
  listBatches,
  listCandidates,
  listQEAssets,
  previewCandidate,
  registerCandidate,
  retryFailedBatch,
} from "@/lib/hmm-evolution/api";
import type {
  BatchDetail,
  BatchItem,
  BatchSummary,
  CandidatePreview,
  CandidateRecord,
  CandidateSourcePayload,
  CandidateSourceType,
  EvaluationSpecPayload,
  QEAssetCatalog,
} from "@/lib/hmm-research/contracts";
import { TERMINAL_BATCH_STATUSES } from "@/lib/hmm-research/contracts";
import EvidencePanel from "@/components/hmm-research/EvidencePanel";
import HMMResearchShell from "@/components/hmm-research/HMMResearchShell";
import StatusBadge from "@/components/hmm-research/StatusBadge";
import VisibleErrorState from "@/components/hmm-research/VisibleErrorState";
import QEAssetBrowser from "@/components/hmm-evolution/QEAssetBrowser";
import styles from "@/components/hmm-research/hmm-research.module.css";

const POLL_FAST_MS = 3_000;
const POLL_SLOW_MS = 10_000;
const POLL_BACKOFF_AFTER_MS = 60_000;
const POLL_TIMEOUT_MS = 15 * 60_000;

type EvaluationForm = {
  baseLoopRef: string;
  windowStart: string;
  windowEnd: string;
  asOfPolicy: "explicit" | "latest_common_completed";
  requestedDate: string;
  labelHorizonDays: string;
  topk: string;
  marketMode: "required" | "disabled";
};

type CandidateForm = {
  sourceType: CandidateSourceType;
  snapshotId: string;
  artifactName: string;
  rootAlias: string;
  relativePath: string;
  taskId: string;
  loopName: string;
  displayName: string;
  description: string;
};

const DEFAULT_EVALUATION_FORM: EvaluationForm = {
  baseLoopRef: "",
  windowStart: "",
  windowEnd: "",
  asOfPolicy: "latest_common_completed",
  requestedDate: "",
  labelHorizonDays: "20",
  topk: "50",
  marketMode: "required",
};

const DEFAULT_CANDIDATE_FORM: CandidateForm = {
  sourceType: "qe_experiment_coefficients",
  snapshotId: "",
  artifactName: "",
  rootAlias: "",
  relativePath: "",
  taskId: "",
  loopName: "",
  displayName: "",
  description: "",
};

export default function EvolutionDashboard() {
  const [candidates, setCandidates] = useState<CandidateRecord[]>([]);
  const [batches, setBatches] = useState<BatchSummary[]>([]);
  const [selectedBatchId, setSelectedBatchId] = useState("");
  const [selectedBatch, setSelectedBatch] = useState<BatchDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [globalError, setGlobalError] = useState<unknown>(null);
  const [pollingError, setPollingError] = useState<unknown>(null);
  const [showEvaluationForm, setShowEvaluationForm] = useState(false);
  const [showCandidateForm, setShowCandidateForm] = useState(false);
  const [evaluationForm, setEvaluationForm] = useState(DEFAULT_EVALUATION_FORM);
  const [candidateForm, setCandidateForm] = useState(DEFAULT_CANDIDATE_FORM);
  const [selectedCandidateIds, setSelectedCandidateIds] = useState<string[]>([]);
  const [candidatePreview, setCandidatePreview] = useState<CandidatePreview | null>(null);
  const [candidateActionError, setCandidateActionError] = useState<unknown>(null);
  const [batchActionError, setBatchActionError] = useState<unknown>(null);
  const [submittingCandidate, setSubmittingCandidate] = useState(false);
  const [submittingBatch, setSubmittingBatch] = useState(false);
  const [assetTaskId, setAssetTaskId] = useState("");
  const [assetLoopName, setAssetLoopName] = useState("");
  const [assetCatalog, setAssetCatalog] = useState<QEAssetCatalog | null>(null);
  const [assetError, setAssetError] = useState<unknown>(null);
  const [assetLoading, setAssetLoading] = useState(false);
  const [overviewStale, setOverviewStale] = useState(false);
  const [batchStale, setBatchStale] = useState(false);
  const [lastSuccessfulRefresh, setLastSuccessfulRefresh] = useState<string | null>(null);
  const hasOverviewData = useRef(false);
  const pollStartedAt = useRef<number | null>(null);

  const loadOverview = useCallback(async () => {
    setLoading(true);
    setGlobalError(null);
    try {
      const [candidateRows, batchRows] = await Promise.all([listCandidates(), listBatches()]);
      setCandidates(candidateRows);
      setBatches(batchRows);
      hasOverviewData.current = true;
      setOverviewStale(false);
      setLastSuccessfulRefresh(new Date().toISOString());
      setSelectedBatchId((current) => current || batchRows[0]?.batch_id || "");
    } catch (error) {
      setGlobalError(error);
      setOverviewStale(hasOverviewData.current);
    } finally {
      setLoading(false);
    }
  }, []);

  const loadSelectedBatch = useCallback(async (batchId: string) => {
    if (!batchId) {
      setSelectedBatch(null);
      return;
    }
    try {
      const detail = await getBatch(batchId);
      setSelectedBatch(detail);
      setPollingError(null);
      setBatchStale(false);
      setLastSuccessfulRefresh(new Date().toISOString());
    } catch (error) {
      setPollingError(error);
      setBatchStale(true);
    }
  }, []);

  useEffect(() => {
    void loadOverview();
  }, [loadOverview]);

  useEffect(() => {
    void loadSelectedBatch(selectedBatchId);
  }, [loadSelectedBatch, selectedBatchId]);

  useEffect(() => {
    if (!selectedBatch || TERMINAL_BATCH_STATUSES.has(selectedBatch.status)) {
      pollStartedAt.current = null;
      return;
    }
    if (pollStartedAt.current === null) pollStartedAt.current = Date.now();
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const poll = async () => {
      const elapsed = Date.now() - (pollStartedAt.current || Date.now());
      if (elapsed >= POLL_TIMEOUT_MS) {
        setBatchStale(true);
        setPollingError(
          new HMMApiError(
            {
              error_code: "HMM_EVOLUTION_CLIENT_TIMEOUT",
              reason_code: "hmm_evolution_client_polling_timeout",
              message: "批次轮询已达到客户端上限，页面停止自动请求。",
              context: { retry_condition: "确认 worker 与批次状态后手工刷新。" },
            },
            504,
          ),
        );
        return;
      }
      try {
        const detail = await getBatch(selectedBatch.batch_id);
        if (cancelled) return;
        setSelectedBatch(detail);
        setBatchStale(false);
        setLastSuccessfulRefresh(new Date().toISOString());
        setBatches((rows) => rows.map((row) => (row.batch_id === detail.batch_id ? detail : row)));
        if (TERMINAL_BATCH_STATUSES.has(detail.status)) {
          pollStartedAt.current = null;
          return;
        }
        const delay = elapsed >= POLL_BACKOFF_AFTER_MS ? POLL_SLOW_MS : POLL_FAST_MS;
        timer = setTimeout(poll, delay);
      } catch (error) {
        if (!cancelled) {
          setPollingError(error);
          setBatchStale(true);
        }
      }
    };

    timer = setTimeout(poll, POLL_FAST_MS);
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [selectedBatch]);

  const rankedItems = useMemo(() => {
    const items = [...(selectedBatch?.items || [])];
    return items.sort((left, right) => {
      if (left.recommendation_rank === null && right.recommendation_rank === null) {
        return left.ordinal - right.ordinal;
      }
      if (left.recommendation_rank === null) return 1;
      if (right.recommendation_rank === null) return -1;
      return left.recommendation_rank - right.recommendation_rank;
    });
  }, [selectedBatch]);

  const topThree = rankedItems.filter((item) => item.is_top3).slice(0, 3);
  const selectedProgress = selectedBatch
    ? selectedBatch.succeeded_count +
      selectedBatch.failed_count +
      selectedBatch.cancelled_count +
      selectedBatch.timed_out_count
    : 0;
  const commonWatermark = selectedBatch?.items[0]?.as_of_date || "尚无批次证据";
  const medianDuration = useMemo(() => formatMedianDuration(batches), [batches]);

  const submitEvaluation = async () => {
    setBatchActionError(null);
    try {
      const spec = buildEvaluationSpec(evaluationForm);
      if (selectedCandidateIds.length === 0) {
        throw new Error("请至少选择一个 research_only 候选。 ");
      }
      setSubmittingBatch(true);
      const result = await createBatch({
        candidate_ids: selectedCandidateIds,
        evaluation_spec: spec,
      });
      await loadOverview();
      setSelectedBatchId(result.batch.batch_id);
      setShowEvaluationForm(false);
    } catch (error) {
      setBatchActionError(error);
    } finally {
      setSubmittingBatch(false);
    }
  };

  const candidatePayload = (): CandidateSourcePayload => {
    if (candidateForm.sourceType === "existing_snapshot_coefficients") {
      return {
        source_type: candidateForm.sourceType,
        snapshot_id: candidateForm.snapshotId.trim(),
        artifact_name: candidateForm.artifactName.trim(),
      };
    }
    if (candidateForm.sourceType === "configured_local_coefficients") {
      return {
        source_type: candidateForm.sourceType,
        root_alias: candidateForm.rootAlias.trim(),
        relative_path: candidateForm.relativePath.trim(),
      };
    }
    return {
      source_type: candidateForm.sourceType,
      task_id: candidateForm.taskId.trim(),
      loop_name: candidateForm.loopName.trim(),
      relative_path: candidateForm.relativePath.trim(),
    };
  };

  const runCandidatePreview = async () => {
    setCandidateActionError(null);
    setCandidatePreview(null);
    try {
      setSubmittingCandidate(true);
      setCandidatePreview(await previewCandidate(candidatePayload()));
    } catch (error) {
      setCandidateActionError(error);
    } finally {
      setSubmittingCandidate(false);
    }
  };

  const submitCandidate = async () => {
    setCandidateActionError(null);
    try {
      if (!candidatePreview) throw new Error("请先完成只读预检，再登记候选。 ");
      if (!candidateForm.displayName.trim()) throw new Error("候选名称不能为空。 ");
      setSubmittingCandidate(true);
      await registerCandidate({
        ...candidatePayload(),
        display_name: candidateForm.displayName.trim(),
        description: candidateForm.description.trim() || undefined,
      });
      setCandidateForm(DEFAULT_CANDIDATE_FORM);
      setCandidatePreview(null);
      setShowCandidateForm(false);
      await loadOverview();
    } catch (error) {
      setCandidateActionError(error);
    } finally {
      setSubmittingCandidate(false);
    }
  };

  const loadAssets = async () => {
    setAssetError(null);
    setAssetCatalog(null);
    try {
      if (!assetTaskId.trim() || !assetLoopName.trim()) {
        throw new Error("请输入 QE task 与 Loop。 ");
      }
      setAssetLoading(true);
      setAssetCatalog(await listQEAssets(assetTaskId.trim(), assetLoopName.trim()));
    } catch (error) {
      setAssetError(error);
    } finally {
      setAssetLoading(false);
    }
  };

  const requestCancel = async () => {
    if (!selectedBatch) return;
    setBatchActionError(null);
    try {
      await cancelBatch(selectedBatch.batch_id);
      await loadSelectedBatch(selectedBatch.batch_id);
    } catch (error) {
      setBatchActionError(error);
    }
  };

  const retryBatch = async () => {
    if (!selectedBatch) return;
    setBatchActionError(null);
    try {
      const retry = await retryFailedBatch(selectedBatch.batch_id);
      await loadOverview();
      setSelectedBatchId(retry.batch_id);
    } catch (error) {
      setBatchActionError(error);
    }
  };

  return (
    <HMMResearchShell>
      <main className={styles.page}>
        <section className={styles.hero}>
          <div>
            <div className={styles.eyebrow}>HMM Evolution Lab</div>
            <h1 className={styles.title}>让每个候选的优势和缺口一眼可见</h1>
            <p className={styles.heroCopy}>
              只读复用 QE 资产，集中呈现候选评估、批次进度和研究推荐。页面不展示原始 manifest / JSON，
              不修改 QE、Selection、Paper、QMT 或生产 HMM。
            </p>
          </div>
          <div className={styles.heroTools}>
            <select
              className={styles.select}
              aria-label="选择评估批次"
              value={selectedBatchId}
              onChange={(event) => setSelectedBatchId(event.target.value)}
            >
              <option value="">尚无批次</option>
              {batches.map((batch) => (
                <option value={batch.batch_id} key={batch.batch_id}>
                  {batch.batch_id} · {batch.candidate_count} 个候选 · {batch.status}
                </option>
              ))}
            </select>
            <button
              type="button"
              className={`${styles.button} ${styles.buttonSoft}`}
              onClick={() => setShowCandidateForm((value) => !value)}
            >
              <Archive size={15} aria-hidden="true" /> 候选接入
            </button>
            <button
              type="button"
              className={`${styles.button} ${styles.buttonPrimary}`}
              onClick={() => setShowEvaluationForm((value) => !value)}
            >
              <Plus size={15} aria-hidden="true" /> 新建评估
            </button>
          </div>
        </section>

        {globalError ? (
          <VisibleErrorState error={globalError} onRetry={() => void loadOverview()} />
        ) : null}
        {overviewStale ? <div className={`${styles.notice} ${styles.noticeWarning}`}>候选与批次概览刷新失败；当前展示 {formatDateTime(lastSuccessfulRefresh)} 的最后成功数据，不得视为最新状态。</div> : null}

        {showCandidateForm ? (
          <CandidateRegistrationPanel
            form={candidateForm}
            setForm={setCandidateForm}
            preview={candidatePreview}
            error={candidateActionError}
            busy={submittingCandidate}
            onPreview={() => void runCandidatePreview()}
            onRegister={() => void submitCandidate()}
          />
        ) : null}

        {showEvaluationForm ? (
          <EvaluationCreatePanel
            form={evaluationForm}
            setForm={setEvaluationForm}
            candidates={candidates}
            selectedCandidateIds={selectedCandidateIds}
            setSelectedCandidateIds={setSelectedCandidateIds}
            error={batchActionError}
            busy={submittingBatch}
            onSubmit={() => void submitEvaluation()}
          />
        ) : null}

        <section className={styles.metricsGrid} aria-label="HMM 演进概览">
          <MetricCard className={styles.metricGreen} label="当前加载候选" value={String(candidates.length)} note="API 当前页；不伪装为全库总数" />
          <MetricCard
            className={styles.metricBlue}
            label="本批次完成"
            value={selectedBatch ? `${selectedProgress} / ${selectedBatch.candidate_count}` : "—"}
            note={selectedBatch ? `状态：${selectedBatch.status}` : "尚无可显示批次"}
          />
          <MetricCard className={styles.metricAmber} label="共同数据水位" value={commonWatermark} note="来自已固化评估身份" date />
          <MetricCard className={styles.metricSlate} label="近期中位评估耗时" value={medianDuration} note="基于当前加载的最近批次样本" />
        </section>

        {loading ? <BoundedLoading resource="候选与批次概览" /> : null}
        {pollingError ? (
          <VisibleErrorState
            error={pollingError}
            title="批次状态刷新失败"
            onRetry={() => void loadSelectedBatch(selectedBatchId)}
          />
        ) : null}
        {batchStale && selectedBatch ? <div className={`${styles.notice} ${styles.noticeWarning}`}>当前批次状态已标记为 stale；手动刷新成功前不会继续自动轮询。</div> : null}
        {selectedBatch?.items.some((item) => item.evidence_quality === "degraded") ? <div className={`${styles.notice} ${styles.noticeWarning}`}><strong>批次包含降级证据</strong><span>{selectedBatch.items.filter((item) => item.evidence_quality === "degraded").length} 个候选存在日期交集或行情覆盖 warning；排名仅供研究终审。</span></div> : null}

        <section className={styles.evolutionLayout}>
          <section className={styles.panel}>
            <div className={styles.panelHeader}>
              <div>
                <h2 className={styles.panelTitle}>候选排行榜</h2>
                <div className={styles.panelSubtitle}>排序是研究推荐，不构成淘汰、交易或生产替换门禁</div>
              </div>
              {selectedBatch ? <StatusBadge status={selectedBatch.status} /> : null}
            </div>
            <div className={styles.panelBodyTable}>
              {rankedItems.length === 0 ? (
                <EmptyState title="暂无排行榜结果" detail="创建批次并由人工 worker 完成评估后，这里展示真实指标。" />
              ) : (
                <RankingTable items={rankedItems} />
              )}
            </div>
          </section>

          <div className={styles.stack}>
            <CurrentBatchPanel
              batch={selectedBatch}
              progress={selectedProgress}
              onRefresh={() => void loadSelectedBatch(selectedBatchId)}
              onCancel={() => void requestCancel()}
              onRetry={() => void retryBatch()}
            />
            <section className={styles.panel}>
              <div className={styles.panelHeader}>
                <div>
                  <h2 className={styles.panelTitle}>Top-3 研究推荐</h2>
                  <div className={styles.panelSubtitle}>仍需 QE 终审，不自动替换生产版本</div>
                </div>
              </div>
              <div className={styles.panelBody}>
                {topThree.length === 0 ? (
                  <EmptyState title="尚无可排名候选" detail="无 efficacy 指标的候选会保留展示，但不会伪造推荐分。" />
                ) : (
                  topThree.map((item) => <RecommendationCard key={item.candidate_id} item={item} />)
                )}
              </div>
            </section>
          </div>

          <section className={`${styles.panel} ${styles.fullWidth}`}>
            <div className={styles.panelHeader}>
              <div>
                <h2 className={styles.panelTitle}>候选库</h2>
                <div className={styles.panelSubtitle}>内容寻址、只读 provenance 与 research_only 生命周期</div>
              </div>
              <span className={`${styles.tag} ${styles.tagGood}`}><ShieldCheck size={13} /> 独立研究域</span>
            </div>
            <div className={styles.panelBodyTable}>
              <CandidateTable candidates={candidates} />
            </div>
          </section>

          <QEAssetBrowser
            taskId={assetTaskId}
            loopName={assetLoopName}
            setTaskId={setAssetTaskId}
            setLoopName={setAssetLoopName}
            catalog={assetCatalog}
            error={assetError}
            loading={assetLoading}
            onLoad={() => void loadAssets()}
          />

          <section className={`${styles.panel} ${styles.fullWidth}`}>
            <div className={styles.panelHeader}>
              <div>
                <h2 className={styles.panelTitle}>固定证据区</h2>
                <div className={styles.panelSubtitle}>输入身份、数据水位、计算版本和失败证据均显式呈现</div>
              </div>
            </div>
            <div className={styles.panelBody}>
              <EvidencePanel sections={buildBatchEvidence(selectedBatch)} />
            </div>
          </section>
        </section>
      </main>
    </HMMResearchShell>
  );
}

function MetricCard({
  label,
  value,
  note,
  className,
  date = false,
}: {
  label: string;
  value: string;
  note: string;
  className: string;
  date?: boolean;
}) {
  return (
    <article className={`${styles.metricCard} ${className}`}>
      <div className={styles.metricLabel}>{label}</div>
      <div className={`${styles.metricValue} ${date ? styles.metricValueDate : ""}`}>{value}</div>
      <div className={styles.metricNote}>{note}</div>
    </article>
  );
}

function BoundedLoading({ resource }: { resource: string }) {
  return (
    <div className={styles.loadingState} role="status" aria-live="polite">
      正在加载{resource}；请求超时会转为可见错误，不会无限等待。
    </div>
  );
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <div className={styles.emptyState}>
      <strong>{title}</strong>
      <div>{detail}</div>
    </div>
  );
}

function RankingTable({ items }: { items: BatchItem[] }) {
  return (
    <table className={styles.table}>
      <thead>
        <tr><th>排名</th><th>候选</th><th>推荐分</th><th>净标签收益</th><th>DB 10D</th><th>正值日比例</th><th>覆盖率</th><th>状态</th></tr>
      </thead>
      <tbody>
        {items.map((item) => (
          <tr key={item.candidate_id}>
            <td>{item.recommendation_rank ? <span className={styles.rankChip}>{item.recommendation_rank}</span> : "—"}</td>
            <td>
              <Link href={`/hmm-evolution/evaluations/${encodeURIComponent(item.eval_id)}`} className={styles.tableLink}>
                {item.candidate_display_name}
              </Link>
              <div className={styles.candidateMeta}>{shortHash(item.candidate_id)} · {sourceLabel(item.candidate_source_type)}</div>
            </td>
            <td>{formatNumber(item.recommendation_score, 1)}</td>
            <td className={metricTone(item.net_label_return)}>{formatPercent(item.net_label_return)}</td>
            <td className={metricTone(item.net_db_10d)}>{formatPercent(item.net_db_10d)}</td>
            <td>{formatPercent(item.positive_net_label_day_ratio)}</td>
            <td><CoverageBar value={item.primary_coverage_ratio} /></td>
            <td><StatusBadge status={item.evaluation_status} /></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function CoverageBar({ value }: { value: number | null }) {
  const percentage = value === null ? 0 : Math.max(0, Math.min(100, value * 100));
  return (
    <div aria-label={value === null ? "覆盖率未知" : `覆盖率 ${percentage.toFixed(1)}%`}>
      <div className={styles.barTrack}><div className={styles.barFill} style={{ width: `${percentage}%` }} /></div>
      <span className={styles.candidateMeta}>{value === null ? "未计算" : `${percentage.toFixed(1)}%`}</span>
    </div>
  );
}

function RecommendationCard({ item }: { item: BatchItem }) {
  return (
    <article className={styles.recommendCard}>
      <div className={styles.recommendTop}>
        <div>
          <Link href={`/hmm-evolution/evaluations/${encodeURIComponent(item.eval_id)}`} className={styles.tableLink}>
            {item.candidate_display_name}
          </Link>
          <div className={styles.candidateMeta}>
            证据置信度 {formatPercent(item.evidence_confidence)} · {item.evidence_quality || "未标记"}
          </div>
        </div>
        <div className={styles.recommendScore}>{formatNumber(item.recommendation_score, 1)}</div>
      </div>
    </article>
  );
}

function CurrentBatchPanel({
  batch,
  progress,
  onRefresh,
  onCancel,
  onRetry,
}: {
  batch: BatchDetail | null;
  progress: number;
  onRefresh: () => void;
  onCancel: () => void;
  onRetry: () => void;
}) {
  const terminal = batch ? TERMINAL_BATCH_STATUSES.has(batch.status) : false;
  return (
    <section className={styles.panel}>
      <div className={styles.panelHeader}>
        <div>
          <h2 className={styles.panelTitle}>当前批次</h2>
          <div className={styles.panelSubtitle}>{batch ? `${batch.batch_id} · 共享只读输入` : "尚未选择批次"}</div>
        </div>
        {batch ? <StatusBadge status={batch.status} /> : null}
      </div>
      <div className={styles.panelBody}>
        <div className={styles.steps}>
          <Step index="01" name="输入校验" state={batch ? "done" : "idle"} />
          <Step index="02" name="评估计算" state={batch?.running_count ? "active" : progress > 0 ? "done" : "idle"} />
          <Step index="03" name="证据归集" state={terminal ? "done" : progress > 0 ? "active" : "idle"} />
          <Step index="04" name="研究推荐" state={batch?.status === "completed" || batch?.status === "partial_failed" ? "done" : "idle"} />
        </div>
        {batch ? (
          <div className={styles.evidenceList} style={{ marginTop: 16 }}>
            <EvidenceRow label="进度" value={`${progress} / ${batch.candidate_count} 完成`} />
            <EvidenceRow label="最近心跳" value={formatDateTime(batch.heartbeat_at)} />
            <EvidenceRow label="失败项目" value={String(batch.failed_count + batch.timed_out_count)} />
            <EvidenceRow label="更新时间" value={formatDateTime(batch.updated_at)} />
          </div>
        ) : <EmptyState title="尚无批次" detail="从候选库选择 1 至 50 个候选创建评估。" />}
        {batch ? (
          <div className={styles.inlineActions} style={{ marginTop: 14 }}>
            <button type="button" className={styles.button} onClick={onRefresh}><RefreshCw size={14} />刷新</button>
            {!terminal ? <button type="button" className={`${styles.button} ${styles.buttonDanger}`} onClick={onCancel}><StopCircle size={14} />取消</button> : null}
            {["partial_failed", "failed", "timed_out"].includes(batch.status) ? <button type="button" className={`${styles.button} ${styles.buttonSoft}`} onClick={onRetry}><RefreshCw size={14} />仅重试失败项</button> : null}
            <Link className={styles.backLink} href={`/hmm-evolution/batches/${encodeURIComponent(batch.batch_id)}`}>批次详情 <ChevronRight size={12} /></Link>
          </div>
        ) : null}
      </div>
    </section>
  );
}

function Step({ index, name, state }: { index: string; name: string; state: "idle" | "active" | "done" }) {
  return (
    <div className={`${styles.step} ${state === "done" ? styles.stepDone : ""} ${state === "active" ? styles.stepActive : ""}`}>
      <div className={styles.stepIndex}>{index}</div>
      <div className={styles.stepName}>{name}</div>
    </div>
  );
}

function EvidenceRow({ label, value }: { label: string; value: string }) {
  return <div className={styles.evidenceRow}><span className={styles.evidenceKey}>{label}</span><span className={styles.evidenceValue}>{value}</span></div>;
}

function CandidateTable({ candidates }: { candidates: CandidateRecord[] }) {
  if (candidates.length === 0) return <EmptyState title="候选库为空" detail="通过候选接入先做只读预检，再登记 coefficient artifact。" />;
  return (
    <table className={styles.table}>
      <thead><tr><th>候选</th><th>来源</th><th>覆盖区间</th><th>板块范围</th><th>Artifact SHA</th><th>Lifecycle</th></tr></thead>
      <tbody>
        {candidates.map((candidate) => (
          <tr key={candidate.candidate_id}>
            <td><div className={styles.candidateName}>{candidate.display_name}</div><div className={styles.candidateMeta}>{shortHash(candidate.candidate_id)}</div></td>
            <td>{sourceLabel(candidate.source_type)}</td>
            <td>{candidate.artifact_manifest.coverage.start_date} → {candidate.artifact_manifest.coverage.end_date}</td>
            <td>{candidate.artifact_manifest.coverage.sector_count_min}–{candidate.artifact_manifest.coverage.sector_count_max}</td>
            <td className={styles.hash}>{shortHash(candidate.artifact_manifest.artifact_sha256)}</td>
            <td><StatusBadge status={candidate.lifecycle_status} /></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function CandidateRegistrationPanel({
  form,
  setForm,
  preview,
  error,
  busy,
  onPreview,
  onRegister,
}: {
  form: CandidateForm;
  setForm: (form: CandidateForm) => void;
  preview: CandidatePreview | null;
  error: unknown;
  busy: boolean;
  onPreview: () => void;
  onRegister: () => void;
}) {
  const update = (key: keyof CandidateForm, value: string) => setForm({ ...form, [key]: value });
  return (
    <section className={styles.panel} style={{ marginBottom: 18 }}>
      <div className={styles.panelHeader}>
        <div><h2 className={styles.panelTitle}>候选接入</h2><div className={styles.panelSubtitle}>先只读解析 hash / coverage，再登记到独立研究候选库</div></div>
        <span className={`${styles.tag} ${styles.tagInfo}`}><FileSearch size={13} /> 零生产写入</span>
      </div>
      <div className={styles.panelBody}>
        <div className={styles.formGrid}>
          <Field label="来源类型"><select className={styles.input} value={form.sourceType} onChange={(event) => update("sourceType", event.target.value)}><option value="qe_experiment_coefficients">QE 实验资产</option><option value="existing_snapshot_coefficients">已有 snapshot 系数</option><option value="configured_local_coefficients">配置化本地资产</option></select></Field>
          {form.sourceType === "qe_experiment_coefficients" ? <><Field label="QE Task"><input className={styles.input} value={form.taskId} onChange={(event) => update("taskId", event.target.value)} /></Field><Field label="Loop"><input className={styles.input} value={form.loopName} onChange={(event) => update("loopName", event.target.value)} /></Field><Field label="资产相对路径"><input className={styles.input} value={form.relativePath} onChange={(event) => update("relativePath", event.target.value)} /></Field></> : null}
          {form.sourceType === "existing_snapshot_coefficients" ? <><Field label="Snapshot ID"><input className={styles.input} value={form.snapshotId} onChange={(event) => update("snapshotId", event.target.value)} /></Field><Field label="系数文件名"><input className={styles.input} value={form.artifactName} onChange={(event) => update("artifactName", event.target.value)} /></Field></> : null}
          {form.sourceType === "configured_local_coefficients" ? <><Field label="Root alias"><input className={styles.input} value={form.rootAlias} onChange={(event) => update("rootAlias", event.target.value)} /></Field><Field label="资产相对路径"><input className={styles.input} value={form.relativePath} onChange={(event) => update("relativePath", event.target.value)} /></Field></> : null}
          <Field label="候选名称"><input className={styles.input} value={form.displayName} onChange={(event) => update("displayName", event.target.value)} /></Field>
          <Field label="研究说明" wide><input className={styles.input} value={form.description} onChange={(event) => update("description", event.target.value)} /></Field>
        </div>
        <div className={styles.inlineActions} style={{ marginTop: 14 }}>
          <button type="button" className={styles.button} onClick={onPreview} disabled={busy}><FileSearch size={14} />只读预检</button>
          <button type="button" className={`${styles.button} ${styles.buttonPrimary}`} onClick={onRegister} disabled={busy || !preview}><Plus size={14} />登记候选</button>
        </div>
        {error ? <div style={{ marginTop: 14 }}><VisibleErrorState error={error} title="候选接入失败" onRetry={onPreview} /></div> : null}
        {preview ? <div className={styles.assetSummary} style={{ marginTop: 14 }}><SummaryCell label="Candidate ID" value={shortHash(preview.candidate_id)} /><SummaryCell label="覆盖区间" value={`${preview.manifest.coverage.start_date} → ${preview.manifest.coverage.end_date}`} /><SummaryCell label="日期数" value={String(preview.manifest.coverage.date_count)} /><SummaryCell label="Artifact SHA" value={shortHash(preview.manifest.artifact_sha256)} /></div> : null}
      </div>
    </section>
  );
}

function EvaluationCreatePanel({
  form,
  setForm,
  candidates,
  selectedCandidateIds,
  setSelectedCandidateIds,
  error,
  busy,
  onSubmit,
}: {
  form: EvaluationForm;
  setForm: (form: EvaluationForm) => void;
  candidates: CandidateRecord[];
  selectedCandidateIds: string[];
  setSelectedCandidateIds: (ids: string[]) => void;
  error: unknown;
  busy: boolean;
  onSubmit: () => void;
}) {
  const update = (key: keyof EvaluationForm, value: string) => setForm({ ...form, [key]: value });
  const eligible = candidates.filter((candidate) => candidate.lifecycle_status === "research_only");
  const toggleCandidate = (candidateId: string) => setSelectedCandidateIds(
    selectedCandidateIds.includes(candidateId)
      ? selectedCandidateIds.filter((id) => id !== candidateId)
      : [...selectedCandidateIds, candidateId],
  );
  return (
    <section className={styles.panel} style={{ marginBottom: 18 }}>
      <div className={styles.panelHeader}>
        <div><h2 className={styles.panelTitle}>新建离线评估</h2><div className={styles.panelSubtitle}>同批候选共享只读 pred/label 与共同完成行情输入</div></div>
        <span className={`${styles.tag} ${styles.tagGood}`}><FlaskConical size={13} /> 1–50 个候选</span>
      </div>
      <div className={styles.panelBody}>
        <div className={styles.formGrid}>
          <Field label="Base loop ref"><input className={styles.input} placeholder="qe_task/Loop8" value={form.baseLoopRef} onChange={(event) => update("baseLoopRef", event.target.value)} /></Field>
          <Field label="窗口开始"><input type="date" className={styles.input} value={form.windowStart} onChange={(event) => update("windowStart", event.target.value)} /></Field>
          <Field label="窗口结束"><input type="date" className={styles.input} value={form.windowEnd} onChange={(event) => update("windowEnd", event.target.value)} /></Field>
          <Field label="As-of policy"><select className={styles.input} value={form.asOfPolicy} onChange={(event) => update("asOfPolicy", event.target.value)}><option value="latest_common_completed">最新共同完成交易日</option><option value="explicit">显式交易日</option></select></Field>
          {form.asOfPolicy === "explicit" ? <Field label="显式 as-of"><input type="date" className={styles.input} value={form.requestedDate} onChange={(event) => update("requestedDate", event.target.value)} /></Field> : null}
          <Field label="标签 horizon"><input type="number" min="1" max="30" className={styles.input} value={form.labelHorizonDays} onChange={(event) => update("labelHorizonDays", event.target.value)} /></Field>
          <Field label="TopK"><input type="number" min="1" className={styles.input} value={form.topk} onChange={(event) => update("topk", event.target.value)} /></Field>
          <Field label="DB 10D 收益"><select className={styles.input} value={form.marketMode} onChange={(event) => update("marketMode", event.target.value)}><option value="required">必需</option><option value="disabled">禁用并记录证据</option></select></Field>
          <div className={`${styles.field} ${styles.fieldFull}`}><span className={styles.label}>选择候选</span><div className={styles.checkboxGrid}>{eligible.length === 0 ? <span className={styles.muted}>没有 research_only 候选。</span> : eligible.map((candidate) => <label key={candidate.candidate_id} className={styles.checkboxItem}><input type="checkbox" checked={selectedCandidateIds.includes(candidate.candidate_id)} onChange={() => toggleCandidate(candidate.candidate_id)} /><span><strong>{candidate.display_name}</strong><div className={styles.candidateMeta}>{sourceLabel(candidate.source_type)} · {shortHash(candidate.candidate_id)}</div></span></label>)}</div></div>
        </div>
        <div className={styles.notice} style={{ marginTop: 14 }}><ShieldCheck size={15} /><span>本操作只写 hmm_evolution.* 评估状态；不会重跑、终止或修改 QE 实验，也不会生成生产 snapshot。</span></div>
        <div className={styles.inlineActions} style={{ marginTop: 14 }}><button type="button" className={`${styles.button} ${styles.buttonPrimary}`} disabled={busy} onClick={onSubmit}>{busy ? "正在固化输入…" : "创建批次"}</button></div>
        {error ? <div style={{ marginTop: 14 }}><VisibleErrorState error={error} title="评估创建失败" onRetry={onSubmit} /></div> : null}
      </div>
    </section>
  );
}

function Field({ label, children, wide = false }: { label: string; children: ReactNode; wide?: boolean }) {
  return <label className={`${styles.field} ${wide ? styles.fieldWide : ""}`}><span className={styles.label}>{label}</span>{children}</label>;
}

function SummaryCell({ label, value }: { label: string; value: string }) {
  return <div className={styles.summaryCell}><div className={styles.summaryLabel}>{label}</div><div className={styles.summaryValue}>{value}</div></div>;
}

function buildEvaluationSpec(form: EvaluationForm): EvaluationSpecPayload {
  const labelHorizonDays = Number(form.labelHorizonDays);
  const topk = Number(form.topk);
  if (!form.baseLoopRef.trim() || !form.windowStart || !form.windowEnd) throw new Error("Base loop 和评估窗口不能为空。 ");
  if (!Number.isInteger(labelHorizonDays) || labelHorizonDays < 1 || labelHorizonDays > 30) throw new Error("标签 horizon 必须是 1–30 的整数。 ");
  if (!Number.isInteger(topk) || topk < 1) throw new Error("TopK 必须是正整数。 ");
  if (form.asOfPolicy === "explicit" && !form.requestedDate) throw new Error("显式 as-of policy 必须选择日期。 ");
  return {
    schema_version: "hmm_evaluation_spec_v1",
    base_loop_ref: form.baseLoopRef.trim(),
    window_start: form.windowStart,
    window_end: form.windowEnd,
    as_of: { policy: form.asOfPolicy, requested_date: form.asOfPolicy === "explicit" ? form.requestedDate : null },
    label_horizon_days: labelHorizonDays,
    universe: { type: "prediction_artifact_all" },
    topk,
    date_coverage_policy: "batch_common_intersection_with_evidence",
    missing_sector_policy: "neutral_with_evidence",
    market_forward_return: { mode: form.marketMode, horizon_trading_days: 10 },
    sort_policy: "score_desc_symbol_asc_v1",
    metric_version: "hmm_replacement_metrics_v1",
    recommendation_version: "hmm_recommendation_v1",
  };
}

function buildBatchEvidence(batch: BatchDetail | null) {
  if (!batch) return [{ title: "批次证据", rows: [{ label: "状态", value: "尚未选择批次" }, { label: "下一步", value: "创建或选择真实批次" }] }];
  const first = batch.items[0];
  const degraded = batch.items.filter((item) => item.evidence_quality === "degraded").length;
  return [
    { title: "输入身份", rows: [{ label: "Batch ID", value: batch.batch_id }, { label: "候选数", value: String(batch.candidate_count) }, { label: "评估窗口", value: first ? `${first.window_start} → ${first.window_end}` : "未生成 item" }, { label: "标签 horizon", value: first ? `${first.label_horizon_days} 个交易日` : "未知" }] },
    { title: "数据水位与质量", rows: [{ label: "Resolved as-of", value: first?.as_of_date || "未知" }, { label: "完整证据", value: String(batch.items.filter((item) => item.evidence_quality === "complete").length) }, { label: "降级证据", value: String(degraded) }, { label: "失败/超时", value: String(batch.failed_count + batch.timed_out_count) }] },
    { title: "计算与推荐", rows: [{ label: "推荐版本", value: batch.recommendation_version }, { label: "Top-3", value: String(batch.items.filter((item) => item.is_top3).length) }, { label: "研究语义", value: "推荐供 QE 终审；无淘汰阈值" }, { label: "生产副作用", value: "无" }] },
  ];
}

function formatMedianDuration(batches: BatchSummary[]): string {
  const durations = batches.map((batch) => batch.started_at && batch.completed_at ? new Date(batch.completed_at).getTime() - new Date(batch.started_at).getTime() : null).filter((value): value is number => value !== null && value >= 0).sort((a, b) => a - b);
  if (durations.length === 0) return "—";
  const middle = Math.floor(durations.length / 2);
  const median = durations.length % 2 ? durations[middle] : (durations[middle - 1] + durations[middle]) / 2;
  const minutes = Math.floor(median / 60_000);
  const seconds = Math.floor((median % 60_000) / 1_000);
  return `${minutes}m ${seconds}s`;
}

function formatNumber(value: number | null, digits = 2): string { return value === null ? "未排名" : value.toFixed(digits); }
function formatPercent(value: number | null): string { return value === null ? "未计算" : `${value >= 0 ? "+" : ""}${(value * 100).toFixed(2)}%`; }
function metricTone(value: number | null): string { return value === null ? styles.muted : value >= 0 ? styles.positive : styles.negative; }
function shortHash(value: string): string { return value.length <= 16 ? value : `${value.slice(0, 8)}…${value.slice(-6)}`; }
function formatDateTime(value: string | null): string { return value ? new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : "未记录"; }
function formatBytes(value: number): string { if (value < 1024) return `${value} B`; if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KB`; return `${(value / 1024 ** 2).toFixed(1)} MB`; }
function sourceLabel(source: CandidateSourceType): string { return source === "qe_experiment_coefficients" ? "QE 实验" : source === "existing_snapshot_coefficients" ? "已有 snapshot" : "配置化本地"; }
