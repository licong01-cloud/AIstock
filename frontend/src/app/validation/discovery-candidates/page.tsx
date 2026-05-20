"use client";

import { Suspense, useCallback, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";

import {
  type DiscoveryFilters,
  EvidenceDrawer,
  IssueCandidateTable,
  display,
} from "@/components/validation/discovery/ActiveDiscoveryComponents";
import { candidateEvidenceId, emptyPage, errorMessage, loadDiscoveryEvidence } from "@/components/validation/discovery/pageUtils";
import {
  type ValidationDiscoveryCandidate,
  type ValidationDiscoveryEvidenceManifest,
  type ValidationPage,
  validationApi,
} from "@/lib/validation/api";

const PAGE_SIZE = 20;

export default function DiscoveryCandidatesPage() {
  return (
    <Suspense fallback={<div className="pv2-notice pv2-notice-info">正在加载候选 Issue 页面...</div>}>
      <DiscoveryCandidatesContent />
    </Suspense>
  );
}

function DiscoveryCandidatesContent() {
  const searchParams = useSearchParams();
  const [filters, setFilters] = useState<DiscoveryFilters>({
    module: searchParams.get("module") || "",
    status: searchParams.get("review_status") || "",
  });
  const [page, setPage] = useState(1);
  const [candidates, setCandidates] = useState<ValidationPage<ValidationDiscoveryCandidate>>(emptyPage(PAGE_SIZE));
  const [selected, setSelected] = useState<ValidationDiscoveryCandidate | null>(null);
  const [evidence, setEvidence] = useState<ValidationDiscoveryEvidenceManifest | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [evidenceError, setEvidenceError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const payload = await validationApi.discoveryCandidates({
        page,
        page_size: PAGE_SIZE,
        search: filters.search,
        severity: filters.severity,
        module: filters.module,
        review_status: filters.status,
        source: filters.source,
      });
      setCandidates(payload);
    } catch (err) {
      setError(errorMessage(err));
      setCandidates(emptyPage(PAGE_SIZE));
    } finally {
      setLoading(false);
    }
  }, [filters, page]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const openCandidate = async (candidate: ValidationDiscoveryCandidate) => {
    setSelected(candidate);
    setDrawerOpen(true);
    setEvidence(null);
    setEvidenceError(null);
    try {
      const detail = await validationApi.discoveryCandidate(candidate.candidate_id);
      setSelected(detail);
      setEvidence(await loadDiscoveryEvidence(candidateEvidenceId(detail)));
    } catch (err) {
      setEvidenceError(errorMessage(err));
    }
  };

  const review = async (candidate: ValidationDiscoveryCandidate, action: string) => {
    const reviewer = window.prompt("请输入 reviewer（P0/P1 必填）", "codex-app");
    if (!reviewer) return;
    try {
      const result = await validationApi.reviewDiscoveryCandidate(candidate.candidate_id, {
        action,
        reviewer,
        comment: `${action} from active discovery UI`,
        evidence_checklist: candidate.evidence_types || [],
      });
      setActionMessage(`审核动作已记录：${display(result)}`);
      await refresh();
    } catch (err) {
      setActionMessage(`审核失败：${errorMessage(err)}`);
    }
  };

  const promote = async (candidate: ValidationDiscoveryCandidate) => {
    const reviewer = window.prompt(`晋级 ${candidate.candidate_id} 需要 reviewer`, "codex-app");
    if (!reviewer) return;
    const confirmPromote = window.prompt("请输入 candidate_id 进行确认", candidate.candidate_id);
    if (confirmPromote !== candidate.candidate_id) {
      setActionMessage("确认文本不匹配，已取消晋级。");
      return;
    }
    try {
      const result = await validationApi.promoteDiscoveryCandidate(candidate.candidate_id, {
        confirm_promote: confirmPromote,
        reviewer,
        comment: "Promotion requested from active discovery UI; GitHub sync is enforced by MCP workflow.",
        evidence_checklist: candidate.evidence_types || [],
      });
      setActionMessage(`晋级请求完成：${display(result)}`);
      await refresh();
    } catch (err) {
      setActionMessage(`晋级失败：${errorMessage(err)}`);
    }
  };

  return (
    <>
      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-eyebrow">Candidate Review</div>
            <h2>候选 Issue 审核与 GitHub 同步门禁</h2>
            <p className="pv2-muted">表格支持搜索、排序、筛选、分页、详情抽屉；P0/P1 晋级必须输入 reviewer、证据 checklist 和确认文本。</p>
          </div>
          <button className="pv2-button-primary" onClick={refresh} disabled={loading} type="button">{loading ? "刷新中" : "刷新候选"}</button>
        </div>
        {error ? <div className="pv2-notice pv2-notice-warning">{error}</div> : null}
        {actionMessage ? <div className="pv2-notice pv2-notice-info">{actionMessage}</div> : null}
      </section>
      <IssueCandidateTable
        candidates={candidates}
        page={page}
        filters={filters}
        onFilterChange={(next) => {
          setFilters(next);
          setPage(1);
        }}
        onPageChange={setPage}
        onOpenCandidate={openCandidate}
        onOpenEvidence={openCandidate}
        onReview={review}
        onPromote={promote}
      />
      <EvidenceDrawer open={drawerOpen} title={selected?.title} candidate={selected} manifest={evidence} loading={drawerOpen && !evidence && !evidenceError} error={evidenceError} onClose={() => setDrawerOpen(false)} />
    </>
  );
}
