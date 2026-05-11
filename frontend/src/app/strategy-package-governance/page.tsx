"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ConfirmAction from "@/components/paper-v2/ConfirmAction";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import {
  API_BASE,
  EVIDENCE_KEYS,
  EVIDENCE_LABELS,
  type EvidenceCheck,
  type EvidenceKey,
  type GovernanceEligibility,
  type StrategyPackageSummary,
  governanceApi,
} from "@/lib/strategy-package-governance/api";

const ENABLE_CONFIRM = "ENABLE_PAPER_CONFIRM";

function statusToTone(status: EvidenceCheck["status"]): "success" | "warning" | "danger" | "info" | "neutral" {
  switch (status) {
    case "pass":
      return "success";
    case "fail":
      return "danger";
    case "missing":
      return "warning";
    case "skipped":
      return "neutral";
    default:
      return "info";
  }
}

function statusLabel(status: EvidenceCheck["status"]): string {
  switch (status) {
    case "pass":
      return "通过";
    case "fail":
      return "未通过";
    case "missing":
      return "缺证据";
    case "skipped":
      return "跳过";
    default:
      return "待评估";
  }
}

function shortDate(value?: string | null): string {
  if (!value) return "-";
  return value.replace("T", " ").slice(0, 19);
}

function EvidenceTile({ keyName, check }: { keyName: EvidenceKey; check: EvidenceCheck }) {
  const tone = statusToTone(check.status);
  return (
    <div className="pv2-readable-panel" data-testid={`evidence-${keyName}`}>
      <div className="pv2-readable-table">
        <div className="pv2-readable-row">
          <div className="pv2-readable-key">{EVIDENCE_LABELS[keyName]}</div>
          <div className="pv2-readable-value">
            <StatusBadge status={statusLabel(check.status)} />
          </div>
        </div>
        {check.reason ? (
          <div className="pv2-readable-row">
            <div className="pv2-readable-key">原因</div>
            <div className="pv2-readable-value" data-testid={`evidence-${keyName}-reason`}>{check.reason}</div>
          </div>
        ) : null}
      </div>
      <div className="pv2-help" data-testid={`evidence-${keyName}-tone`}>{`tone=${tone}`}</div>
    </div>
  );
}

export default function GovernancePage() {
  const [packages, setPackages] = useState<StrategyPackageSummary[]>([]);
  const [filterStatus, setFilterStatus] = useState<string>("all");
  const [search, setSearch] = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [eligibility, setEligibility] = useState<GovernanceEligibility | null>(null);
  const [loading, setLoading] = useState(false);
  const [eligibilityBusy, setEligibilityBusy] = useState(false);
  const [enableBusy, setEnableBusy] = useState(false);
  const [enableMessage, setEnableMessage] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);

  const loadPackages = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const items = await governanceApi.listPackages();
      setPackages(items);
      if (!selectedId && items.length) {
        setSelectedId(items[0].package_id);
      }
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [selectedId]);

  const loadEligibility = useCallback(async (packageId: string) => {
    setEligibilityBusy(true);
    setError(null);
    try {
      const data = await governanceApi.eligibility(packageId);
      setEligibility(data);
    } catch (err) {
      setEligibility(null);
      setError(err);
    } finally {
      setEligibilityBusy(false);
    }
  }, []);

  useEffect(() => {
    void loadPackages();
  }, [loadPackages]);

  useEffect(() => {
    if (selectedId) void loadEligibility(selectedId);
  }, [selectedId, loadEligibility]);

  const visiblePackages = useMemo(() => {
    return packages.filter((pkg) => {
      if (filterStatus !== "all" && (pkg.status || "").toLowerCase() !== filterStatus) return false;
      if (search && !(pkg.package_id + (pkg.package_name || "")).toLowerCase().includes(search.toLowerCase())) {
        return false;
      }
      return true;
    });
  }, [packages, filterStatus, search]);

  const paperReadyCount = useMemo(
    () => packages.filter((pkg) => (pkg.paper_status || "").toLowerCase() === "enabled").length,
    [packages],
  );

  async function handleEnablePaper() {
    if (!selectedId) return;
    setEnableBusy(true);
    setEnableMessage(null);
    setError(null);
    try {
      const result = await governanceApi.enablePaper(selectedId);
      setEnableMessage(result.ok ? "Paper 已启用" : "启用未确认成功");
      await loadPackages();
      await loadEligibility(selectedId);
    } catch (err) {
      setError(err);
    } finally {
      setEnableBusy(false);
    }
  }

  const enableDisabled = !selectedId || !eligibility?.paper_ready || enableBusy;

  return (
    <main className="pv2-shell">
      <section className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">Strategy Package Governance</div>
            <h1>策略包治理</h1>
            <p>
              展示策略包的 governance_eligibility（5 项 evidence）与 paper_ready 状态；启用 paper 需通过严格 gate 与二次确认。
              <span className="pv2-mono"> {API_BASE}/strategy-packages </span> API。
            </p>
          </div>
          <div className="pv2-row-actions">
            <button
              className="pv2-button-primary"
              type="button"
              onClick={() => void loadPackages()}
              disabled={loading}
              data-testid="refresh-packages"
            >
              {loading ? "刷新中" : "刷新列表"}
            </button>
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="治理 API 调用失败" />

      <div className="pv2-grid pv2-grid-3">
        <MetricCard label="包总数" value={String(packages.length)} hint="dev DB 当前条数" />
        <MetricCard label="paper enabled" value={String(paperReadyCount)} hint="paper_status=enabled" tone="success" />
        <MetricCard
          label="当前选中"
          value={selectedId ? selectedId.slice(0, 12) : "-"}
          hint={eligibility ? `paper_ready=${String(eligibility.paper_ready)}` : "尚未加载"}
          tone={eligibility?.paper_ready ? "success" : "warning"}
        />
      </div>

      <SectionCard title="策略包列表" eyebrow="select to inspect governance evidence">
        <div className="pv2-form-grid">
          <label className="pv2-field">
            <span>状态过滤</span>
            <select
              className="pv2-select"
              value={filterStatus}
              onChange={(event) => setFilterStatus(event.target.value)}
              data-testid="status-filter"
            >
              <option value="all">全部状态</option>
              <option value="draft">draft</option>
              <option value="active">active</option>
              <option value="retired">retired</option>
            </select>
          </label>
          <label className="pv2-field">
            <span>搜索</span>
            <input
              className="pv2-input"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="package_id 或 name"
              data-testid="search-input"
            />
          </label>
        </div>
        <PaperTable
          rows={visiblePackages}
          empty="暂无符合条件的策略包"
          columns={[
            {
              key: "select",
              header: "选择",
              render: (row) => (
                <button
                  type="button"
                  className={selectedId === row.package_id ? "pv2-button" : "pv2-button-ghost"}
                  onClick={() => setSelectedId(row.package_id)}
                  data-testid={`select-${row.package_id}`}
                >
                  {selectedId === row.package_id ? "已选" : "查看"}
                </button>
              ),
            },
            {
              key: "package",
              header: "Package",
              render: (row) => (
                <>
                  <div className="pv2-mono">{row.package_id}</div>
                  <div className="pv2-muted">{row.package_name || "-"}</div>
                </>
              ),
            },
            {
              key: "status",
              header: "状态",
              render: (row) => (
                <>
                  <StatusBadge status={row.status} />
                  <div className="pv2-muted">paper={row.paper_status || "-"}</div>
                </>
              ),
            },
            {
              key: "source",
              header: "来源",
              render: (row) => (
                <>
                  <div>{row.source_system || "-"}</div>
                  <div className="pv2-muted pv2-mono">{row.source_id || "-"}</div>
                </>
              ),
            },
            {
              key: "time",
              header: "时间",
              render: (row) => (
                <>
                  <div>{shortDate(row.created_at)}</div>
                  <div className="pv2-muted">{shortDate(row.updated_at)}</div>
                </>
              ),
            },
          ]}
        />
      </SectionCard>

      <SectionCard title="Governance Evidence (5 项)" eyebrow="manifest / retest / validation / asset / runtime">
        {!selectedId ? (
          <div className="pv2-help" data-testid="no-selection">请先在列表中选择一个策略包。</div>
        ) : eligibilityBusy ? (
          <div className="pv2-help" data-testid="evidence-loading">加载 eligibility 中...</div>
        ) : !eligibility ? (
          <div className="pv2-help" data-testid="evidence-empty">尚未获取 eligibility 数据。</div>
        ) : (
          <>
            <div className="pv2-grid pv2-grid-3">
              {EVIDENCE_KEYS.map((key) => (
                <EvidenceTile key={key} keyName={key} check={eligibility[key]} />
              ))}
            </div>
            <div className="pv2-readable-panel" style={{ marginTop: 12 }} data-testid="paper-ready-summary">
              <div className="pv2-readable-table">
                <div className="pv2-readable-row">
                  <div className="pv2-readable-key">paper_ready</div>
                  <div className="pv2-readable-value">
                    <StatusBadge status={eligibility.paper_ready ? "READY" : "NOT_READY"} />
                  </div>
                </div>
                {!eligibility.paper_ready && eligibility.paper_ready_block_reason ? (
                  <div className="pv2-readable-row">
                    <div className="pv2-readable-key">不通过原因</div>
                    <div className="pv2-readable-value" data-testid="block-reason">
                      {eligibility.paper_ready_block_reason}
                    </div>
                  </div>
                ) : null}
                <div className="pv2-readable-row">
                  <div className="pv2-readable-key">评估时间</div>
                  <div className="pv2-readable-value">{shortDate(eligibility.evaluated_at)}</div>
                </div>
              </div>
            </div>
          </>
        )}
      </SectionCard>

      <SectionCard title="启用 Paper" eyebrow="strict gate + two-step confirm">
        <div className="pv2-help" data-testid="enable-help">
          仅当 paper_ready=true 时按钮可触发；点击后需输入 <code>{ENABLE_CONFIRM}</code> 二次确认。
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 12 }}>
          <ConfirmAction
            label={enableBusy ? "启用中..." : "启用 Paper"}
            confirmText={ENABLE_CONFIRM}
            danger
            disabled={enableDisabled}
            onConfirm={handleEnablePaper}
            testId="enable-paper-action"
          />
        </div>
        {enableMessage ? (
          <div className="pv2-help" data-testid="enable-message">{enableMessage}</div>
        ) : null}
      </SectionCard>
    </main>
  );
}
