"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";
import ConfirmAction from "@/components/paper-v2/ConfirmAction";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import ReadinessFailureCard from "@/components/paper-v2/ReadinessFailureCard";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { paperV2Api } from "@/lib/paper-v2/api";
import { dataSourceLabel, shortHash, todayIso } from "@/lib/paper-v2/format";
import type {
  Activation,
  ExecutionPolicy,
  JsonObject,
  PaperPortfolio,
  PaperRun,
  PaperSchedulerStatus,
  PaperSession,
  PaperSessionCapabilities,
  PaperSessionMode,
  PaperSessionProgress,
  ReadinessResult,
  ReplayResult,
  RuntimeConfigActivation,
  RuntimeProfile,
  RuntimeProfileVersion,
} from "@/lib/paper-v2/types";

function splitList(text: string): string[] {
  return text
    .split(/[\n,，]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function asDate(value: unknown): string {
  if (!value) return "-";
  return String(value).slice(0, 19).replace("T", " ");
}

function executionPolicyId(policy: ExecutionPolicy): string {
  return String(policy.policy_id || policy.validated_execution_policy_id || "");
}

const LIVE_TICK_SETTLED_STATUSES = ["LIVE_WAITING_FOR_BAR", "LIVE_WAITING_NEXT_TRADING_DAY", "SUCCEEDED", "FAILED", "STOPPED"];
const TERMINAL_SESSION_STATUSES = ["SUCCEEDED", "FAILED", "STOPPED"];
const SESSION_MODE_OPTIONS: Array<{ value: PaperSessionMode; label: string; description: string }> = [
  { value: "REPLAY_ONLY", label: "仅历史追赶", description: "追赶到当前最新已入库交易日后停止，不自动切实时。" },
  { value: "CATCHUP_THEN_LIVE", label: "历史追赶后自动实时", description: "追赶历史分钟线，开盘后由调度器切入 TDX 实时运行。" },
  { value: "LIVE_ONLY", label: "完全实时运行", description: "不做历史追赶，直接使用 TDX 实时分钟线。" },
];

function isActiveSession(session: PaperSession | null | undefined): boolean {
  return Boolean(session && !TERMINAL_SESSION_STATUSES.includes(String(session.status || "").toUpperCase()));
}

function CapabilityErrorList({ node }: { node: { can_start: boolean; errors: JsonObject[] } | undefined }) {
  if (!node) return <div className="pv2-muted">尚未加载能力诊断。</div>;
  const errors = (node.errors || []) as JsonObject[];
  if (!errors.length) return <div className="pv2-muted">{node.can_start ? "可启动。" : "已禁用，但未返回具体错误。"}</div>;
  return (
    <ul className="pv2-readiness-context" style={{ paddingLeft: 16 }}>
      {errors.map((err, index) => {
        const code = String(err.error_code || err.code || "ERROR");
        const message = String(err.message || err.detail || "");
        return (
          <li key={`${code}-${index}`}>
            <span className="pv2-badge pv2-badge-danger" style={{ marginRight: 6 }} title={code}>{code}</span>
            <span>{message}</span>
          </li>
        );
      })}
    </ul>
  );
}

export default function PaperV2RunConsolePage() {
  const params = useParams<{ portfolioId: string }>();
  const portfolioId = String(params.portfolioId || "");
  const [portfolio, setPortfolio] = useState<PaperPortfolio | null>(null);
  const [runs, setRuns] = useState<PaperRun[]>([]);
  const [sessions, setSessions] = useState<PaperSession[]>([]);
  const [sessionProgress, setSessionProgress] = useState<PaperSessionProgress | null>(null);
  const [schedulerStatus, setSchedulerStatus] = useState<PaperSchedulerStatus | null>(null);
  const [events, setEvents] = useState<JsonObject[]>([]);
  const [errors, setErrors] = useState<JsonObject[]>([]);
  const [policies, setPolicies] = useState<ExecutionPolicy[]>([]);
  const [activations, setActivations] = useState<Activation[]>([]);
  const [capabilities, setCapabilities] = useState<PaperSessionCapabilities | null>(null);
  const [runtimeProfiles, setRuntimeProfiles] = useState<RuntimeProfile[]>([]);
  const [runtimeVersions, setRuntimeVersions] = useState<RuntimeProfileVersion[]>([]);
  const [runtimeActivations, setRuntimeActivations] = useState<RuntimeConfigActivation[]>([]);
  const [configAudit, setConfigAudit] = useState<JsonObject[]>([]);
  const [runtimeProfileName, setRuntimeProfileName] = useState("开盘前运行配置");
  const [runtimeProfileId, setRuntimeProfileId] = useState("");
  const [runtimeVersionId, setRuntimeVersionId] = useState("");
  const [runtimeActivationDate, setRuntimeActivationDate] = useState(todayIso());
  const [runtimeActivationReason, setRuntimeActivationReason] = useState("开盘前调整运行配置");
  const [replaceRuntimeActivation, setReplaceRuntimeActivation] = useState(false);
  const [tradeDate, setTradeDate] = useState(todayIso());
  const [runtimeTopK, setRuntimeTopK] = useState(20);
  const [runtimeExcludeSuspended, setRuntimeExcludeSuspended] = useState(true);
  const [runtimeIndustryBlacklist, setRuntimeIndustryBlacklist] = useState("");
  const [runtimeHmmEnabled, setRuntimeHmmEnabled] = useState(false);
  const [runtimeHmmSnapshotId, setRuntimeHmmSnapshotId] = useState("");
  const [runtimeHmmPreset, setRuntimeHmmPreset] = useState("preset_A");
  const [runtimeHmmCoefficientsPath, setRuntimeHmmCoefficientsPath] = useState("");
  const [readiness, setReadiness] = useState<ReadinessResult | null>(null);
  const [runResult, setRunResult] = useState<JsonObject | null>(null);
  const [replayResult, setReplayResult] = useState<ReplayResult | null>(null);
  const [replayStart, setReplayStart] = useState(todayIso());
  const [replayEnd, setReplayEnd] = useState(todayIso());
  const [sessionMode, setSessionMode] = useState<PaperSessionMode>("REPLAY_ONLY");
  const [switchMode, setSwitchMode] = useState<PaperSessionMode>("CATCHUP_THEN_LIVE");
  const [switchStart, setSwitchStart] = useState(todayIso());
  const [switchEnd, setSwitchEnd] = useState(todayIso());
  const [liveStartDate, setLiveStartDate] = useState(todayIso());
  const [schedulerInterval, setSchedulerInterval] = useState(30);
  const [policyId, setPolicyId] = useState("");
  const [activationDate, setActivationDate] = useState(todayIso());
  const [activationReason, setActivationReason] = useState("每日开盘前验证策略更新");
  const [replaceExisting, setReplaceExisting] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const readinessPassed = useMemo(
    () => Boolean(readiness && readiness.trade_date === tradeDate && readiness.checks.every((check) => check.status === "passed")),
    [readiness, tradeDate],
  );
  const latestSession = useMemo(() => sessions[0] || null, [sessions]);
  const activeSession = useMemo(() => sessions.find((item) => isActiveSession(item)) || null, [sessions]);
  const replayCapability = capabilities?.modes?.REPLAY_ONLY;
  const liveCapability = capabilities?.modes?.LIVE_ONLY;
  const catchupCapability = capabilities?.modes?.CATCHUP_THEN_LIVE;
  const replayBlocked = Boolean(replayCapability && !replayCapability.can_start);
  const liveBlocked = Boolean(liveCapability && !liveCapability.can_start);
  const catchupBlocked = Boolean(catchupCapability && !catchupCapability.can_start);
  const sessionModeBlocked = sessionMode === "REPLAY_ONLY" ? replayBlocked : sessionMode === "CATCHUP_THEN_LIVE" ? catchupBlocked : liveBlocked;
  const switchModeBlocked = switchMode === "REPLAY_ONLY" ? replayBlocked : switchMode === "CATCHUP_THEN_LIVE" ? catchupBlocked : liveBlocked;
  const runtimeConfig = useMemo<JsonObject>(() => {
    const safeTopK = Number.isFinite(runtimeTopK) ? Math.min(50, Math.max(1, Math.trunc(runtimeTopK))) : 20;
    const hmm: JsonObject = {
      enabled: runtimeHmmEnabled,
      model_snapshot_id: runtimeHmmEnabled ? runtimeHmmSnapshotId || null : null,
      signal_preset: runtimeHmmEnabled ? runtimeHmmPreset || null : null,
    };
    if (runtimeHmmEnabled && runtimeHmmCoefficientsPath.trim()) {
      hmm.coefficients_path = runtimeHmmCoefficientsPath.trim();
    }
    return {
      paper_v2_session: { signal_data_source: "DB_HISTORICAL" },
      selection_artifact_config: { auto_generate: true, inference_backend: "wsl" },
      runtime_profile: {
        selection: { top_k: safeTopK },
        tradability: { exclude_suspended: runtimeExcludeSuspended },
        industry_blacklist: splitList(runtimeIndustryBlacklist),
        hmm,
      },
    };
  }, [runtimeExcludeSuspended, runtimeHmmCoefficientsPath, runtimeHmmEnabled, runtimeHmmPreset, runtimeHmmSnapshotId, runtimeIndustryBlacklist, runtimeTopK]);
  const runtimeProfileConfig = useMemo<JsonObject>(() => {
    const { paper_v2_session: _session, paper_v2_replay: _replay, ...profile } = runtimeConfig;
    return profile;
  }, [runtimeConfig]);
  const sessionRuntimeConfig = useCallback((manualTickOnly = false): JsonObject => ({
    ...runtimeConfig,
    paper_v2_session: {
      ...((runtimeConfig.paper_v2_session as JsonObject | undefined) || {}),
      manual_tick_only: manualTickOnly,
    },
  }), [runtimeConfig]);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [
        portfolioRow,
        runRows,
        sessionRows,
        schedulerRow,
        eventRows,
        errorRows,
        policyRows,
        activationRows,
        capabilityRow,
        runtimeProfileRows,
        runtimeActivationRows,
        auditRows,
      ] = await Promise.all([
        paperV2Api.getPortfolio(portfolioId),
        paperV2Api.runs(portfolioId),
        paperV2Api.listSessions(portfolioId),
        paperV2Api.schedulerStatus(),
        paperV2Api.runEvents(portfolioId),
        paperV2Api.errors(portfolioId),
        paperV2Api.executionPolicies(portfolioId),
        paperV2Api.activations(portfolioId),
        paperV2Api.sessionCapabilities(portfolioId),
        paperV2Api.runtimeProfiles(portfolioId),
        paperV2Api.runtimeConfigActivations(portfolioId),
        paperV2Api.configChangeAudit(portfolioId),
      ]);
      setPortfolio(portfolioRow);
      setRuns(runRows);
      setSessions(sessionRows);
      setSchedulerStatus(schedulerRow);
      setEvents(eventRows);
      setErrors(errorRows);
      setPolicies(policyRows);
      setActivations(activationRows);
      setCapabilities(capabilityRow);
      setRuntimeProfiles(runtimeProfileRows);
      setRuntimeActivations(runtimeActivationRows);
      setConfigAudit(auditRows);
      if (!policyId) {
        const defaultPolicy = policyRows.find((item) => item.is_portfolio_default) || policyRows.find((item) => item.paper_enabled);
        setPolicyId(defaultPolicy ? executionPolicyId(defaultPolicy) : "");
      }
      const nextProfileId = runtimeProfileId || runtimeProfileRows[0]?.profile_id || "";
      if (!runtimeProfileId) setRuntimeProfileId(nextProfileId);
      if (!runtimeActivationDate) setRuntimeActivationDate(todayIso());
    } catch (exc) {
      setError(exc);
    }
  }, [portfolioId, policyId, runtimeActivationDate, runtimeProfileId]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    if (!runtimeProfileId) {
      setRuntimeVersions([]);
      setRuntimeVersionId("");
      return;
    }
    let alive = true;
    paperV2Api.runtimeProfileVersions(portfolioId, runtimeProfileId).then((rows) => {
      if (!alive) return;
      setRuntimeVersions(rows);
      if (!rows.find((item) => item.profile_version_id === runtimeVersionId)) {
        setRuntimeVersionId(rows[0]?.profile_version_id || "");
      }
    }).catch((exc) => {
      if (alive) setError(exc);
    });
    return () => { alive = false; };
  }, [portfolioId, runtimeProfileId, runtimeVersionId]);
  useEffect(() => {
    setReadiness(null);
    setRunResult(null);
  }, [tradeDate, runtimeConfig]);

  async function runReadiness() {
    setBusy(true);
    setError(null);
    setReadiness(null);
    try {
      const result = await paperV2Api.readiness(portfolioId, { trade_date: tradeDate, runtime_config: runtimeConfig });
      setReadiness(result);
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function runDay() {
    setBusy(true);
    setError(null);
    setRunResult(null);
    try {
      const result = await paperV2Api.runDay(portfolioId, { trade_date: tradeDate, runtime_config: runtimeConfig });
      setRunResult(result);
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function replay(rerunPolicy: "reject_existing" | "reset_portfolio") {
    setBusy(true);
    setError(null);
    setReplayResult(null);
    setSessionProgress(null);
    try {
      const isReplayOnly = sessionMode === "REPLAY_ONLY";
      const isCatchupThenLive = sessionMode === "CATCHUP_THEN_LIVE";
      const session = await paperV2Api.createSession(portfolioId, {
        mode: sessionMode,
        start_date: sessionMode === "LIVE_ONLY" ? liveStartDate : replayStart,
        end_date: isReplayOnly || isCatchupThenLive ? replayEnd : null,
        historical_data_source: isReplayOnly || isCatchupThenLive ? "DB_HISTORICAL" : null,
        live_data_source: isCatchupThenLive || sessionMode === "LIVE_ONLY" ? "TDX_REALTIME" : null,
        runtime_config: sessionRuntimeConfig(false),
        rerun_policy: rerunPolicy,
        auto_switch_to_live: false,
        confirm_reset: rerunPolicy === "reset_portfolio",
        confirm_text: rerunPolicy === "reset_portfolio" ? portfolioId : null,
        created_by: "paper_v2_ui",
      });
      setSessionProgress({ session, day_count: 0, events: [] });
      setSessionProgress(await paperV2Api.tickSessionAndWait(
        session.session_id,
        {},
        sessionMode !== "REPLAY_ONLY"
          ? { timeoutMs: 600_000, pollMs: 2_000, settleStatuses: LIVE_TICK_SETTLED_STATUSES }
          : { timeoutMs: 600_000, pollMs: 2_000 },
      ));
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function startLiveSession() {
    setBusy(true);
    setError(null);
    setSessionProgress(null);
    try {
      const session = await paperV2Api.createSession(portfolioId, {
        mode: "LIVE_ONLY",
        start_date: liveStartDate,
        live_data_source: "TDX_REALTIME",
        runtime_config: sessionRuntimeConfig(false),
        rerun_policy: "reject_existing",
        created_by: "paper_v2_ui",
      });
      setSessionProgress({ session, day_count: 0, events: [] });
      setSessionProgress(await paperV2Api.tickSessionAndWait(
        session.session_id,
        {},
        { timeoutMs: 180_000, pollMs: 2_000, settleStatuses: LIVE_TICK_SETTLED_STATUSES },
      ));
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function tickSession(sessionId: string) {
    setBusy(true);
    setError(null);
    try {
      const mode = sessions.find((item) => item.session_id === sessionId)?.mode;
      setSessionProgress(await paperV2Api.tickSessionAndWait(
        sessionId,
        {},
        mode && mode !== "REPLAY_ONLY"
          ? { timeoutMs: 180_000, pollMs: 2_000, settleStatuses: LIVE_TICK_SETTLED_STATUSES }
          : { timeoutMs: 600_000, pollMs: 2_000 },
      ));
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function sessionLifecycle(sessionId: string, action: "pause" | "resume" | "stop") {
    setBusy(true);
    setError(null);
    try {
      await paperV2Api.sessionLifecycle(sessionId, action);
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function switchActiveSessionMode() {
    if (!activeSession) return;
    setBusy(true);
    setError(null);
    setSessionProgress(null);
    try {
      const isReplayOnly = switchMode === "REPLAY_ONLY";
      const isCatchupThenLive = switchMode === "CATCHUP_THEN_LIVE";
      const session = await paperV2Api.switchSessionMode(activeSession.session_id, {
        target_mode: switchMode,
        start_date: switchMode === "LIVE_ONLY" ? liveStartDate : switchStart,
        end_date: isReplayOnly || isCatchupThenLive ? switchEnd : null,
        historical_data_source: isReplayOnly || isCatchupThenLive ? "DB_HISTORICAL" : null,
        live_data_source: isCatchupThenLive || switchMode === "LIVE_ONLY" ? "TDX_REALTIME" : null,
        runtime_config: sessionRuntimeConfig(false),
        rerun_policy: "reject_existing",
        created_by: "paper_v2_ui",
      });
      setSessionProgress({ session, day_count: 0, events: [] });
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function schedulerAction(action: "start" | "stop" | "run_once") {
    setBusy(true);
    setError(null);
    try {
      if (action === "start") setSchedulerStatus(await paperV2Api.startScheduler({ interval_seconds: schedulerInterval }));
      if (action === "stop") setSchedulerStatus(await paperV2Api.stopScheduler());
      if (action === "run_once") {
        const result = await paperV2Api.runSchedulerOnce({ limit: 50 });
        setSchedulerStatus({ ...(await paperV2Api.schedulerStatus()), last_result: result });
      }
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function activatePolicy() {
    setBusy(true);
    setError(null);
    try {
      if (!policyId) throw new Error("请先选择已验证执行策略。");
      await paperV2Api.activatePolicy(portfolioId, {
        trade_date: activationDate,
        policy_id: policyId,
        reason: activationReason,
        replace_existing: replaceExisting,
        activated_by: "paper_v2_ui",
      });
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function saveRuntimeProfile() {
    setBusy(true);
    setError(null);
    try {
      const saved = await paperV2Api.createRuntimeProfile(portfolioId, {
        profile_name: runtimeProfileName,
        config_json: runtimeProfileConfig,
        created_by: "paper_v2_ui",
        reason: runtimeActivationReason,
      });
      setRuntimeProfileId(saved.profile.profile_id);
      setRuntimeVersionId(saved.version.profile_version_id);
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function saveRuntimeProfileVersion() {
    setBusy(true);
    setError(null);
    try {
      if (!runtimeProfileId) throw new Error("请先选择运行配置 Profile。");
      const version = await paperV2Api.createRuntimeProfileVersion(portfolioId, runtimeProfileId, {
        config_json: runtimeProfileConfig,
        created_by: "paper_v2_ui",
        reason: runtimeActivationReason,
      });
      setRuntimeVersionId(version.profile_version_id);
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  async function activateRuntimeProfile() {
    setBusy(true);
    setError(null);
    try {
      if (!runtimeVersionId) throw new Error("请先选择运行配置版本。");
      await paperV2Api.activateRuntimeConfig(portfolioId, {
        trade_date: runtimeActivationDate,
        profile_version_id: runtimeVersionId,
        activated_by: "paper_v2_ui",
        reason: runtimeActivationReason,
        replace_existing: replaceRuntimeActivation,
      });
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setBusy(false);
    }
  }

  return (
    <main>
      <div className="pv2-detail-nav">
        <Link href={`/paper-v2/portfolios/${portfolioId}`}>详情</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/live-dashboard`}>实时详情</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/ledger`}>账本</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/performance`}>绩效</Link>
      </div>
      <ErrorPanel error={error} title="运行控制台操作失败" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="组合" value={portfolio ? shortHash(portfolio.portfolio_id) : "-"} hint={portfolio?.portfolio_name} />
        <MetricCard label="状态" value={portfolio?.status || "-"} tone={portfolio?.status === "READY" ? "success" : "warning"} />
        <MetricCard label="运行次数" value={runs.length} hint={runs[0]?.trade_date || "尚未运行"} />
        <MetricCard label="就绪检查" value={readinessPassed ? "已通过" : "需要检查"} tone={readinessPassed ? "success" : "warning"} />
      </div>

      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="单日运行控制" eyebrow="先就绪检查再单日运行">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>交易日期</label><input className="pv2-input" data-testid="console-trade-date" type="date" value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} /></div>
            <div className="pv2-field"><label>数据源</label><input className="pv2-input" value={portfolio?.data_source || "-"} readOnly /></div>
            <div className="pv2-field"><label>组合状态</label><input className="pv2-input" value={portfolio?.status || "-"} readOnly /></div>
          </div>
          <div className="pv2-card" style={{ marginTop: 12 }}>
            <div className="pv2-eyebrow">运行配置</div>
            <div className="pv2-form-grid">
              <div className="pv2-field">
                <label>选股数量 TopK</label>
                <input className="pv2-input" data-testid="console-runtime-top-k" min={1} max={50} type="number" value={runtimeTopK} onChange={(event) => setRuntimeTopK(Number(event.target.value))} />
              </div>
              <div className="pv2-field">
                <label>已确认停牌股票</label>
                <label className="pv2-chip"><input data-testid="console-runtime-exclude-suspended" type="checkbox" checked={runtimeExcludeSuspended} onChange={(event) => setRuntimeExcludeSuspended(event.target.checked)} /> 选股与交易前剔除</label>
              </div>
              <div className="pv2-field">
                <label>信号数据源</label>
                <input className="pv2-input" value={dataSourceLabel("DB_HISTORICAL")} readOnly />
              </div>
              <div className="pv2-field">
                <label>选股产物</label>
                <input className="pv2-input" value="自动生成：WSL 最新数据推理" readOnly />
              </div>
            </div>
            <div className="pv2-field" style={{ marginTop: 12 }}>
              <label>行业黑名单（按行或逗号分隔）</label>
              <textarea className="pv2-textarea" data-testid="console-runtime-industry-blacklist" value={runtimeIndustryBlacklist} onChange={(event) => setRuntimeIndustryBlacklist(event.target.value)} placeholder={"示例：银行\n房地产"} />
            </div>
            <div className="pv2-form-grid" style={{ marginTop: 12 }}>
              <div className="pv2-field">
                <label>HMM 调整</label>
                <label className="pv2-chip"><input data-testid="console-runtime-hmm-enabled" type="checkbox" checked={runtimeHmmEnabled} onChange={(event) => setRuntimeHmmEnabled(event.target.checked)} /> 启用 HMM</label>
              </div>
              <div className="pv2-field">
                <label>HMM 快照 ID</label>
                <input className="pv2-input" data-testid="console-runtime-hmm-snapshot" disabled={!runtimeHmmEnabled} value={runtimeHmmSnapshotId} onChange={(event) => setRuntimeHmmSnapshotId(event.target.value)} placeholder="选择或粘贴已完成快照 ID" />
              </div>
              <div className="pv2-field">
                <label>HMM 信号预设</label>
                <select className="pv2-select" data-testid="console-runtime-hmm-preset" disabled={!runtimeHmmEnabled} value={runtimeHmmPreset} onChange={(event) => setRuntimeHmmPreset(event.target.value)}>
                  <option value="preset_A">preset_A</option>
                  <option value="preset_B">preset_B</option>
                </select>
              </div>
            </div>
            <div className="pv2-field" style={{ marginTop: 12 }}>
              <label>HMM 系数文件路径（可选，必须来自已审计快照）</label>
              <input className="pv2-input" data-testid="console-runtime-hmm-coefficients" disabled={!runtimeHmmEnabled} value={runtimeHmmCoefficientsPath} onChange={(event) => setRuntimeHmmCoefficientsPath(event.target.value)} placeholder="不填写时由后端按快照和交易日严格校验" />
            </div>
            <JsonPanel value={runtimeConfig} />
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" data-testid="console-readiness" disabled={busy} onClick={runReadiness} type="button">{busy ? "处理中..." : "执行就绪检查"}</button>
            <button className="pv2-button-primary" data-testid="console-run-day" disabled={busy || !readinessPassed} onClick={runDay} type="button">执行单日模拟</button>
          </div>
          {!readinessPassed ? (
            <NoticePanel title="就绪检查通过前禁止单日运行" tone="warning">
              本页面严格遵守 v2 流程。同一交易日和同一运行配置通过后端就绪门禁前，单日运行保持禁用。
            </NoticePanel>
          ) : null}
          {readiness ? <ReadinessFailureCard result={readiness} /> : null}
          {runResult ? <JsonPanel value={runResult} /> : null}
        </SectionCard>

        <SectionCard title="执行策略激活" eyebrow="仅限已验证策略">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>交易日期</label><input className="pv2-input" data-testid="console-policy-date" type="date" value={activationDate} onChange={(event) => setActivationDate(event.target.value)} /></div>
            <div className="pv2-field"><label>策略</label><select className="pv2-select" data-testid="console-policy-select" value={policyId} onChange={(event) => setPolicyId(event.target.value)}><option value="">选择已启用策略</option>{policies.map((policy) => {
              const id = executionPolicyId(policy);
              return <option value={id} key={id}>{policy.policy_name || id} / {policy.algo_code || "-"} / {policy.paper_enabled ? "可用于模拟盘" : "未启用"}</option>;
            })}</select></div>
            <div className="pv2-field"><label>替换已有记录</label><label className="pv2-chip"><input data-testid="console-policy-replace" type="checkbox" checked={replaceExisting} onChange={(event) => setReplaceExisting(event.target.checked)} /> 替换同日期记录</label></div>
          </div>
          <div className="pv2-field" style={{ marginTop: 12 }}><label>原因</label><input className="pv2-input" data-testid="console-policy-reason" value={activationReason} onChange={(event) => setActivationReason(event.target.value)} /></div>
          <button className="pv2-button" data-testid="console-policy-activate" disabled={busy || !policyId} onClick={activatePolicy} type="button" style={{ marginTop: 12 }}>按日期激活</button>
          <PaperTable
            rows={activations.slice(0, 8)}
            empty="暂无激活记录。"
            columns={[
              { key: "date", header: "日期", render: (row) => row.trade_date },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
              { key: "policy", header: "策略", render: (row) => row.policy_name || shortHash(row.policy_id) },
              { key: "at", header: "激活时间", render: (row) => asDate(row.activated_at) },
            ]}
          />
        </SectionCard>

        <SectionCard title="运行配置版本与审计" eyebrow="HMM / 黑名单 / TopK / 停牌过滤">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>Profile 名称</label><input className="pv2-input" data-testid="console-runtime-profile-name" value={runtimeProfileName} onChange={(event) => setRuntimeProfileName(event.target.value)} /></div>
            <div className="pv2-field"><label>已保存 Profile</label><select className="pv2-select" data-testid="console-runtime-profile-select" value={runtimeProfileId} onChange={(event) => setRuntimeProfileId(event.target.value)}><option value="">选择 Profile</option>{runtimeProfiles.map((item) => <option value={item.profile_id} key={item.profile_id}>{item.profile_name} / {item.status}</option>)}</select></div>
            <div className="pv2-field"><label>Profile 版本</label><select className="pv2-select" data-testid="console-runtime-version-select" value={runtimeVersionId} onChange={(event) => setRuntimeVersionId(event.target.value)}><option value="">选择版本</option>{runtimeVersions.map((item) => <option value={item.profile_version_id} key={item.profile_version_id}>v{item.version_no} / {shortHash(item.config_sha256)}</option>)}</select></div>
            <div className="pv2-field"><label>激活日期</label><input className="pv2-input" data-testid="console-runtime-activation-date" type="date" value={runtimeActivationDate} onChange={(event) => setRuntimeActivationDate(event.target.value)} /></div>
            <div className="pv2-field"><label>替换同日激活</label><label className="pv2-chip"><input data-testid="console-runtime-replace" type="checkbox" checked={replaceRuntimeActivation} onChange={(event) => setReplaceRuntimeActivation(event.target.checked)} /> 需要原因并写审计</label></div>
          </div>
          <div className="pv2-field" style={{ marginTop: 12 }}><label>变更原因</label><input className="pv2-input" data-testid="console-runtime-reason" value={runtimeActivationReason} onChange={(event) => setRuntimeActivationReason(event.target.value)} /></div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" data-testid="console-runtime-save-profile" disabled={busy} onClick={saveRuntimeProfile} type="button">保存为新 Profile</button>
            <button className="pv2-button" data-testid="console-runtime-save-version" disabled={busy || !runtimeProfileId} onClick={saveRuntimeProfileVersion} type="button">保存为新版本</button>
            <button className="pv2-button-primary" data-testid="console-runtime-activate" disabled={busy || !runtimeVersionId} onClick={activateRuntimeProfile} type="button">按日期激活版本</button>
          </div>
          <NoticePanel title="运行配置不会修改策略包资产" tone="info">这里保存和激活的是模拟盘运行配置版本。它只影响未来 run/session，并会复制版本 hash 到运行快照；不会修改 StrategyPackage manifest、模型权重、HMM 资产或已验证执行策略。</NoticePanel>
          <PaperTable
            rows={runtimeActivations.slice(0, 8)}
            empty="暂无运行配置激活记录。"
            columns={[
              { key: "date", header: "日期", render: (row) => row.trade_date },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
              { key: "version", header: "版本", render: (row) => shortHash(row.profile_version_id) },
              { key: "hash", header: "配置 Hash", render: (row) => shortHash(row.context?.config_sha256 as string | undefined) },
            ]}
          />
          <JsonPanel value={{ runtime_profiles: runtimeProfiles, runtime_versions: runtimeVersions, config_change_audit: configAudit.slice(0, 6) }} />
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="运行场景启动" eyebrow="仅历史追赶 / 追赶后自动实时 / 完全实时">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>运行场景</label><select className="pv2-select" data-testid="console-session-mode" value={sessionMode} onChange={(event) => setSessionMode(event.target.value as PaperSessionMode)}>{SESSION_MODE_OPTIONS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select></div>
            <div className="pv2-field"><label>历史追赶开始</label><input className="pv2-input" data-testid="console-replay-start" type="date" value={replayStart} onChange={(event) => setReplayStart(event.target.value)} disabled={sessionMode === "LIVE_ONLY"} /></div>
            <div className="pv2-field"><label>历史追赶结束</label><input className="pv2-input" data-testid="console-replay-end" type="date" value={replayEnd} onChange={(event) => setReplayEnd(event.target.value)} disabled={sessionMode === "LIVE_ONLY"} /></div>
            <div className="pv2-field"><label>实时开始日期</label><input className="pv2-input" data-testid="console-live-start-inline" type="date" value={liveStartDate} onChange={(event) => setLiveStartDate(event.target.value)} disabled={sessionMode !== "LIVE_ONLY"} /></div>
            <div className="pv2-field"><label>数据源角色</label><input className="pv2-input" value={sessionMode === "LIVE_ONLY" ? dataSourceLabel("TDX_REALTIME") : sessionMode === "CATCHUP_THEN_LIVE" ? `${dataSourceLabel("DB_HISTORICAL")} → ${dataSourceLabel("TDX_REALTIME")}` : dataSourceLabel("DB_HISTORICAL")} readOnly /></div>
            <div className="pv2-field"><label>重跑策略</label><input className="pv2-input" value="reject_existing / reset_portfolio" readOnly /></div>
          </div>
          <NoticePanel title="场景含义" tone="info">
            {SESSION_MODE_OPTIONS.find((item) => item.value === sessionMode)?.description} 会话创建、暂停、恢复、停止和切换均由后端校验非交易时间；交易时间内会返回 INVALID_STATE_TRANSITION。
          </NoticePanel>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" data-testid="console-replay-reject" disabled={busy || sessionModeBlocked} onClick={() => replay("reject_existing")} type="button">{sessionMode === "LIVE_ONLY" ? "启动完全实时" : sessionMode === "CATCHUP_THEN_LIVE" ? "启动追赶后自动实时" : "启动仅历史追赶"}</button>
            <ConfirmAction label="重置并重跑历史追赶" danger disabled={busy || sessionMode === "LIVE_ONLY" || sessionModeBlocked} confirmText={portfolioId} onConfirm={() => replay("reset_portfolio")} testId="console-replay-reset" />
          </div>
          {sessionModeBlocked ? <NoticePanel title="当前场景被后端能力诊断禁用" tone="warning"><CapabilityErrorList node={sessionMode === "REPLAY_ONLY" ? replayCapability : sessionMode === "CATCHUP_THEN_LIVE" ? catchupCapability : liveCapability} /></NoticePanel> : null}
          <NoticePanel title="重置会删除该组合账本历史" tone="warning">
            重置只用于需要替换已有历史运行、订单、成交、现金、持仓、快照、事件和错误时使用；否则使用 reject_existing 追赶缺失交易日。
          </NoticePanel>
          {sessionProgress ? <JsonPanel value={sessionProgress} /> : null}
          {replayResult ? <JsonPanel value={replayResult} /> : null}
        </SectionCard>

        <SectionCard title="实时模拟与后台调度" eyebrow="TDX 实时分钟线">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>实时开始日期</label><input className="pv2-input" data-testid="console-live-start" type="date" value={liveStartDate} onChange={(event) => setLiveStartDate(event.target.value)} /></div>
            <div className="pv2-field"><label>实时数据源</label><input className="pv2-input" value={dataSourceLabel("TDX_REALTIME")} readOnly /></div>
            <div className="pv2-field"><label>调度间隔（秒）</label><input className="pv2-input" data-testid="console-scheduler-interval" type="number" min={1} max={3600} value={schedulerInterval} onChange={(event) => setSchedulerInterval(Number(event.target.value))} /></div>
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button-primary" data-testid="console-live-create" disabled={busy || liveBlocked} onClick={startLiveSession} type="button">创建实时会话并执行一次 Tick</button>
            <button className="pv2-button" data-testid="console-scheduler-run-once" disabled={busy} onClick={() => schedulerAction("run_once")} type="button">后台调度执行一次</button>
            <button className="pv2-button" data-testid="console-scheduler-start" disabled={busy || schedulerStatus?.running} onClick={() => schedulerAction("start")} type="button">启动后台调度</button>
            <button className="pv2-button" data-testid="console-scheduler-stop" disabled={busy || !schedulerStatus?.running} onClick={() => schedulerAction("stop")} type="button">停止后台调度</button>
          </div>
          <div className="pv2-card" style={{ marginTop: 12 }}>
            <div className="pv2-eyebrow">运行场景切换（仅非交易时间）</div>
            <div className="pv2-form-grid">
              <div className="pv2-field"><label>当前活跃会话</label><input className="pv2-input" value={activeSession ? `${activeSession.mode} / ${activeSession.status}` : "无活跃会话"} readOnly /></div>
              <div className="pv2-field"><label>目标场景</label><select className="pv2-select" data-testid="console-switch-mode" value={switchMode} onChange={(event) => setSwitchMode(event.target.value as PaperSessionMode)}>{SESSION_MODE_OPTIONS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select></div>
              <div className="pv2-field"><label>切换后开始日</label><input className="pv2-input" data-testid="console-switch-start" type="date" value={switchStart} onChange={(event) => setSwitchStart(event.target.value)} disabled={switchMode === "LIVE_ONLY"} /></div>
              <div className="pv2-field"><label>切换后追赶结束</label><input className="pv2-input" data-testid="console-switch-end" type="date" value={switchEnd} onChange={(event) => setSwitchEnd(event.target.value)} disabled={switchMode === "LIVE_ONLY"} /></div>
            </div>
            <button className="pv2-button-primary" data-testid="console-switch-mode-apply" disabled={busy || !activeSession || switchModeBlocked} onClick={switchActiveSessionMode} type="button" style={{ marginTop: 12 }}>切换活跃任务场景</button>
            <NoticePanel title="切换约束" tone="warning">切换会先停止原活跃会话，再按目标场景创建新会话；后端在 09:15-15:00 A 股交易时间内拒绝执行该操作。</NoticePanel>
          </div>
          {liveBlocked ? <NoticePanel title="实时模式被后端能力诊断禁用" tone="warning"><CapabilityErrorList node={liveCapability} /></NoticePanel> : null}
          <NoticePanel title="后台调度不会改变业务逻辑" tone="info">
            调度器只调用与页面相同的 session tick 接口。无新分钟线时进入等待状态；数据、算法或策略产物缺失会显示后端错误，不会降级到日频或其他算法。
          </NoticePanel>
          <div className="pv2-chip-row">
            <span className="pv2-chip">调度器: {schedulerStatus?.running ? "运行中" : "未运行"}</span>
            <span className="pv2-chip">最近会话: {latestSession ? `${latestSession.mode}/${latestSession.status}` : "暂无"}</span>
          </div>
          {schedulerStatus ? <JsonPanel value={schedulerStatus} /> : null}
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="会话列表" eyebrow="回放 / 追赶 / 实时">
          <PaperTable
            rows={sessions.slice(0, 12)}
            empty="暂无持久化会话。"
            columns={[
              { key: "mode", header: "模式", render: (row) => <StatusBadge status={row.mode} /> },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
              { key: "phase", header: "阶段", render: (row) => <StatusBadge status={row.phase} /> },
              { key: "dates", header: "日期", render: (row) => `${row.start_date}${row.end_date ? ` 至 ${row.end_date}` : ""}` },
              { key: "actions", header: "操作", render: (row) => <div className="pv2-row-actions"><button className="pv2-link-button" disabled={busy} onClick={() => tickSession(row.session_id)} type="button">Tick</button><button className="pv2-link-button" disabled={busy || row.status === "PAUSED"} onClick={() => sessionLifecycle(row.session_id, "pause")} type="button">暂停</button><button className="pv2-link-button" disabled={busy || row.status !== "PAUSED"} onClick={() => sessionLifecycle(row.session_id, "resume")} type="button">恢复</button><button className="pv2-link-button" disabled={busy} onClick={() => sessionLifecycle(row.session_id, "stop")} type="button">停止</button></div> },
            ]}
          />
          {sessionProgress ? <JsonPanel value={sessionProgress} /> : null}
        </SectionCard>

        <SectionCard title="运行时间线与错误" eyebrow="持久化追踪">
          <PaperTable
            rows={events.slice(0, 12)}
            empty="暂无运行事件。"
            columns={[
              { key: "date", header: "日期", render: (row) => String(row.trade_date || "-") },
              { key: "stage", header: "阶段", render: (row) => String(row.stage || row.event_type || "-") },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={String(row.status || "event")} /> },
              { key: "message", header: "消息", render: (row) => String(row.message || row.event_message || "-") },
            ]}
          />
          <PaperTable
            rows={errors.slice(0, 8)}
            empty="暂无持久化错误。"
            columns={[
              { key: "date", header: "日期", render: (row) => String(row.trade_date || "-") },
              { key: "code", header: "代码", render: (row) => <StatusBadge status={String(row.error_code || "error")} /> },
              { key: "message", header: "消息", render: (row) => String(row.message || row.error_message || "-") },
            ]}
          />
        </SectionCard>
      </div>
    </main>
  );
}
