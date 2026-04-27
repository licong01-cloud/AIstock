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
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { paperV2Api } from "@/lib/paper-v2/api";
import { shortHash, todayIso } from "@/lib/paper-v2/format";
import type {
  Activation,
  ExecutionPolicy,
  JsonObject,
  PaperPortfolio,
  PaperRun,
  PaperSchedulerStatus,
  PaperSession,
  PaperSessionProgress,
  ReadinessResult,
  ReplayResult,
} from "@/lib/paper-v2/types";

const DEFAULT_RUNTIME = {
  paper_v2_session: { signal_data_source: "DB_HISTORICAL" },
  runtime_profile: {
    selection: { top_k: 20 },
    tradability: { exclude_suspended: true },
    industry_blacklist: [],
    hmm: { enabled: false, model_snapshot_id: null, signal_preset: null },
  },
};

function parseRuntime(text: string): JsonObject {
  const parsed = JSON.parse(text) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("运行配置必须是 JSON 对象。");
  }
  return parsed as JsonObject;
}

function asDate(value: unknown): string {
  if (!value) return "-";
  return String(value).slice(0, 19).replace("T", " ");
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
  const [tradeDate, setTradeDate] = useState(todayIso());
  const [runtimeText, setRuntimeText] = useState(JSON.stringify(DEFAULT_RUNTIME, null, 2));
  const [readiness, setReadiness] = useState<ReadinessResult | null>(null);
  const [runResult, setRunResult] = useState<JsonObject | null>(null);
  const [replayResult, setReplayResult] = useState<ReplayResult | null>(null);
  const [replayStart, setReplayStart] = useState(todayIso());
  const [replayEnd, setReplayEnd] = useState(todayIso());
  const [autoSwitchToLive, setAutoSwitchToLive] = useState(false);
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

  const load = useCallback(async () => {
    setError(null);
    try {
      const [portfolioRow, runRows, sessionRows, schedulerRow, eventRows, errorRows, policyRows, activationRows] = await Promise.all([
        paperV2Api.getPortfolio(portfolioId),
        paperV2Api.runs(portfolioId),
        paperV2Api.sessions(portfolioId),
        paperV2Api.schedulerStatus(),
        paperV2Api.runEvents(portfolioId),
        paperV2Api.errors(portfolioId),
        paperV2Api.executionPolicies(portfolioId),
        paperV2Api.activations(portfolioId),
      ]);
      setPortfolio(portfolioRow);
      setRuns(runRows);
      setSessions(sessionRows);
      setSchedulerStatus(schedulerRow);
      setEvents(eventRows);
      setErrors(errorRows);
      setPolicies(policyRows);
      setActivations(activationRows);
      if (!policyId) {
        const defaultPolicy = policyRows.find((item) => item.is_portfolio_default) || policyRows.find((item) => item.paper_enabled);
        setPolicyId(defaultPolicy?.policy_id || "");
      }
    } catch (exc) {
      setError(exc);
    }
  }, [portfolioId, policyId]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    setReadiness(null);
    setRunResult(null);
  }, [tradeDate, runtimeText]);

  async function runReadiness() {
    setBusy(true);
    setError(null);
    setReadiness(null);
    try {
      const result = await paperV2Api.readiness(portfolioId, { trade_date: tradeDate, runtime_config: parseRuntime(runtimeText) });
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
      const result = await paperV2Api.runDay(portfolioId, { trade_date: tradeDate, runtime_config: parseRuntime(runtimeText) });
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
      const session = await paperV2Api.createSession(portfolioId, {
        mode: "REPLAY_ONLY",
        start_date: replayStart,
        end_date: replayEnd,
        historical_data_source: "DB_HISTORICAL",
        live_data_source: autoSwitchToLive ? "TDX_REALTIME" : null,
        runtime_config: parseRuntime(runtimeText),
        rerun_policy: rerunPolicy,
        auto_switch_to_live: autoSwitchToLive,
        confirm_reset: rerunPolicy === "reset_portfolio",
        confirm_text: rerunPolicy === "reset_portfolio" ? portfolioId : null,
        created_by: "paper_v2_ui",
      });
      setSessionProgress(await paperV2Api.tickSession(session.session_id));
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
        runtime_config: parseRuntime(runtimeText),
        rerun_policy: "reject_existing",
        created_by: "paper_v2_ui",
      });
      setSessionProgress(await paperV2Api.tickSession(session.session_id));
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
      setSessionProgress(await paperV2Api.tickSession(sessionId));
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

  return (
    <main>
      <div className="pv2-detail-nav">
        <Link href={`/paper-v2/portfolios/${portfolioId}`}>详情</Link>
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
            <div className="pv2-field"><label>交易日期</label><input className="pv2-input" type="date" value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} /></div>
            <div className="pv2-field"><label>数据源</label><input className="pv2-input" value={portfolio?.data_source || "-"} readOnly /></div>
            <div className="pv2-field"><label>组合状态</label><input className="pv2-input" value={portfolio?.status || "-"} readOnly /></div>
          </div>
          <div className="pv2-field" style={{ marginTop: 12 }}>
            <label>运行配置 JSON</label>
            <textarea className="pv2-textarea" value={runtimeText} onChange={(event) => setRuntimeText(event.target.value)} />
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" disabled={busy} onClick={runReadiness} type="button">{busy ? "处理中..." : "执行就绪检查"}</button>
            <button className="pv2-button-primary" disabled={busy || !readinessPassed} onClick={runDay} type="button">执行单日模拟</button>
          </div>
          {!readinessPassed ? (
            <NoticePanel title="就绪检查通过前禁止单日运行" tone="warning">
              本页面严格遵守 v2 流程。同一交易日和同一运行配置通过后端就绪门禁前，单日运行保持禁用。
            </NoticePanel>
          ) : null}
          {readiness ? <JsonPanel value={readiness} /> : null}
          {runResult ? <JsonPanel value={runResult} /> : null}
        </SectionCard>

        <SectionCard title="执行策略激活" eyebrow="仅限已验证策略">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>交易日期</label><input className="pv2-input" type="date" value={activationDate} onChange={(event) => setActivationDate(event.target.value)} /></div>
            <div className="pv2-field"><label>策略</label><select className="pv2-select" value={policyId} onChange={(event) => setPolicyId(event.target.value)}><option value="">选择已启用策略</option>{policies.map((policy) => <option value={policy.policy_id} key={policy.policy_id}>{policy.policy_name || policy.policy_id} / {policy.algo_code || "-"} / {policy.paper_enabled ? "可用于模拟盘" : "未启用"}</option>)}</select></div>
            <div className="pv2-field"><label>替换已有记录</label><label className="pv2-chip"><input type="checkbox" checked={replaceExisting} onChange={(event) => setReplaceExisting(event.target.checked)} /> 替换同日期记录</label></div>
          </div>
          <div className="pv2-field" style={{ marginTop: 12 }}><label>原因</label><input className="pv2-input" value={activationReason} onChange={(event) => setActivationReason(event.target.value)} /></div>
          <button className="pv2-button" disabled={busy || !policyId} onClick={activatePolicy} type="button" style={{ marginTop: 12 }}>按日期激活</button>
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
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="历史回放" eyebrow="数据库分钟线回放">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>开始</label><input className="pv2-input" type="date" value={replayStart} onChange={(event) => setReplayStart(event.target.value)} /></div>
            <div className="pv2-field"><label>结束</label><input className="pv2-input" type="date" value={replayEnd} onChange={(event) => setReplayEnd(event.target.value)} /></div>
            <div className="pv2-field"><label>重跑策略</label><input className="pv2-input" value="reject_existing / reset_portfolio" readOnly /></div>
            <div className="pv2-field"><label>追赶后切实时</label><label className="pv2-chip"><input type="checkbox" checked={autoSwitchToLive} onChange={(event) => setAutoSwitchToLive(event.target.checked)} /> 回放完成后进入 TDX 实时模拟</label></div>
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" disabled={busy} onClick={() => replay("reject_existing")} type="button">{autoSwitchToLive ? "回放追赶到实时（拒绝已有）" : "回放（拒绝已有）"}</button>
            <ConfirmAction label={autoSwitchToLive ? "重置、回放并切实时" : "重置并回放"} danger disabled={busy} confirmText={portfolioId} onConfirm={() => replay("reset_portfolio")} />
          </div>
          <NoticePanel title="重置会删除该组合账本历史" tone="warning">
            系统支持重置回放，但必须输入完整 portfolio_id 确认。仅在需要替换该组合所有现有运行、订单、成交、现金、持仓、快照、事件和错误时使用。
          </NoticePanel>
          {sessionProgress ? <JsonPanel value={sessionProgress} /> : null}
          {replayResult ? <JsonPanel value={replayResult} /> : null}
        </SectionCard>

        <SectionCard title="实时模拟与后台调度" eyebrow="TDX 实时分钟线">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>实时开始日期</label><input className="pv2-input" type="date" value={liveStartDate} onChange={(event) => setLiveStartDate(event.target.value)} /></div>
            <div className="pv2-field"><label>实时数据源</label><input className="pv2-input" value="TDX_REALTIME" readOnly /></div>
            <div className="pv2-field"><label>调度间隔（秒）</label><input className="pv2-input" type="number" min={1} max={3600} value={schedulerInterval} onChange={(event) => setSchedulerInterval(Number(event.target.value))} /></div>
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button-primary" disabled={busy} onClick={startLiveSession} type="button">创建实时会话并执行一次 Tick</button>
            <button className="pv2-button" disabled={busy} onClick={() => schedulerAction("run_once")} type="button">后台调度执行一次</button>
            <button className="pv2-button" disabled={busy || schedulerStatus?.running} onClick={() => schedulerAction("start")} type="button">启动后台调度</button>
            <button className="pv2-button" disabled={busy || !schedulerStatus?.running} onClick={() => schedulerAction("stop")} type="button">停止后台调度</button>
          </div>
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
              { key: "mode", header: "模式", render: (row) => row.mode },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
              { key: "phase", header: "阶段", render: (row) => row.phase },
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
