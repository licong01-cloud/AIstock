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
import type { Activation, ExecutionPolicy, JsonObject, PaperPortfolio, PaperRun, ReadinessResult, ReplayResult } from "@/lib/paper-v2/types";

const DEFAULT_RUNTIME = {
  runtime_profile: {
    selection: { top_k: 50 },
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

  const load = useCallback(async () => {
    setError(null);
    try {
      const [portfolioRow, runRows, eventRows, errorRows, policyRows, activationRows] = await Promise.all([
        paperV2Api.getPortfolio(portfolioId),
        paperV2Api.runs(portfolioId),
        paperV2Api.runEvents(portfolioId),
        paperV2Api.errors(portfolioId),
        paperV2Api.executionPolicies(portfolioId),
        paperV2Api.activations(portfolioId),
      ]);
      setPortfolio(portfolioRow);
      setRuns(runRows);
      setEvents(eventRows);
      setErrors(errorRows);
      setPolicies(policyRows);
      setActivations(activationRows);
      if (!policyId) {
        const defaultPolicy = policyRows.find((item) => item.is_portfolio_default) || policyRows.find((item) => item.paper_enabled);
        setPolicyId(defaultPolicy?.policy_id || policyRows[0]?.policy_id || "");
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
    try {
      const result = await paperV2Api.replay(portfolioId, {
        start_date: replayStart,
        end_date: replayEnd,
        runtime_config: parseRuntime(runtimeText),
        rerun_policy: rerunPolicy,
        confirm_reset: rerunPolicy === "reset_portfolio",
        confirm_text: rerunPolicy === "reset_portfolio" ? portfolioId : null,
      });
      setReplayResult(result);
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
            <div className="pv2-field"><label>策略</label><select className="pv2-select" value={policyId} onChange={(event) => setPolicyId(event.target.value)}><option value="">选择策略</option>{policies.map((policy) => <option value={policy.policy_id} key={policy.policy_id}>{policy.policy_name || policy.policy_id} / {policy.algo_code || "-"} / {policy.paper_enabled ? "paper" : "disabled"}</option>)}</select></div>
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
          </div>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button" disabled={busy} onClick={() => replay("reject_existing")} type="button">回放（拒绝已有）</button>
            <ConfirmAction label="重置并回放" danger disabled={busy} confirmText={portfolioId} onConfirm={() => replay("reset_portfolio")} />
          </div>
          <NoticePanel title="重置会删除该组合账本历史" tone="warning">
            系统支持重置回放，但必须输入完整 portfolio_id 确认。仅在需要替换该组合所有现有运行、订单、成交、现金、持仓、快照、事件和错误时使用。
          </NoticePanel>
          {replayResult ? <JsonPanel value={replayResult} /> : null}
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
