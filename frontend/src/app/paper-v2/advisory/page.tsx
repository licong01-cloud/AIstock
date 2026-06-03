"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import {
  advisoryApi,
  type AdvisoryEpisode,
  type AdvisoryLeaderboardRow,
  type AdvisoryProgram,
  type AdvisoryQualityReport,
  type AdvisoryReviewDecision,
  type AdvisoryReviewResult,
} from "@/lib/api/advisory";
import type { JsonObject } from "@/lib/api/selectionCenter";

const DEFAULT_CANDIDATES = "";

const DEFAULT_MARKET = "";

const DEFAULT_REPLAY_CANDIDATES = "";

const DEFAULT_REPLAY_MARKET = "";

const DEFAULT_QUALITY_RECORDS = "";

function fmtPct(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(1)}%` : "-";
}

function fmtBps(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? `${(value / 100).toFixed(2)}%` : "-";
}

function short(value: unknown, len = 10): string {
  const text = String(value ?? "-");
  return text.length > len ? `${text.slice(0, len)}...` : text;
}

function parseJson<T>(text: string, fallback: T): T {
  const trimmed = text.trim();
  if (!trimmed) return fallback;
  return JSON.parse(trimmed) as T;
}

function packageIdsFromText(text: string): string[] {
  return text.split(/[,\n]/).map((item) => item.trim()).filter(Boolean);
}

function weightsFromText(text: string, packageIds: string[]): Record<string, number> {
  const parsed = parseJson<Record<string, number>>(text, {});
  if (Object.keys(parsed).length) return parsed;
  return Object.fromEntries(packageIds.map((packageId) => [packageId, 1]));
}

function AdvisoryPageContent() {
  const params = useSearchParams();
  const prefillRunId = params.get("selection_run_id") || "";
  const prefillPackages = params.get("package_ids") || "";
  const [programs, setPrograms] = useState<AdvisoryProgram[]>([]);
  const [leaderboard, setLeaderboard] = useState<AdvisoryLeaderboardRow[]>([]);
  const [selectedProgramId, setSelectedProgramId] = useState("");
  const [sortBy, setSortBy] = useState("win_rate");
  const [programName, setProgramName] = useState("每日 Top20 荐股任务");
  const [packageMode, setPackageMode] = useState<"single_package" | "fusion_pool">("single_package");
  const [packageIdsText, setPackageIdsText] = useState(prefillPackages);
  const [weightsText, setWeightsText] = useState("{}");
  const [targetCount, setTargetCount] = useState(20);
  const [tradeDate, setTradeDate] = useState("");
  const [selectionRunId, setSelectionRunId] = useState(prefillRunId);
  const [candidatesText, setCandidatesText] = useState(DEFAULT_CANDIDATES);
  const [marketText, setMarketText] = useState(DEFAULT_MARKET);
  const [activePool, setActivePool] = useState<AdvisoryEpisode[]>([]);
  const [reviews, setReviews] = useState<AdvisoryReviewDecision[]>([]);
  const [returns, setReturns] = useState<AdvisoryEpisode[]>([]);
  const [reviewResult, setReviewResult] = useState<AdvisoryReviewResult | null>(null);
  const [replayStart, setReplayStart] = useState("");
  const [replayEnd, setReplayEnd] = useState("");
  const [replayCandidatesText, setReplayCandidatesText] = useState(DEFAULT_REPLAY_CANDIDATES);
  const [replayMarketText, setReplayMarketText] = useState(DEFAULT_REPLAY_MARKET);
  const [replayResult, setReplayResult] = useState<JsonObject | null>(null);
  const [qualityInput, setQualityInput] = useState(DEFAULT_QUALITY_RECORDS);
  const [qualityReport, setQualityReport] = useState<AdvisoryQualityReport | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const selectedProgram = useMemo(
    () => programs.find((item) => item.program_id === selectedProgramId) || programs[0],
    [programs, selectedProgramId],
  );

  async function refreshAll(nextProgramId?: string) {
    setLoading(true);
    setError(null);
    try {
      const [programRows, boardRows] = await Promise.all([
        advisoryApi.programs(false),
        advisoryApi.leaderboard(sortBy),
      ]);
      setPrograms(programRows);
      setLeaderboard(boardRows);
      const resolvedProgramId = nextProgramId || selectedProgramId || programRows[0]?.program_id || "";
      setSelectedProgramId(resolvedProgramId);
      if (resolvedProgramId) {
        const [poolRows, reviewRows, returnRows] = await Promise.all([
          advisoryApi.activePool(resolvedProgramId),
          advisoryApi.reviews(resolvedProgramId),
          advisoryApi.returns(resolvedProgramId),
        ]);
        setActivePool(poolRows);
        setReviews(reviewRows);
        setReturns(returnRows.returns || []);
      } else {
        setActivePool([]);
        setReviews([]);
        setReturns([]);
      }
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refreshAll().catch((exc) => setError(exc instanceof Error ? exc.message : String(exc)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sortBy]);

  useEffect(() => {
    if (prefillPackages) {
      const ids = packageIdsFromText(prefillPackages);
      setPackageMode(ids.length > 1 ? "fusion_pool" : "single_package");
      setPackageIdsText(prefillPackages);
    }
    if (prefillRunId) setSelectionRunId(prefillRunId);
  }, [prefillPackages, prefillRunId]);

  async function createProgram() {
    setError(null);
    try {
      const packageIds = packageIdsFromText(packageIdsText);
      const program = await advisoryApi.createProgram({
        program_name: programName,
        package_mode: packageMode,
        package_ids: packageIds,
        target_count: targetCount,
        package_weights: weightsFromText(weightsText, packageIds),
        status: "ENABLED",
      });
      await refreshAll(program.program_id);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  async function setStatus(programId: string, action: "enable" | "pause" | "archive") {
    setError(null);
    try {
      if (action === "enable") await advisoryApi.enable(programId);
      if (action === "pause") await advisoryApi.pause(programId);
      if (action === "archive") await advisoryApi.archive(programId);
      await refreshAll(programId);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  async function cloneProgram(programId: string) {
    setError(null);
    try {
      const program = await advisoryApi.clone(programId);
      await refreshAll(program.program_id);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  async function runReview(preview: boolean) {
    if (!selectedProgram) return;
    setError(null);
    setReviewResult(null);
    try {
      const payload = {
        trade_date: tradeDate,
        selection_run_id: selectionRunId || undefined,
        candidates: selectionRunId ? undefined : parseJson<JsonObject[]>(candidatesText, []),
        market_by_symbol: parseJson<Record<string, JsonObject>>(marketText, {}),
      };
      const result = preview
        ? await advisoryApi.previewReview(selectedProgram.program_id, payload)
        : await advisoryApi.runReview(selectedProgram.program_id, payload);
      setReviewResult(result);
      await refreshAll(selectedProgram.program_id);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  async function runReplay() {
    if (!selectedProgram) return;
    setError(null);
    setReplayResult(null);
    try {
      const replay = await advisoryApi.replay(selectedProgram.program_id, {
        start_date: replayStart,
        end_date: replayEnd,
        candidates_by_date: parseJson<JsonObject>(replayCandidatesText, {}),
        market_by_date: parseJson<JsonObject>(replayMarketText, {}),
      });
      setReplayResult(replay);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  async function buildQualityReport() {
    setError(null);
    setQualityReport(null);
    try {
      setQualityReport(await advisoryApi.qualityReport(parseJson<JsonObject[]>(qualityInput, []), 1));
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    }
  }

  return (
    <main className="pv2-main">
      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-kicker">荐股中心</div>
            <h2>运行中的荐股任务排行榜</h2>
            <p className="pv2-muted">默认按胜率排序；自动保留的质量/状态字段仅为 last_review_status。</p>
          </div>
          <div className="pv2-row-actions">
            <select className="pv2-select" value={sortBy} onChange={(event) => setSortBy(event.target.value)}>
              <option value="win_rate">胜率</option>
              <option value="avg_return_bps">平均涨幅</option>
              <option value="median_return_bps">中位涨幅</option>
              <option value="entered_episode_count">累计荐股数</option>
              <option value="enabled_since">启用时间</option>
              <option value="max_drawdown_bps">最大回撤</option>
            </select>
            <button className="pv2-button" onClick={() => refreshAll()} disabled={loading} type="button">刷新</button>
          </div>
        </div>
        {error ? <div className="pv2-error-panel">{error}</div> : null}
        <div className="pv2-table-wrap">
          <table className="pv2-table">
            <thead>
              <tr>
                <th>荐股任务</th>
                <th>状态</th>
                <th>启用时间</th>
                <th>累计</th>
                <th>当前持有</th>
                <th>止盈</th>
                <th>止损</th>
                <th>胜率</th>
                <th>平均涨幅</th>
                <th>最近复评</th>
              </tr>
            </thead>
            <tbody>
              {leaderboard.map((row) => (
                <tr key={row.program_id} onClick={() => setSelectedProgramId(row.program_id)}>
                  <td><strong>{row.program_name}</strong><br /><span className="pv2-muted pv2-mono">{short(row.package_ids.join("+"), 24)}</span></td>
                  <td>{row.status}</td>
                  <td>{short(row.enabled_since, 16)}</td>
                  <td>{row.entered_episode_count ?? 0}</td>
                  <td>{row.active_count ?? 0}</td>
                  <td>{row.take_profit_count ?? 0}</td>
                  <td>{row.stop_loss_count ?? 0}</td>
                  <td>{fmtPct(row.win_rate)}</td>
                  <td>{fmtBps(row.avg_return_bps)}</td>
                  <td>{row.last_review_status || "STALE"}</td>
                </tr>
              ))}
              {!leaderboard.length ? (
                <tr><td colSpan={10}>暂无启用中的荐股任务；请在下方创建，页面不会展示 mock 行。</td></tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-kicker">任务设置</div>
            <h2>创建或管理独立荐股任务</h2>
            <p className="pv2-muted">每个任务按 program_id 隔离，支持单策略包或加权融合策略包组合。</p>
          </div>
          <button className="pv2-button-primary" onClick={createProgram} type="button">创建并启用</button>
        </div>
        <div className="pv2-form-grid">
          <label className="pv2-field">任务名称<input className="pv2-input" value={programName} onChange={(event) => setProgramName(event.target.value)} /></label>
          <label className="pv2-field">策略模式<select className="pv2-select" value={packageMode} onChange={(event) => setPackageMode(event.target.value as "single_package" | "fusion_pool")}><option value="single_package">single_package</option><option value="fusion_pool">fusion_pool</option></select></label>
          <label className="pv2-field">目标数量<input className="pv2-input" type="number" min={1} max={100} value={targetCount} onChange={(event) => setTargetCount(Number(event.target.value))} /></label>
          <label className="pv2-field">策略包 ID<textarea className="pv2-textarea" rows={3} value={packageIdsText} onChange={(event) => setPackageIdsText(event.target.value)} placeholder="pkg_a, pkg_b" /></label>
          <label className="pv2-field">权重 JSON<textarea className="pv2-textarea" rows={3} value={weightsText} onChange={(event) => setWeightsText(event.target.value)} placeholder='{"pkg_a":0.6,"pkg_b":0.4}' /></label>
        </div>
        <div className="pv2-table-wrap" style={{ marginTop: 16 }}>
          <table className="pv2-table">
            <thead><tr><th>名称</th><th>策略模式</th><th>状态</th><th>版本</th><th>操作</th></tr></thead>
            <tbody>
              {programs.map((program) => (
                <tr key={program.program_id}>
                  <td><button className="pv2-link-button" onClick={() => setSelectedProgramId(program.program_id)} type="button">{program.program_name}</button></td>
                  <td>{program.package_mode}</td>
                  <td>{program.status}</td>
                  <td>{program.version}</td>
                  <td className="pv2-row-actions">
                    <button className="pv2-button" onClick={() => setStatus(program.program_id, "enable")} type="button">启用</button>
                    <button className="pv2-button-ghost" onClick={() => setStatus(program.program_id, "pause")} type="button">暂停</button>
                    <button className="pv2-button-ghost" onClick={() => cloneProgram(program.program_id)} type="button">克隆</button>
                    <button className="pv2-button-danger" onClick={() => setStatus(program.program_id, "archive")} type="button">归档</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-kicker">每日复评</div>
            <h2>{selectedProgram ? selectedProgram.program_name : "未选择荐股任务"}</h2>
            <p className="pv2-muted">每日复评使用 active_pool ∪ TopK；缺 rank 或价格时进入 WAITING_DATA，不静默成功。</p>
          </div>
          <div className="pv2-row-actions">
            <button className="pv2-button" onClick={() => runReview(true)} disabled={!selectedProgram || !tradeDate} type="button">预览</button>
            <button className="pv2-button-primary" onClick={() => runReview(false)} disabled={!selectedProgram || !tradeDate} type="button">执行复评</button>
          </div>
        </div>
        <div className="pv2-form-grid">
          <label className="pv2-field">交易日<input className="pv2-input" type="date" value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} /></label>
          <label className="pv2-field">选股运行 ID（可选）<input className="pv2-input" value={selectionRunId} onChange={(event) => setSelectionRunId(event.target.value)} placeholder="填写真实 Selection Center run 后，将忽略高级候选 JSON" /></label>
          <label className="pv2-field">高级候选 JSON<textarea className="pv2-textarea" rows={7} value={candidatesText} onChange={(event) => setCandidatesText(event.target.value)} /></label>
          <label className="pv2-field">高级行情 JSON<textarea className="pv2-textarea" rows={7} value={marketText} onChange={(event) => setMarketText(event.target.value)} /></label>
        </div>
        {reviewResult ? (
          <div className="pv2-readable-panel" style={{ marginTop: 12 }}>
            <strong>复评状态： {reviewResult.review_status}</strong>
            <span className="pv2-muted"> 决策数： {reviewResult.decisions.length}; 活跃快照数： {reviewResult.active_pool.length}</span>
          </div>
        ) : null}
      </section>

      <div className="pv2-grid pv2-grid-2">
        <section className="pv2-card">
          <div className="pv2-card-head"><div><div className="pv2-kicker">当前荐股池</div><h2>当前推荐股票</h2></div></div>
          <table className="pv2-table">
            <thead><tr><th>股票</th><th>入选</th><th>排名</th><th>涨跌幅</th><th>状态</th><th>退出</th></tr></thead>
            <tbody>
              {activePool.map((row) => (
                <tr key={row.episode_id}><td>{row.symbol}</td><td>{row.signal_date} / {row.entry_price}</td><td>{row.current_rank ?? row.entry_rank}</td><td>{fmtBps(row.return_bps)}</td><td>{row.status}</td><td>{row.exit_reason || "-"}</td></tr>
              ))}
              {!activePool.length ? <tr><td colSpan={6}>当前任务暂无活跃荐股。</td></tr> : null}
            </tbody>
          </table>
        </section>
        <section className="pv2-card">
          <div className="pv2-card-head"><div><div className="pv2-kicker">复评记录</div><h2>只追加决策记录</h2></div></div>
          <table className="pv2-table">
            <thead><tr><th>日期</th><th>股票</th><th>动作</th><th>原因</th><th>状态</th><th>排名</th></tr></thead>
            <tbody>
              {reviews.map((row) => (
                <tr key={`${row.trade_date}-${row.symbol}-${row.action}`}><td>{row.trade_date}</td><td>{row.symbol}</td><td>{row.action}</td><td>{row.reason_code}</td><td>{row.review_status}</td><td>{row.rank ?? "-"}</td></tr>
              ))}
              {!reviews.length ? <tr><td colSpan={6}>暂无复评记录。</td></tr> : null}
            </tbody>
          </table>
        </section>
      </div>

      <section className="pv2-card">
        <div className="pv2-card-head"><div><div className="pv2-kicker">荐股收益</div><h2>Episode 收益与胜率证据</h2><p className="pv2-muted">默认入选价格口径为 next_open_executable；signal_close 与 next_close 仅作敏感性口径。</p></div></div>
        <table className="pv2-table">
          <thead><tr><th>股票</th><th>入选口径</th><th>入选生效日</th><th>退出原因</th><th>涨跌幅</th><th>胜负</th><th>回撤</th></tr></thead>
          <tbody>
            {returns.map((row) => (
              <tr key={row.episode_id}><td>{row.symbol}</td><td>{row.entry_price_basis}</td><td>{row.effective_entry_date}</td><td>{row.exit_reason || row.status}</td><td>{fmtBps(row.return_bps)}</td><td>{row.is_win === true ? "Y" : row.is_win === false ? "N" : "-"}</td><td>{fmtBps(row.max_drawdown_bps)}</td></tr>
            ))}
            {!returns.length ? <tr><td colSpan={7}>暂无 episode 收益。</td></tr> : null}
          </tbody>
        </table>
      </section>

      <section className="pv2-card">
        <div className="pv2-card-head">
          <div><div className="pv2-kicker">生命周期回放</div><h2>历史荐股生命周期回放</h2><p className="pv2-muted">回放仅为投顾事后诊断：不模拟账户，也不产生交易副作用。</p></div>
          <button className="pv2-button-primary" onClick={runReplay} disabled={!selectedProgram || !replayStart || !replayEnd} type="button">执行回放</button>
        </div>
        <div className="pv2-form-grid">
          <label className="pv2-field">开始<input className="pv2-input" type="date" value={replayStart} onChange={(event) => setReplayStart(event.target.value)} /></label>
          <label className="pv2-field">结束<input className="pv2-input" type="date" value={replayEnd} onChange={(event) => setReplayEnd(event.target.value)} /></label>
          <label className="pv2-field">按日期候选 JSON<textarea className="pv2-textarea" rows={8} value={replayCandidatesText} onChange={(event) => setReplayCandidatesText(event.target.value)} /></label>
          <label className="pv2-field">按日期行情 JSON<textarea className="pv2-textarea" rows={8} value={replayMarketText} onChange={(event) => setReplayMarketText(event.target.value)} /></label>
        </div>
        {replayResult ? <div className="pv2-readable-panel">回放状态： {String((replayResult.replay_run as JsonObject | undefined)?.status || "-")} / 胜率 {fmtPct((replayResult.summary as JsonObject | undefined)?.win_rate as number | null | undefined)}</div> : null}
      </section>

      <section className="pv2-card">
        <div className="pv2-card-head">
          <div><div className="pv2-kicker">质量报告</div><h2>事后诊断</h2><p className="pv2-muted">decision input 中禁止未来结果字段；报告不是 validated PnL。</p></div>
          <button className="pv2-button" onClick={buildQualityReport} type="button">生成报告</button>
        </div>
        <textarea className="pv2-textarea" rows={8} value={qualityInput} onChange={(event) => setQualityInput(event.target.value)} />
        {qualityReport ? (
          <div className="pv2-readable-panel" style={{ marginTop: 12 }}>
            样本数 {qualityReport.sample_count}; 买入区间命中 {String(qualityReport.metrics.entry_zone_hit_rate ?? "-")}; 可成交 {String(qualityReport.metrics.entry_zone_fillable_rate ?? "-")}
          </div>
        ) : null}
      </section>
    </main>
  );
}

export default function PaperV2AdvisoryPage() {
  return (
    <Suspense fallback={<main className="pv2-main"><section className="pv2-card">荐股中心加载中...</section></main>}>
      <AdvisoryPageContent />
    </Suspense>
  );
}


