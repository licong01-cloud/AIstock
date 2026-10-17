"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { localSimApi } from "@/lib/simulation/localsim/api";
import type { LocalSimRuntimeProfileVersion } from "@/lib/simulation/localsim/types";
import { useLocalSimReadback } from "@/lib/simulation/localsim/use-readback";
import { chinaTime } from "@/lib/simulation/localsim/presentation";
import { LocalSimReadbackStatus } from "../components/readback-status";
import { LocalSimReplayStatus } from "../components/replay-status";
import styles from "../localsim.module.css";

const readReplays = (signal: AbortSignal) => localSimApi.listReplays(undefined, signal);

const dateIso = (value: Date) => {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
};

export default function LocalSimReplaysPage() {
  const readiness = useLocalSimReadback(localSimApi.readiness);
  const scheduler = useLocalSimReadback(localSimApi.schedulerVerification);
  const replayReadback = useLocalSimReadback(readReplays);
  const replays = replayReadback.data?.items || [];
  const [packages, setPackages] = useState<Array<{ package_id: string; package_name?: string }>>([]);
  const [versions, setVersions] = useState<LocalSimRuntimeProfileVersion[]>([]);
  const [policies, setPolicies] = useState<Array<{ policy_id: string; policy_name: string; algo_code: string }>>([]);
  const [packageId, setPackageId] = useState("");
  const [profileVersionId, setProfileVersionId] = useState("");
  const [policyId, setPolicyId] = useState("");
  const [accountName, setAccountName] = useState("LocalSIM 六个月回放");
  const [capital, setCapital] = useState("1000000.0000");
  const [startDate, setStartDate] = useState("");
  const [finishDate, setFinishDate] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    try {
      const yesterday = new Date(); yesterday.setDate(yesterday.getDate() - 1);
      const sixMonthsAgo = new Date(); sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
      const [packageRows, endCalendar, startCalendar] = await Promise.all([
        localSimApi.listPackages(),
        localSimApi.tradingCalendarStatus(dateIso(yesterday)),
        localSimApi.tradingCalendarStatus(dateIso(sixMonthsAgo)),
      ]);
      setPackages(packageRows);
      const queryPackage = new URLSearchParams(window.location.search).get("package_id") || "";
      setPackageId((current) => current || queryPackage || packageRows[0]?.package_id || "");
      setFinishDate((current) => current || endCalendar.latest_completed_trading_day || "");
      setStartDate((current) => current || startCalendar.latest_completed_trading_day || "");
      setError(null);
    } catch (exc) { setError(exc instanceof Error ? exc.message : String(exc)); }
  }
  useEffect(() => { void load(); }, []);
  useEffect(() => {
    if (!packageId) return;
    Promise.all([localSimApi.listProfiles(packageId), localSimApi.listExecutionPolicies(packageId)]).then(async ([profiles, policyRows]) => {
      const allVersions = (await Promise.all(profiles.items.map((profile) => localSimApi.listProfileVersions(profile.profile_id)))).flatMap((page) => page.items);
      setVersions(allVersions); setProfileVersionId(allVersions[0]?.profile_version_id || "");
      const twap = policyRows.filter((item) => item.algo_code?.toUpperCase() === "TWAP");
      setPolicies(twap); setPolicyId(twap[0]?.policy_id || "");
    }).catch((exc) => setError(exc instanceof Error ? exc.message : String(exc)));
  }, [packageId]);

  async function createReplay() {
    setBusy(true); setError(null);
    try {
      await localSimApi.createReplay({
        account_name: accountName,
        package_id: packageId,
        initial_capital: capital,
        runtime_profile_version_id: profileVersionId,
        execution_policy_version_id: policyId,
        effective_from: startDate,
        effective_to: finishDate,
        start_trade_date: startDate,
        end_trade_date: finishDate,
      });
      await Promise.all([replayReadback.refresh(), readiness.refresh()]);
    } catch (exc) { setError(exc instanceof Error ? exc.message : String(exc)); }
    finally { setBusy(false); }
  }

  return <main className={styles.shell}>
    <header className={styles.header}><div><h1>LocalSIM 历史回放</h1><p>使用独立账户和账本回放历史交易；追赶完成后，在安全时点接续实时模拟盘。</p></div><nav className={styles.nav}><Link href="/simulation/localsim">账户与配置</Link></nav></header>
    {error && <div className={styles.error}>{error}</div>}
    <LocalSimReadbackStatus readiness={readiness} scheduler={scheduler} onRefresh={() => {
      void Promise.all([readiness.refresh(), scheduler.refresh(), replayReadback.refresh()]);
    }} />
    <p className={styles.muted}>新回放范围需追赶到最新完成交易日 {finishDate || "读取中"}；现有任务继续使用各自冻结的回放范围。</p>
    <div className={styles.grid}>
      <section className={styles.card}><h2>创建回放</h2><div className={styles.form}>
        <label className={styles.field}>策略包<select value={packageId} onChange={(e) => setPackageId(e.target.value)}>{packages.map((item) => <option key={item.package_id} value={item.package_id}>{item.package_name || item.package_id}</option>)}</select></label>
        <label className={styles.field}>账户名称<input value={accountName} onChange={(e) => setAccountName(e.target.value)} /></label>
        <label className={styles.field}>配置版本<select value={profileVersionId} onChange={(e) => setProfileVersionId(e.target.value)}>{versions.map((item) => <option key={item.profile_version_id} value={item.profile_version_id}>v{item.version_no} · {item.profile_version_id}</option>)}</select></label>
        <label className={styles.field}>TWAP 策略<select value={policyId} onChange={(e) => setPolicyId(e.target.value)}>{policies.map((item) => <option key={item.policy_id} value={item.policy_id}>{item.policy_name}</option>)}</select></label>
        <label className={styles.field}>开始交易日<input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} /></label>
        <label className={styles.field}>结束交易日<input type="date" value={finishDate} onChange={(e) => setFinishDate(e.target.value)} /></label>
        <label className={styles.field}>初始资金<input value={capital} onChange={(e) => setCapital(e.target.value)} /></label>
      </div><div className={styles.actions}><button className={styles.button} disabled={busy || readiness.stale || !!readiness.error || !readiness.data?.ready || !profileVersionId || !policyId || !startDate || !finishDate} onClick={createReplay}>原子创建回放</button></div></section>
      <section className={styles.card}><h2>回放任务</h2>
        {replayReadback.error && <p role="alert" className={styles.error}>回放列表读取失败：{replayReadback.error}；下方为上次成功读取的记录。</p>}
        {replayReadback.stale && <p className={styles.muted}>回放列表尚未获得最新核验，请刷新运行状态。</p>}
        <p className={styles.muted}>列表最近读取：{chinaTime(replayReadback.updatedAt)}</p>
        <table className={styles.table}><thead><tr><th>任务 / 账户</th><th>范围</th><th>进度</th><th>状态 / 原因</th></tr></thead><tbody>{replays.map((item) => <tr key={item.replay_job_id}><td><Link href={`/simulation/localsim/replays/${item.replay_job_id}`}>{item.replay_job_id}</Link><div><Link href={`/simulation/localsim/accounts/${item.simulation_account_id}`}>{item.simulation_account_id}</Link></div></td><td>{item.start_trade_date} → {item.end_trade_date}</td><td><p>已完成：{item.completed_trade_date || "未开始"}</p><p>下一交易日：{item.next_trade_date || "无"}</p></td><td><LocalSimReplayStatus replay={item} /></td></tr>)}</tbody></table>
      </section>
    </div>
  </main>;
}
