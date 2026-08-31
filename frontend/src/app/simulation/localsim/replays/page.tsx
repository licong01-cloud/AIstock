"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { localSimApi } from "@/lib/simulation/localsim/api";
import type { LocalSimCutoverReadiness, LocalSimReplay, LocalSimRuntimeProfileVersion } from "@/lib/simulation/localsim/types";
import styles from "../localsim.module.css";

const dateIso = (value: Date) => {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
};

export default function LocalSimReplaysPage() {
  const [replays, setReplays] = useState<LocalSimReplay[]>([]);
  const [readiness, setReadiness] = useState<LocalSimCutoverReadiness | null>(null);
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
      const [replayPage, packageRows, nextReadiness, endCalendar, startCalendar] = await Promise.all([
        localSimApi.listReplays(),
        localSimApi.listPackages(),
        localSimApi.readiness(),
        localSimApi.tradingCalendarStatus(dateIso(yesterday)),
        localSimApi.tradingCalendarStatus(dateIso(sixMonthsAgo)),
      ]);
      setReplays(replayPage.items); setPackages(packageRows); setReadiness(nextReadiness);
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
      await load();
    } catch (exc) { setError(exc instanceof Error ? exc.message : String(exc)); }
    finally { setBusy(false); }
  }

  return <main className={styles.shell}>
    <header className={styles.header}><div><h1>LocalSIM 历史回放</h1><p>独立账户、独立账本与 writer scope；追赶完成后由生命周期 owner 自动准备切入实时运行。</p></div><nav className={styles.nav}><Link href="/simulation/localsim">账户与配置</Link></nav></header>
    {error && <div className={styles.error}>{error}</div>}
    <div className={readiness?.ready ? styles.ready : styles.blocked}>{readiness?.ready ? `回放范围必须追赶到最新完成交易日 ${finishDate || "-"}。` : `切流尚未就绪：${readiness?.blockers?.join("；") || "正在读取"}`}</div>
    <div className={styles.grid}>
      <section className={styles.card}><h2>创建回放</h2><div className={styles.form}>
        <label className={styles.field}>策略包<select value={packageId} onChange={(e) => setPackageId(e.target.value)}>{packages.map((item) => <option key={item.package_id} value={item.package_id}>{item.package_name || item.package_id}</option>)}</select></label>
        <label className={styles.field}>账户名称<input value={accountName} onChange={(e) => setAccountName(e.target.value)} /></label>
        <label className={styles.field}>配置版本<select value={profileVersionId} onChange={(e) => setProfileVersionId(e.target.value)}>{versions.map((item) => <option key={item.profile_version_id} value={item.profile_version_id}>v{item.version_no} · {item.profile_version_id}</option>)}</select></label>
        <label className={styles.field}>TWAP 策略<select value={policyId} onChange={(e) => setPolicyId(e.target.value)}>{policies.map((item) => <option key={item.policy_id} value={item.policy_id}>{item.policy_name}</option>)}</select></label>
        <label className={styles.field}>开始交易日<input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} /></label>
        <label className={styles.field}>结束交易日<input type="date" value={finishDate} onChange={(e) => setFinishDate(e.target.value)} /></label>
        <label className={styles.field}>初始资金<input value={capital} onChange={(e) => setCapital(e.target.value)} /></label>
      </div><div className={styles.actions}><button className={styles.button} disabled={busy || !readiness?.ready || !profileVersionId || !policyId || !startDate || !finishDate} onClick={createReplay}>原子创建回放</button></div></section>
      <section className={styles.card}><h2>回放任务</h2><table className={styles.table}><thead><tr><th>任务</th><th>范围</th><th>进度</th><th>状态</th></tr></thead><tbody>{replays.map((item) => <tr key={item.replay_job_id}><td><Link href={`/simulation/localsim/replays/${item.replay_job_id}`}>{item.replay_job_id}</Link></td><td>{item.start_trade_date} → {item.end_trade_date}</td><td>{item.completed_trade_date || "未开始"}</td><td><span className={styles.badge}>{item.status}</span></td></tr>)}</tbody></table></section>
    </div>
  </main>;
}
