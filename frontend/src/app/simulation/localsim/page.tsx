"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { localSimApi } from "@/lib/simulation/localsim/api";
import type {
  LocalSimAccount,
  LocalSimCutoverReadiness,
  LocalSimRuntimeProfile,
  LocalSimRuntimeProfileVersion,
  RuntimeProfileConfigRequest,
} from "@/lib/simulation/localsim/types";
import styles from "./localsim.module.css";

const errorText = (value: unknown) => value instanceof Error ? value.message : String(value);

function chinaClock() {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(new Date());
  const value = Object.fromEntries(parts.map((item) => [item.type, item.value]));
  return {
    date: `${value.year}-${value.month}-${value.day}`,
    minutes: Number(value.hour) * 60 + Number(value.minute),
  };
}

export default function LocalSimHomePage() {
  const [readiness, setReadiness] = useState<LocalSimCutoverReadiness | null>(null);
  const [accounts, setAccounts] = useState<LocalSimAccount[]>([]);
  const [packages, setPackages] = useState<Array<{ package_id: string; package_name?: string; package_status: string }>>([]);
  const [profiles, setProfiles] = useState<LocalSimRuntimeProfile[]>([]);
  const [versions, setVersions] = useState<LocalSimRuntimeProfileVersion[]>([]);
  const [policies, setPolicies] = useState<Array<{ policy_id: string; policy_name: string; algo_code: string }>>([]);
  const [packageId, setPackageId] = useState("");
  const [profileId, setProfileId] = useState("");
  const [profileVersionId, setProfileVersionId] = useState("");
  const [policyId, setPolicyId] = useState("");
  const [profileName, setProfileName] = useState("LocalSIM 日常配置");
  const [dailyStrategyId, setDailyStrategyId] = useState("daily_selection_v1");
  const [dailyStrategyVersion, setDailyStrategyVersion] = useState("v1");
  const [topK, setTopK] = useState(20);
  const [hmmEnabled, setHmmEnabled] = useState(false);
  const [hmmSnapshot, setHmmSnapshot] = useState("");
  const [hmmPreset, setHmmPreset] = useState("preset_A");
  const [riskJson, setRiskJson] = useState('{"max_position_weight":0.1}');
  const [feeJson, setFeeJson] = useState('{"commission_bps":3}');
  const [accountName, setAccountName] = useState("LocalSIM 模拟盘");
  const [capital, setCapital] = useState("1000000.0000");
  const [effectiveFrom, setEffectiveFrom] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const selectedProfile = useMemo(() => profiles.find((item) => item.profile_id === profileId), [profiles, profileId]);
  const ready = readiness?.ready === true;

  const load = useCallback(async () => {
    setError(null);
    try {
      const [nextReadiness, nextAccounts, nextPackages, calendar] = await Promise.all([
        localSimApi.readiness(),
        localSimApi.listAccounts({ limit: 100 }),
        localSimApi.listPackages(),
        localSimApi.tradingCalendarStatus(),
      ]);
      setReadiness(nextReadiness);
      setAccounts(nextAccounts.items);
      setPackages(nextPackages);
      const query = typeof window === "undefined" ? new URLSearchParams() : new URLSearchParams(window.location.search);
      const queryPackage = query.get("package_id") || "";
      const queryTopK = Number(query.get("top_k") || "");
      setPackageId((current) => current || queryPackage || nextPackages[0]?.package_id || "");
      if (Number.isInteger(queryTopK) && queryTopK > 0 && queryTopK <= 10000) setTopK(queryTopK);
      const clock = chinaClock();
      setEffectiveFrom(
        calendar.is_trading_day && clock.minutes < 9 * 60 + 10
          ? calendar.as_of_date
          : calendar.next_trading_day || "",
      );
    } catch (exc) {
      setError(errorText(exc));
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  useEffect(() => {
    if (!packageId) return;
    Promise.all([localSimApi.listProfiles(packageId), localSimApi.listExecutionPolicies(packageId)])
      .then(([profilePage, policyRows]) => {
        setProfiles(profilePage.items);
        setPolicies(policyRows.filter((item) => item.algo_code?.toUpperCase() === "TWAP"));
        setProfileId((current) => profilePage.items.some((item) => item.profile_id === current)
          ? current
          : profilePage.items[0]?.profile_id || "");
        setPolicyId((current) => policyRows.some((item) => item.policy_id === current)
          ? current
          : policyRows.find((item) => item.algo_code?.toUpperCase() === "TWAP")?.policy_id || "");
      })
      .catch((exc) => setError(errorText(exc)));
  }, [packageId]);

  useEffect(() => {
    if (!profileId) { setVersions([]); setProfileVersionId(""); return; }
    localSimApi.listProfileVersions(profileId).then((page) => {
      setVersions(page.items);
      setProfileVersionId((current) => page.items.some((item) => item.profile_version_id === current)
        ? current
        : page.items[0]?.profile_version_id || "");
    }).catch((exc) => setError(errorText(exc)));
  }, [profileId]);

  async function createProfile() {
    if (!packageId || !profileName.trim()) return;
    setBusy(true); setError(null);
    try {
      const result = await localSimApi.createProfile(packageId, profileName.trim());
      const page = await localSimApi.listProfiles(packageId);
      setProfiles(page.items); setProfileId(result.profile.profile_id);
    } catch (exc) { setError(errorText(exc)); } finally { setBusy(false); }
  }

  async function createProfileVersion() {
    if (!selectedProfile) return;
    setBusy(true); setError(null);
    try {
      const config: RuntimeProfileConfigRequest = {
        schema_version: "localsim_runtime_profile_config_request_v1",
        daily_strategy: {
          strategy_id: dailyStrategyId,
          strategy_version: dailyStrategyVersion,
          top_k: topK,
          industry_filters: [],
          sector_filters: [],
          parameters: {},
        },
        hmm: {
          enabled: hmmEnabled,
          snapshot_id: hmmEnabled ? hmmSnapshot : null,
          model_version: hmmEnabled ? hmmSnapshot : null,
          preset: hmmEnabled ? hmmPreset : null,
          state_mapping: {},
        },
        risk_policy: JSON.parse(riskJson),
        fee_policy: JSON.parse(feeJson),
        runtime_variant_id: null,
        runtime_variant_hash: null,
        notes: "LocalSIM UI validated configuration",
        metadata: {},
      };
      const result = await localSimApi.createProfileVersion(profileId, selectedProfile.version, config);
      const page = await localSimApi.listProfileVersions(profileId);
      setProfiles((items) => items.map((item) => item.profile_id === result.profile.profile_id ? result.profile : item));
      setVersions(page.items); setProfileVersionId(result.version.profile_version_id);
    } catch (exc) { setError(errorText(exc)); } finally { setBusy(false); }
  }

  async function createAccount() {
    if (!packageId || !profileVersionId || !policyId || !effectiveFrom) return;
    setBusy(true); setError(null);
    try {
      await localSimApi.createAccount({
        account_name: accountName,
        package_id: packageId,
        initial_capital: capital,
        runtime_profile_version_id: profileVersionId,
        execution_policy_version_id: policyId,
        effective_from: effectiveFrom,
      });
      setAccounts((await localSimApi.listAccounts({ limit: 100 })).items);
    } catch (exc) { setError(errorText(exc)); } finally { setBusy(false); }
  }

  return <main className={styles.shell}>
    <header className={styles.header}>
      <div><h1>LocalSIM 模拟盘</h1><p>数据、信号与执行分层；实时分钟行情仅由 TDX 路径提供，执行固定使用已验证 TWAP。</p></div>
      <nav className={styles.nav}><Link href="/simulation/localsim/replays">历史回放</Link></nav>
    </header>
    {error && <div className={styles.error}>{error}</div>}
    <div className={readiness?.ready ? styles.ready : styles.blocked}>
      {readiness?.ready ? "切流前置条件已闭合，可以创建新 LocalSIM 账户。" : `切流尚未就绪：${readiness?.blockers?.join("；") || "正在读取"}`}
    </div>
    <div className={styles.grid} style={{ marginTop: 18 }}>
      <section className={`${styles.card} ${styles.half}`}><h2>1. 选择策略包与配置档案</h2>
        <div className={styles.form}>
          <label className={`${styles.field} ${styles.wide}`}>Strategy Package<select value={packageId} onChange={(e) => setPackageId(e.target.value)}><option value="">请选择</option>{packages.map((item) => <option key={item.package_id} value={item.package_id}>{item.package_name || item.package_id} · {item.package_status}</option>)}</select></label>
          <label className={styles.field}>新档案名称<input value={profileName} onChange={(e) => setProfileName(e.target.value)} /></label>
          <label className={styles.field}>现有档案<select value={profileId} onChange={(e) => setProfileId(e.target.value)}><option value="">请选择</option>{profiles.map((item) => <option key={item.profile_id} value={item.profile_id}>{item.profile_name} · v{item.version}</option>)}</select></label>
        </div><div className={styles.actions}><button className={styles.button} disabled={!ready || busy} onClick={createProfile}>创建配置档案</button></div>
      </section>
      <section className={`${styles.card} ${styles.half}`}><h2>2. 追加不可变配置版本</h2>
        <div className={styles.form}>
          <label className={styles.field}>日频策略 ID<input value={dailyStrategyId} onChange={(e) => setDailyStrategyId(e.target.value)} /></label>
          <label className={styles.field}>策略版本<input value={dailyStrategyVersion} onChange={(e) => setDailyStrategyVersion(e.target.value)} /></label>
          <label className={styles.field}>Top K<input type="number" min={1} max={10000} value={topK} onChange={(e) => setTopK(Number(e.target.value))} /></label>
          <label className={styles.field}>HMM<input type="checkbox" checked={hmmEnabled} onChange={(e) => setHmmEnabled(e.target.checked)} /></label>
          {hmmEnabled && <><label className={styles.field}>HMM 快照 ID<input value={hmmSnapshot} onChange={(e) => setHmmSnapshot(e.target.value)} /></label><label className={styles.field}>Preset<input value={hmmPreset} onChange={(e) => setHmmPreset(e.target.value)} /></label></>}
          <label className={styles.field}>风险策略 JSON<textarea value={riskJson} onChange={(e) => setRiskJson(e.target.value)} /></label>
          <label className={styles.field}>费用策略 JSON<textarea value={feeJson} onChange={(e) => setFeeJson(e.target.value)} /></label>
        </div><div className={styles.actions}><button className={styles.button} disabled={!ready || busy || !profileId} onClick={createProfileVersion}>验证并创建版本</button></div>
      </section>
      <section className={styles.card}><h2>3. 创建账户</h2>
        <div className={styles.form}>
          <label className={styles.field}>账户名称<input value={accountName} onChange={(e) => setAccountName(e.target.value)} /></label>
          <label className={styles.field}>初始资金<input value={capital} onChange={(e) => setCapital(e.target.value)} /></label>
          <label className={styles.field}>生效交易日<input value={effectiveFrom} readOnly /></label>
          <label className={styles.field}>配置版本<select value={profileVersionId} onChange={(e) => setProfileVersionId(e.target.value)}><option value="">请选择</option>{versions.map((item) => <option key={item.profile_version_id} value={item.profile_version_id}>v{item.version_no} · {item.profile_version_id}</option>)}</select></label>
          <label className={styles.field}>TWAP 执行策略<select value={policyId} onChange={(e) => setPolicyId(e.target.value)}><option value="">请选择</option>{policies.map((item) => <option key={item.policy_id} value={item.policy_id}>{item.policy_name}</option>)}</select></label>
        </div><div className={styles.actions}><button className={styles.button} disabled={!ready || busy || !profileVersionId || !policyId || !effectiveFrom} onClick={createAccount}>原子创建 LocalSIM</button></div>
      </section>
      <section className={styles.card}><h2>现有账户</h2>
        <table className={styles.table}><thead><tr><th>账户</th><th>策略包</th><th>资金</th><th>状态</th><th>更新时间</th></tr></thead><tbody>{accounts.map((item) => <tr key={item.account_id}><td><Link href={`/simulation/localsim/accounts/${item.account_id}`}>{item.account_name}</Link><div className={`${styles.muted} ${styles.code}`}>{item.account_id}</div></td><td className={styles.code}>{item.package_id}</td><td>{item.initial_capital.toLocaleString()}</td><td><span className={styles.badge}>{item.status}</span></td><td>{new Date(item.updated_at).toLocaleString()}</td></tr>)}</tbody></table>
      </section>
    </div>
  </main>;
}
