"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { localSimApi } from "@/lib/simulation/localsim/api";
import type { JsonObject, LocalSimControlResponse } from "@/lib/simulation/localsim/types";
import styles from "../../localsim.module.css";

export default function LocalSimAccountPage() {
  const accountId = String(useParams<{ accountId: string }>().accountId || "");
  const [detail, setDetail] = useState<LocalSimControlResponse | null>(null);
  const [runs, setRuns] = useState<JsonObject[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const load = useCallback(async () => {
    try {
      const [nextDetail, nextRuns] = await Promise.all([
        localSimApi.getAccount(accountId),
        localSimApi.accountRuns(accountId),
      ]);
      setDetail(nextDetail); setRuns(nextRuns); setError(null);
    } catch (exc) { setError(exc instanceof Error ? exc.message : String(exc)); }
  }, [accountId]);
  useEffect(() => { void load(); }, [load]);

  async function transition(action: "pause" | "resume" | "retire") {
    if (!detail?.account) return;
    setBusy(true);
    try { await localSimApi.transitionAccount(accountId, action, detail.account.version); await load(); }
    catch (exc) { setError(exc instanceof Error ? exc.message : String(exc)); }
    finally { setBusy(false); }
  }

  const account = detail?.account;
  return <main className={styles.shell}>
    <header className={styles.header}><div><h1>{account?.account_name || "LocalSIM 账户"}</h1><p className={styles.code}>{accountId}</p></div><nav className={styles.nav}><Link href="/simulation/localsim">返回账户列表</Link><Link href={`/simulation/localsim/accounts/${accountId}/ledger`}>账本</Link><Link href={`/simulation/localsim/accounts/${accountId}/performance`}>绩效</Link></nav></header>
    {error && <div className={styles.error}>{error}</div>}
    <div className={styles.grid}>
      <section className={`${styles.card} ${styles.half}`}><h2>账户状态</h2>{account ? <><p><span className={styles.badge}>{account.status}</span> · CAS v{account.version}</p><p>策略包：<span className={styles.code}>{account.package_id}</span></p><p>初始资金：{account.initial_capital.toLocaleString()}</p><div className={styles.actions}>{account.status === "ACTIVE" && <button className={styles.button} disabled={busy} onClick={() => transition("pause")}>暂停</button>}{account.status === "PAUSED" && <button className={styles.button} disabled={busy} onClick={() => transition("resume")}>恢复</button>}{account.status !== "RETIRED" && <button className={`${styles.button} ${styles.danger}`} disabled={busy} onClick={() => transition("retire")}>退役</button>}</div></> : <p>读取中…</p>}</section>
      <section className={`${styles.card} ${styles.half}`}><h2>不可变身份</h2><pre className={styles.code}>{JSON.stringify({ ledger_scope: detail?.ledger_scope, manifest_sha256: account?.manifest_sha256, admission_receipt_id: account?.admission_receipt_id }, null, 2)}</pre></section>
      <section className={styles.card}><h2>运行记录</h2><table className={styles.table}><thead><tr><th>日期</th><th>状态</th><th>类型</th><th>Run ID</th></tr></thead><tbody>{runs.map((run) => <tr key={String(run.run_id)}><td>{String(run.trade_date || "")}</td><td><span className={styles.badge}>{String(run.status || "")}</span></td><td>{String(run.run_kind || "")}</td><td className={styles.code}>{String(run.run_id || "")}</td></tr>)}</tbody></table></section>
    </div>
  </main>;
}
