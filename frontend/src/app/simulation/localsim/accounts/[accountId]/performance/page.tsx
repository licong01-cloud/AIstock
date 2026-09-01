"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import { localSimApi } from "@/lib/simulation/localsim/api";
import type { JsonObject } from "@/lib/simulation/localsim/types";
import styles from "../../../localsim.module.css";

export default function LocalSimPerformancePage() {
  const accountId = String(useParams<{ accountId: string }>().accountId || "");
  const [performance, setPerformance] = useState<JsonObject | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => { localSimApi.accountPerformance(accountId).then(setPerformance).catch((exc) => setError(exc instanceof Error ? exc.message : String(exc))); }, [accountId]);
  const rows = Array.isArray(performance?.snapshots) ? performance.snapshots as JsonObject[] : [];
  return <main className={styles.shell}><header className={styles.header}><div><h1>LocalSIM 绩效</h1><p className={styles.code}>{accountId}</p></div><nav className={styles.nav}><Link href={`/simulation/localsim/accounts/${accountId}`}>返回账户</Link></nav></header>{error && <div className={styles.error}>{error}</div>}<section className={styles.card}><table className={styles.table}><thead><tr><th>交易日</th><th>NAV</th><th>现金</th><th>市值</th><th>持仓数</th></tr></thead><tbody>{rows.map((row) => <tr key={String(row.snapshot_id)}><td>{String(row.trade_date || "")}</td><td>{String(row.nav ?? "")}</td><td>{String(row.cash ?? "")}</td><td>{String(row.market_value ?? "")}</td><td>{String(row.position_count ?? "")}</td></tr>)}</tbody></table></section></main>;
}
