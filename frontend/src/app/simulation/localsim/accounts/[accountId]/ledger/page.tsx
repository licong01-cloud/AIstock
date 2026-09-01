"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import { localSimApi } from "@/lib/simulation/localsim/api";
import type { JsonObject } from "@/lib/simulation/localsim/types";
import styles from "../../../localsim.module.css";

export default function LocalSimLedgerPage() {
  const accountId = String(useParams<{ accountId: string }>().accountId || "");
  const [ledger, setLedger] = useState<JsonObject | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => { localSimApi.accountLedger(accountId).then(setLedger).catch((exc) => setError(exc instanceof Error ? exc.message : String(exc))); }, [accountId]);
  return <main className={styles.shell}><header className={styles.header}><div><h1>LocalSIM 账本</h1><p className={styles.code}>{accountId}</p></div><nav className={styles.nav}><Link href={`/simulation/localsim/accounts/${accountId}`}>返回账户</Link></nav></header>{error && <div className={styles.error}>{error}</div>}<section className={styles.card}><pre className={styles.code}>{JSON.stringify(ledger, null, 2)}</pre></section></main>;
}
