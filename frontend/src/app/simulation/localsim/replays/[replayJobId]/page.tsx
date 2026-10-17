"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { localSimApi } from "@/lib/simulation/localsim/api";
import type { LocalSimReplay } from "@/lib/simulation/localsim/types";
import styles from "../../localsim.module.css";

export default function LocalSimReplayDetailPage() {
  const replayJobId = String(useParams<{ replayJobId: string }>().replayJobId || "");
  const [replay, setReplay] = useState<LocalSimReplay | null>(null);
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(() => localSimApi.getReplay(replayJobId).then((value) => setReplay(value.replay || null)).catch((exc) => setError(exc instanceof Error ? exc.message : String(exc))), [replayJobId]);
  useEffect(() => { void load(); const timer = window.setInterval(load, 5000); return () => window.clearInterval(timer); }, [load]);
  async function cancel() { if (!replay) return; try { await localSimApi.cancelReplay(replayJobId, replay.version); await load(); } catch (exc) { setError(exc instanceof Error ? exc.message : String(exc)); } }
  return <main className={styles.shell}><header className={styles.header}><div><h1>回放任务</h1><p className={styles.code}>{replayJobId}</p></div><nav className={styles.nav}><Link href="/simulation/localsim/replays">返回回放列表</Link>{replay && <Link href={`/simulation/localsim/accounts/${replay.simulation_account_id}`}>查看账户</Link>}</nav></header>{error && <div className={styles.error}>{error}</div>}<section className={styles.card}>{replay ? <><p><span className={styles.badge}>{replay.status}</span> · CAS v{replay.version}</p><p>范围：{replay.start_trade_date} → {replay.end_trade_date}</p><p>已完成：{replay.completed_trade_date || "尚未开始"}；下一交易日：{replay.next_trade_date || "无"}</p><pre className={styles.code}>{JSON.stringify(replay.failure_context || {}, null, 2)}</pre>{!["LIVE_ACTIVE", "FAILED_TERMINAL", "CANCELLED"].includes(replay.status) && <button className={`${styles.button} ${styles.danger}`} onClick={cancel}>取消回放</button>}</> : <p>读取中…</p>}</section></main>;
}
