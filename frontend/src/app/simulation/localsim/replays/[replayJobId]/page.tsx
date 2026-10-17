"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useState } from "react";
import { Button } from "@/components/ui/button";
import { localSimApi } from "@/lib/simulation/localsim/api";
import { useLocalSimReadback } from "@/lib/simulation/localsim/use-readback";
import { chinaTime } from "@/lib/simulation/localsim/presentation";
import { LocalSimReplayStatus } from "../../components/replay-status";
import styles from "../../localsim.module.css";

export default function LocalSimReplayDetailPage() {
  const replayJobId = String(useParams<{ replayJobId: string }>().replayJobId || "");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const load = useCallback(async (signal: AbortSignal) => {
    const value = await localSimApi.getReplay(replayJobId, signal);
    if (!value.replay) throw new Error("回放记录返回为空，请重试");
    if (value.replay.replay_job_id !== replayJobId) throw new Error("回放任务身份不一致，请重新核验");
    return value.replay;
  }, [replayJobId]);
  const readback = useLocalSimReadback(load);
  const replay = readback.data;

  async function cancel() {
    if (!replay || readback.stale || readback.error || busy) return;
    setBusy(true); setError(null);
    try { await localSimApi.cancelReplay(replayJobId, replay.version); await readback.refresh(); }
    catch (exc) { setError(exc instanceof Error ? exc.message : String(exc)); }
    finally { setBusy(false); }
  }

  return <main className={styles.shell}>
    <header className={styles.header}><div><h1>回放任务</h1><p className={styles.code}>{replayJobId}</p></div><nav className={styles.nav}><Link href="/simulation/localsim/replays">返回回放列表</Link>{replay && <Link href={`/simulation/localsim/accounts/${replay.simulation_account_id}`}>查看账户</Link>}</nav></header>
    {error && <div role="alert" className={styles.error}>{error}</div>}
    {readback.error && <div role="alert" className={styles.error}>回放状态读取失败：{readback.error}</div>}
    <div className={styles.actions}><Button className={styles.statusRefresh} variant="outline" size="sm" disabled={readback.refreshing} onClick={() => { void readback.refresh(); }}>刷新回放状态</Button><p className={styles.muted}>最近读取：{chinaTime(readback.updatedAt)}；页面可见时每 5 秒刷新。</p></div>
    {readback.stale && <p className={styles.muted}>尚未获得最新核验，已有内容仅供参考，控制操作暂不可用。</p>}
    <section className={styles.card}>{replay ? <>
      <LocalSimReplayStatus replay={replay} />
      <p>范围：{replay.start_trade_date} → {replay.end_trade_date}</p>
      <p>已完成：{replay.completed_trade_date || "尚未开始"}；下一交易日：{replay.next_trade_date || "无"}</p>
      {!["LIVE_ACTIVE", "FAILED_TERMINAL", "CANCELLED"].includes(replay.status) && <button className={`${styles.button} ${styles.danger}`} disabled={busy || readback.stale || !!readback.error} onClick={cancel}>取消回放</button>}
    </> : <p>尚未读到回放记录。</p>}</section>
  </main>;
}
