"use client";
import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { localSimApi } from "@/lib/simulation/localsim/api";
import { useLocalSimReadback } from "@/lib/simulation/localsim/use-readback";
import { chinaTime, READ_ERROR, rememberAccount, rememberReplay, safeDate } from "@/lib/simulation/localsim/workbench";
import { LocalSimReplayStatus } from "../../components/replay-status";
import styles from "../../localsim.module.css";
export default function ReplayDetailPage(){
  const router=useRouter(),segment=String(useParams<{replayJobId:string}>().replayJobId||"");
  const [id,setId]=useState(""),[error,setError]=useState(""),[busy,setBusy]=useState(false);
  useEffect(()=>{setId(segment==="details"?sessionStorage.getItem("localsim-replay-selection")||"":segment);},[segment]);
  const load=useCallback(async(signal:AbortSignal)=>{if(!id)throw Error();const value=await localSimApi.getReplay(id,signal);if(!value.replay||value.replay.replay_job_id!==id)throw Error();if(!signal.aborted){rememberReplay(id);if(segment!=="details")history.replaceState(history.state,"","/simulation/localsim/replays/details");}return value.replay;},[id,segment]);
  const readback=useLocalSimReadback(load),r=readback.data;
  async function cancel(){if(!r||busy||readback.stale||readback.error)return;setBusy(true);setError("");try{await localSimApi.cancelReplay(id,r.version);await readback.refresh();}catch{setError("取消操作未通过原任务验证，请刷新后核查状态。");}finally{setBusy(false);}}
  return <main className={styles.shell}><header className={styles.header}><div><h1>回放任务详情</h1><p>计划、执行与估值分别核验，页面读取不会恢复或重建回放。</p></div><nav className={styles.nav}><Link href="/simulation/localsim/replays">回放列表</Link><Link href="/simulation/localsim/workbench">交易工作台</Link></nav></header>
    {(error||readback.error)&&<p role="alert" className={styles.error}>{error||READ_ERROR}</p>}<div className={styles.actions}><button className={styles.button} disabled={readback.refreshing||!id} onClick={()=>void readback.refresh()}>刷新回放状态</button><span>页面最近读取 {chinaTime(readback.updatedAt)}</span></div>{readback.stale&&<p className={styles.muted}>尚未取得最新核验，控制操作暂不可用。</p>}
    <section className={styles.card} style={{marginTop:16}}>{r?<><LocalSimReplayStatus replay={r}/><p>计划区间：{safeDate(r.start_trade_date)} 至 {safeDate(r.end_trade_date)}</p><p>已完成交易日：{safeDate(r.completed_trade_date)}；下一交易日：{safeDate(r.next_trade_date)}</p><p>实时接续日：{safeDate(r.activation_trade_date)}</p><p>行情来源：{r.historical_source_id==="dataset_files.frozen_market.v1"?"数据集文件":r.historical_source_id==="market.kline_minute_raw.v1"?"行情数据库":"来源待核验"}</p><div className={styles.actions}><button className={styles.button} onClick={()=>{rememberAccount(r.simulation_account_id,"events");router.push("/simulation/localsim/workbench");}}>查看本账户进度与资产</button>{!["LIVE_ACTIVE","FAILED_TERMINAL","CANCELLED"].includes(r.status)&&<button className={styles.button+" "+styles.danger} disabled={busy||readback.stale||!!readback.error} onClick={()=>void cancel()}>取消回放</button>}</div></>:<p>{id?"正在核验原回放…":"请从回放列表选择任务；未自动选择其他任务。"}</p>}</section>
  </main>;
}