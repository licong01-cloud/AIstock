"use client";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { localSimApi, request } from "@/lib/simulation/localsim/api";
import { packageLabel, policyLabel, useControlCatalog } from "@/lib/simulation/localsim/control-catalog";
import { human, amount, base, chinaTime, Overview, READ_ERROR, rememberAccount, rememberReplay, safeDate } from "@/lib/simulation/localsim/workbench";
import { useLocalSimReadback } from "@/lib/simulation/localsim/use-readback";
import { LocalSimReadbackStatus } from "../components/readback-status";
import { LocalSimReplayStatus } from "../components/replay-status";
import styles from "../localsim.module.css";

const readReplays=async(signal:AbortSignal)=>{let page=await localSimApi.listReplays(undefined,signal);const items=[...page.items];while(page.next_cursor){if(items.length>10000)throw Error();page=await localSimApi.listReplays(page.next_cursor,signal);items.push(...page.items);}return {...page,items};};
const dateIso=(d:Date)=>d.toLocaleDateString("en-CA",{timeZone:"Asia/Shanghai"});
export default function ReplaysPage(){
  const router=useRouter(),catalog=useControlCatalog(),readiness=useLocalSimReadback(localSimApi.readiness),scheduler=useLocalSimReadback(localSimApi.schedulerVerification),replays=useLocalSimReadback(readReplays);
  const [accounts,setAccounts]=useState<Overview|null>(null),[versionId,setVersionId]=useState(""),[policyId,setPolicyId]=useState("");
  const [name,setName]=useState("历史回放模拟账户"),[capital,setCapital]=useState("5000000"),[start,setStart]=useState(""),[end,setEnd]=useState("");
  const [error,setError]=useState(""),[notice,setNotice]=useState(""),[busy,setBusy]=useState(false);
  useEffect(()=>{let valid=true;const last=new Date();last.setDate(last.getDate()-1);const first=new Date();first.setMonth(first.getMonth()-6);
    Promise.all([request<Overview>(base+"/workbench/overview"),localSimApi.tradingCalendarStatus(dateIso(first)),localSimApi.tradingCalendarStatus(dateIso(last))]).then(([a,s,e])=>{if(valid){setAccounts(a);setStart(s.latest_completed_trading_day||"");setEnd(e.latest_completed_trading_day||"");}}).catch(()=>{if(valid)setError(READ_ERROR);});return()=>{valid=false;};},[]);
  useEffect(()=>{setVersionId(catalog.versions.find(v=>v.validation_status==="VALIDATED")?.profile_version_id||"");setPolicyId(catalog.policies[0]?.policy_id||"");},[catalog.versions,catalog.policies]);
  const ready=readiness.data?.ready&&!readiness.stale&&!readiness.error&&!catalog.loading&&!catalog.error;
  async function create(){if(!ready||busy)return;setBusy(true);setError("");setNotice("");try{
    await localSimApi.createReplay({account_name:name.trim(),package_id:catalog.packageId,initial_capital:capital,runtime_profile_version_id:versionId,execution_policy_version_id:policyId,effective_from:start,effective_to:end,start_trade_date:start,end_trade_date:end});
    await Promise.all([replays.refresh(),readiness.refresh()]);setAccounts(await request<Overview>(base+"/workbench/overview"));setNotice("回放已按正式合同创建；交易与实时接续进度请以原任务状态为准。");
  }catch{setError("创建回放未通过原生服务验证，请核对范围、输入覆盖与配置；未自动改用其他数据源。");}finally{setBusy(false);}}
  return <main className={styles.shell}><header className={styles.header}><div><h1>历史回放</h1><p>独立账户与账本；范围、行情来源和政策按原生合同冻结。</p></div><nav className={styles.nav}><Link href="/simulation/localsim/workbench">交易工作台</Link><Link href="/simulation/localsim/configure">创建与配置</Link></nav></header>
    {(error||catalog.error||replays.error)&&<p role="alert" className={styles.error}>{error||catalog.error||READ_ERROR}</p>}{notice&&<p role="status" className={styles.ready}>{notice}</p>}
    <LocalSimReadbackStatus readiness={readiness} scheduler={scheduler} onRefresh={()=>{catalog.reload();void Promise.all([readiness.refresh(),scheduler.refresh(),replays.refresh()]);}}/>
    <section className={styles.card}><h2>创建新的历史回放</h2><p className={styles.muted}>新任务使用原有数据集文件来源；覆盖或政策不满足时由服务拒绝，不静默回退数据库。此表单不会重置已有任务。</p><div className={styles.form}>
      <label className={styles.field}>策略<select value={catalog.packageId} onChange={e=>catalog.setPackageId(e.target.value)}><option value="">请选择策略</option>{catalog.packages.map((p,i)=><option key={p.package_id} value={p.package_id}>{packageLabel(p,i)}</option>)}</select></label>
      <label className={styles.field}>账户名称<input value={name} onChange={e=>setName(e.target.value)} maxLength={100}/></label><label className={styles.field}>初始资金（元）<input type="number" min="1" step="0.01" value={capital} onChange={e=>setCapital(e.target.value)}/></label>
      <label className={styles.field}>已验证的配置版本<select value={versionId} onChange={e=>setVersionId(e.target.value)}><option value="">请选择配置</option>{catalog.versions.filter(v=>v.validation_status==="VALIDATED").map(v=><option key={v.profile_version_id} value={v.profile_version_id}>{v.label}</option>)}</select></label>
      <label className={styles.field}>均匀分时执行规则<select value={policyId} onChange={e=>setPolicyId(e.target.value)}><option value="">请选择执行规则</option>{catalog.policies.map((p,i)=><option key={p.policy_id} value={p.policy_id}>{policyLabel(p,i)}</option>)}</select></label>
      <label className={styles.field}>回放开始日<input type="date" value={start} onChange={e=>setStart(e.target.value)}/></label><label className={styles.field}>回放截止日<input type="date" value={end} onChange={e=>setEnd(e.target.value)}/></label>
    </div><div className={styles.actions}><button className={styles.button} disabled={!ready||busy||!versionId||!policyId||!start||!end||start>end||!capital||Number(capital)<=0||human(name,"")!==name.trim()} onClick={()=>void create()}>创建历史回放</button></div></section>
    <section className={styles.card} style={{marginTop:16}}><h2>已有回放与账户</h2><p className={styles.muted}>最近读取 {chinaTime(replays.updatedAt)}；已有任务保持冻结范围与原来源。{replays.stale?"状态尚待最新核验。":""}</p><div style={{overflowX:"auto"}}><table className={styles.table}><thead><tr><th>账户与初始资金</th><th>状态与处理事项</th><th>计划区间</th><th>完成 / 下一日</th><th>操作</th></tr></thead><tbody>{replays.data?.items.map(r=>{const a=accounts?.items.find(a=>a.key===r.simulation_account_id);return <tr key={r.replay_job_id}><td>{human(a?.summary.name,"历史模拟账户")}<small>{human(a?.summary.strategy,"策略名称待完善")}</small><p>{amount(a?.summary.initial_capital)} 元</p></td><td><LocalSimReplayStatus replay={r}/></td><td>{safeDate(r.start_trade_date)} 至 {safeDate(r.end_trade_date)}</td><td>{safeDate(r.completed_trade_date)}<p>下一日 {safeDate(r.next_trade_date)}</p></td><td><div className={styles.actions}><button className={styles.button} onClick={()=>{rememberReplay(r.replay_job_id);router.push("/simulation/localsim/replays/details");}}>回放详情</button><button className={styles.button+" "+styles.secondary} onClick={()=>{rememberAccount(r.simulation_account_id);router.push("/simulation/localsim/workbench");}}>账户持仓</button></div></td></tr>;})}</tbody></table></div>{!replays.data?.items.length&&<p>{replays.data?"当前完整列表没有回放任务。":"正在读取回放列表…"}</p>}</section>
  </main>;
}