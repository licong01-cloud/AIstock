"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { localSimApi } from "@/lib/simulation/localsim/api";
import { configRequest, loadHmmChoices, packageLabel, policyLabel, profileLabel, useControlCatalog, type HmmChoice } from "@/lib/simulation/localsim/control-catalog";
import { human, READ_ERROR, safeDate } from "@/lib/simulation/localsim/workbench";
import { useLocalSimReadback } from "@/lib/simulation/localsim/use-readback";
import { LocalSimReadbackStatus } from "../components/readback-status";
import styles from "../localsim.module.css";

export default function ConfigurePage(){
  const catalog=useControlCatalog(),readiness=useLocalSimReadback(localSimApi.readiness),scheduler=useLocalSimReadback(localSimApi.schedulerVerification);
  const [profileId,setProfileId]=useState(""),[versionId,setVersionId]=useState(""),[policyId,setPolicyId]=useState("");
  const [profileName,setProfileName]=useState("日常模拟配置"),[accountName,setAccountName]=useState("日常模拟账户"),[capital,setCapital]=useState("5000000");
  const [effective,setEffective]=useState(""),[config,setConfig]=useState(configRequest()),[topK,setTopK]=useState(20),[weight,setWeight]=useState("0.1"),[commission,setCommission]=useState("3");
  const [hmm,setHmm]=useState<HmmChoice[]>([]),[hmmError,setHmmError]=useState(""),[hmmRevision,setHmmRevision]=useState(0);
  const [error,setError]=useState(""),[notice,setNotice]=useState(""),[busy,setBusy]=useState(false);
  useEffect(()=>{let valid=true;localSimApi.tradingCalendarStatus().then(c=>{
    const parts=Object.fromEntries(new Intl.DateTimeFormat("en-CA",{timeZone:"Asia/Shanghai",hour:"2-digit",minute:"2-digit",hourCycle:"h23"}).formatToParts(new Date()).map(p=>[p.type,p.value]));
    if(valid)setEffective(c.is_trading_day&&Number(parts.hour)*60+Number(parts.minute)<550?c.as_of_date:c.next_trading_day||"");
  }).catch(()=>{if(valid)setError(READ_ERROR);});return()=>{valid=false;};},[]);
  useEffect(()=>{setProfileId(catalog.profiles[0]?.profile_id||"");},[catalog.profiles]);
  useEffect(()=>{setVersionId(catalog.versions.find(v=>v.profile_id===profileId)?.profile_version_id||"");},[catalog.versions,profileId]);
  useEffect(()=>{setPolicyId(catalog.policies[0]?.policy_id||"");},[catalog.policies]);
  useEffect(()=>{const c=configRequest(catalog.versions.find(v=>v.profile_version_id===versionId));setConfig(c);let preferred:number|undefined;try{const v=JSON.parse(sessionStorage.getItem("localsim-configure-selection")||"null");if(v?.packageId===catalog.packageId&&Number.isInteger(v.topK)&&v.topK>0&&v.topK<=10000)preferred=v.topK;}catch{}setTopK(preferred||c.daily_strategy.top_k);setWeight(String(c.risk_policy.max_position_weight??""));setCommission(String(c.fee_policy.commission_bps??""));},[versionId,catalog.versions,catalog.packageId]);
  useEffect(()=>{if(!config.hmm.enabled)return;let valid=true;loadHmmChoices().then(v=>{if(valid){setHmm(v);setHmmError("");}}).catch(()=>{if(valid)setHmmError("市场状态模型目录未读取成功；不能使用未核验的快照。");});return()=>{valid=false;};},[config.hmm.enabled,hmmRevision]);
  const ready=readiness.data?.ready===true&&!readiness.stale&&!readiness.error&&!catalog.loading&&!catalog.error;
  async function perform(action:()=>Promise<void>){setBusy(true);setError("");setNotice("");try{await action();}catch{setError("操作未通过原生服务验证，请刷新配置与运行状态后核对；未将失败视为成功。");}finally{setBusy(false);}}
  const selectedHmm=hmm.find(h=>h.snapshot===config.hmm.snapshot_id);
  return <main className={styles.shell}>
    <header className={styles.header}><div><h1>模拟账户创建与配置</h1><p>选择业务规则；原生服务验证冻结身份、不可变版本及生效日期。</p></div><nav className={styles.nav}><Link href="/simulation/localsim/workbench">交易工作台</Link><Link href="/simulation/localsim/replays">历史回放</Link></nav></header>
    {(error||catalog.error)&&<p role="alert" className={styles.error}>{error||catalog.error}</p>}{catalog.error&&<button className={styles.button} onClick={catalog.reload}>重新读取配置目录</button>}{notice&&<p role="status" className={styles.ready}>{notice}</p>}
    <LocalSimReadbackStatus readiness={readiness} scheduler={scheduler} onRefresh={()=>{void Promise.all([readiness.refresh(),scheduler.refresh()]);}}/>
    <div className={styles.grid}><section className={`${styles.card} ${styles.half}`}><h2>选择策略与配置档案</h2><div className={styles.form}>
      <label className={`${styles.field} ${styles.wide}`}>策略<select value={catalog.packageId} onChange={e=>catalog.setPackageId(e.target.value)}><option value="">请选择策略</option>{catalog.packages.map((p,i)=><option key={p.package_id} value={p.package_id}>{packageLabel(p,i)}</option>)}</select></label>
      <label className={styles.field}>配置档案<select value={profileId} onChange={e=>setProfileId(e.target.value)}><option value="">请选择配置档案</option>{catalog.profiles.map((p,i)=><option key={p.profile_id} value={p.profile_id}>{profileLabel(p,i)}</option>)}</select></label>
      <label className={styles.field}>新档案名称<input value={profileName} onChange={e=>setProfileName(e.target.value)} maxLength={100}/></label>
    </div><div className={styles.actions}><button className={styles.button} disabled={!ready||busy||!catalog.packageId||human(profileName,"")!==profileName.trim()} onClick={()=>void perform(async()=>{await localSimApi.createProfile(catalog.packageId,profileName.trim());catalog.reload();setNotice("配置档案已创建，请选择档案并追加经过验证的规则版本。");})}>创建配置档案</button></div></section>
    <section className={`${styles.card} ${styles.half}`}><h2>追加经过验证的规则版本</h2><p className={styles.muted}>基于选定版本保留冻结策略、行业筛选和其他原生规则；更改业务字段后创建新版本。</p><div className={styles.form}>
      <label className={`${styles.field} ${styles.wide}`}>规则来源<select value={versionId} onChange={e=>setVersionId(e.target.value)}><option value="">标准逐日选股规则</option>{catalog.versions.filter(v=>v.profile_id===profileId).map(v=><option key={v.profile_version_id} value={v.profile_version_id}>{v.label}</option>)}</select></label>
      <label className={styles.field}>目标持仓股票数<input type="number" min={1} max={10000} value={topK} onChange={e=>setTopK(Number(e.target.value))}/></label>
      <label className={styles.field}>单只股票权重上限（比例）<input type="number" min={0} max={1} step="0.01" value={weight} onChange={e=>setWeight(e.target.value)}/></label>
      <label className={styles.field}>配置佣金费率（万分之一）<input type="number" min={0} step="0.01" value={commission} onChange={e=>setCommission(e.target.value)}/></label>
      <label className={styles.field}>市场状态模型<input type="checkbox" checked={config.hmm.enabled} onChange={e=>setConfig(c=>({...c,hmm:e.target.checked?{...c.hmm,enabled:true}:{enabled:false,snapshot_id:null,model_version:null,preset:null,state_mapping:{}}}))}/></label>
      {config.hmm.enabled&&<><label className={`${styles.field} ${styles.wide}`}>已训练的市场状态模型<select value={config.hmm.snapshot_id||""} onChange={e=>setConfig(c=>({...c,hmm:{...c.hmm,snapshot_id:e.target.value,model_version:e.target.value,preset:hmm.find(h=>h.snapshot===e.target.value)?.presets[0]?.key||null}}))}><option value="">请选择合法模型快照</option>{config.hmm.snapshot_id&&!selectedHmm&&<option value={config.hmm.snapshot_id}>原版本冻结模型（提交时重新校验）</option>}{hmm.map(h=><option key={h.snapshot} value={h.snapshot}>{h.label}</option>)}</select></label>
      <label className={styles.field}>市场状态信号规则<select value={config.hmm.preset||""} onChange={e=>setConfig(c=>({...c,hmm:{...c.hmm,preset:e.target.value}}))}><option value="">请选择信号规则</option>{config.hmm.preset&&!selectedHmm?.presets.some(p=>p.key===config.hmm.preset)&&<option value={config.hmm.preset}>原版本冻结信号规则</option>}{selectedHmm?.presets.map(p=><option key={p.key} value={p.key}>{p.label}</option>)}</select></label>{hmmError&&<p role="alert">{hmmError}<button onClick={()=>setHmmRevision(v=>v+1)}>重新读取模型</button></p>}</>}
    </div><p className={styles.muted}>成交费用仍以执行政策与实际账本为准；这里不覆盖原引擎最低佣金及税费规则。</p><button className={styles.button} disabled={!ready||busy||!profileId||!Number.isInteger(topK)||topK<1||topK>10000||!weight||Number(weight)<0||Number(weight)>1||!commission||Number(commission)<0||(config.hmm.enabled&&(!config.hmm.snapshot_id||!config.hmm.preset))} onClick={()=>void perform(async()=>{
      const p=catalog.profiles.find(v=>v.profile_id===profileId);if(!p)throw Error();
      await localSimApi.createProfileVersion(profileId,p.version,{...config,daily_strategy:{...config.daily_strategy,top_k:topK},risk_policy:{...config.risk_policy,max_position_weight:Number(weight)},fee_policy:{...config.fee_policy,commission_bps:Number(commission)}});catalog.reload();setNotice("规则版本已通过原生验证并创建，请选择对应版本。");
    })}>验证并创建版本</button></section>
    <section className={styles.card}><h2>创建日常模拟账户</h2><div className={styles.form}>
      <label className={styles.field}>账户名称<input value={accountName} onChange={e=>setAccountName(e.target.value)} maxLength={100}/></label><label className={styles.field}>初始资金（元）<input type="number" min="1" step="0.01" value={capital} onChange={e=>setCapital(e.target.value)}/></label>
      <label className={styles.field}>生效交易日<input value={safeDate(effective)} readOnly/></label><label className={styles.field}>绑定规则版本<select value={versionId} onChange={e=>setVersionId(e.target.value)}><option value="">请选择已验证规则</option>{catalog.versions.filter(v=>v.profile_id===profileId&&v.validation_status==="VALIDATED").map(v=><option key={v.profile_version_id} value={v.profile_version_id}>{v.label}</option>)}</select></label>
      <label className={styles.field}>均匀分时执行规则<select value={policyId} onChange={e=>setPolicyId(e.target.value)}><option value="">请选择执行规则</option>{catalog.policies.map((p,i)=><option key={p.policy_id} value={p.policy_id}>{policyLabel(p,i)}</option>)}</select></label>
    </div><div className={styles.actions}><button className={styles.button} disabled={!ready||busy||!versionId||!policyId||!effective||Number(capital)<=0||human(accountName,"")!==accountName.trim()} onClick={()=>void perform(async()=>{await localSimApi.createAccount({account_name:accountName.trim(),package_id:catalog.packageId,initial_capital:capital,runtime_profile_version_id:versionId,execution_policy_version_id:policyId,effective_from:effective});await readiness.refresh();setNotice("账户已按原生合同创建，可返回工作台查看。该提示不代表已经完成交易或获得收益。");})}>创建模拟账户</button><Link href="/simulation/localsim/workbench">查看账户与资产</Link></div></section></div>
  </main>;
}
