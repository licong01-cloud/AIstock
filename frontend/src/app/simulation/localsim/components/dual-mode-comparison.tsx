"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { amount, chinaTime, DualModeDetail, DualModeStudy, DualModeView, human, profitAmount, readDualModeDetail, readDualModeStudies, readDualModeView, safeDate } from "@/lib/simulation/localsim/workbench";
import styles from "./trading-workbench.module.css";

export function DualModeStudyList(){
 const [items,setItems]=useState<DualModeStudy[]>([]),[error,setError]=useState("");
 useEffect(()=>{const c=new AbortController();void readDualModeStudies(c.signal).then(r=>{if(!c.signal.aborted)setItems(r);}).catch(()=>{if(!c.signal.aborted)setError("对比研究本次读取失败，现有策略账户仍可正常查看。");});return()=>c.abort();},[]);
 return <section className={styles.panel} aria-label="原生与人工模拟对比"><h2>原生执行与模拟人工</h2><p>独立账户自动比较收益、回撤与人工操作负担；不改变现有策略账户。</p>
 {error&&<p role="alert">{error}</p>}{!error&&!items.length&&<p>尚无已登记的双模式研究。原生模拟盘继续按现有配置运行。</p>}
 {items.map(s=><article key={s.key} className={styles.researchCard}><h3>{human(s.name,"策略包对比研究")}</h3><p>{safeDate(s.start_date)} 至 {safeDate(s.end_date)} · {s.account_count}个独立账户 · {human(s.purpose,"历史研究")} · {human(s.status,"状态待核验")}</p><Link href={`/simulation/localsim/comparisons/${encodeURIComponent(s.key)}`}>查看两种模式与账户详情</Link></article>)}</section>;
}

const views=[['positions','持仓'],['trades','历史成交'],['orders','委托记录'],['actions','人工操作'],['pnl','区间盈亏']] as const;
const columns:Record<string,[string,string,string][]>= {
 positions:[['name','证券','text'],['quantity','持仓数量','money'],['available','可卖数量','money'],['cost','成本价','money'],['price','估值价','money'],['value','持仓资产','money'],['pnl','浮动盈亏','profit']],
 trades:[['time','成交时间','time'],['name','证券','text'],['side','方向','text'],['quantity','数量','money'],['price','成交价','money'],['fees','实际费用','money']],
 orders:[['time','委托时间','time'],['name','证券','text'],['side','方向','text'],['quantity','委托数量','money'],['filled','成交数量','money'],['status','状态','text']],
 actions:[['visible_time','首次建议','time'],['ready_time','最早可操作','time'],['dispatch_time','实际操作','time'],['deadline_time','有效截止','time'],['name','证券','text'],['kind','操作','text'],['status','状态','text'],['waiting_seconds','响应与排队秒数','money']],
 pnl:[['date','日期','date'],['opening_nav','期初资产','money'],['nav','期末资产','money'],['pnl','当日净盈亏','profit'],['fees','实际费用','money']]
};

export function DualModeComparison({studyId}:{studyId:string}){
 const [detail,setDetail]=useState<DualModeDetail|null>(null),[account,setAccount]=useState(""),[view,setView]=useState("positions");
 const [start,setStart]=useState(""),[end,setEnd]=useState(""),[data,setData]=useState<DualModeView|null>(null),[error,setError]=useState("");
 const [mode,setMode]=useState("全部模式"),[refresh,setRefresh]=useState(0),[busy,setBusy]=useState(false);
 const moreRequest=useRef<AbortController|null>(null);
 useEffect(()=>{const c=new AbortController();setDetail(null);setAccount("");setData(null);setError("");void readDualModeDetail(studyId,c.signal).then(r=>{if(!c.signal.aborted){setDetail(r);setAccount(r.accounts[0]?.key||"");}}).catch(()=>{if(!c.signal.aborted)setError("研究结果读取失败，请重新读取；未生成零收益结果。");});return()=>c.abort();},[studyId,refresh]);
 useEffect(()=>{const c=new AbortController();moreRequest.current?.abort();setData(null);setBusy(Boolean(account));if(account){void readDualModeView(studyId,account,{view,start,end},c.signal).then(r=>{if(!c.signal.aborted){setData(r);setError("");}}).catch(()=>{if(!c.signal.aborted)setError("账户明细读取失败，请核对日期后重试。");}).finally(()=>{if(!c.signal.aborted)setBusy(false);});}return()=>{c.abort();moreRequest.current?.abort();};},[studyId,account,view,start,end]);
 const selected=detail?.accounts.find(a=>a.key===account);
 const comparisons=detail?.comparisons.filter(c=>c.account_id===account)||[];
 async function more(){if(!data?.next_cursor||busy)return;const c=new AbortController();moreRequest.current?.abort();moreRequest.current=c;setBusy(true);try{const next=await readDualModeView(studyId,account,{view,start,end,cursor:data.next_cursor},c.signal);if(c.signal.aborted)return;if(next.snapshot_set!==data.snapshot_set)throw Error();setData({...next,items:[...data.items,...next.items]});}catch{if(!c.signal.aborted)setError("明细分页读取失败，已保留当前页。");}finally{if(!c.signal.aborted)setBusy(false);}}
 const curveAccounts=detail?.accounts.filter(a=>a.key===account||a.mode==='原生执行')||[];
 const curveValues=curveAccounts.flatMap(a=>a.curve.map(n=>Number(n.nav_cny)));
 const curveLow=Math.min(...curveValues),curveHigh=Math.max(...curveValues),curveSpan=curveHigh-curveLow||1;
 return <div className={styles.shell}><header className={styles.heading}><div><h1>{human(detail?.name,"双模式对比研究")}</h1><p>原生执行与模拟人工 · 每个账户独立计账</p></div><Link href="/simulation/localsim/workbench">返回模拟盘</Link><button onClick={()=>setRefresh(v=>v+1)}>重新读取</button></header>
 {error&&<p role="alert" className={styles.notice}>{error}</p>}{detail&&<p className={styles.explanation}>{safeDate(detail.start_date)} 至 {safeDate(detail.end_date)} · {human(detail.explanation,"分钟代理不保证真实成交，研究未证明稳定超额收益。")}</p>}
 {detail&&!detail.complete&&<p>回放尚未完成，暂不展示完整区间收益。</p>}
 <label>模式筛选<select aria-label="模式筛选" value={mode} onChange={e=>setMode(e.target.value)}>{['全部模式','原生执行','模拟人工'].map(m=><option key={m}>{m}</option>)}</select></label>
 <section className={styles.tablewrap}><table><thead><tr><th>模式与规则</th><th>期末资产</th><th>累计盈亏</th><th>收益率</th><th>最大回撤</th><th>实际费用</th><th>完全完成率</th><th>人工操作次数</th><th>详情</th></tr></thead><tbody>{detail?.accounts.filter(a=>mode==='全部模式'||a.mode===mode).map(a=><tr key={a.key}><td>{human(a.mode)} · {human(a.name)}</td><td>{amount(a.nav)}</td><td>{profitAmount(a.pnl)}</td><td>{amount(a.return_pct)}%</td><td>{amount(a.max_drawdown_pct)}%</td><td>{amount(a.fees)}</td><td>{amount(a.completion_pct)}%</td><td>{a.mode==='模拟人工'?amount(a.operations.action_count,0):'无需人工操作'}</td><td><button aria-pressed={account===a.key} onClick={()=>{if(account===a.key)return;setAccount(a.key);setData(null);}}>账户详情</button></td></tr>)}</tbody></table></section>
 {selected&&<><h2>{human(selected.mode)} · {human(selected.name)}</h2><p>本账户初始现金 {amount(selected.initial_cash)} 元；累计盈亏 {profitAmount(selected.pnl)} 元。</p>
 <p>可用现金 {amount(selected.cash)} 元 · 持仓资产 {amount(selected.holdings_value)} 元 · {safeDate(selected.last_trade_date)} 当日盈亏 {profitAmount(selected.last_day_pnl)} 元。</p>
 <p>{selected.statuses?.map(s=>`${human(s.name)} ${s.count}项`).join(' · ')}</p>
 {selected.mode==='模拟人工'&&<p>操作尝试 {amount(selected.operations.action_count,0)} 次 · 机会失效 {amount(selected.operations.expired_actions,0)} 项 · 到期待撤单 {amount(selected.operations.pending_cancellation_at_deadline,0)} 项 · 取消竞态 {amount(selected.operations.cancellation_races,0)} 次。</p>}
 {!!selected.constraints?.length&&<details><summary>计划中观察到的等待与执行约束（计数可重叠）</summary><ul>{selected.constraints.map((r,i)=><li key={i}>{human(r.name)}：{r.count}项</li>)}</ul></details>}
 <section className={styles.researchCurves} aria-label="独立账户净值走势">{curveAccounts.map(a=>{const values=a.curve.map(n=>Number(n.nav_cny));return <figure key={a.key}><figcaption>{human(a.mode)} · {human(a.name)}</figcaption><svg viewBox="0 0 600 140" role="img" aria-label={`${human(a.mode)}账户资产走势`}><polyline fill="none" stroke="currentColor" strokeWidth="2" points={values.map((v,i)=>`${values.length>1?i*600/(values.length-1):300},${130-(v-curveLow)*120/curveSpan}`).join(' ')}/></svg><p>纵轴 {amount(curveLow)} 至 {amount(curveHigh)} 元；使用相同日期与资产比例尺。</p></figure>;})}</section>
 {comparisons.map(c=><p key={`${c.family}-${c.baseline_account_id}`}>{c.family==='NATIVE_VS_HUMAN_WORKFLOW'?'相对原生执行的整流程净增量':'相同人工条件下相对简单执行的净增量'}：{profitAmount(c.net_increment_cny)} 元；回撤差异 {amount(String(Number(c.mdd_reduction)*100))} 个百分点。</p>)}
 <nav className={styles.tabs} aria-label="研究账户详情标签">{views.map(([key,label])=><button key={key} aria-current={view===key?'page':undefined} onClick={()=>setView(key)}>{label}</button>)}</nav>
 <div className={styles.toolbar}><label>开始日期<input aria-label="研究开始日期" type="date" value={start} onChange={e=>setStart(e.target.value)}/></label><label>结束日期<input aria-label="研究结束日期" type="date" value={end} onChange={e=>setEnd(e.target.value)}/></label></div>
 <div className={styles.tablewrap}><table><thead><tr>{columns[view].map(([key,label])=><th key={key}>{label}</th>)}</tr></thead><tbody>{data?.items.map((row,i)=><tr key={i}>{columns[view].map(([key,,type])=><td key={key}>{type==='text'?human(row[key],'信息待核验'):type==='time'?chinaTime(row[key]):type==='date'?safeDate(String(row[key]||'')):type==='profit'?profitAmount(row[key]):amount(row[key])}</td>)}</tr>)}</tbody></table>{!data&&<p>{busy?'正在读取本账户明细…':'尚无完整明细'}</p>}</div>
 <p>已载入 {data?.items.length??0} / {data?.total_count??0} 条。{view==='actions'?'提醒、操作和成交分别记录；操作完成不代表成交。':''}</p><button disabled={!data?.next_cursor||busy} onClick={()=>void more()}>加载更多</button>
 </>}
 </div>;
}
