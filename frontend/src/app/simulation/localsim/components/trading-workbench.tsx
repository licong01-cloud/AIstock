"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";
import { localSimApi, request } from "@/lib/simulation/localsim/api";
import { accountPath, amount, profitAmount, base, chinaTime, Filters, human, Overview, query, READ_ERROR, readView, readAllView, Result, Row, safeDate, Tab, TAB_NAMES, rememberAccount, selectionKey } from "@/lib/simulation/localsim/workbench";
import { useLocalSimReadback } from "@/lib/simulation/localsim/use-readback";
import styles from "./trading-workbench.module.css";

type Selection = { accountId: string; tab: Tab; filters: Filters };
type Bundle = { accountId: string; filters: string; summary: Result; income: Result; views: Partial<Record<Tab, Result>> };
type Column = [keyof Row, string, "money" | "count" | "date" | "time" | "text"];
const positions: Column[] = [["name","股票","text"],["quantity","持仓","count"],["available","可卖","count"],["frozen","冻结","count"],["cost","成本价","money"],["price","估值价","money"],["value","持仓市值","money"],["unrealized","浮动盈亏","money"],["return_pct","浮盈比例","money"],["pnl","估值日净贡献","money"],["weight","账户占比","money"]];
const trades: Column[] = [["time","成交时间","time"],["name","股票","text"],["side","方向","text"],["price","成交价","money"],["quantity","数量","count"],["amount","成交金额","money"],["commission","佣金分项","money"],["tax","税费分项","money"],["fees","实际费用合计","money"],["description","费用说明","text"]];
const orders: Column[] = [["time","委托时间","time"],["name","股票","text"],["side","方向","text"],["price","委托价","money"],["quantity","委托量","count"],["filled","成交量","count"],["remaining","剩余量","count"],["status","状态","text"],["description","说明","text"]];
const cash: Column[] = [["time","发生时间","time"],["name","股票或资金","text"],["side","方向","text"],["amount","交易金额","money"],["fees","实际费用","money"],["cash_delta","资金净变动","money"],["balance","变动后余额","money"],["description","说明","text"]];
const daily: Column[] = [["date","交易日","date"],["opening_value","期初总资产","money"],["value","期末总资产","money"],["cash_delta","净外部流入","money"],["pnl","净盈亏","money"],["realized","已实现（费用前）","money"],["unrealized_change","浮盈变动","money"],["fees","交易费用","money"]];
const stocks: Column[] = [["name","股票","text"],["opening_quantity","期初数量","count"],["closing_quantity","期末数量","count"],["buy_amount","买入金额","money"],["sell_amount","卖出金额","money"],["realized","已实现（费用前）","money"],["unrealized_change","浮盈变动","money"],["fees","交易费用","money"],["pnl","区间净贡献","money"]];
const events: Column[] = [["date","交易日","date"],["time","更新时间","time"],["name","事项","text"],["status","状态","text"],["description","业务说明","text"]];
const storageKey = selectionKey;
const empty: Selection = { accountId:"", tab:"positions", filters:{} };
function stockFilters(filters: Filters, row: Row, tab: Tab): Filters {
  const end = tab === "positions" || tab === "monitor" ? filters.as_of || row.date || filters.end : filters.end || row.date;
  return { ...filters, start: filters.start && end && filters.start > end ? undefined : filters.start,
    end: end || undefined, security: row.security || undefined };
}

function gain(value: unknown) { return value == null ? "" : String(value).startsWith("-") ? styles.loss : styles.gain; }
function Metric({ label, value, note, profit=false }: { label:string; value:string|null|undefined; note?:string; profit?:boolean }) {
  return <div className={styles.metric}><span>{label}</span><strong className={profit?gain(value):""}>{profit?profitAmount(value):amount(value)}</strong>{note&&<small>{note}</small>}</div>;
}
function Table({ rows, columns, onStock, onOrder, onDay }: { rows:Row[]; columns:Column[]; onStock?:(r:Row)=>void; onOrder?:(r:Row)=>void; onDay?:(r:Row)=>void }) {
  return <div className={styles.tablewrap}><table><thead><tr>{columns.map(([key,label,type])=><th key={key} className={type==="money"||type==="count"?styles.numeric:""}>{label}</th>)}{onOrder&&<th>业务关联</th>}</tr></thead><tbody>{rows.map(row=><tr key={row.key}>{columns.map(([key,,type])=>{
    const value = row[key];
    let content = type==="money" ? (["pnl","unrealized","realized","unrealized_change","cash_delta"].includes(key)?profitAmount(value as string|null):amount(value as string|null)) + (value!=null && (key==="weight"||key==="return_pct")?"%":"") : type==="count" ? amount(value as number|null,0) : type==="date" ? safeDate(value as string) : type==="time" ? chinaTime(value as string) : human(value, value===""?"—":"信息待核验");
    return <td key={key} className={`${type==="money"||type==="count"?styles.numeric:""} ${["pnl","unrealized","realized","unrealized_change","cash_delta"].includes(key)?gain(value):""}`}>{key==="name"&&row.security&&onStock?<button className={styles.link} onClick={()=>onStock(row)}>{content}<small>{human(row.market,"市场待核验")}</small></button>:key==="date"&&onDay?<button className={styles.link} onClick={()=>onDay(row)}>{content}</button>:content}{!row.known&&key==="name"&&<small>部分信息待核验</small>}</td>;
  })}{onOrder&&<td><button className={styles.link} onClick={()=>onOrder(row)}>查看关联委托</button></td>}</tr>)}</tbody></table>{!rows.length&&<p className={styles.empty}>当前已读取的筛选结果中没有记录；完整性与覆盖范围见下方说明。</p>}</div>;
}
function Curve({ result, points }: { result?:Result; points?:Row[] }) {
  const rows = [...(points||result?.items || [])].sort((a,b)=>a.date.localeCompare(b.date));
  const values = rows.map(r=>r.known&&r.value!=null?Number(r.value):null).filter((x):x is number=>x!==null&&Number.isFinite(x));
  if(values.length<2)return <div className={styles.empty}>当前区间不足两个合法估值点，暂不绘制趋势。</div>;
  const min=Math.min(...values), max=Math.max(...values), scale=max-min||1;
  let commands="", pen=false;
  rows.forEach((r,i)=>{const v=Number(r.value);if(!r.known||r.value==null||!Number.isFinite(v)){pen=false;return;}commands+=`${pen?" L":" M"}${20+i*740/Math.max(rows.length-1,1)} ${150-(v-min)/scale*125}`;pen=true;});
  return <><svg viewBox="0 0 800 180" role="img" aria-label="权威收盘资产走势"><path d="M20 155 H780 M20 90 H780 M20 25 H780" fill="none" stroke="#e8edf4"/><path d={commands} fill="none" stroke="#356de8" strokeWidth="2.5"/><text x="20" y="178" fill="#718096" fontSize="11">{safeDate(rows[0].date)}</text><text x="760" y="178" textAnchor="end" fill="#718096" fontSize="11">{safeDate(rows.at(-1)?.date)}</text></svg><p className={styles.muted}>仅绘制本账户已读取的权威估值点，缺失点断开；共 {rows.length} 个点。</p></>;
}

export default function TradingWorkbench() {
  const [selection,setSelection]=useState<Selection>(empty),[overview,setOverview]=useState<Overview|null>(null),[bundle,setBundle]=useState<Bundle|null>(null);
  const [error,setError]=useState(""),[busy,setBusy]=useState(false),[refresh,setRefresh]=useState(0),[search,setSearch]=useState(""),[stage,setStage]=useState("");
  const [entryError,setEntryError]=useState(false),[failedAt,setFailedAt]=useState<string|null>(null);
  const [drawer,setDrawer]=useState<Row|null>(null),[drawerData,setDrawerData]=useState<Result|null>(null),[drawerError,setDrawerError]=useState("");
  const [drawerPnl,setDrawerPnl]=useState<Result|null>(null);
  const [ready,setReady]=useState<{ready:boolean;time:string;count:number}|null>(null),[readAt,setReadAt]=useState<string|null>(null);
  const [securityName,setSecurityName]=useState(""),[exporting,setExporting]=useState(false);
  const [accountSort,setAccountSort]=useState("newest"),[orderDetail,setOrderDetail]=useState<Row|null>(null),[orderParts,setOrderParts]=useState<Result[]>([]),[orderError,setOrderError]=useState("");
  const selectionRef=useRef(selection),bundleRef=useRef(bundle),pageRequest=useRef<AbortController|null>(null);
  const loading=useRef(false);
  const overviewRef=useRef<Overview|null>(null),overviewReadAt=useRef(0);
  selectionRef.current=selection;bundleRef.current=bundle;
  const dialog=useRef<HTMLDivElement>(null), returnFocus=useRef<HTMLElement|null>(null);
  const change=useCallback((next:Selection, replace=false)=>{
    pageRequest.current?.abort();setSelection(next);setDrawer(null);setDrawerData(null);setOrderDetail(null);if(next.accountId)setEntryError(false);
    sessionStorage.setItem(storageKey,JSON.stringify(next));
    const url=new URL(window.location.href);url.pathname="/simulation/localsim/workbench";url.search="";url.hash="";
    (replace?history.replaceState:history.pushState).call(history,{localsim:next},"",url);
  },[]);
  useEffect(()=>{
    const restore=()=>{try{const v=history.state?.localsim || JSON.parse(sessionStorage.getItem(storageKey)||"null");if(v&&typeof v.accountId==="string"&&v.tab in TAB_NAMES&&v.filters&&typeof v.filters==="object")setSelection(v);else setSelection(empty);}catch{sessionStorage.removeItem(storageKey);setSelection(empty);}};
    const logout=()=>{[storageKey,"localsim-replay-selection","localsim-configure-selection"].forEach(key=>sessionStorage.removeItem(key));setSelection(empty);setBundle(null);setOverview(null);overviewRef.current=null;overviewReadAt.current=0;history.replaceState({...history.state,localsim:empty},"","/simulation/localsim/workbench");};
    if(sessionStorage.getItem("localsim-entry-error")){setEntryError(true);sessionStorage.removeItem("localsim-entry-error");}
    restore();window.addEventListener("popstate",restore);window.addEventListener("logout",logout);
    return()=>{window.removeEventListener("popstate",restore);window.removeEventListener("logout",logout);};
  },[]);
  useEffect(()=>{document.title="模拟盘交易工作台";},[]);
  useEffect(()=>{
    const timer=setInterval(()=>{const current=bundleRef.current?.views[selectionRef.current.tab];if(!document.hidden&&!loading.current&&(!current||current.items.length<=50))setRefresh(v=>v+1);},selection.accountId?10000:5000);
    const visible=()=>{if(!document.hidden)setRefresh(v=>v+1);};document.addEventListener("visibilitychange",visible);
    return()=>{clearInterval(timer);document.removeEventListener("visibilitychange",visible);};
  },[selection.accountId]);
  useEffect(()=>{
    const controller=new AbortController();
    const timeout=setTimeout(()=>{controller.abort();loading.current=false;setBusy(false);setReady(null);setFailedAt(new Date().toISOString());setError(READ_ERROR);},30000);
    async function load(){
      loading.current=true;
      setBusy(true);
      try{
        const reuse=selection.accountId&&overviewRef.current&&Date.now()-overviewReadAt.current<60000;
        const list=reuse?overviewRef.current!:await request<Overview>(`${base}/workbench/overview`,{signal:controller.signal});
        if(controller.signal.aborted)return;
        if(!Array.isArray(list.items)||!list.complete)throw Error();
        setOverview(list);
        overviewRef.current=list;if(!reuse)overviewReadAt.current=Date.now();
        const id=selection.accountId;
        if(id&&!list.items.some(a=>a.key===id)){setBundle(null);change(empty,true);setError("原选账户已不可用，请重新选择。");return;}
        if(id){
          const summary=await readView(id,"summary",{},controller.signal);
          const version=summary.snapshot_set, common={snapshot_set:version};
          const income=await readAllView(id,"pnl",common,controller.signal);
          const keys=selection.tab==="monitor"?["positions","trades","orders","events"]:[selection.tab];
          const responses=await Promise.all(keys.map(k=>readView(id,k,{...selection.filters,...common,page_size:50},controller.signal)));
          if(controller.signal.aborted)return;
          if(responses.some(r=>r.snapshot_set!==version)||income.snapshot_set!==version)throw Error();
          setBundle({accountId:id,filters:JSON.stringify(selection.filters),summary,income,views:Object.fromEntries(keys.map((k,i)=>[k,responses[i]]))});
        }
        if(!controller.signal.aborted){setError("");setReadAt(new Date().toISOString());}
      }catch{if(!controller.signal.aborted){setFailedAt(new Date().toISOString());setError(READ_ERROR);}}finally{clearTimeout(timeout);if(!controller.signal.aborted){loading.current=false;setBusy(false);}}
    }
    void load();
    void localSimApi.readiness(controller.signal).then(r=>{if(!controller.signal.aborted)setReady({ready:r.ready,time:r.checked_at,count:r.in_flight_economic_run_count});}).catch(()=>{if(!controller.signal.aborted)setReady(null);});
    return()=>{clearTimeout(timeout);controller.abort();};
  },[selection,refresh,change]);
  useEffect(()=>{
    if(!drawer||!selection.accountId)return;
    returnFocus.current=document.activeElement as HTMLElement;dialog.current?.focus();
    const controller=new AbortController();setDrawerData(null);setDrawerPnl(null);setDrawerError("");
    const filters={...stockFilters(selection.filters,drawer,selection.tab),snapshot_set:bundle?.summary.snapshot_set};
    Promise.all([readView(selection.accountId,"trades",filters,controller.signal),readView(selection.accountId,"pnl",{...filters,group_by:"security"},controller.signal)])
      .then(([t,p])=>{if(!controller.signal.aborted){setDrawerData(t);setDrawerPnl(p);}}).catch(()=>{if(!controller.signal.aborted)setDrawerError(READ_ERROR);});
    return()=>{controller.abort();returnFocus.current?.focus();};
  },[drawer,selection.accountId,selection.filters,selection.tab,bundle?.summary.snapshot_set]);
  useEffect(()=>{if(!orderDetail||!selection.accountId)return;const controller=new AbortController();setOrderParts([]);setOrderError("");
    Promise.all(["orders","trades","events","cash-flows"].map(k=>readAllView(selection.accountId,k,{order_key:orderDetail.order_key||orderDetail.key,snapshot_set:bundle?.summary.snapshot_set},controller.signal)))
      .then(r=>{if(!controller.signal.aborted)setOrderParts(r);}).catch(()=>{if(!controller.signal.aborted)setOrderError(READ_ERROR);});return()=>controller.abort();
  },[orderDetail,selection.accountId,bundle?.summary.snapshot_set]);
  const active=bundle?.accountId===selection.accountId?bundle:null;
  const overviewName=overview?.items.find(a=>a.key===selection.accountId)?.summary.name;
  const summary=active?.summary.summary;
  const selected=selection.tab==="monitor"||active?.filters!==JSON.stringify(selection.filters)?undefined:active?.views[selection.tab];
  const financial=selected&&selection.tab==="pnl"?selected.summary:active?.income.summary;
  const filter=(values:Filters)=>change({...selection,filters:{...selection.filters,...values}},true);
  const openStock=(row:Row)=>{setDrawer(row);setDrawerData(null);};
  const fullStock=(tab:Tab)=>{if(!drawer)return;setSecurityName(human(drawer.name));change({...selection,tab,filters:{...stockFilters(selection.filters,drawer,selection.tab),group_by:tab==="pnl"?"security":undefined}});};
  async function more(){
    if(!selected?.next_cursor||busy)return;
    const id=selection.accountId, tab=selection.tab;
    const fingerprint=JSON.stringify(selection);const controller=new AbortController();pageRequest.current?.abort();pageRequest.current=controller;
    setBusy(true);
    try{
      const next=await readView(id,tab,{...selection.filters,cursor:selected.next_cursor,snapshot_set:selected.snapshot_set},controller.signal);
      if(!controller.signal.aborted&&JSON.stringify(selectionRef.current)===fingerprint)setBundle(prev=>prev?.accountId===id&&prev.views[tab]?.snapshot_set===next.snapshot_set&&prev.views[tab]?.next_cursor===selected.next_cursor?{...prev,views:{...prev.views,[tab]:{...next,items:[...(prev.views[tab]?.items||[]),...next.items]}}}:prev);
    }catch{if(!controller.signal.aborted&&JSON.stringify(selectionRef.current)===fingerprint)setError(READ_ERROR);}finally{if(!controller.signal.aborted&&JSON.stringify(selectionRef.current)===fingerprint)setBusy(false);}
  }
  async function download(){
    if(!selected||exporting)return;setExporting(true);
    try{
      const apiBase=process.env.NEXT_PUBLIC_API_BASE||"http://127.0.0.1:8001/api/v1";
      const response=await fetch(`${apiBase}${accountPath(selection.accountId)}/export?${query({...selection.filters,kind:selection.tab,snapshot_set:selected.snapshot_set})}`,{cache:"no-store"});
      if(!response.ok)throw Error();
      const blob=await response.blob(),url=URL.createObjectURL(blob),link=document.createElement("a");
      link.href=url;link.download=`模拟盘${TAB_NAMES[selection.tab]}明细.csv`;link.click();URL.revokeObjectURL(url);
    }catch{setError("导出失败，请重新读取后重试；未生成完整文件。");}finally{setExporting(false);}
  }
  function preset(days:number|null){const end=summary?.economic_as_of?.slice(0,10);if(!end)return;const d=new Date(`${end}T00:00:00+08:00`);if(days!==null)d.setDate(d.getDate()-days);filter({start:days===null?`${end.slice(0,4)}-01-01`:d.toLocaleDateString("en-CA",{timeZone:"Asia/Shanghai"}),end});}
  const section=(kind:Tab,result?:Result)=>{
    const cols=kind==="positions"?positions:kind==="trades"?trades:kind==="orders"?orders:kind==="cash-flows"?cash:kind==="events"?events:selection.filters.group_by==="security"?stocks:daily;
    return result?<Table rows={result.items} columns={cols} onStock={openStock} onOrder={["trades","orders","cash-flows"].includes(kind)?r=>setOrderDetail(r):undefined} onDay={kind==="pnl"&&selection.filters.group_by!=="security"?r=>change({...selection,tab:"trades",filters:{...selection.filters,start:r.date,end:r.date}}):undefined}/>:<p className={styles.empty}>正在读取本账户数据…</p>;
  };
  const accounts=(overview?.items||[]).filter(a=>(!stage||a.summary.stage===stage)&&`${a.summary.name} ${a.summary.strategy}`.includes(search)).sort((a,b)=>(accountSort==="oldest"?1:-1)*(a.summary.economic_as_of||"").localeCompare(b.summary.economic_as_of||""));
  const blocked=overview?.items.filter(a=>a.summary.stage.includes("受阻")).length;
  const drawerStock=drawerPnl?.items.find(r=>r.security===drawer?.security);
  return <div className={styles.shell}>
    <header className={styles.heading}><div><h1>{selection.accountId?human(overviewName||summary?.name,"模拟账户详情"):"模拟盘总览"}</h1><p>以账户为中心查看持仓、交易与收益 · 金额单位：人民币元</p></div><div className={styles.actions}><span className={styles.muted}>页面最近读取：{chinaTime(readAt)}</span><button onClick={()=>{overviewReadAt.current=0;setRefresh(v=>v+1);}} disabled={busy}>{busy?"读取中…":"刷新"}</button><Link href="/simulation/localsim/configure">创建与配置</Link></div></header>
    {(error||entryError)&&<div role="alert" className={styles.notice}>{error||"原账户入口未通过核验，请从总览重新选择。"}<small>下方如有数据，为最后一次成功读取的内容。{error&&failedAt?`失败时间 ${chinaTime(failedAt)}`:""}</small></div>}
    {!selection.accountId?<>
      {!!blocked&&<div className={styles.notice}><b>{blocked} 个账户需要处理</b>{overview?.items.filter(a=>a.summary.stage.includes("受阻")).map(a=><button key={a.key} onClick={()=>change({accountId:a.key,tab:"events",filters:{}})}>{human(a.summary.name)} · 已完成 {safeDate(a.summary.completed_date)}</button>)}</div>}
      <div className={styles.cards}><Metric label="可比实时账户总资产" value={overview?.nav} note={`纳入 ${overview?.included_count??0} 个；其他 ${overview?.excluded_count??0} 个按各自经济日查看`}/><Metric label="最近同日净盈亏" value={overview?.daily_pnl} profit note={chinaTime(overview?.economic_as_of)}/><Metric label="可比累计净盈亏" value={overview?.pnl} profit note={overview?.return_pct==null?"边界或口径不一致时不合成收益率":`统一期初收益率 ${amount(overview.return_pct)}%`}/><div className={styles.metric}><span>策略账户 / 回放受阻</span><strong>{overview?`${overview.items.length} / ${blocked}`:"尚未读取"}</strong><small>每个账户保持独立账本</small></div></div>
      <p className={styles.muted}>{human(overview?.explanation,"历史回放不加入实时合计；仅汇总同一估值时点的实时账户。")}</p>
      <div className={styles.chartrow}><section className={styles.panel}><div className={styles.panelhead}><h2>可比实时账户资产走势</h2><span>相同日期逐点核对，不插值</span></div><Curve points={overview?.curve}/></section><section className={styles.panel}><div className={styles.panelhead}><h2>各账户资产分布</h2><span>各自截止日 · 不相加为今日</span></div><dl>{overview?.items.map(a=><div key={a.key}><dt><button className={styles.link} onClick={()=>change({accountId:a.key,tab:"positions",filters:{}})}>{human(a.summary.name)}</button></dt><dd>{amount(a.summary.nav)}<small>{chinaTime(a.summary.economic_as_of)}</small></dd></div>)}</dl></section></div>
      <section className={styles.panel}><div className={styles.panelhead}><h2>我的策略账户</h2><div className={styles.actions}><input aria-label="搜索策略账户" placeholder="搜索策略或账户名称" value={search} onChange={e=>setSearch(e.target.value)}/><select aria-label="运行阶段筛选" value={stage} onChange={e=>setStage(e.target.value)}><option value="">全部阶段</option>{Array.from(new Set(overview?.items.map(a=>a.summary.stage))).map(s=><option key={s} value={s}>{human(s,"状态待核验")}</option>)}</select><select aria-label="资产截止时间排序" value={accountSort} onChange={e=>setAccountSort(e.target.value)}><option value="newest">资产时点从近到远</option><option value="oldest">资产时点从远到近</option></select></div></div>
      <div className={styles.tablewrap}><table><thead><tr><th>策略 / 账户</th><th>运行阶段</th><th>资产截止时间</th><th>总资产</th><th>累计净盈亏 / 收益率</th><th>持仓数 / 仓位</th><th>完成 / 下一交易日</th><th>下一事项</th><th/></tr></thead><tbody>{accounts.map(a=><tr key={a.key} tabIndex={0} aria-label={`查看 ${human(a.summary.name)} 的账户详情`} onClick={e=>{if(!(e.target as HTMLElement).closest("button"))change({accountId:a.key,tab:"positions",filters:{}});}} onKeyDown={e=>{if(e.key==="Enter"&&e.target===e.currentTarget)change({accountId:a.key,tab:"positions",filters:{}});}}><td><button className={styles.link} onClick={()=>change({accountId:a.key,tab:"positions",filters:{}})}>{human(a.summary.strategy)}<small>{human(a.summary.name)}</small></button><small>初始资金 {amount(a.summary.initial_capital)}</small><small>创建于 {safeDate(a.summary.created_at.slice(0,10))}</small></td><td><span className={styles.badge}>{human(a.summary.stage,"阶段待核验")}</span></td><td>{chinaTime(a.summary.economic_as_of)}</td><td className={styles.numeric}>{amount(a.summary.nav)}</td><td className={styles.numeric}><span className={gain(a.summary.pnl)}>{profitAmount(a.summary.pnl)}</span><small>{a.summary.return_pct==null?"收益率暂不可计算":amount(a.summary.return_pct)+"%"}</small><small>正式期初至完整收盘；证据不足则未知</small></td><td className={styles.numeric}>{amount(a.summary.position_count,0)}<small>{a.summary.position_ratio==null?"仓位待核验":amount(a.summary.position_ratio)+"%"}</small></td><td>{safeDate(a.summary.completed_date)}<small>下一日 {safeDate(a.summary.next_date)}</small></td><td>{human(a.summary.issue,"请进入账户核验")}</td><td><button onClick={()=>change({accountId:a.key,tab:"positions",filters:{}})}>详情</button></td></tr>)}</tbody></table></div>{!accounts.length&&<p className={styles.empty}>{overview?"此筛选下没有账户":"正在读取账户列表…"}</p>}</section>
    </>:<>
      <div className={styles.accountbar}><button onClick={()=>change(empty)}>返回总览</button><select aria-label="切换策略账户" value={selection.accountId} onChange={e=>{setSecurityName("");change({accountId:e.target.value,tab:"positions",filters:{}});}}>{overview?.items.map(a=><option key={a.key} value={a.key}>{human(a.summary.name)} · {human(a.summary.strategy)}</option>)}</select><span className={styles.badge}>{human(summary?.stage,"阶段待核验")}</span><span>资产截止 {chinaTime(summary?.economic_as_of)}</span></div>
      {summary&&<div className={styles.notice}><b>{human(summary.issue,"请核验当前进度")}</b><span>{human(summary.valuation_kind,"估值状态待核验")} · {human(summary.source,"来源待核验")}</span></div>}
      <div className={styles.cards}><Metric label="总资产" value={summary?.nav}/><Metric label="累计净盈亏" value={active?.income.summary.pnl} note="完整性与实际区间见分析页" profit/><Metric label="最近估值日净盈亏" value={summary?.daily_pnl} note={safeDate(summary?.daily_date)} profit/><Metric label="持仓浮动盈亏" value={summary?.unrealized} profit/><Metric label="持仓市值" value={summary?.value}/><Metric label="可用现金" value={summary?.available_cash} note={`现金余额 ${amount(summary?.cash)}；${summary?.frozen_cash==null?"冻结与可用拆分待核验":`冻结 ${amount(summary.frozen_cash)}`}`}/></div>
      {selection.tab!=="monitor"&&<div className={styles.chartrow}><section className={styles.panel}><div className={styles.panelhead}><h2>账户收盘资产走势</h2><span>权威估值 · 不跨账户拼接</span></div><Curve result={active?.income}/></section><section className={styles.panel}><div className={styles.panelhead}><h2>持仓与交易费用</h2></div><dl><dt>初始资金</dt><dd>{amount(summary?.initial_capital)}</dd><dt>持仓股票</dt><dd>{amount(summary?.position_count,0)}</dd><dt>已读取区间费用</dt><dd>{amount(active?.income.summary.fees)}</dd><dt>成本口径</dt><dd>{human(summary?.cost_basis,"成本口径待核验")}</dd></dl></section></div>}
      <section className={styles.panel}><nav className={styles.tabs} aria-label="账户详情标签">{Object.entries(TAB_NAMES).map(([key,label])=><button key={key} aria-current={selection.tab===key?"page":undefined} onClick={()=>change({...selection,tab:key as Tab,filters:key==="pnl"&&(selection.filters.security||selection.filters.search)?{...selection.filters,group_by:"security"}:selection.filters})}>{label}</button>)}</nav>
      {selection.tab!=="monitor"&&<div className={styles.toolbar}>
        <div className={styles.filters}>{selection.tab==="positions"?<label>持仓估值日<input aria-label="持仓估值日期" type="date" value={selection.filters.as_of||summary?.economic_as_of?.slice(0,10)||""} onChange={e=>filter({as_of:e.target.value})}/></label>:<><label>开始日期<input type="date" value={selection.filters.start||""} onChange={e=>filter({start:e.target.value})}/></label><label>结束日期<input type="date" value={selection.filters.end||""} onChange={e=>filter({end:e.target.value})}/></label></>}
        {selection.tab==="pnl"&&<><button onClick={()=>preset(7)}>近一周</button><button onClick={()=>preset(30)}>近一月</button><button onClick={()=>preset(null)}>年初至今</button><select aria-label="盈亏统计维度" value={selection.filters.group_by||"date"} onChange={e=>filter({group_by:e.target.value as "date"|"security",...(e.target.value==="date"?{security:undefined,search:undefined}:{})})}><option value="date">按日（账户整体）</option><option value="security">按股票（含已清仓）</option></select></>}
        {(selection.tab==="trades"||selection.tab==="orders")&&<select aria-label="买卖方向" value={selection.filters.side||""} onChange={e=>filter({side:e.target.value})}><option value="">全部方向</option><option value="BUY">买入</option><option value="SELL">卖出</option></select>}
        {selection.tab==="orders"&&<select aria-label="委托状态筛选" value={selection.filters.status||""} onChange={e=>filter({status:e.target.value})}><option value="">全部状态</option>{[["NEW","等待处理"],["FILLED","全部成交"],["PARTIALLY_FILLED","部分成交"],["EXPIRED_WITH_RESIDUAL","交易约束下仍有剩余"],["REJECTED","已拒绝"],["CANCELLED","已取消"]].map(([key,label])=><option key={key} value={key}>{label}</option>)}</select>}
        {selection.tab!=="events"&&!(selection.tab==="pnl"&&selection.filters.group_by!=="security")&&<><label>证券名称<input aria-label="证券名称筛选" value={selection.filters.search||""} onChange={e=>filter({search:e.target.value})} placeholder="按中文简称查询"/></label><select aria-label="明细排序" value={selection.filters.sort||"time"} onChange={e=>filter({sort:e.target.value})}><option value="time">发生时间</option><option value="name">证券名称</option><option value="value">资产或市值</option><option value="pnl">净盈亏或贡献</option><option value="quantity">数量</option><option value="fees">实际费用</option></select><button onClick={()=>filter({descending:selection.filters.descending===false})}>{selection.filters.descending===false?"升序":"降序"}</button></>}
        {selection.tab==="pnl"&&<button onClick={()=>filter({start:new Date().toLocaleDateString("en-CA",{timeZone:"Asia/Shanghai"}),end:new Date().toLocaleDateString("en-CA",{timeZone:"Asia/Shanghai"})})}>今天</button>}
        {selection.filters.security&&<span>股票：{human(securityName,"已选择证券")} <button onClick={()=>filter({security:undefined})}>清除股票</button></span>}
        <button onClick={()=>{setSecurityName("");change({...selection,filters:{}});}}>清除筛选</button></div>{selection.tab!=="events"&&<button onClick={()=>void download()} disabled={!selected||exporting}>{exporting?"导出中…":"导出完整筛选"}</button>}</div>}
      {selection.tab==="pnl"&&<><div className={styles.cards}><Metric label="区间净盈亏" value={financial?.pnl} profit/><Metric label="已实现（费用前）" value={financial?.realized} profit/><Metric label="浮盈变动" value={financial?.unrealized_change} profit/><Metric label="区间实际交易费用" value={financial?.fees}/></div><p className={styles.explanation}>卡片为账户整体区间金额；按股票表为筛选后的证券贡献。期初资产 {amount(financial?.opening_nav)} ＋ 净外部流入 {amount(financial?.net_flows)} ＋ 净盈亏 {amount(financial?.pnl)} ＝ 期末资产 {amount(financial?.closing_nav)}。费用已在净盈亏中计入，不重复扣减。实际估值边界 {safeDate(selected?.actual_start)} 至 {safeDate(selected?.actual_end)}。</p></>}
      {selection.tab==="monitor"?<div className={styles.monitor}>{(["positions","trades","orders","events"] as Tab[]).map(k=><section key={k} className={styles.subpanel}><header><h3>{TAB_NAMES[k]} · 最近一页</h3><button onClick={()=>change({...selection,tab:k})}>完整查询</button></header><p className={styles.explanation}>读取于 {chinaTime(active?.views[k]?.generated_at)}；完整记录请进入查询。</p>{section(k,active?.filters===JSON.stringify(selection.filters)?active?.views[k]:undefined)}</section>)}</div>:section(selection.tab,selected)}
      {selected&&<footer className={styles.footer}><span>已载入 {selected.items.length} / {selected.total_count} 条 · {selected.status==="complete"?"当前查询完整":"存在未核验或缺失信息"}</span><button disabled={!selected.next_cursor||busy} onClick={()=>void more()}>加载更多</button></footer>}
      {Array.from(new Set([...(selected?.warnings||[]),...(active?.income.warnings||[])])).map((w,i)=><p className={styles.warning} key={i}>{human(w,"数据覆盖需要核验")}</p>)}
      {selected&&<p className={styles.explanation}>本查询经济集合读取于 {chinaTime(selected.generated_at)}；翻页超过一页后固定本集合，点击刷新重新查询。</p>}
      {selection.tab==="events"&&<><div className={styles.toolbar}><p>计划区间 {safeDate(summary?.start_date)} 至 {safeDate(summary?.end_date)}；已完成 {safeDate(summary?.completed_date)}；下一交易日 {safeDate(summary?.next_date)}。资产截止时间见页首。</p><Link href="/simulation/localsim/replays">回放列表与任务控制</Link></div><AccountControls accountId={selection.accountId} ready={ready?.ready===true}/></>}
      {selected?.missing_dates.length? <p className={styles.warning}>缺失估值日：{selected.missing_dates.map(safeDate).join("、")}</p>:null}
      </section>
    </>}
    <footer className={styles.system}><b>切流条件 · {ready?(ready.ready?"前置检查通过":"前置检查尚未通过"):"暂未取得核验结果"}</b><span>{ready?.ready?"各账户仍须完成回放并满足安全接续时点。":"请结合各账户进度与处理事项核验。"}</span>{ready&&<span>后端核验 {chinaTime(ready.time)} · 在途交易 {ready.count} 项</span>}</footer>
    {drawer&&<div className={styles.backdrop} onClick={e=>{if(e.target===e.currentTarget)setDrawer(null);}}><div className={styles.drawer} ref={dialog} role="dialog" aria-modal="true" aria-label="个股持仓与交易明细" tabIndex={-1} onKeyDown={e=>{if(e.key==="Escape")setDrawer(null);if(e.key==="Tab"){const nodes=dialog.current?.querySelectorAll<HTMLElement>("button,a,input,select");if(!nodes?.length)return;const first=nodes[0],last=nodes[nodes.length-1];if(e.shiftKey&&(document.activeElement===first||document.activeElement===dialog.current)){e.preventDefault();last.focus();}else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}}}}><header><div><h2>{human(drawer.name,"证券名称暂不可用")}</h2><p>{human(drawer.market,"市场待核验")} · {human(overviewName||summary?.name)}</p></div><button aria-label="关闭个股明细" onClick={()=>setDrawer(null)}>关闭</button></header><div className={styles.cards}><Metric label="期末持仓数量" value={drawerStock?.known?String(drawerStock.closing_quantity):drawer.quantity==null?null:String(drawer.quantity)}/><Metric label="期末持仓市值" value={drawerStock?.known?drawerStock.value:drawer.value}/><Metric label="浮动盈亏" value={drawer.unrealized} profit/></div><div className={styles.actions}><button onClick={()=>fullStock("trades")}>查看完整交易记录</button><button onClick={()=>fullStock("pnl")}>查看区间盈亏</button></div><p>{human(drawer.description,"价格与费用按所选经济集合核验")}</p><p>区间增仓 {amount(drawerStock?.buy_quantity,0)} 股；减仓 {amount(drawerStock?.sell_quantity,0)} 股；实际费用 {amount(drawerStock?.fees)} 元；净贡献 {amount(drawerStock?.pnl)} 元。期末估值日 {safeDate(drawerPnl?.actual_end)}。</p><h3>这只股票在本账户的成交</h3>{drawerError&&<p role="alert">{drawerError}</p>}{drawerData?<Table rows={drawerData.items} columns={trades}/>:<p>正在读取成交…</p>}<p className={styles.muted}>此处为最近一页；完整历史、费用和已清仓股票可从上方入口查询。</p></div></div>}
    {orderDetail&&<OrderDialog row={orderDetail} parts={orderParts} error={orderError} close={()=>setOrderDetail(null)} onStock={r=>{setOrderDetail(null);openStock(r);}}/>}
  </div>;
}

function OrderDialog({row,parts,error,close,onStock}:{row:Row;parts:Result[];error:string;close:()=>void;onStock:(row:Row)=>void}){
  const ref=useRef<HTMLDivElement>(null);
  useEffect(()=>{const previous=document.activeElement as HTMLElement;ref.current?.focus();return()=>previous?.focus();},[]);
  return <div className={styles.backdrop} onClick={e=>{if(e.target===e.currentTarget)close();}}><div ref={ref} className={styles.drawer} role="dialog" aria-modal="true" aria-label="关联委托与部分成交" tabIndex={-1} onKeyDown={e=>{if(e.key==="Escape")close();if(e.key==="Tab"){const nodes=ref.current?.querySelectorAll<HTMLElement>("button,a");if(!nodes?.length)return;const first=nodes[0],last=nodes[nodes.length-1];if(e.shiftKey&&(document.activeElement===first||document.activeElement===ref.current)){e.preventDefault();last.focus();}else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}}}}>
    <header><div><h2>关联委托与部分成交</h2><p>{human(row.name,"证券名称暂不可用")} · {human(row.side,"方向待核验")}</p></div><button onClick={close}>关闭委托明细</button></header>{error&&<p role="alert">{error}</p>}{!parts.length&&!error&&<p>正在读取本账户完整关联记录…</p>}
    {parts.map((part,i)=><section key={i}><h3>{["原始委托","全部部分成交","委托状态时间线","对应资金流水"][i]} · {part.total_count} 条</h3><Table rows={part.items} columns={[orders,trades,events,cash][i]} onStock={onStock}/>{part.warnings.map((w,j)=><p className={styles.warning} key={j}>{human(w,"关联完整性待核验")}</p>)}</section>)}
  </div></div>;
}

function AccountControls({accountId,ready}:{accountId:string;ready:boolean}){
  const load=useCallback(async(signal:AbortSignal)=>{const result=await localSimApi.getAccount(accountId,signal);if(result.account?.account_id!==accountId)throw Error(READ_ERROR);return result.account;},[accountId]);
  const readback=useLocalSimReadback(load),[pending,setPending]=useState<"pause"|"resume"|"retire"|null>(null),[busy,setBusy]=useState(false),[error,setError]=useState("");
  const fresh=ready&&!readback.error&&!readback.stale&&readback.data?.account_id===accountId;
  const labels={pause:"暂停",resume:"恢复",retire:"停用"};
  async function submit(){if(!pending||!fresh||!readback.data||busy)return;setBusy(true);setError("");try{await localSimApi.transitionAccount(accountId,pending,readback.data.version);setPending(null);await readback.refresh();}catch{setError("账户操作未通过正式版本或运行保护检查，请刷新核验。");}finally{setBusy(false);}}
  return <section className={styles.toolbar}><div><h3>账户运行控制</h3><p>仅在您确认后提交原有控制服务；在途交易和版本保护仍由服务执行。</p>{(error||readback.error)&&<p role="alert">{error||READ_ERROR}</p>}{!fresh&&<p>尚未获得有效前置检查和最新账户状态，控制操作暂不可用。</p>}</div><div className={styles.actions}>{readback.data?.status==="ACTIVE"&&<button disabled={!fresh||busy} onClick={()=>setPending("pause")}>暂停账户</button>}{readback.data?.status==="PAUSED"&&<button disabled={!fresh||busy} onClick={()=>setPending("resume")}>恢复账户</button>}{readback.data&&readback.data.status!=="RETIRED"&&<button disabled={!fresh||busy} onClick={()=>setPending("retire")}>停用账户</button>}{pending&&<div role="group" aria-label="确认账户操作"><span>确认{labels[pending]} {human(readback.data?.account_name,"当前模拟账户")}？</span><button disabled={!fresh||busy} onClick={()=>void submit()}>确认{labels[pending]}账户</button><button onClick={()=>setPending(null)}>返回</button></div>}</div></section>;
}

export function LegacyAccountEntry({accountId,tab}:{accountId:string;tab:Tab}){
  const router=useRouter(),[error,setError]=useState(false);
  useEffect(()=>{const controller=new AbortController();localSimApi.getAccount(accountId,controller.signal).then(result=>{if(controller.signal.aborted)return;if(result.account?.account_id!==accountId)throw Error(READ_ERROR);rememberAccount(accountId,tab);router.replace("/simulation/localsim/workbench");}).catch(()=>{if(!controller.signal.aborted){setError(true);sessionStorage.removeItem(storageKey);sessionStorage.setItem("localsim-entry-error","1");router.replace("/simulation/localsim/workbench");}});return()=>controller.abort();},[accountId,tab,router]);
  return <div className={styles.shell}>{error?<p role="alert">{READ_ERROR}</p>:<p>正在验证账户归属并打开交易工作台…</p>}<Link href="/simulation/localsim/workbench">返回模拟盘总览</Link></div>;
}
