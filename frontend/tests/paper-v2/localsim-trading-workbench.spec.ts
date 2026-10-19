import { expect, test, type Page, type Route } from "@playwright/test";
import { readFileSync } from "node:fs";

const a="simacct_private_a", b="simacct_private_b", symbol="600000.SH";
const stamp="2026-01-06T15:00:00+08:00";
const base="/simulation-runtime/localsim";
type Seen={method:string;path:string;query:string;body:unknown};
type State={many?:boolean;fail?:boolean;wrongIdentity?:boolean;badAccount?:boolean;malicious?:boolean;gate?:Promise<void>;paused?:boolean;seen:Seen[]};
const accountName=(id:string)=>id===a?"历史模拟账户甲":"日常模拟账户乙";
function summary(id:string){
  return {name:accountName(id),strategy:"沪深300增强",stage:"历史回放",status:"启用",initial_capital:"10000",created_at:"2026-01-01T00:00:00Z",
    economic_as_of:stamp,valuation_kind:"已完成收盘估值",nav:id===a?"10190":"20200",cash:"10190",value:"0",available_cash:null,frozen_cash:null,
    opening_nav:"10000",closing_nav:"10190",net_flows:"0",pnl:"190",daily_pnl:"95",daily_date:"2026-01-06",return_pct:"1.9",realized:"200",
    unrealized:"0",unrealized_change:"0",fees:"10",position_count:0,position_ratio:"0",completed_date:"2026-01-06",next_date:"2026-01-07",
    start_date:"2026-01-05",end_date:"2026-08-31",source:"数据集文件",issue:"等待下一处理步骤",cost_basis:"账本平均成本不含交易费用，费用单列",comparable_live:false};
}
const securityRow={key:symbol,security:symbol,name:"测试证券",market:"上海市场",date:"2026-01-05",time:"2026-01-05T15:00:00+08:00",side:"买入",status:"",description:"历史分钟收盘估值",known:true};
const curve=[{key:"2026-01-06",date:"2026-01-06",value:"10190",pnl:"95",fees:"5",opening_value:"10095",cash_delta:"0",known:true},{key:"2026-01-05",date:"2026-01-05",value:"10095",pnl:"95",fees:"5",opening_value:"10000",cash_delta:"0",known:true}];
const profile={profile_id:"profile_private",profile_name:"日常运行规则",package_id:"qe_private_package",version:7,status:"ACTIVE",created_at:stamp};
const version={profile_id:profile.profile_id,profile_version_id:"profile_version_private",version_no:1,created_at:stamp,validation_status:"VALIDATED",config_json:{daily_strategy:{strategy_id:"daily_selection_v1",strategy_version:"v1",top_k:50,industry_filters:[],sector_filters:[],parameters:{}},hmm:{enabled:false,snapshot_id:null,model_version:null,preset:null,state_mapping:{}},risk_policy:{max_position_weight:0.1},fee_policy:{commission_bps:3},metadata:{}}};
const list=(items:unknown[])=>({ok:true,schema_version:"localsim_list_response_v1",items,next_cursor:null,limit:200});
const readiness=()=>({schema_version:"localsim_cutover_readiness_v1",ready:true,checked_at:new Date().toISOString(),blockers:[],retained_legacy_account_ids:[],missing_lineage_account_ids:[],legacy_active_session_count:0,legacy_auto_run_count:0,legacy_sentinel_count:0,in_flight_economic_run_count:0});
const replay={schema_version:"localsim_replay_job_v1",replay_job_id:"lsreplay_private",simulation_account_id:a,start_trade_date:"2026-01-05",end_trade_date:"2026-08-31",completed_trade_date:"2026-01-06",next_trade_date:"2026-01-07",status:"FAILED_TERMINAL",historical_source_id:"dataset_files.frozen_market.v1",failure_code:"PRIVATE_ERROR_CODE",failure_context:{message:"qe_private_package C:/private/model.json"},version:3,created_at:stamp,updated_at:stamp};
async function json(route:Route,payload:unknown,status=200){await route.fulfill({status,contentType:"application/json",body:JSON.stringify(payload)});}
async function mock(page:Page,state:State){
  await page.route("**/api/v1/**",async route=>{
    const req=route.request(),url=new URL(req.url()),path=url.pathname.replace(/^\/api\/v1/,"");
    state.seen.push({method:req.method(),path,query:url.search,body:req.postData()?req.postDataJSON():null});
    if(path.endsWith("/cutover-readiness"))return json(route,{readiness:readiness()});
    if(path.includes("scheduler/verification-status"))return json(route,{scheduler:{running:true,thread_alive:true,scheduler_loop_health:{status:"HEALTHY"},current_trade_date_blockers:{trade_date:"2026-10-19",status:"CLEAR",blocker_count:0},effective_runtime_health:"NO_CURRENT_DAY_BLOCKER"}});
    if(path===base+"/workbench/overview")return json(route,{items:[a,b].map(key=>({key,summary:{...summary(key),name:state.malicious?"精选 qe_20261007_141342_0221":accountName(key)},curve})),generated_at:stamp,complete:true,nav:null,pnl:null,daily_pnl:null,return_pct:null,economic_as_of:null,included_count:0,excluded_count:2,explanation:"历史回放不纳入实时合计；按各账户经济日查看",curve:[]});
    if(path==="/strategy-packages")return json(route,{packages:[{package_id:"qe_private_package",display_name:"沪深300增强",package_status:"PAPER_READY",created_at:stamp}]});
    if(path.endsWith("/execution-policies"))return json(route,{execution_policies:[{policy_id:"policy_private",policy_name:"均匀分时规则",algo_code:"TWAP"}]});
    if(path===base+"/runtime-profiles")return json(route,list([profile]));
    if(path.includes("/runtime-profiles/")&&path.endsWith("/versions"))return json(route,list([version]));
    if(path==="/trading-calendar/status")return json(route,{as_of_date:"2026-10-19",is_trading_day:false,latest_completed_trading_day:"2026-08-31",next_trading_day:"2026-10-20"});
    if(path===base+"/replays")return json(route,list([replay]));
    if(path.includes("/replays/"))return json(route,{ok:true,schema_version:"localsim_control_response_v1",replay});
    if(path.startsWith(base+"/accounts/")){
      const bits=path.slice((base+"/accounts/").length).split("/"),id=decodeURIComponent(bits[0]),view=bits[1];
      if(req.method()==="POST"){
        if(view==="pause")state.paused=true;
        return json(route,{ok:true,schema_version:"localsim_control_response_v1",account:{account_id:id,account_name:accountName(id),status:state.paused?"PAUSED":"ACTIVE",version:8}});
      }
      if(!view)return json(route,{ok:true,schema_version:"localsim_control_response_v1",account:{account_id:state.badAccount?b:id,account_name:accountName(id),status:state.paused?"PAUSED":"ACTIVE",version:7}});
      if(view==="summary"&&id===a&&state.gate)await state.gate;
      if(state.fail)return json(route,{detail:{message:"qe_20261007_141342_0221 C:/private/model.json",code:"RAW_FAILURE"}},500);
      if(view==="export")return route.fulfill({contentType:"text/csv; charset=utf-8",body:"\ufeff证券名称,数量,实际费用\n"+Array.from({length:state.many?121:2},()=>"测试证券,100,5.00").join("\n")});
      let rows:Record<string,unknown>[]=[];
      if(view==="positions"&&url.searchParams.get("as_of")==="2026-01-05")rows=[{...securityRow,name:id===a?"测试证券":"乙方证券",quantity:100,available:0,frozen:null,cost:"10",price:"11",value:"1100",unrealized:"100",return_pct:"10",weight:"10.89",pnl:"95"}];
      if(view==="positions"&&id===b)rows=[{...securityRow,name:"乙方证券",quantity:200,available:200,cost:"10",price:"11",value:"2200",unrealized:"200"}];
      if(view==="trades")rows=Array.from({length:state.many?121:2},(_,i)=>({...securityRow,key:"fill_private_"+i,order_key:"order_private",quantity:100,price:"12",amount:"1200",fees:"5",commission:null,tax:null,description:"费用合计按实际账本；分项未单独记录"}));
      if(view==="orders")rows=[{...securityRow,key:"order_private",order_key:"order_private",quantity:300,filled:200,remaining:100,status:"部分成交",price:"12"}];
      if(view==="events")rows=[{...securityRow,key:"event_private_1",order_key:"order_private",name:"委托状态变化",status:"已受理"},{...securityRow,key:"event_private_2",order_key:"order_private",name:"委托状态变化",status:"部分成交"}];
      if(view==="cash-flows")rows=[{...securityRow,key:"cash_private",order_key:"order_private",amount:"1200",fees:"5",cash_delta:"1195",balance:"10190"}];
      if(view==="pnl")rows=url.searchParams.get("group_by")==="security"?[{...securityRow,opening_quantity:0,closing_quantity:0,opening_value:"0",value:"0",buy_quantity:100,sell_quantity:100,buy_amount:"1000",sell_amount:"1200",fees:"10",pnl:"190",realized:"200",unrealized_change:"0"}]:curve;
      const q=url.searchParams.get("search");if(q)rows=rows.filter(r=>String(r.name||"").includes(q));
      const offset=Number((url.searchParams.get("cursor")||"").split("_").at(-1))||0,size=Number(url.searchParams.get("page_size")||50);
      return json(route,{schema_version:"localsim_workbench_v1",account_id:state.wrongIdentity&&view!=="summary"?(id===a?b:a):id,snapshot_set:id+"_private_generation",generated_at:stamp,status:"complete",summary:summary(id),items:rows.slice(offset,offset+size),total_count:rows.length,next_cursor:offset+size<rows.length?"cursor_private_"+(offset+size):null,missing_dates:[],warnings:[],actual_start:"2026-01-05",actual_end:"2026-01-06"});
    }
    return json(route,{items:[],count:0,alerts:[]});
  });
}
async function seed(page:Page,tab="positions"){
  await page.addInitScript(({id,tab})=>sessionStorage.setItem("localsim-workbench-selection",JSON.stringify({accountId:id,tab,filters:{}})),{id:a,tab});
}
const tabs=(page:Page)=>page.getByRole("navigation",{name:"账户详情标签"});
async function noOpaque(page:Page){
  const main=page.locator(".app-main"),text=await main.innerText(),aria=await main.ariaSnapshot();
  const attrs=await main.locator("[title],[aria-label],[placeholder]").evaluateAll(nodes=>nodes.map(n=>[n.getAttribute("title"),n.getAttribute("aria-label"),n.getAttribute("placeholder")].join(" ")).join("\n"));
  expect(text+"\n"+aria+"\n"+attrs).not.toMatch(/(?:simacct_|qe_2026|lsreplay_|profile_private|order_private|fill_private|private_generation|600000\.SH|C:\/private|\{\s*"schema_version")/);
  expect(page.url()).not.toMatch(/simacct_|lsreplay_|package_id=|600000/);
  expect(await page.title()).not.toMatch(/simacct_|qe_|lsreplay_/);
}

test("overview keeps separate accounts and the original navigation, with honest comparable totals",async({page},info)=>{
  const state:State={seen:[]};await mock(page,state);await page.setViewportSize({width:1440,height:1000});await page.goto("/simulation/localsim");
  await expect(page.getByRole("heading",{name:"模拟盘总览",exact:true})).toBeVisible();
  await expect(page.getByRole("button",{name:"详情",exact:true})).toHaveCount(2);
  await expect(page.getByText("历史回放不纳入实时合计；按各账户经济日查看", {exact:true})).toBeVisible();
  await expect(page.locator("aside.sidebar")).toHaveCount(1);
  await page.locator(".sidebar-group-title").filter({hasText:"策略包与模拟交易"}).click();
  await expect(page.locator(".sidebar a[href='/simulation/localsim']")).toHaveCount(1);
  expect((await page.locator("aside.sidebar").boundingBox())!.width).toBeCloseTo(260,0);
  await noOpaque(page);await page.screenshot({path:info.outputPath("overview-wide.png"),fullPage:true});
  await page.setViewportSize({width:698,height:1000});
  const contentWidth=await page.locator(".app-main > div").first().evaluate(node=>node.getBoundingClientRect().width);
  expect(contentWidth).toBeCloseTo(390,0);
  expect(await page.evaluate(()=>document.documentElement.scrollWidth)).toBeLessThanOrEqual(699);
  await page.screenshot({path:info.outputPath("overview-content-390.png"),fullPage:true});
  await page.setViewportSize({width:390,height:844});
  expect((await page.locator("aside.sidebar").boundingBox())!.width).toBeCloseTo(260,0);
  expect(await page.evaluate(()=>document.documentElement.scrollWidth)).toBeLessThanOrEqual(391);
  await page.screenshot({path:info.outputPath("overview-narrow.png"),fullPage:true});
  expect(state.seen.filter(r=>r.method!=="GET")).toEqual([]);
});

test("historical holdings, cleared-stock drill-down, dates, focus and fixed address",async({page},info)=>{
  const state:State={seen:[]};await mock(page,state);await seed(page);await page.goto("/simulation/localsim/workbench");
  await expect(page.getByText("已载入 0 / 0 条", {exact:false})).toBeVisible();
  await page.getByLabel("持仓估值日期").fill("2026-01-05");
  const stock=page.getByRole("button",{name:"测试证券 上海市场",exact:false});await expect(stock).toBeVisible();await stock.click();
  const dialog=page.getByRole("dialog",{name:"个股持仓与交易明细"});await expect(dialog).toBeVisible();
  await expect(dialog.getByText("区间增仓", {exact:false})).toContainText("100 股");
  await noOpaque(page);await page.keyboard.press("Escape");await expect(dialog).toHaveCount(0);await expect(stock).toBeFocused();
  await tabs(page).getByRole("button",{name:"区间盈亏",exact:true}).click();await page.getByLabel("盈亏统计维度").selectOption("security");
  await expect(page.getByRole("columnheader",{name:"期末数量"})).toBeVisible();
  await page.getByRole("button",{name:"测试证券 上海市场",exact:false}).click();
  await expect(page.getByRole("dialog")).toContainText("期末持仓数量");
  await page.getByRole("button",{name:"查看完整交易记录"}).click();await expect(tabs(page).getByRole("button",{name:"历史成交"})).toHaveAttribute("aria-current","page");
  await expect(page.getByText("股票：测试证券", {exact:false})).toBeVisible();
  expect(state.seen.some(r=>r.path.endsWith("/trades")&&r.query.includes("security=600000.SH")&&r.query.includes("end=2026-01-05"))).toBeTruthy();
  await noOpaque(page);await page.screenshot({path:info.outputPath("account-holdings.png"),fullPage:true});
});

test("complete stable pagination and full CSV keep duplicate-looking fills",async({page})=>{
  const state:State={seen:[],many:true};await mock(page,state);await seed(page,"trades");await page.goto("/simulation/localsim/workbench");
  await expect(page.getByText("已载入 50 / 121 条",{exact:false})).toBeVisible();
  await page.getByRole("button",{name:"加载更多",exact:true}).click();await expect(page.getByText("已载入 100 / 121 条",{exact:false})).toBeVisible();
  await page.getByRole("button",{name:"加载更多",exact:true}).click();await expect(page.getByText("已载入 121 / 121 条",{exact:false})).toBeVisible();
  await expect(page.getByRole("button",{name:"加载更多",exact:true})).toBeDisabled();
  const [download]=await Promise.all([page.waitForEvent("download"),page.getByRole("button",{name:"导出完整筛选"}).click()]);
  expect(download.suggestedFilename()).toBe("模拟盘历史成交明细.csv");
  const data=readFileSync((await download.path())!,"utf8");expect(data.split("\n").length).toBe(122);expect(data).not.toMatch(/private|600000\.SH/);
  await noOpaque(page);
});

test("order detail shows original order, all partial fills, event timeline and cash",async({page})=>{
  const state:State={seen:[],many:true};await mock(page,state);await seed(page,"orders");await page.goto("/simulation/localsim/workbench");
  await page.getByRole("button",{name:"查看关联委托"}).first().click();
  const dialog=page.getByRole("dialog",{name:"关联委托与部分成交"});await expect(dialog.getByRole("heading",{name:"全部部分成交 · 121 条"})).toBeVisible();
  await expect(dialog.getByRole("heading",{name:"委托状态时间线 · 2 条"})).toBeVisible();
  await expect(dialog.getByRole("heading",{name:"对应资金流水 · 1 条"})).toBeVisible();await noOpaque(page);
  await page.keyboard.press("Escape");await expect(dialog).toHaveCount(0);
  expect(state.seen.filter(r=>r.method!=="GET")).toEqual([]);
});

test("switching account cancels slow reads and rejects another account's response",async({page})=>{
  let release!:()=>void;const gate=new Promise<void>(r=>release=r),state:State={seen:[],gate};
  await mock(page,state);await seed(page);await page.goto("/simulation/localsim/workbench");
  await expect(page.getByLabel("切换策略账户")).toBeVisible();await page.getByLabel("切换策略账户").selectOption(b);
  await expect(page.getByRole("button",{name:"乙方证券 上海市场",exact:false})).toBeVisible();
  release();await expect(page.getByRole("heading",{name:accountName(b),exact:true})).toBeVisible();
  await expect(page.getByRole("button",{name:"测试证券 上海市场",exact:false})).toHaveCount(0);
  state.wrongIdentity=true;await page.getByRole("button",{name:"刷新",exact:true}).click();await expect(page.getByRole("alert")).toBeVisible();
  await noOpaque(page);
});

test("all tabs and safe failures contain no raw objects, IDs or arbitrary backend error text",async({page},info)=>{
  const state:State={seen:[],malicious:true};await mock(page,state);await seed(page);await page.goto("/simulation/localsim/workbench");
  await expect(tabs(page)).toBeVisible();
  for(const tab of ["持仓","历史成交","委托记录","区间盈亏","资金流水","回放与运行","交易监控"]){
    await tabs(page).getByRole("button",{name:tab,exact:true}).click();
    await expect(page.getByRole("button",{name:"刷新",exact:true})).toBeEnabled();await noOpaque(page);
  }
  await page.screenshot({path:info.outputPath("monitor-wide.png"),fullPage:true});
  state.fail=true;await page.getByRole("button",{name:"刷新",exact:true}).click();await expect(page.getByRole("alert").first()).toContainText("本次读取失败");
  await noOpaque(page);expect(state.seen.filter(r=>r.method!=="GET")).toEqual([]);
});

test("legacy account links validate identity and open the same fixed workbench",async({page})=>{
  const state:State={seen:[]};await mock(page,state);await page.goto("/simulation/localsim/accounts/"+a+"/performance");
  await expect(page).toHaveURL(/\/simulation\/localsim\/workbench$/);
  await expect(tabs(page).getByRole("button",{name:"区间盈亏",exact:true})).toHaveAttribute("aria-current","page");await noOpaque(page);
});

test("an invalid legacy account link returns to the fixed overview with a visible identity error",async({page})=>{
  const state:State={seen:[],badAccount:true};await mock(page,state);await page.goto("/simulation/localsim/accounts/"+a+"/ledger");
  await expect(page).toHaveURL(/\/simulation\/localsim\/workbench$/);
  await expect(page.getByText("原账户入口未通过核验，请从总览重新选择。",{exact:false})).toBeVisible();
  await expect(page.getByRole("heading",{name:"模拟盘总览",exact:true})).toBeVisible();await noOpaque(page);
  expect(state.seen.filter(r=>r.method!=="GET")).toEqual([]);
});

test("configuration and replay pages preserve native controls without exposing technical strings",async({page})=>{
  const state:State={seen:[]};await mock(page,state);await page.goto("/simulation/localsim?package_id=qe_private_package&top_k=30");
  await expect(page).toHaveURL(/\/configure$/);await expect(page.getByLabel("目标持仓股票数")).toHaveValue("30");await noOpaque(page);
  await page.goto("/simulation/localsim/replays");await expect(page.getByRole("heading",{name:"历史回放",exact:true})).toBeVisible();await noOpaque(page);
  await page.getByRole("button",{name:"回放详情",exact:true}).click();await expect(page).toHaveURL(/\/replays\/details$/);
  await expect(page.getByText("回放已终止，需诊断", {exact:true})).toBeVisible();await noOpaque(page);
  expect(state.seen.filter(r=>r.method!=="GET")).toEqual([]);
});

test("account lifecycle controls submit only after a deliberate confirmation using the native version",async({page})=>{
  const state:State={seen:[]};await mock(page,state);await seed(page,"events");await page.goto("/simulation/localsim/workbench");
  await expect(page.getByRole("button",{name:"暂停账户",exact:true})).toBeEnabled();
  expect(state.seen.filter(r=>r.method!=="GET")).toEqual([]);
  await page.getByRole("button",{name:"暂停账户",exact:true}).click();expect(state.seen.filter(r=>r.method!=="GET")).toEqual([]);
  await page.getByRole("button",{name:"确认暂停账户",exact:true}).click();
  await expect(page.getByRole("button",{name:"恢复账户",exact:true})).toBeVisible();
  expect(state.seen.filter(r=>r.method==="POST")).toEqual([{method:"POST",path:base+"/accounts/"+a+"/pause",query:"",body:{schema_version:"localsim_lifecycle_request_v1",expected_version:7}}]);await noOpaque(page);
});

