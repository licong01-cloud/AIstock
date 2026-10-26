import { expect, test } from "@playwright/test";

const study="a".repeat(64),native="lsdm_"+"1".repeat(40),human="lsdm_"+"2".repeat(40);
const root="/simulation-runtime/localsim/comparisons";

test("two modes keep independent accounts, readable details, filters and read-only requests",async({page})=>{
 const seen:string[]=[];
 await page.route("**/api/v1/**",async route=>{
  const request=route.request(),url=new URL(request.url()),path=url.pathname.replace(/^\/api\/v1/,"");
  seen.push(request.method());
  let response:unknown={items:[],count:0,alerts:[]};
  if(path===`${root}/${study}`)response={schema_version:"localsim_dual_mode_detail_v1",key:study,name:"LightGBM＋12项因子",complete:true,start_date:"2026-01-05",end_date:"2026-09-30",explanation:"同初态两模式整流程比较；分钟代理不保证真实成交。",comparisons:[{family:"NATIVE_VS_HUMAN_WORKFLOW",account_id:human,baseline_account_id:native,net_increment_cny:"-2000",mdd_reduction:"0.01"}],accounts:[
   {key:native,mode:"原生执行",rule:"原生分时执行",name:"原生分时执行",delay_seconds:null,initial_cash:"10000000",nav:"10020000",pnl:"20000",cash:"1000000",holdings_value:"9020000",last_day_pnl:"200",last_trade_date:"2026-09-30",return_pct:"0.2",max_drawdown_pct:"2",fees:"1000",completion_pct:"99",operations:{},curve:[{date:"2026-01-05",nav_cny:"10000000"},{date:"2026-09-30",nav_cny:"10020000"}]},
   {key:human,mode:"模拟人工",rule:"简单执行",name:"简单执行 · 1分钟响应",delay_seconds:60,initial_cash:"10000000",nav:"10018000",pnl:"18000",cash:"9000000",holdings_value:"1018000",last_day_pnl:"-300",last_trade_date:"2026-09-30",return_pct:"0.18",max_drawdown_pct:"1",fees:"900",completion_pct:"60",operations:{action_count:123,expired_actions:10,pending_cancellation_at_deadline:0,cancellation_races:0},statuses:[{name:"未成交",count:10}],constraints:[{name:"响应或排队期间机会过期",count:10}],curve:[{date:"2026-01-05",nav_cny:"10000000"},{date:"2026-09-30",nav_cny:"10018000"}]}]};
  if(path.includes("/accounts/")){
   const view=url.searchParams.get("view")||"positions";
   response={schema_version:"localsim_dual_mode_account_view_v1",view,items:view==='positions'?[{name:"浦发银行",quantity:1000,available:1000,cost:"10",price:"11",value:"11000",pnl:"1000"}]:view==='trades'?[{name:"浦发银行",side:"买入",quantity:1000,price:"10",fees:"5",time:"2026-09-21T09:35:00+08:00"}]:view==='actions'?[{name:"浦发银行",time:"2026-09-21T09:33:10+08:00",kind:"下单尝试",status:"操作已确认",waiting_seconds:"60"}]:[],total_count:1,next_cursor:null,snapshot_set:"private_snapshot_never_display"};
  }
  await route.fulfill({contentType:"application/json",body:JSON.stringify(response)});
 });
 await page.goto(`/simulation/localsim/comparisons/${study}`);
 await expect(page.getByRole('heading',{name:'LightGBM＋12项因子'})).toBeVisible();
 await expect(page.getByText('10,020,000.00',{exact:true})).toBeVisible();
 await expect(page.getByText('10,018,000.00',{exact:true})).toBeVisible();
 await expect(page.getByText('浦发银行',{exact:true})).toBeVisible();
 await page.getByRole('button',{name:'账户详情',exact:true}).nth(1).click();
 await expect(page.getByText('可用现金 9,000,000.00 元',{exact:false})).toBeVisible();
 await expect(page.getByText('当日盈亏 −300.00 元',{exact:false})).toBeVisible();
 await page.getByText('计划中观察到的等待与执行约束',{exact:false}).click();
 await expect(page.getByText('响应或排队期间机会过期：10项',{exact:true})).toBeVisible();
 await page.getByRole('button',{name:'人工操作',exact:true}).click();
 await expect(page.getByText('下单尝试',{exact:true})).toBeVisible();
 await expect(page.getByText('操作完成不代表成交',{exact:false})).toBeVisible();
 await page.getByLabel('研究开始日期').fill('2026-09-01');
 await page.getByRole('button',{name:'历史成交',exact:true}).click();
 await expect(page.getByText('浦发银行',{exact:true})).toBeVisible();
 const visible=await page.locator('body').innerText();
 expect(visible).not.toContain(study);expect(visible).not.toContain(native);expect(visible).not.toContain(human);
 expect(visible).not.toContain('private_snapshot');expect(seen.every(method=>method==='GET')).toBeTruthy();
});
