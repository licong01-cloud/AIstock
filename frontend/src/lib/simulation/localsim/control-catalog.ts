"use client";

import { useEffect, useState } from "react";
import { localSimApi, request } from "./api";
import { human, safeDate, READ_ERROR } from "./workbench";
import type { LocalSimRuntimeProfile, LocalSimRuntimeProfileVersion, RuntimeProfileConfigRequest } from "./types";

export type PackageChoice = Awaited<ReturnType<typeof localSimApi.listPackages>>[number];
export type PolicyChoice = Awaited<ReturnType<typeof localSimApi.listExecutionPolicies>>[number];
export type VersionChoice = LocalSimRuntimeProfileVersion & { label: string };
export const packageLabel = (p: PackageChoice, index: number) => `${human(p.display_name, human(p.package_name, "策略名称待完善"))} · 列表第 ${index+1} 项${p.created_at ? ` · 创建于 ${safeDate(p.created_at.slice(0,10))}` : ""}`;
export const profileLabel = (p: LocalSimRuntimeProfile, index: number) => `${human(p.profile_name,"日常配置")} · 创建于 ${safeDate(p.created_at.slice(0,10))} · 第 ${index+1} 项`;
export const policyLabel = (p: PolicyChoice, index: number) => `${human(p.policy_name,"均匀分时成交规则")} · 第 ${index+1} 项`;
export function versionLabel(v: LocalSimRuntimeProfileVersion, profileName = "日常配置") {
  const daily = v.config_json.daily_strategy as { top_k?: number } | undefined;
  return `${human(profileName,"日常配置")} · 第 ${v.version_no} 版${Number.isInteger(daily?.top_k)?` · 目标 ${daily!.top_k} 只`:""} · ${safeDate(v.created_at.slice(0,10))}`;
}
export function useControlCatalog() {
  const [packages,setPackages]=useState<PackageChoice[]>([]),[packageId,setPackageId]=useState("");
  const [profiles,setProfiles]=useState<LocalSimRuntimeProfile[]>([]),[versions,setVersions]=useState<VersionChoice[]>([]),[policies,setPolicies]=useState<PolicyChoice[]>([]);
  const [error,setError]=useState(""),[loading,setLoading]=useState(true),[revision,setRevision]=useState(0);
  useEffect(()=>{const clear=()=>{["localsim-configure-selection","localsim-replay-selection","localsim-workbench-selection"].forEach(key=>sessionStorage.removeItem(key));setPackageId("");setPackages([]);setProfiles([]);setVersions([]);setPolicies([]);};window.addEventListener("logout",clear);return()=>window.removeEventListener("logout",clear);},[]);
  useEffect(()=>{const q=new URLSearchParams(window.location.search);if(q.get("package_id")){sessionStorage.setItem("localsim-configure-selection",JSON.stringify({packageId:q.get("package_id"),topK:Number(q.get("top_k"))}));history.replaceState(history.state,"",window.location.pathname);}},[]);
  useEffect(()=>{let valid=true;localSimApi.listPackages().then(rows=>{if(valid){setPackages(rows);let preferred="";try{preferred=JSON.parse(sessionStorage.getItem("localsim-configure-selection")||"null")?.packageId||"";}catch{}setPackageId(id=>rows.some(p=>p.package_id===id)?id:rows.some(p=>p.package_id===preferred)?preferred:rows[0]?.package_id||"");}}).catch(()=>{if(valid)setError("策略目录读取失败，请刷新重试。");});return()=>{valid=false;};},[revision]);
  useEffect(()=>{
    let valid=true;setLoading(true);setProfiles([]);setVersions([]);setPolicies([]);
    if(!packageId){setLoading(false);return;}
    Promise.all([localSimApi.listProfiles(packageId),localSimApi.listExecutionPolicies(packageId)]).then(async([ps,es])=>{
      if(ps.next_cursor)throw Error();
      const pages=await Promise.all(ps.items.map(p=>localSimApi.listProfileVersions(p.profile_id)));
      if(pages.some(p=>p.next_cursor))throw Error();
      if(valid){setProfiles(ps.items);setVersions(pages.flatMap((p,i)=>p.items.map(v=>({...v,label:versionLabel(v,ps.items[i].profile_name)}))));setPolicies(es.filter(p=>p.algo_code?.toUpperCase()==="TWAP"));setError("");}
    }).catch(()=>{if(valid)setError("配置目录未能完整读取，请刷新后再操作。");}).finally(()=>{if(valid)setLoading(false);});
    return()=>{valid=false;};
  },[packageId,revision]);
  return {packages,packageId,setPackageId,profiles,versions,policies,error,loading,reload:()=>setRevision(v=>v+1)};
}

export interface HmmChoice { snapshot: string; label: string; presets: {key:string;label:string}[] }
export async function loadHmmChoices(): Promise<HmmChoice[]> {
  const configs=await request<{config_id:string;display_name:string;config_json:{signal_presets?:Record<string,unknown>}}[]>("/hmm-training/configs");
  const pages=await Promise.all(configs.map(c=>request<{snapshot_id:string;display_name?:string;trained_at:string;status:string}[]>(`/hmm-training/configs/${encodeURIComponent(c.config_id)}/snapshots`)));
  return pages.flatMap((rows,i)=>rows.filter(r=>["ready","completed","success","succeeded"].includes(r.status.toLowerCase())).map((r,j)=>({snapshot:r.snapshot_id,
    label:`${human(r.display_name,human(configs[i].display_name,"市场状态模型"))} · 训练于 ${safeDate(r.trained_at.slice(0,10))} · 第 ${i+1} 组第 ${j+1} 项`,
    presets:Object.keys(configs[i].config_json.signal_presets||{}).length ? Object.keys(configs[i].config_json.signal_presets!).map((key,n)=>({key,label:`信号规则第 ${n+1} 项`})) : [{key:"preset_A",label:"默认信号规则"}]}))) ;
}

export function configRequest(base?: LocalSimRuntimeProfileVersion): RuntimeProfileConfigRequest {
  const c=base?.config_json;
  const daily=c?.daily_strategy as RuntimeProfileConfigRequest["daily_strategy"]|undefined;
  const hmm=c?.hmm as RuntimeProfileConfigRequest["hmm"]|undefined;
  return {schema_version:"localsim_runtime_profile_config_request_v1",
    daily_strategy:daily?structuredClone(daily):{strategy_id:"daily_selection_v1",strategy_version:"v1",top_k:20,industry_filters:[],sector_filters:[],parameters:{}},
    hmm:hmm?structuredClone(hmm):{enabled:false,snapshot_id:null,model_version:null,preset:null,state_mapping:{}},
    risk_policy:structuredClone(c?.risk_policy as Record<string,unknown>||{max_position_weight:0.1}),fee_policy:structuredClone(c?.fee_policy as Record<string,unknown>||{commission_bps:3}),
    runtime_variant_id:c?.runtime_variant_id as string|null||null,runtime_variant_hash:c?.runtime_variant_hash as string|null||null,
    metadata:structuredClone(c?.metadata as Record<string,unknown>||{}),notes:"交易工作台创建并由原生服务验证"};
}
