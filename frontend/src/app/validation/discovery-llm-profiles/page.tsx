"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";

import {
  LlmReportPanel,
  ToolAdapterPanel,
  display,
} from "@/components/validation/discovery/ActiveDiscoveryComponents";
import { errorMessage } from "@/components/validation/discovery/pageUtils";
import {
  type JsonObject,
  type ValidationDiscoveryLlmProfile,
  type ValidationDiscoveryLlmReport,
  type ValidationDiscoveryToolAdapter,
  validationApi,
} from "@/lib/validation/api";

export default function DiscoveryLlmProfilesPage() {
  const [profiles, setProfiles] = useState<ValidationDiscoveryLlmProfile[]>([]);
  const [llmReport, setLlmReport] = useState<ValidationDiscoveryLlmReport | null>(null);
  const [adapters, setAdapters] = useState<ValidationDiscoveryToolAdapter[]>([]);
  const [adapterResults, setAdapterResults] = useState<JsonObject[]>([]);
  const [evalResult, setEvalResult] = useState<JsonObject | null>(null);
  const [search, setSearch] = useState("");
  const [provider, setProvider] = useState("");
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [profilePage, llm, toolPage, evalSummary] = await Promise.all([
        validationApi.discoveryLlmProfiles(),
        validationApi.discoveryNightlyLlmReport("current"),
        validationApi.discoveryToolAdapters(),
        validationApi.discoveryLlmEvals(),
      ]);
      setProfiles(profilePage.items);
      setLlmReport(llm);
      setAdapters(toolPage.items);
      setEvalResult(evalSummary);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const filtered = useMemo(() => profiles.filter((profile) => {
    const text = `${profile.profile_id} ${profile.agent_role} ${profile.provider_id} ${profile.model_id} ${profile.prompt_id}`.toLowerCase();
    return (!search || text.includes(search.toLowerCase())) && (!provider || profile.provider_id === provider);
  }), [profiles, provider, search]);
  const pageSize = 7;
  const pageRows = filtered.slice((page - 1) * pageSize, page * pageSize);
  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));

  const runEval = async () => {
    try {
      const result = await validationApi.runDiscoveryLlmEval({ dry_run: true, profiles: profiles.slice(0, 2).map((item) => item.profile_id) });
      setEvalResult(result);
      setMessage(`Eval dry-run 完成：${display(result)}`);
    } catch (err) {
      setMessage(`Eval dry-run 失败：${errorMessage(err)}`);
    }
  };

  const runAdapter = async (adapter: ValidationDiscoveryToolAdapter) => {
    try {
      const result = await validationApi.runDiscoveryToolAdapter(adapter.adapter_id, { dry_run: true });
      setAdapterResults((items) => [result, ...items].slice(0, 5));
      setMessage(`${adapter.adapter_id} dry-run 完成`);
    } catch (err) {
      setMessage(`${adapter.adapter_id} dry-run 失败：${errorMessage(err)}`);
    }
  };

  return (
    <>
      <section className="pv2-card">
        <div className="pv2-card-head"><div><div className="pv2-eyebrow">LLM Profiles</div><h2>Discovery Agent Profile 引用</h2><p className="pv2-muted">本页只引用既有 Prompt 和模型配置，不录入 token；DeepSeek V4 Pro 等 key 仅由后端环境检测，不在 UI 暴露。</p></div><button className="pv2-button-primary" disabled={loading} onClick={refresh} type="button">{loading ? "刷新中" : "刷新配置"}</button></div>
        {error ? <div className="pv2-notice pv2-notice-warning">{error}</div> : null}
        {message ? <div className="pv2-notice pv2-notice-info">{message}</div> : null}
        <div className="disc-filter-grid disc-filter-grid-compact"><input className="pv2-input" placeholder="搜索 profile / role / prompt" value={search} onChange={(event) => { setSearch(event.target.value); setPage(1); }} /><select className="pv2-select" value={provider} onChange={(event) => { setProvider(event.target.value); setPage(1); }}><option value="">全部 Provider</option><option value="deepseek">deepseek</option></select></div>
        <div className="pv2-row-actions"><Link className="pv2-button-ghost" href="/quantevolver/prompts?agent_type=validation_discovery">打开 Prompt 管理</Link><Link className="pv2-button-ghost" href="/config/rdagent-llm">打开模型配置</Link></div>
      </section>
      <section className="pv2-card">
        <div className="pv2-card-head"><div><div className="pv2-eyebrow">Profile Table</div><h2>Profile 表格</h2></div></div>
        <div className="pv2-table-wrap"><table className="pv2-table"><thead><tr><th>Profile</th><th>Provider / Model</th><th>Prompt</th><th>开关</th><th>质量</th><th>引用</th></tr></thead><tbody>{pageRows.length ? pageRows.map((profile) => <tr key={profile.profile_id}><td><strong>{display(profile.agent_role)}</strong><br /><span className="pv2-mono">{profile.profile_id}</span></td><td>{display(profile.provider_id)} / {display(profile.model_id)}<br />{display(profile.provider_status)}</td><td>{display(profile.prompt_id)}<br />v{display(profile.prompt_version)}</td><td>nightly={display(profile.enabled_for_nightly)}<br />manual={display(profile.enabled_for_manual_mcp)}</td><td>{display(profile.last_7_runs)}</td><td><Link className="pv2-link-button" href={profile.prompt_management_url || "/quantevolver/prompts"}>Prompt</Link> / <Link className="pv2-link-button" href={profile.model_config_url || "/config/rdagent-llm"}>Model</Link></td></tr>) : <tr><td className="pv2-empty-cell" colSpan={6}>暂无匹配的 profile。</td></tr>}</tbody></table></div>
        <div className="pv2-pagination"><span>第 {page} / {totalPages} 页，总数 {filtered.length}</span><button className="pv2-button-ghost" disabled={page <= 1} onClick={() => setPage(page - 1)} type="button">上一页</button><button className="pv2-button-ghost" disabled={page >= totalPages} onClick={() => setPage(page + 1)} type="button">下一页</button></div>
      </section>
      <LlmReportPanel profiles={profiles} report={llmReport} onRunEval={runEval} />
      <section className="pv2-card"><div className="pv2-card-head"><div><div className="pv2-eyebrow">Eval Result</div><h2>promptfoo 风格 dry-run 结果</h2></div></div><pre className="disc-json-block">{JSON.stringify(evalResult, null, 2)}</pre></section>
      <ToolAdapterPanel adapters={adapters} results={adapterResults} onRun={runAdapter} />
    </>
  );
}
