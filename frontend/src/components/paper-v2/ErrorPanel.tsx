"use client";

import { useMemo, useState } from "react";
import { PaperV2ApiError } from "@/lib/paper-v2/api";

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function buildDiagnostic(error: unknown): string {
  const apiError = error instanceof PaperV2ApiError ? error : null;
  const message = error instanceof Error ? error.message : String(error);
  const payload = {
    error_code: apiError?.errorCode || null,
    http_status: apiError?.status || null,
    message,
    context: apiError?.context || null,
    raw: apiError?.raw || null,
  };
  return [
    "Paper v2 错误诊断",
    `错误码: ${payload.error_code || "-"}`,
    `HTTP: ${payload.http_status || "-"}`,
    `说明: ${message}`,
    "",
    safeJson(payload),
  ].join("\n");
}

export default function ErrorPanel({ error, title = "操作失败" }: { error: unknown; title?: string }) {
  const [copied, setCopied] = useState(false);
  const diagnostic = useMemo(() => (error ? buildDiagnostic(error) : ""), [error]);
  if (!error) return null;

  const apiError = error instanceof PaperV2ApiError ? error : null;
  const message = error instanceof Error ? error.message : String(error);

  async function copyDiagnostic() {
    await navigator.clipboard.writeText(diagnostic);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1800);
  }

  return (
    <div className="pv2-error-panel">
      <div className="pv2-error-kicker">{title}</div>
      <div className="pv2-error-main">
        {apiError?.errorCode ? <strong>{apiError.errorCode}: </strong> : null}
        {message}
      </div>
      {apiError ? <div className="pv2-error-meta">HTTP {apiError.status}</div> : null}
      <textarea className="pv2-input pv2-diagnostic-text" readOnly rows={7} value={diagnostic} />
      <button className="pv2-button pv2-button-ghost" onClick={copyDiagnostic} type="button">
        {copied ? "已复制" : "复制诊断信息给 Codex"}
      </button>
    </div>
  );
}
