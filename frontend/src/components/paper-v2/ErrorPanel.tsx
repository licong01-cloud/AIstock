"use client";

import { useState } from "react";
import { PaperV2ApiError } from "@/lib/paper-v2/api";
import JsonPanel from "./JsonPanel";

export default function ErrorPanel({ error, title = "操作失败" }: { error: unknown; title?: string }) {
  const [open, setOpen] = useState(false);
  if (!error) return null;

  const apiError = error instanceof PaperV2ApiError ? error : null;
  const message = error instanceof Error ? error.message : String(error);
  const context = apiError?.context || (apiError?.raw && typeof apiError.raw === "object" ? apiError.raw as Record<string, unknown> : undefined);

  return (
    <div className="pv2-error-panel">
      <div className="pv2-error-kicker">{title}</div>
      <div className="pv2-error-main">
        {apiError?.errorCode ? <strong>{apiError.errorCode}: </strong> : null}{message}
      </div>
      {apiError ? <div className="pv2-error-meta">HTTP {apiError.status}</div> : null}
      {context ? (
        <button className="pv2-link-button" onClick={() => setOpen((value) => !value)} type="button">
          {open ? "隐藏错误详情" : "显示错误详情"}
        </button>
      ) : null}
      {open && context ? <JsonPanel value={context} /> : null}
    </div>
  );
}
