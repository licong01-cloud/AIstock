"use client";

import { useState } from "react";

export default function CopyChip({
  label,
  value,
  title,
}: {
  label: string;
  value: string | null | undefined;
  title?: string;
}) {
  const [copied, setCopied] = useState(false);
  const text = String(value || "");

  async function copy() {
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch {
      setCopied(false);
    }
  }

  return (
    <button
      type="button"
      className="pv2-chip pv2-chip-copy"
      onClick={copy}
      title={title || text || ""}
      disabled={!text}
    >
      <span>{label}</span>
      <span className="pv2-chip-copy-icon" aria-hidden="true">{copied ? "已复制" : "复制"}</span>
    </button>
  );
}
