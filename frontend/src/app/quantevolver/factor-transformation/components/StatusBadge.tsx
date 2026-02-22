"use client";

import React from "react";
import { STATUS_CONFIG } from "./types";

export function StatusBadge({ status }: { status: string | null }) {
  const cfg = STATUS_CONFIG[status ?? "PENDING"] ?? STATUS_CONFIG["PENDING"];
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cfg.bg} ${cfg.color}`}
    >
      {cfg.label}
    </span>
  );
}
