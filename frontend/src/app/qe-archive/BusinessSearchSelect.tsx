"use client";

import React from "react";

export type BusinessSearchOption = {
  value: string;
  label: string;
  description?: string;
};

type Props = {
  label: string;
  value: string;
  options: BusinessSearchOption[];
  search: string;
  onSearchChange: (value: string) => void;
  onValueChange: (value: string) => void;
  searchPlaceholder: string;
  emptyLabel: string;
  required?: boolean;
  loading?: boolean;
  testId: string;
};

const controlStyle: React.CSSProperties = {
  width: "100%",
  padding: "8px 10px",
  border: "1px solid #cbd5e1",
  borderRadius: 7,
  background: "#fff",
  boxSizing: "border-box",
  fontSize: 12,
};

export default function BusinessSearchSelect({
  label,
  value,
  options,
  search,
  onSearchChange,
  onValueChange,
  searchPlaceholder,
  emptyLabel,
  required,
  loading,
  testId,
}: Props) {
  const selected = options.find((option) => option.value === value);
  return (
    <div data-testid={testId} style={{ display: "grid", gap: 6 }}>
      <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>
        {label}{required ? "（必选）" : "（可选）"}
        <input
          data-testid={`${testId}-search`}
          type="search"
          value={search}
          onChange={(event) => onSearchChange(event.target.value)}
          placeholder={searchPlaceholder}
          aria-label={`${label}业务搜索`}
          style={{ ...controlStyle, marginTop: 4 }}
        />
      </label>
      <select
        data-testid={`${testId}-select`}
        value={value}
        onChange={(event) => onValueChange(event.target.value)}
        aria-label={`${label}候选列表`}
        style={controlStyle}
      >
        <option value="">{loading ? "正在加载候选…" : emptyLabel}</option>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}{option.description ? `｜${option.description}` : ""}
          </option>
        ))}
      </select>
      {selected?.description ? <div style={{ color: "#64748b", fontSize: 11 }}>{selected.description}</div> : null}
    </div>
  );
}
