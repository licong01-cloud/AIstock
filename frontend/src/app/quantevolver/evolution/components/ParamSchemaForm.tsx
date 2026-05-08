"use client";
import React from "react";

interface FieldSchema {
  type: string;
  default?: any;
  minimum?: number;
  maximum?: number;
  min?: number;
  max?: number;
  enum?: string[];
  options?: string[];
  description?: string;
  desc?: string;
  title?: string;
}

interface ParamSchemaFormProps {
  /** JSON Schema properties 对象，key=参数名，value=字段定义 */
  schema: Record<string, FieldSchema>;
  /** 当前参数值对象 */
  values: Record<string, any>;
  /** 值变更回调 */
  onChange: (key: string, value: any) => void;
}

const LABEL_STYLE: React.CSSProperties = {
  display: "block", fontSize: "11px", fontWeight: 600,
  color: "#64748b", marginBottom: "3px",
};

const INPUT_STYLE: React.CSSProperties = {
  width: "100%", padding: "5px 8px", borderRadius: "5px",
  border: "1px solid #cbd5e1", fontSize: "12px",
  boxSizing: "border-box" as const, backgroundColor: "white",
};

function FieldControl({
  name, schema, value, onChange,
}: {
  name: string;
  schema: FieldSchema;
  value: any;
  onChange: (v: any) => void;
}) {
  const label = schema.title || name;
  const defaultVal = schema.default;
  const isDefault = value === undefined || value === null || value === "";
  const displayVal = isDefault ? defaultVal : value;

  const normalizedType = schema.type === "bool"
    ? "boolean"
    : schema.type === "int"
      ? "integer"
      : schema.type === "float"
        ? "number"
        : schema.type === "enum"
          ? "string"
          : schema.type;
  const minimum = schema.minimum ?? schema.min;
  const maximum = schema.maximum ?? schema.max;
  const enumOptions = schema.enum ?? schema.options;

  if (normalizedType === "boolean") {
    return (
      <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
        <input
          type="checkbox"
          id={`psf-${name}`}
          checked={!!displayVal}
          onChange={e => onChange(e.target.checked)}
          style={{ width: "14px", height: "14px", accentColor: "#3b82f6" }}
        />
        <label htmlFor={`psf-${name}`} style={{ fontSize: "12px", color: "#475569", cursor: "pointer" }}>
          {label}
          {defaultVal !== undefined && (
            <span style={{ color: "#94a3b8", marginLeft: "4px" }}>（默认: {String(defaultVal)}）</span>
          )}
        </label>
      </div>
    );
  }

  if (normalizedType === "string" && enumOptions) {
    return (
      <div>
        <label style={LABEL_STYLE}>
          {label}
          {defaultVal !== undefined && <span style={{ color: "#94a3b8", fontWeight: 400 }}>（默认: {defaultVal}）</span>}
        </label>
        <select
          value={displayVal ?? ""}
          onChange={e => onChange(e.target.value)}
          style={INPUT_STYLE}
        >
          {enumOptions.map(opt => (
            <option key={opt} value={opt}>{opt}</option>
          ))}
        </select>
      </div>
    );
  }

  if (normalizedType === "integer" || normalizedType === "number") {
    const step = normalizedType === "integer" ? 1 : 0.01;
    const hasRange = minimum !== undefined && maximum !== undefined;
    return (
      <div>
        <label style={LABEL_STYLE}>
          {label}
          {minimum !== undefined && maximum !== undefined && (
            <span style={{ color: "#94a3b8", fontWeight: 400 }}> [{minimum}, {maximum}]</span>
          )}
          {defaultVal !== undefined && <span style={{ color: "#94a3b8", fontWeight: 400 }}>（默认: {defaultVal}）</span>}
        </label>
        <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
          {hasRange && (
            <input
              type="range"
              min={minimum}
              max={maximum}
              step={step}
              value={displayVal ?? defaultVal ?? 0}
              onChange={e => onChange(normalizedType === "integer" ? parseInt(e.target.value) : parseFloat(e.target.value))}
              style={{ flex: 1, accentColor: "#3b82f6" }}
            />
          )}
          <input
            type="number"
            min={minimum}
            max={maximum}
            step={step}
            value={displayVal ?? ""}
            placeholder={String(defaultVal ?? "")}
            onChange={e => {
              const v = normalizedType === "integer" ? parseInt(e.target.value) : parseFloat(e.target.value);
              onChange(isNaN(v) ? undefined : v);
            }}
            style={{ ...INPUT_STYLE, width: hasRange ? "80px" : "100%" }}
          />
        </div>
      </div>
    );
  }

  // string fallback
  return (
    <div>
      <label style={LABEL_STYLE}>
        {label}
        {defaultVal !== undefined && <span style={{ color: "#94a3b8", fontWeight: 400 }}>（默认: {defaultVal}）</span>}
      </label>
      <input
        type="text"
        value={displayVal ?? ""}
        placeholder={String(defaultVal ?? "")}
        onChange={e => onChange(e.target.value || undefined)}
        style={INPUT_STYLE}
      />
    </div>
  );
}

export default function ParamSchemaForm({ schema, values, onChange }: ParamSchemaFormProps) {
  const entries = Object.entries(schema);
  if (entries.length === 0) return null;

  // boolean 字段放最后，其余字段按原顺序
  const nonBool = entries.filter(([, s]) => s.type !== "boolean");
  const bools = entries.filter(([, s]) => s.type === "boolean");
  const sorted = [...nonBool, ...bools];

  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))",
      gap: "10px",
      padding: "12px",
      backgroundColor: "#f8fafc",
      borderRadius: "6px",
      border: "1px solid #e2e8f0",
    }}>
      {sorted.map(([key, fieldSchema]) => (
        <FieldControl
          key={key}
          name={key}
          schema={fieldSchema}
          value={values[key]}
          onChange={v => onChange(key, v)}
        />
      ))}
    </div>
  );
}
