export default function MetricCard({ label, value, hint, tone = "neutral" }: { label: string; value: string | number; hint?: string; tone?: "neutral" | "success" | "warning" | "danger" | "info" }) {
  return (
    <div className={`pv2-metric pv2-metric-${tone}`}>
      <div className="pv2-metric-label">{label}</div>
      <div className="pv2-metric-value">{value}</div>
      {hint ? <div className="pv2-metric-hint">{hint}</div> : null}
    </div>
  );
}
