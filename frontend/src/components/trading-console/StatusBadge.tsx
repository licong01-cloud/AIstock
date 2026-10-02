import { statusLabel, statusTone } from "@/lib/paper-v2/format";

export default function StatusBadge({ status }: { status: unknown }) {
  const tone = statusTone(status);
  return <span className={`pv2-badge pv2-badge-${tone}`} title={String(status || "unknown")}>{statusLabel(status)}</span>;
}
