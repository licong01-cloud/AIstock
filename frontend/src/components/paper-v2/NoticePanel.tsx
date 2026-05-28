import type { ReactNode } from "react";

type NoticeTone = "info" | "warning" | "success";

export default function NoticePanel({
  title,
  children,
  tone = "info",
  context,
}: {
  title: string;
  children: ReactNode;
  tone?: NoticeTone;
  context?: unknown;
}) {
  return (
    <div className={`pv2-notice pv2-notice-${tone}`}>
      <div className="pv2-notice-title">{title}</div>
      <div className="pv2-notice-body">{children}</div>
      {context ? <pre className="pv2-diagnostic-text" style={{ marginTop: 10, whiteSpace: "pre-wrap", wordBreak: "break-word" }}>{JSON.stringify(context, null, 2)}</pre> : null}
    </div>
  );
}
