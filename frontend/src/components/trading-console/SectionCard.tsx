import type { ReactNode } from "react";

export default function SectionCard({ title, eyebrow, action, children }: { title: string; eyebrow?: string; action?: ReactNode; children: ReactNode }) {
  return (
    <section className="pv2-card">
      <div className="pv2-card-head">
        <div>
          {eyebrow ? <div className="pv2-eyebrow">{eyebrow}</div> : null}
          <h2>{title}</h2>
        </div>
        {action ? <div className="pv2-card-action">{action}</div> : null}
      </div>
      {children}
    </section>
  );
}
