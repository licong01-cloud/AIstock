"use client";

import Link from "next/link";

export type WorkflowStepStatus = "done" | "current" | "locked" | "available";

export type WorkflowStep = {
  key: string;
  label: string;
  hint?: string;
  href?: string;
  status: WorkflowStepStatus;
};

export default function WorkflowStepper({
  steps,
  title = "Paper v2 操作流程",
  compact = false,
}: {
  steps: WorkflowStep[];
  title?: string;
  compact?: boolean;
}) {
  return (
    <div className={`pv2-workflow-stepper ${compact ? "pv2-workflow-stepper-compact" : ""}`}>
      {title ? <div className="pv2-workflow-title">{title}</div> : null}
      <ol className="pv2-workflow-list">
        {steps.map((step, index) => {
          const inner = (
            <>
              <span className={`pv2-workflow-num pv2-workflow-num-${step.status}`} aria-hidden="true">
                {step.status === "done" ? "✓" : index + 1}
              </span>
              <span className="pv2-workflow-body">
                <span className="pv2-workflow-label">{step.label}</span>
                {step.hint && !compact ? <span className="pv2-workflow-hint">{step.hint}</span> : null}
              </span>
            </>
          );
          const className = `pv2-workflow-step pv2-workflow-step-${step.status}`;
          if (step.href && step.status !== "locked") {
            return (
              <li className={className} key={step.key}>
                <Link href={step.href} className="pv2-workflow-link">{inner}</Link>
              </li>
            );
          }
          return (
            <li className={className} key={step.key}>
              <span className="pv2-workflow-link" aria-disabled={step.status === "locked"}>{inner}</span>
            </li>
          );
        })}
      </ol>
    </div>
  );
}
