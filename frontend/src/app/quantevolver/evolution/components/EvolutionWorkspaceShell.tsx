"use client";

import React from "react";

type Props = {
  taskType: "single_alpha" | "multi_alpha_combine";
  children: React.ReactNode;
};

/**
 * Canonical route-level boundary shared by all QE evolution task adapters.
 * It deliberately has no visual styling of its own so extracting the shell
 * cannot shift the established single-alpha page geometry or design tokens.
 */
export default function EvolutionWorkspaceShell({ taskType, children }: Props) {
  return <div data-qe-workspace-shell="canonical" data-qe-task-type={taskType} style={{ display: "contents" }}>{children}</div>;
}
