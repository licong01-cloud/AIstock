"use client";

import React from "react";
import MultiAlphaCombineBacktestWorkspace from "../../multi-alpha/combine-backtest/page";
import MultiAlphaCombineBacktestDetailWorkspace from "../../multi-alpha/combine-backtest/[taskKey]/page";

export default function MultiAlphaEvolutionWorkspace({ taskId }: { taskId?: string | null }) {
  return taskId
    ? <MultiAlphaCombineBacktestDetailWorkspace params={{ taskKey: taskId }} />
    : <MultiAlphaCombineBacktestWorkspace />;
}
