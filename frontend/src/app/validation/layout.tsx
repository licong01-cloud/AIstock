"use client";

import type { ReactNode } from "react";
import "@xyflow/react/dist/style.css";
import "../paper-v2/paper-v2.css";
import "@/components/validation/discovery/discovery.css";
import { ValidationDiscoveryShell } from "@/components/validation/discovery/ActiveDiscoveryComponents";

export default function ValidationDiscoveryLayout({ children }: { children: ReactNode }) {
  return <ValidationDiscoveryShell>{children}</ValidationDiscoveryShell>;
}
