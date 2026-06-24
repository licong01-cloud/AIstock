import { redirect } from "next/navigation";

export default function ResearchAssistantAgentRunsRedirectPage() {
  redirect("/research-assistant/audit?tab=agent-runs");
}
