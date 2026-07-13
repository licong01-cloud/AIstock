import { redirect } from "next/navigation";

export default function ResearchAssistantMcpToolsRedirectPage() {
  redirect("/research-assistant/settings?tab=mcp");
}
