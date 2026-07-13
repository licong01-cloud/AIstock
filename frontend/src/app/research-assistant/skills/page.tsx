import { redirect } from "next/navigation";

export default function ResearchAssistantSkillsRedirectPage() {
  redirect("/research-assistant/settings?tab=skills");
}
