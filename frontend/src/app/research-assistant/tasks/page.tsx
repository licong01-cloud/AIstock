import { redirect } from "next/navigation";

export default function ResearchAssistantTasksRedirectPage() {
  redirect("/research-assistant/audit?tab=tasks");
}
