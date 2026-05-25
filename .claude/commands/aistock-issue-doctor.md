# aistock-issue-doctor

Run this before any AIstock issue workflow in Claude Code:

```powershell
python F:\Dev\AIstock\scripts\aistock_issue_workflow.py doctor
```

Report `workflow_gate`, blocking items, warnings, GitHub fallback status, MCP stale-worktree hints, and the returned `next_command`.
