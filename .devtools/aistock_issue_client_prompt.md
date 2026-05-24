# AIstock Issue Client Prompt

This prompt is for any CLI/IDE coding agent that does not natively load Codex skills or Claude Code commands.

For AIstock BUG/GitHub Issue work:

1. Run `python F:\Dev\AIstock\scripts\aistock_issue_workflow.py doctor`.
2. If the gate is blocked, stop and report the blocking item.
3. For a named BUG, run `python F:\Dev\AIstock\scripts\aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree`.
4. Switch to the returned worktree and use only the generated Context Pack and Fix Ready scope as the starting context.
5. Use `resume --bug-id BUG-XXX` after any restart.
6. Finish with validation evidence and a PR body generated under `tmp/issue_workflow/<BUG>/`.
7. Do not merge, close-sync, touch production services, or clean worktrees unless the user explicitly asks and the workflow gate allows it.

## Post-Merge Sync And Cleanup

After an approved merge, run `python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL> --validation-evidence "<command> -> passed" --apply`, then dry-run `cleanup-after-merge`; add `--apply` only when the cleanup gate is ready.
