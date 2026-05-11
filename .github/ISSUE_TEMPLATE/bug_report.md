---
name: Bug report
about: Report a defect tracked in tests/aistock_validation/bugs/
title: "[BUG] "
labels: bug, needs-triage
assignees: []
---

## Quick file via MCP (preferred)

If you have Claude Code or Codex App with the `aistock-validation` MCP server
configured, please prefer:

```
aistock-validation/report_bug(
  title="<one-line summary>",
  severity="P0" | "P1" | "P2" | "P3",
  module="<module_id from module_registry.yaml>",
  files=["backend/path/to/file.py", ...],
  reproduce_command="<one-line shell or pytest command>",
  expected="<what should happen>",
  actual="<what actually happens>",
  fix_owner="claude_code" | "codex_app" | "human",
  related_drawer=None
)
```

The server writes to `tests/aistock_validation/bugs/` directly with
fingerprint-based dedup. Paste the resulting `BUG-NNN` here.

## Or fill this template

Bug ID (will be assigned `BUG-NNN` after triage):

### Severity

- [ ] P0 - production blocker
- [ ] P1 - high, must fix this iteration
- [ ] P2 - medium, fix this month
- [ ] P3 - low, nice to fix

### Module

<!-- Pick from tests/aistock_validation/catalog/module_registry.yaml -->

`module_id:` 

### Reproduce

```bash
# minimum command that reproduces the bug
```

### Expected

<!-- what should have happened -->

### Actual

<!-- what actually happened -->

### Suspected files

- backend/...
- frontend/...

### Evidence

<!-- drawer ids, log paths, screenshots (uploaded to a public artifact, not
     attached here), commit SHAs -->

- drawer:cross-tool/codex-claude-coord/...
- file:...

### Production status

- [ ] Production was NOT touched
- [ ] Production was potentially affected (explain)

## Schema reference

The canonical bug record schema is `aistock_validation_bug_v1` defined in
`backend/services/validation/finding_store.py`. The full set of fields and
the discover -> register -> fix -> verify -> close workflow is documented
at `docs/process/bug_registry_workflow_20260510.md`.
