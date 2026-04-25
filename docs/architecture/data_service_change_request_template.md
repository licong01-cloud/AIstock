# Data Service Change Request Template

> Use this template before changing `backend/data_service` for Strategy Package, Selection Center, Trading Core, or Paper Trading v2.

## 1. Change Summary

```text
request_id:
owner:
date:
affected_module:
proposed_change:
reason:
```

## 2. Current API / Behavior

```text
file_path:
function_or_class:
current_signature:
current_return_shape:
current_fallback_behavior:
known_callers:
```

## 3. Proposed API / Behavior

```text
new_signature:
new_return_shape:
new_fallback_behavior:
backward_compatible: yes/no
```

## 4. Impact Assessment

Check at least:

- factor live-data metric analysis;
- factor correlation analysis;
- QE experiments and evolution;
- RD-Agent asset sync or diagnostics;
- old selection features kept for compatibility;
- new Strategy Package / Selection Center / Paper Trading v2 flow.

## 5. Alternatives Considered

```text
1. Keep data_service unchanged and adapt inside paper_trading_v2:
2. Add a new wrapper API without changing existing behavior:
3. Modify existing data_service API:
```

## 6. Verification Plan

```text
unit_tests:
integration_tests:
manual_checks:
rollback_plan:
```

## 7. Approval

```text
approved_by:
approval_date:
notes:
```
