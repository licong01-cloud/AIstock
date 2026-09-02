# Direct monthly workflow

## State flow

```text
REQUESTED -> PLANNING_DIRECT -> BUILDING -> VALIDATING -> CANDIDATE_READY
```

The v2 monthly flow has no source-freeze, source-content hashing, prepublish source recheck, source-drift waiting or re-attestation stage.

Planning inputs are limited to:

- target cutoff;
- canonical PIT authority and coverage;
- existing candidate component inventory;
- database/provider availability for the target month;
- explicit historical repair inventory when present;
- producer/schema/toolchain version required to write the candidate.

Missing repair inventory never triggers a historical hash scan. It expands only the affected component to `COMPONENT_REBUILD`.

## Component decisions

- `REUSE`: component inputs and target coverage are unchanged.
- `INCREMENTAL`: append the new month.
- `SELECTIVE_REBUILD`: rebuild listed instruments/months affected by PIT or data repair.
- `COMPONENT_REBUILD`: rebuild one complete component when the exact repair scope is unavailable.

One component rebuild must not expand to unrelated components.

## 2026-08-31

July minute data was repaired after the old export. The August candidate therefore rebuilds the minute component, or the exact affected minute instruments when a complete repair inventory exists, and appends August data. The July candidate remains read-only.

## Completion

Completion requires structural coverage, key/range validation, layered value sampling and QE/HMM producer smoke. It does not require full content hashes, source equivalence proofs or production activation.
