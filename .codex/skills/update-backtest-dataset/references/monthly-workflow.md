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

The direct v2 path does not open the legacy control store or inherited resource/source-freeze profile. An earlier validated baseline is optional context, not an admission requirement.

Missing repair inventory never triggers a historical hash scan. It expands only the affected component to `COMPONENT_REBUILD`.

## Component decisions

- `REUSE`: component inputs and target coverage are unchanged.
- `INCREMENTAL`: append the new month.
- `SELECTIVE_REBUILD`: rebuild listed instruments/months affected by PIT or data repair.
- `COMPONENT_REBUILD`: rebuild one complete component when the exact repair scope is unavailable.

One component rebuild must not expand to unrelated components.

## 2026-08-31

July minute data was repaired after the old export, and the current August candidate already contains PASS daily, minute and index components. Resume those components without rebuilding. Rebuild only `factor_h5_static_candidate_v2`, leaving the failed candidate's older factor directory untouched.

Sector publication uses stock classification PIT to select the published Shenwan L2 series. Official index membership is a separate research authority and is not a per-stock publication prerequisite. Published SW daily fields and aggregated stock moneyflow are independent; absence of one does not null the other.

## Completion

Completion requires structural coverage, key/range validation, layered value sampling and a QE/HMM producer contract smoke. The smoke proves that Qlib/H5/index inputs are readable and required representative fields are non-empty; it does not gate on 85% factor coverage, IC, signal dates, portfolio return, full content hashes, source equivalence proofs or production activation.
