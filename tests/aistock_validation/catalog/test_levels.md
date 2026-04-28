# AIstock Local Validation Levels

| Level | Name | Trigger | Minimum evidence |
|---|---|---|---|
| L0 | Static gate | Any code or test-infra change | Skill YAML validation, guardrail scan, type/lint checks where relevant |
| L1 | Single capability | One API/service/component/bug fix | Targeted pytest/API/UI test and fail-fast regression |
| L2 | Business flow | A workflow crosses service/repository/API/UI | API response, DB side effect, log/error event, UI state if applicable |
| L3 | Module regression | Paper v2 + Selection Center first-stage validation | L0 + backend tests + API flow + UI E2E + run record |
| L4 | Cross-module integration | StrategyPackage -> Selection -> Paper v2 | End-to-end trace and business-output evidence |
| L5 | Local release candidate | Before version tag/release | High-risk module L3 suites, smoke tests, asset-safety report |

The authoritative execution location is the local AIstock workstation. Cloud CI is not
required and must not be treated as business-validation authority.
