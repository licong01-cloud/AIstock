# AIstock Local Validation Levels

| Level | Name | Trigger | Minimum evidence | Claim boundary |
|---|---|---|---|---|
| L0 | Static gate | Any code or test-infra change | Skill YAML validation, guardrail scan, type/lint checks where relevant | Proves only static quality checks; no runtime business availability. |
| L1 | Single capability | One API/service/component/bug fix | Targeted pytest/API/UI test, contract edge cases, fail-fast regression | Proves one isolated capability; cannot claim a user workflow works. |
| L2 | Component/API/DB flow | A workflow crosses service/repository/API and may touch isolated DB state | API response, DB side effect, log/error event, coverage/evidence metadata | Proves backend/component flow; UI and real asset availability still unproven unless recorded. |
| L3 | Module real UI/API regression | A module must be usable through dev backend/frontend ports | L0 + backend tests + API flow + UI E2E + run record + no pageerror/console/request failures | Can claim module-level UI path only when `pass_scope` records real backend/DB/UI and positive success. |
| L4 | Cross-module business integration | StrategyPackage -> Selection -> Paper v2, QE -> archive/warehouse, HMM/data chains | End-to-end trace, real/controlled asset evidence, API/DB/UI/log consistency, business-output oracle | Can claim real cross-module business path when current commit and asset evidence are recorded. |
| L5 | Local release candidate | Before version tag/release or production enablement | High-risk module L3/L4 suites, coverage trend, data-quality smoke, asset-safety report, residual-risk signoff | Can claim local release candidate only for explicitly covered modules and listed residual risks. |

The authoritative execution location is the local AIstock workstation. Cloud CI is not
required and must not be treated as business-validation authority.

## Claim Safety Rules

- Mock UI tests are useful for interaction and copy review, but they cannot prove real backend, DB, node API, or business writes.
- Negative fail-fast tests prove that missing data does not silently succeed; they do not prove the positive user path works.
- Historical L3/L4 records are reference evidence only. High-risk changes must rerun relevant paths on the current commit and current controlled assets.
- Run metadata should include `pass_scope` and `business_assertion`; when they are absent, Validation Center must show the success scope as unproven rather than infer it.
- Production backend `8001` must not be restarted or touched by validation unless the user explicitly authorizes a production validation window.
