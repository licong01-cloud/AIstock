# StrategyPackage Status Capability UI Validation

Date: 2026-05-04
Level: L3 UI smoke / frontend build
Module: Paper v2 + StrategyPackage Center

## Scope

- Clarified the current StrategyPackage card in `/paper-v2/packages`.
- Added visible lifecycle, selection capability, and paper-trading capability summary before action buttons.
- Renamed actions to separate state marking from portfolio creation:
  - `???????`
  - `????????`
  - `?????????`
  - `?????`
- Added confirmation for package retirement by requiring the full `package_id`.
- Updated the Paper v2 Playwright regression expectation to the new action labels.

## Business Oracles

- Operators can see current package state before choosing an action.
- Selection capability and paper capability are shown separately.
- Marking a package as paper-enabled is not presented as creating a portfolio.
- Creating a concrete Paper v2 portfolio remains a separate navigation action.
- Retiring a package is a dangerous lifecycle action and requires explicit confirmation.

## Commands

```powershell
cd frontend
npm run build
npx tsc --noEmit --pretty false
npx playwright test tests/paper-v2/tmp-package-status-ui.spec.ts --config=playwright.paper-v2.config.ts
```

The Playwright spec above was a temporary mocked-API smoke test and was removed after execution. It intercepted `/api/v1/strategy-packages*`, rendered a `PAPER_ENABLED` package with one paper-enabled execution policy, and asserted the new capability cards, clarified buttons, disabled completed-state marking buttons, create-portfolio link, retirement button, and execution-policy count.

## Results

- `npm run build`: passed. Next.js compiled `/paper-v2/packages` and all app routes successfully.
- `npx tsc --noEmit --pretty false`: passed with no TypeScript errors.
- Mocked Playwright UI smoke: `1 passed (8.3s)`.

## Production Impact / Asset Safety

- Production backend `8001` was not restarted.
- No StrategyPackage, QE, Paper v2 ledger, HMM, model, or execution-policy assets were modified.
- The UI smoke used mocked API responses only; it did not write backend state.

## Residual Risks

- Full Paper v2 real-flow E2E was not run in this change to avoid mutating StrategyPackage/Selection/Paper state through a non-isolated backend. The existing real-flow test expectation was updated so the next scheduled L3 run checks the new labels.
