# AIstock

AIstock is a public source repository for an A-share quantitative research and engineering platform. The project is used for local development, data pipeline diagnostics, quantitative experiment management, backtesting, paper-trading simulation, and research workflow validation.

The repository includes components such as a FastAPI backend, a Next.js frontend, TDX market-data integration, QuantEvolver / QE experiment tooling, validation workflows, and local developer-tool integrations.

## Current Repository Scope

This repository is intended for engineering review, documentation, and collaborative development. It is not a complete standalone deployment package: some RD-Agent API integration code, database DDL, runtime infrastructure, credentials, and local environment configuration are intentionally not included.

Do not assume that cloning this repository alone is enough to run a complete production system or reproduce every private runtime workflow.

## Project Safety Context

AIstock is a quantitative research and engineering platform for authorized local development, backtesting, paper-trading simulation, experiment diagnostics, and validation workflows.

Terms such as "QE experiment", "paper trading", "execution policy", "trading ops", and "outbox" refer to internal software-engineering concepts. The outbox is an internal event/job queue, not an external messaging or spam system. Paper trading is simulation only, not live real-money trading.

AIstock is not intended for consumer-facing investment advice, financial eligibility decisions, unauthorized trading, external messaging, credential access, third-party system access, malware, phishing, spam, or other abusive activity. AI-assisted work on this repository should remain human-supervised, authorized, and limited to legitimate software development and research diagnostics.

See the detailed context note: [AI-Assisted Development and Safety Context](docs/compliance/ai_assisted_development_context.md).

## License

No open-source license has been declared yet. Unless a license is added later, all rights are reserved by the repository owner. Public visibility of the source does not grant permission to copy, redistribute, modify, or use it commercially.
