# AI-Assisted Development and Safety Context

AIstock is a public source A-share quantitative research and engineering platform. It is designed for local software development, data pipeline diagnostics, quantitative experiment management, backtesting, paper-trading simulation, and research workflow validation.

The project includes components such as a FastAPI backend, a Next.js frontend, TDX market-data integration, QuantEvolver / QE experiment tooling, validation workflows, and local MCP-style developer tools. These tools are intended to help authorized developers inspect project files, local logs, experiment metadata, and development databases in a human-supervised engineering workflow.

## Repository Scope

The public repository is not a complete standalone deployment package. It does not include all private runtime configuration, credentials, database DDL, RD-Agent API integration code, production infrastructure, or local environment state required to reproduce every deployment workflow.

The repository should therefore be understood as a development and review workspace, not as a ready-to-run production trading product.

## Terminology

Terminology used in this repository should be understood in its software-engineering context:

- **QE experiment** means quantitative research experiment tracking, backtesting, and diagnostics.
- **Paper trading** means simulated trading for development and testing, not live real-money trading.
- **Execution policy** and **trading ops** refer to internal research and simulation modules.
- **Outbox** refers to an internal application event/job queue used for diagnostics, not an email, SMS, marketing, spam, or external messaging system.
- **MCP tools** are local, authorized developer tools for inspecting this project's files, logs, and experiment records.

## Intended Use

AIstock is intended for:

- Local and authorized software development.
- Quantitative research workflow validation.
- Backtesting and paper-trading simulation.
- Data pipeline diagnostics.
- Experiment metadata inspection.
- Documentation, testing, validation, and engineering review.

AIstock is not intended to:

- Provide consumer-facing investment advice.
- Determine financial eligibility or creditworthiness.
- Execute unauthorized trades or control third-party accounts.
- Send external messages, marketing, spam, or bulk communications.
- Access third-party systems without authorization.
- Perform credential theft, phishing, malware, or other abusive activity.

All AI-assisted development on this project should remain human-supervised, local or otherwise authorized, and limited to legitimate software engineering, research diagnostics, documentation, testing, and validation.

## License Status

No open-source license has been declared yet. Public repository visibility does not by itself grant broad rights to copy, redistribute, modify, or commercially use this code. A separate `LICENSE` file should be added if the project owner later decides to publish the code under a specific license.
