# AgentSearch deployment for RA external_research

This directory documents the supported operator path for the free/self-hosted
AgentSearch stack used by `RealExternalResearchProvider`.

BUG-548 removed the old AIstock-local compose template because it referenced a
nonexistent placeholder image (`agent-search-api:latest`). AgentSearch currently
ships as source plus its own Docker Compose project, so operators should run the
official repository outside the AIstock checkout.

## Supported deployment

Clone the official repository outside `F:/Dev/AIstock`:

```bash
git clone git@github.com:brcrusoe72/agent-search.git F:/Dev/agent-search
cd F:/Dev/agent-search
./scripts/prepare-searxng.sh
docker compose up -d
```

The upstream compose exposes AgentSearch on `127.0.0.1:3939` by default. Verify
the stack before wiring AIstock:

```bash
curl "http://localhost:3939/search?q=test"
curl "http://localhost:3939/openapi.json"
```

## Expected local API contract

AIstock expects the upstream FastAPI service to provide:

- `GET /search?q=<query>&count=<limit>`
- `GET /read?url=<url>&max_chars=<n>`

`/search` returns a JSON object with `results`, and each result includes at
least `title`, `url`, and `snippet`.

`/read` returns a JSON object with `url`, `content`, `strategy`, `chars`,
`success`, and optional `error`/`trust` fields.

If the upstream OpenAPI contract changes, do not work around it with env vars.
Register an AIstock BUG and update
`backend/services/research_assistant/real_external_research_provider.py` to
match the real contract.

## AIstock backend environment

After the upstream stack is healthy, set the backend env:

```env
RA_EXTERNAL_RESEARCH_PROVIDER=real
RA_AGENTSEARCH_BASE_URL=http://127.0.0.1:3939
RA_LOCAL_EXTRACT_ALLOWED_HOSTS=
RA_PAPER_PROVIDER=semantic_scholar
S2_API_KEY=
```

Leave `RA_LOCAL_EXTRACT_ALLOWED_HOSTS` empty unless AgentSearch `/read` is
unavailable and operators explicitly approve local `trafilatura` fallback for
specific public hosts. Wildcard, localhost/private/reserved IPs, userinfo, and
non-80/443 ports are rejected by the provider.

AIstock's default behavior remains offline until these env vars and the
user-owned backend restart are applied. Do not clone AgentSearch under the
AIstock repo, do not commit local `.env` changes, and do not restart backend
`8001` from this deployment note.
