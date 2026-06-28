# AgentSearch deployment for RA external_research

This directory is an operator template only. Codex did not start services.

## Expected local API

AIstock `RealExternalResearchProvider` expects the AgentSearch FastAPI service to expose:

- `GET /search?q=<query>&language=<locale>&count=<limit>`
- `GET /read?url=<url>&max_chars=<n>`

`/search` should return a JSON object with `results` or `items`; each row should include `title`, `url`, and a short `summary/snippet/content`.

`/read` should return a JSON object with `title`, `url`, `source`, and short extracted `content/text/extract/content_preview`.

## Operator steps

1. Build or pull the approved free/self-hosted AgentSearch image.
2. Copy `agentsearch.env.example` to `agentsearch.env` and set `AGENTSEARCH_IMAGE`.
3. Start the stack outside Codex if approved:
   `docker compose -f deploy/agentsearch/docker-compose.yml up -d`
4. In the backend environment set:

```env
RA_EXTERNAL_RESEARCH_PROVIDER=real
RA_AGENTSEARCH_BASE_URL=http://127.0.0.1:3939
RA_PAPER_PROVIDER=semantic_scholar
S2_API_KEY=
```

5. Install backend dependencies from the PR dependency gate, then restart backend 8001.

Default AIstock behavior remains offline until these env vars and the user-owned restart are applied.
