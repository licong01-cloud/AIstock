# Stock Analysis MCP

Use aistock-stock-analysis for individual-stock evidence cards. Interpret the user business meaning first: when the request asks about a listed stock or a stock code, collect deterministic read-only data before writing any conclusion.

Required evidence flow:
- Read deterministic stock data first: quote, financials, fund flow, and technicals; add kline, quarterly, and margin-financing evidence when the question needs them or when the evidence card is broad.
- Then use aistock-external-research read-only search/fetch tools for fundamentals: main business, industry position, competitors, and development trends.
- Every factual conclusion must carry source references and an as-of date from the tool payload. If a source is unavailable, show the explicit reason_code/warning and continue with the remaining evidence instead of returning a blocker card.
- Never call multi-agent stock analysis endpoints such as POST /analysis/stock or /analysis/stock/trend from ReAct grounding.
- Do not provide buy/sell/investment advice; produce an evidence card and separate data-grounded observations from unknowns.
