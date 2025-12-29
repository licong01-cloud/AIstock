"""Optional HTTP debug entrypoint for AIstock data service.

This module is *not* required for normal strategy execution. It is
intended for local debugging / inspection only, exposing a very thin
HTTP layer over the core `api` module.
"""

from __future__ import annotations

from fastapi import FastAPI

from . import api


def create_app() -> FastAPI:
    app = FastAPI(title="AIstock Data Service Debug API")

    @app.get("/realtime_snapshot")
    def realtime_snapshot(universe: str):
        symbols = [s.strip() for s in universe.split(",") if s.strip()]
        df = api.get_realtime_snapshot(symbols)
        return {"columns": list(df.columns), "data": df.to_dict(orient="index")}

    @app.get("/history_window")
    def history_window(universe: str, bars: int = 60):
        symbols = [s.strip() for s in universe.split(",") if s.strip()]
        df = api.get_history_window(symbols, bars=bars)
        # For debug purposes we return a simple JSON representation.
        return {
            "index_names": list(df.index.names),
            "columns": list(df.columns),
            "data": df.reset_index().to_dict(orient="records"),
        }

    return app
