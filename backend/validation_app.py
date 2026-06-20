from __future__ import annotations

import os
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


_DEFAULT_CORS_ORIGINS = (
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:3011",
    "http://127.0.0.1:3011",
    "http://localhost:3012",
    "http://127.0.0.1:3012",
)


def _load_validation_router_module() -> ModuleType:
    router_path = Path(__file__).resolve().parent / "routers" / "validation.py"
    spec = spec_from_file_location("_aistock_validation_only_router", router_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load validation router from {router_path}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _cors_origins() -> list[str]:
    origins = list(_DEFAULT_CORS_ORIGINS)
    extra_origins = [
        origin.strip()
        for origin in os.getenv("AISTOCK_CORS_ORIGINS", "").split(",")
        if origin.strip()
    ]
    origins.extend(origin for origin in extra_origins if origin not in origins)
    return origins


def create_app() -> FastAPI:
    """Create a Validation Center-only FastAPI app without main runtime lifespan."""

    validation = _load_validation_router_module()
    app = FastAPI(title="AIstock Validation Center Only", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(validation.router, prefix="/api/v1")
    return app


app = create_app()
