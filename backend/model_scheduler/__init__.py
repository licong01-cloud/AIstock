"""Model training/inference scheduler for quant models (new program only).

This package provides a small DB-driven scheduler that triggers LSTM/DeepAR
training and inference scripts based on entries in app.model_schedule.

It does not modify or depend on legacy tdx_scheduler/tdx_backend; integration
is purely via TimescaleDB and CLI/HTTP in the new next_app backend.
"""
from __future__ import annotations

__all__ = []
