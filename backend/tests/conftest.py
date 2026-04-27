"""
Root conftest.py for the backend test suite.

Adds the backend package root to sys.path so tests can import
backend modules without installing the package.
"""
import sys
from pathlib import Path
import asyncio

import pytest

# backend/ directory
BACKEND_ROOT = Path(__file__).parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# repository root, required for imports that use the backend package name
PROJECT_ROOT = BACKEND_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(autouse=True)
def _legacy_get_event_loop_compat():
    """Keep legacy get_event_loop() tests isolated after asyncio.run() calls."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield
    finally:
        if not loop.is_closed():
            loop.close()
        asyncio.set_event_loop(None)
