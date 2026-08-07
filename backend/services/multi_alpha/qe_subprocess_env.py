"""QE data-plane subprocess environment isolation contract.

QE compute subprocesses (qrun_limit_minute / prepare_factors / read_exp_res)
operate on a file-only data plane (frozen H5 / Parquet / Qlib bin / factor
cache / task workspace). They must never inherit PostgreSQL credentials or hold
any database fallback ability. This module is the authoritative child
environment used by the WSL and remote QE workspace command construction paths:
it subtracts every variable that could directly establish a PostgreSQL
connection while preserving the Prediction Store base URL, task / Loop / Node /
resource-session control-plane variables, and the H5 / Parquet / Qlib bin /
factor-cache file-path variables.

The parent backend's own database environment is intentionally left untouched;
only the QE data-plane subprocess environment is scrubbed. Variable values are
never logged or emitted.
"""

from __future__ import annotations

import os
from typing import Mapping

#: Environment variable prefixes that can directly establish a PostgreSQL
#: connection and must never reach a QE data-plane subprocess.
DB_CREDENTIAL_PREFIXES: tuple[str, ...] = ("TDX_DB_",)

#: Explicit libpq / psycopg2 / SQLAlchemy connection variables that must never
#: reach a QE data-plane subprocess.
DB_CREDENTIAL_KEYS: frozenset[str] = frozenset(
    {
        "DATABASE_URL",
        "PGHOST",
        "PGPORT",
        "PGUSER",
        "PGPASSWORD",
        "PGDATABASE",
        "PGSERVICE",
        "PGPASSFILE",
    }
)


def is_db_credential_key(key: str) -> bool:
    """True when an environment key could directly establish a PostgreSQL
    connection and must be scrubbed from a QE data-plane subprocess."""
    name = str(key or "")
    return name in DB_CREDENTIAL_KEYS or name.startswith(DB_CREDENTIAL_PREFIXES)


def scrubbed_qe_subprocess_env(
    base: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return a copy of the parent environment with every PostgreSQL credential
    variable removed.

    All non-database variables are preserved, including
    ``AISTOCK_PREDICTION_STORE_BASE_URL``, task / Loop / Node / resource-session
    control-plane variables, and H5 / Parquet / Qlib bin / factor-cache file
    path variables. This is subtractive: unknown future DB variables are kept
    unless they match a known prefix or key, so the file-only data-plane
    contract is enforced at the command boundary.
    """
    source = dict(base) if base is not None else dict(os.environ)
    return {key: value for key, value in source.items() if not is_db_credential_key(key)}


def db_credential_scrub_command() -> str:
    """Return a POSIX shell fragment that unsets database credential variables
    before a QE workspace command runs inside ``bash -lc``.

    Used by the remote/WSL command construction path where the child inherits a
    shell environment (the command string is submitted to the QE workspace
    runner rather than launched with an explicit env dict).
    """
    prefix_expansions = " ".join(f"${{!{prefix}*}}" for prefix in DB_CREDENTIAL_PREFIXES)
    explicit_names = " ".join(sorted(DB_CREDENTIAL_KEYS))
    return (
        f"for __qe_dbvar in {prefix_expansions}; do unset \"$__qe_dbvar\"; done; "
        f"unset {explicit_names}; "
    )
