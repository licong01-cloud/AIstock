"""QE data-plane subprocess environment isolation contract.

QE compute subprocesses (qrun_limit_minute / prepare_factors / read_exp_res)
operate on a file-only data plane (frozen H5 / Parquet / Qlib bin / factor
cache / task workspace). They must never inherit PostgreSQL credentials,
external-service secrets, or hold any database fallback ability. This module is
the authoritative child
environment used by the WSL and remote QE workspace command construction paths:
it subtracts database variables and conventional key/token/secret/password
names while preserving the Prediction Store base URL, task / Loop / Node /
resource-session identity variables, and the H5 / Parquet / Qlib bin /
factor-cache file-path variables.  Resource-session secret material is carried
by its chmod-600 workspace file, never inherited from the parent environment.

The parent backend's own database environment is intentionally left untouched;
only the QE data-plane subprocess environment is scrubbed. Variable values are
never logged or emitted.
"""

from __future__ import annotations

import os
from typing import Mapping

#: Environment variable prefixes that can directly establish a PostgreSQL
#: connection and must never reach a QE data-plane subprocess.
DB_CREDENTIAL_PREFIXES: tuple[str, ...] = ("TDX_DB_", "POSTGRES_")

#: Explicit libpq / psycopg2 / SQLAlchemy connection variables that must never
#: reach a QE data-plane subprocess.
DB_CREDENTIAL_KEYS: frozenset[str] = frozenset(
    {
        "DATABASE_URL",
        "DB_HOST",
        "DB_NAME",
        "DB_PASSWORD",
        "DB_PORT",
        "DB_USER",
        "PGHOST",
        "PGPORT",
        "PGUSER",
        "PGPASSWORD",
        "PGDATABASE",
        "PGSERVICE",
        "PGPASSFILE",
        "SQLALCHEMY_DATABASE_URI",
        "SQLALCHEMY_DATABASE_URL",
    }
)

#: Non-database credentials are equally outside the QE file-only data plane.
#: Keep the list deliberately about credential material, not service routing:
#: Prediction Store URLs, task identities and frozen-file paths remain usable.
QE_SECRET_CREDENTIAL_KEYS: frozenset[str] = frozenset(
    {
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "ACCESS_TOKEN",
        "API_KEY",
        "AUTH_TOKEN",
        "GH_TOKEN",
        "GITHUB_TOKEN",
        "HF_TOKEN",
        "PASSWORD",
        "PRIVATE_KEY",
        "SECRET",
        "TOKEN",
        "TUSHARE_TOKEN",
    }
)
QE_SECRET_CREDENTIAL_SUFFIXES: tuple[str, ...] = (
    "_API_KEY",
    "_AUTH_TOKEN",
    "_ACCESS_TOKEN",
    "_ACCESS_KEY_ID",
    "_CLIENT_SECRET",
    "_SECRET_KEY",
    "_SECRET_ACCESS_KEY",
    "_PRIVATE_KEY",
    "_PASSWORD",
    "_SECRET",
    "_TOKEN",
)


def is_db_credential_key(key: str) -> bool:
    """True when an environment key could directly establish a PostgreSQL
    connection and must be scrubbed from a QE data-plane subprocess."""
    name = str(key or "")
    return name in DB_CREDENTIAL_KEYS or name.startswith(DB_CREDENTIAL_PREFIXES)


def is_qe_subprocess_credential_key(key: str) -> bool:
    """Return whether *key* carries credentials forbidden in QE compute.

    The test is name-only.  Values are never inspected, serialized or emitted.
    Control-plane routing and frozen-data paths deliberately do not match these
    keys/suffixes.
    """

    name = str(key or "").upper()
    return (
        is_db_credential_key(name)
        or name in QE_SECRET_CREDENTIAL_KEYS
        or name.endswith(QE_SECRET_CREDENTIAL_SUFFIXES)
    )


def scrubbed_qe_subprocess_env(
    base: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return a copy of the parent environment with credential material removed.

    All non-credential variables are preserved, including
    ``AISTOCK_PREDICTION_STORE_BASE_URL``, task / Loop / Node / resource-session
    control-plane variables, and H5 / Parquet / Qlib bin / factor-cache file
    path variables. This is subtractive and name-only: database connection
    variables plus conventional key/token/secret/password names are removed,
    so the file-only data-plane contract is enforced at the command boundary.
    """
    source = dict(base) if base is not None else dict(os.environ)
    return {key: value for key, value in source.items() if not is_qe_subprocess_credential_key(key)}


def qe_subprocess_credential_scrub_command() -> str:
    """Return a Bash fragment that removes all QE-forbidden credentials.

    ``compgen -e`` enumerates names only.  The fragment never expands or prints
    credential values, and it leaves Prediction Store routing, task identity
    and frozen-file path variables intact.
    """

    prefix_patterns = tuple(f"{prefix}*" for prefix in DB_CREDENTIAL_PREFIXES)
    suffix_patterns = tuple(f"*{suffix}" for suffix in QE_SECRET_CREDENTIAL_SUFFIXES)
    case_patterns = "|".join(
        (*prefix_patterns, *suffix_patterns, *sorted(DB_CREDENTIAL_KEYS | QE_SECRET_CREDENTIAL_KEYS))
    )
    explicit_names = " ".join(sorted(DB_CREDENTIAL_KEYS | QE_SECRET_CREDENTIAL_KEYS))
    return (
        'for __qe_credvar in $(compgen -e); do case "$__qe_credvar" in '
        f'{case_patterns}) unset "$__qe_credvar" ;; esac; done; '
        f"unset {explicit_names}; "
    )


def db_credential_scrub_command() -> str:
    """Compatibility alias for the complete QE credential scrub fragment.

    Existing callers introduced by BUG-997 retain their public import while
    gaining the stricter file-only data-plane boundary.
    """

    return qe_subprocess_credential_scrub_command()
