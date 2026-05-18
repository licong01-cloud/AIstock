"""Profile resolution for the phased AIstock MCP gateway."""

from __future__ import annotations

INITIAL_PROFILES: dict[str, list[str]] = {
    "research": ["research"],
}

FUTURE_PROFILES: dict[str, list[str]] = {
    "research_with_archive": ["research", "qe_archive"],
    "research_with_qe": ["research", "qe_archive", "qe_experiment"],
    "operations": ["validation", "qe_archive", "qe_experiment"],
    "full": ["research", "qe_archive", "qe_experiment", "validation"],
}

_PHASE0_5_MODULES = {"research"}


def parse_modules(value: str | list[str] | tuple[str, ...] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        modules = [item.strip() for item in value.split(",") if item.strip()]
    else:
        modules = [str(item).strip() for item in value if str(item).strip()]
    return modules


def resolve_modules(
    *,
    profile: str | None = "research",
    modules: str | list[str] | tuple[str, ...] | None = None,
) -> list[str]:
    """Resolve Phase 0-5 modules and reject future-only profiles explicitly."""

    explicit_modules = parse_modules(modules)
    if explicit_modules is not None:
        if profile not in {None, "", "research"}:
            raise ValueError("Specify either --profile or --modules, not both")
        future = [name for name in explicit_modules if name not in _PHASE0_5_MODULES]
        if future:
            raise ValueError(f"Modules {future!r} are future-only in Phase 0-5")
        if explicit_modules != ["research"]:
            raise ValueError("Phase 0-5 only allows --modules=research")
        return explicit_modules

    selected = "research" if profile in {None, ""} else profile
    if selected in INITIAL_PROFILES:
        return list(INITIAL_PROFILES[selected])
    if selected in FUTURE_PROFILES:
        raise ValueError(f"Profile {selected!r} is future-only in Phase 0-5")
    raise ValueError(f"Unknown MCP profile {selected!r}; allowed Phase 0-5 profile: 'research'")
