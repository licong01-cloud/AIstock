"""Router package exports without import-time loading of every route module.

Python's package ``from backend.routers import <module>`` behavior loads the
requested submodule on demand.  Keeping this initializer declarative prevents
unrelated route side effects while tooling resolves a single module spec.
"""

__all__ = [
    "health",
    "analysis",
    "watchlist",
    "cloud_screening",
    "monitor",
    "portfolio",
    "sector_strategy",
    "ingestion",
    "settings",
    "config_env",
    "dataset_releases",
    "qmt",
    "qe_archive",
    "research_pipeline",
    "smart_monitor",
    "rdagent",
    "rdagent_templates",
    "stock_universe",
    "tdx_blocks",
    "quantevolver",
    "quantevolver_evolution",
    "hmm_training",
]
