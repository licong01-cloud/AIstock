"""Dependency-free canonical catalog for shared core-index PIT authorities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True, slots=True)
class PoolDefinition:
    pool_id: str
    index_code: str
    source_provider: str
    launch_date: date
    history_start: date
    priority: str


_DATASET_START = date(2018, 8, 1)
POOL_DEFINITIONS: Mapping[str, PoolDefinition] = MappingProxyType(
    {
        "csi300": PoolDefinition("csi300", "000300.SH", "CSI", date(2005, 4, 8), _DATASET_START, "P0"),
        "csi500": PoolDefinition("csi500", "000905.SH", "CSI", date(2007, 1, 15), _DATASET_START, "P0"),
        "csi1000": PoolDefinition("csi1000", "000852.SH", "CSI", date(2014, 10, 17), _DATASET_START, "P0"),
        "star50": PoolDefinition("star50", "000688.SH", "SSE", date(2020, 7, 22), date(2020, 7, 22), "P0"),
        "star100": PoolDefinition("star100", "000698.SH", "SSE", date(2023, 8, 7), date(2023, 8, 7), "P0"),
        "sse50": PoolDefinition("sse50", "000016.SH", "SSE", date(2004, 1, 2), _DATASET_START, "P1"),
        "chinext": PoolDefinition("chinext", "399006.SZ", "CNINDEX", date(2010, 6, 1), _DATASET_START, "P1"),
        "csi_a500": PoolDefinition("csi_a500", "000510.SH", "CSI", date(2024, 9, 23), date(2024, 9, 23), "P1"),
        "csi2000": PoolDefinition("csi2000", "932000.CSI", "CSI", date(2023, 8, 11), date(2023, 8, 11), "P1"),
        "csi800": PoolDefinition("csi800", "000906.SH", "CSI", date(2007, 1, 15), _DATASET_START, "P2"),
        "szse_component": PoolDefinition(
            "szse_component", "399001.SZ", "CNINDEX", date(1995, 1, 23), _DATASET_START, "P2"
        ),
        "sse180": PoolDefinition("sse180", "000010.SH", "SSE", date(2002, 7, 1), _DATASET_START, "P2"),
        "szse100": PoolDefinition("szse100", "399330.SZ", "CNINDEX", date(2006, 1, 24), _DATASET_START, "P2"),
        "chinext50": PoolDefinition("chinext50", "399673.SZ", "CNINDEX", date(2014, 6, 18), _DATASET_START, "P2"),
        "csi_all_share": PoolDefinition("csi_all_share", "000985.CSI", "CSI", date(2005, 1, 4), _DATASET_START, "P2"),
    }
)
P0_POOL_IDS = tuple(pool.pool_id for pool in POOL_DEFINITIONS.values() if pool.priority == "P0")


__all__ = ["POOL_DEFINITIONS", "P0_POOL_IDS", "PoolDefinition"]
