from __future__ import annotations

import pytest

from scripts.miniqmt_current_three_inventory import parse_args


def test_inventory_cli_is_intrinsically_read_only_and_requires_output() -> None:
    with pytest.raises(SystemExit):
        parse_args([])
    args = parse_args(["--output", "inventory.json"])
    assert args.output == "inventory.json"
