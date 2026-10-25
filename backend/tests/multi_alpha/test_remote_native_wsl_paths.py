"""Node-local path contract for multi-alpha replay dispatch (no service calls)."""

import pytest

from backend.services.multi_alpha.combine_backtest import MultiAlphaCombineBacktestError
from backend.services.multi_alpha.remote_dispatch import ComputeNodeInfo, _require_remote_linux_path


@pytest.mark.parametrize(
    ("node_id", "api", "path", "allowed"),
    [
        ("wsl2-5080", "http://127.0.0.1:9000", "/mnt/wsl/aistock-qe-data-v1/releases/r8/components/daily_bin_candidate", True),
        ("wsl2-5080", "http://localhost:9000", "/mnt/wsl/releases/r8", True),
        ("wsl2-5080", "http://[::1]:9000", "/mnt/wsl/releases/r8", True),
        ("wsl2-5080", "http://127.0.0.1:9000", "/mnt/x/candidate", True),
        ("rdagent-node1", "http://192.168.50.215:9000", "/home/lc999/data/releases/r8", True),
        ("rdagent-node1", "http://192.168.50.215:9000", "/mnt/wsl/releases/r8", False),
        ("wsl2-5080", "http://192.168.50.215:9000", "/mnt/wsl/releases/r8", False),
        ("rdagent-node1", "http://127.0.0.1:9000", "/mnt/wsl/releases/r8", False),
        ("wsl2-5080", "http://127.0.0.1:9000", "/mnt/wsl-other/releases/r8", False),
        ("wsl2-5080", "http://127.0.0.1:9000", "/mnt/unknown/releases/r8", False),
        ("wsl2-5080", "http://127.0.0.1:9000", "X:/candidate", False),
        ("wsl2-5080", "http://127.0.0.1:9000", "relative/candidate", False),
    ],
)
def test_remote_native_wsl_path_contract(node_id, api, path, allowed):
    node = ComputeNodeInfo(node_id=node_id, api_base_url=api)
    if allowed:
        _require_remote_linux_path(path_name="qlib_data_path", value=path, node=node)
    else:
        with pytest.raises(MultiAlphaCombineBacktestError) as caught:
            _require_remote_linux_path(path_name="qlib_data_path", value=path, node=node)
        assert caught.value.reason_code == "remote_path_invalid"
        assert caught.value.context["value"] == path
        assert caught.value.context["node_id"] == node_id
