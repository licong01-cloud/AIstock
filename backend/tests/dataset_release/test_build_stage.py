from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.services.dataset_release.build_stage import (
    CandidateBuildStageError,
    _actions,
    run_build_stage,
)
from backend.services.dataset_release.contracts import Component, ComponentAction


def _complete_actions() -> list[dict[str, str]]:
    return [{"component": component.value, "action": ComponentAction.REUSE.value} for component in Component]


def test_action_plan_requires_one_action_for_every_component() -> None:
    assert _actions({"actions": _complete_actions()}) == {component: ComponentAction.REUSE for component in Component}

    with pytest.raises(CandidateBuildStageError, match="incomplete"):
        _actions({"actions": _complete_actions()[:-1]})

    with pytest.raises(CandidateBuildStageError, match="duplicated"):
        _actions({"actions": [*_complete_actions(), _complete_actions()[0]]})


def test_run_build_stage_rejects_unknown_stage_before_any_write() -> None:
    with pytest.raises(CandidateBuildStageError, match="unsupported build stage"):
        run_build_stage(SimpleNamespace(stage="publish"))
