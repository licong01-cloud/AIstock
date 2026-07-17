from __future__ import annotations

from backend.services.hmm_evolution.errors import HMMEvolutionError


def test_nested_context_redacts_secrets_and_absolute_paths() -> None:
    error = HMMEvolutionError(
        "failed",
        context={
            "request": {
                "authorization": "Bearer should-not-leak",
                "nested": [
                    {"api_key": "also-secret"},
                    "failed at F:\\Dev\\AIstock\\private\\artifact.pkl",
                    "failed at /srv/qe/private/artifact.pkl",
                    r"failed at \\server\share\private\artifact.pkl",
                ],
            },
            "safe_count": 3,
        },
    )

    payload = error.as_dict()
    assert payload["context"]["request"]["authorization"] == "<redacted>"
    assert payload["context"]["request"]["nested"][0]["api_key"] == "<redacted>"
    serialized = repr(payload["context"])
    assert "should-not-leak" not in serialized
    assert "also-secret" not in serialized
    assert "AIstock" not in serialized
    assert "/srv/qe" not in serialized
    assert "server" not in serialized
    assert serialized.count("<redacted-path>") == 3
    assert payload["context"]["safe_count"] == 3


def test_context_sanitizer_bounds_cycles_depth_items_and_strings() -> None:
    cyclic: list[object] = []
    cyclic.append(cyclic)
    deep: object = "leaf"
    for _ in range(10):
        deep = {"nested": deep}

    error = HMMEvolutionError(
        "failed",
        context={
            "cycle": cyclic,
            "deep": deep,
            "many": list(range(55)),
            "long": "x" * 800,
            "opaque": object(),
        },
    )

    context = error.context
    assert context["cycle"] == ["<cycle>"]
    assert "<max-depth>" in repr(context["deep"])
    assert context["many"][-1] == "<truncated:5>"
    assert len(context["long"]) == 500
    assert context["opaque"] == "<object>"
