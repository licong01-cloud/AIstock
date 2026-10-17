from pathlib import Path


def test_rotation_product_has_no_trading_or_qe_write_path() -> None:
    root = Path(__file__).resolve().parents[3]
    sources = "\n".join(
        (root / path).read_text(encoding="utf-8").lower()
        for path in (
            "backend/services/hmm_risk/rotation_l1_prediction.py",
            "backend/routers/hmm_risk.py",
        )
    )

    for forbidden in (
        "backend.services.selection",
        "backend.services.paper",
        "backend.services.quantevolver",
        "backend.infra.qmt",
        "can_buy",
        "order_submit",
    ):
        assert forbidden not in sources
