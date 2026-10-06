"""Independent fixed-market-session value; no package eligibility or execution."""
from datetime import date

from pydantic import BaseModel, ConfigDict, model_validator

from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES, KEY, ROSTER
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

POLICY = {"label_contract": "GENERIC_ENTRY_FIXED_5TD_V1", "sessions_including_entry": 5,
          "entry": "T_OPEN_NOMINAL", "endpoint": "T_PLUS_4_CLOSE_NOMINAL",
          "missing": "UNKNOWN_NO_FILL_NO_DELAY", "price_basis": "D_ANCHORED_CNY",
          "buy_bps": .95, "sell_bps": 5.95, "risk_bps": 800.}
POLICY_SHA256 = sha(POLICY)
PARAMETERS = {"n_estimators": 200, "learning_rate": .05, "max_depth": 3,
              "min_samples_leaf": 30, "subsample": 1., "random_state": 20261006}
ARMS = ("candidate", "matched")
STOCK_FEATURES = ("ret_1", "ret_5", "ret_10", "atr14_close", "close_location_in_day", "volume_ratio_5_to_20")


class GenericPrice5TDConfigurationV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    train_start: date
    train_end: date
    validation_start: date
    validation_end: date
    test_start: date
    test_end: date
    label_cutoff: date

    @model_validator(mode="after")
    def ordered(self):
        if not (self.train_start <= self.train_end < self.validation_start <= self.validation_end
                < self.test_start <= self.test_end <= self.label_cutoff):
            raise ValueError("GP5 configuration windows must be ordered and non-overlapping")
        return self


__all__ = ["ARMS", "FEATURES", "KEY", "PARAMETERS", "POLICY", "POLICY_SHA256",
           "ROSTER", "STOCK_FEATURES", "GenericPrice5TDConfigurationV1"]
