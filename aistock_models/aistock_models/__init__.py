"""
AIstock custom Qlib models.

Provides model classes implementing the Qlib Model interface
for architectures not supported natively by Qlib:

- LambdaRankModel: LightGBM LambdaMART ranking for cross-sectional stock selection
- TabPFNModel: Prior-Data Fitted Network (Nature 2025) for zero-shot tabular prediction
- AIStockXGBModel: XGBoost wrapper with effective boosting-round controls
- AIStockGeneralPTNNLTR: GeneralPTNN-compatible adapter for date-grouped LTR loss
"""

__version__ = "1.0.0"
