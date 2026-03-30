"""在相关性引擎中添加稀疏因子检测"""

# 在 correlation_engine.py 的 compute_full_matrix 方法中添加：

def _detect_sparse_factors(self, panel: pd.DataFrame, threshold: float = 0.95) -> dict:
    """检测稀疏因子（零值占比 > threshold）

    Returns:
        dict: {factor_name: zero_ratio}
    """
    sparse_factors = {}
    for col in panel.columns:
        vals = panel[col].dropna()
        if len(vals) > 0:
            zero_ratio = (vals == 0).sum() / len(vals)
            if zero_ratio > threshold:
                sparse_factors[col] = zero_ratio
    return sparse_factors

# 在 compute_full_matrix 的 L244 后添加：
sparse_factors = self._detect_sparse_factors(panel, threshold=0.95)
if sparse_factors:
    logger.warning(
        f"检测到 {len(sparse_factors)} 个稀疏因子（零值>95%）: "
        f"{list(sparse_factors.keys())}"
    )
    # 将稀疏因子信息添加到 metadata
    result.metadata["sparse_factors"] = sparse_factors
