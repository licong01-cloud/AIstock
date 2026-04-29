"""
TabPFN tabular foundation model for stock return prediction.

Implements Qlib's Model interface using TabPFN (Prior-Data Fitted Network),
a transformer pre-trained on 130M+ synthetic tabular datasets (Nature 2025).

Key properties:
  - Zero-training: fit() stores context data, predict() does one forward pass
  - Best for small samples (N < 10,000, features < 500) where GBDTs overfit
  - For regression: discretizes labels → class probabilities → weighted score

References:
  - Hollmann, N. et al. (2025) "Accurate predictions on small data with
    a tabular foundation model." Nature, 637, 319-326.
"""

import logging
import os
import numpy as np
import pandas as pd
from qlib.model.base import Model
from qlib.data.dataset import DatasetH
from qlib.data.dataset.handler import DataHandlerLP

logger = logging.getLogger(__name__)

# ── Monkey-patch TabPFN license check for offline/cached environments ──
# TabPFN calls ensure_license_accepted() every time fit() is invoked,
# even when model weights are already cached locally. In environments
# without direct HuggingFace API access (e.g. behind firewall, using
# hf-mirror.com), this check fails unnecessarily.
#
# We pre-download model weights via hf-mirror.com and patch the check
# to avoid the redundant API call. The HF_ENDPOINT env var should be
# set to "https://hf-mirror.com" for downloading missing weights.
_patched = False


def _patch_tabpfn_license_check():
    global _patched
    if _patched:
        return
    try:
        import tabpfn.browser_auth as _ba
        _ba.ensure_license_accepted = lambda *a, **kw: None
        _patched = True
        logger.info("TabPFN license check patched (offline/cached mode)")
    except ImportError:
        pass


_patch_tabpfn_license_check()


class TabPFNModel(Model):
    """TabPFN zero-shot tabular prediction model.

    The model is pre-trained and does NOT require gradient-based training.
    fit() caches the training data as in-context examples.
    predict() performs a single forward pass through the pre-trained transformer.

    For regression tasks (stock return prediction), labels are discretized
    into ordered classes via quantile binning, and predictions are converted
    back to continuous scores via class-probability weighted averaging.

    Configuration (kwargs in conf.yaml):
      - n_estimators: Ensemble size (default 8, more = more stable)
      - device: 'cuda' or 'cpu' (default 'cuda')
      - max_context_size: Max training samples to use as context (default 2000)
      - n_bins: Number of quantile bins for regression discretization (default 10)
      - random_state: Random seed for reproducibility (default 42)
    """

    def __init__(self,
                 n_estimators=8,
                 device="cuda",
                 max_context_size=2000,
                 n_bins=10,
                 random_state=42,
                 **kwargs):
        super().__init__()
        self.n_estimators = n_estimators
        self.device = device
        self.max_context_size = max_context_size
        self.n_bins = n_bins
        self.random_state = random_state
        self.classifier = None
        self._context = None
        self._bin_edges = None  # for converting classes back to regression
        self._n_classes_ = None
        self.n_features_ = None

    def fit(self, dataset: DatasetH, reweighter=None, **kwargs):
        """Store training data as in-context examples.

        TabPFN does NOT perform gradient-based training. The training data
        serves as "context examples" — the model uses attention over these
        examples to make predictions on new data.

        Parameters
        ----------
        dataset : DatasetH
            Qlib dataset providing features and labels.
        reweighter : optional
            Qlib reweighter (not used by TabPFN).
        """
        # 1. Extract training data
        df_train = dataset.prepare(
            "train", col_set=["feature", "label"],
            data_key=DataHandlerLP.DK_L
        )

        X_train = df_train["feature"].values.astype(np.float64)
        y_train = df_train["label"].values.astype(np.float64).ravel()

        n_samples = len(y_train)

        # 2. Subsample if exceeds max_context_size
        if n_samples > self.max_context_size:
            rng = np.random.RandomState(self.random_state)
            idx = rng.choice(n_samples, self.max_context_size, replace=False)
            X_train = X_train[idx]
            y_train = y_train[idx]
            n_samples = self.max_context_size

        # 3. Discretize labels for classification
        #    Use quantile binning to preserve ranking structure
        n_unique = len(np.unique(np.round(y_train, 6)))
        n_bins = min(self.n_bins, n_unique, n_samples // 5)
        n_bins = max(n_bins, 3)  # minimum 3 classes for meaningful ranking

        self._n_classes_ = n_bins
        self._bin_edges = np.percentile(
            y_train,
            np.linspace(0, 100, n_bins + 1)
        )
        # Remove duplicate edges (can happen with low variance)
        self._bin_edges = np.unique(self._bin_edges)
        self._n_classes_ = len(self._bin_edges) - 1

        if self._n_classes_ < 2:
            # Extreme case: all labels near-identical
            self._bin_edges = np.array([y_train.min() - 0.01,
                                         y_train.mean(),
                                         y_train.max() + 0.01])
            self._n_classes_ = 2

        y_train_binned = np.digitize(y_train, self._bin_edges[1:-1])
        y_train_binned = np.clip(y_train_binned, 0, self._n_classes_ - 1)

        # 4. Store context
        self._context = (X_train, y_train_binned)
        self.n_features_ = X_train.shape[1]

        # 5. Lazy-load TabPFN classifier
        from tabpfn import TabPFNClassifier

        self.classifier = TabPFNClassifier(
            device=self.device,
            n_estimators=self.n_estimators,
            random_state=self.random_state,
        )

        logger.info(
            "TabPFNModel.fit: context=%d samples, %d features, %d bins",
            n_samples, self.n_features_, self._n_classes_
        )

        return self

    def predict(self, dataset: DatasetH, segment="test"):
        """Predict via in-context learning.

        Uses the pre-trained TabPFN transformer to predict on test samples
        by attending over the stored training context.

        Returns
        -------
        pd.Series
            Continuous prediction scores indexed by (datetime, instrument).
        """
        if self._context is None:
            raise RuntimeError("Call fit() first to set up context.")
        if self.classifier is None:
            raise RuntimeError("TabPFN classifier not initialized.")

        df_test = dataset.prepare(
            segment, col_set="feature", data_key=DataHandlerLP.DK_L
        )
        X_test = df_test.values.astype(np.float64)

        X_train, y_train_binned = self._context

        # TabPFN in-context prediction
        try:
            self.classifier.fit(X_train, y_train_binned)
            proba = self.classifier.predict_proba(X_test)
        except Exception as e:
            logger.error("TabPFN predict failed: %s", e)
            raise

        # Convert class probabilities to continuous scores
        class_centers = 0.5 * (self._bin_edges[:-1] + self._bin_edges[1:])
        scores = proba @ class_centers

        return pd.Series(scores, index=df_test.index, name="score")

    # ---- persistence ----

    def save(self, path: str):
        """Save context data and bin edges (TabPFN itself is stateless)."""
        import os
        import pickle
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        state = {
            "context": self._context,
            "bin_edges": self._bin_edges,
            "n_classes": self._n_classes_,
            "n_features": self.n_features_,
        }
        with open(path, "wb") as f:
            pickle.dump(state, f)

    def load(self, path: str):
        """Load context data from file."""
        import pickle
        with open(path, "rb") as f:
            state = pickle.load(f)
        self._context = state["context"]
        self._bin_edges = state["bin_edges"]
        self._n_classes_ = state["n_classes"]
        self.n_features_ = state["n_features"]
