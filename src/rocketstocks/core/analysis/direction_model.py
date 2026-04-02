"""Directional price prediction via logistic regression.

Predicts P(forward_return > 0) using four OHLCV-derived features:

    volume_zscore   — how unusual is current volume vs 20-day baseline
    mfi             — Money Flow Index: continuous buy/sell pressure (0-100)
    obv_velocity    — OBV slope over 10 bars: direction of smart-money flow
    confluence_count — number of RSI/MACD/ADX/OBV indicators signalling bullish

These are complementary to the existing composite_score.py, which uses
ABSOLUTE values (magnitude only). This model uses SIGNED values to infer
direction: MFI > 50 = buying pressure, OBV velocity > 0 = upward flow.

Usage in backtesting strategies::

    model = DirectionModel(forward_bars=5)
    features_df = build_features_for_backtest(data.df)
    forward_ret = compute_forward_returns(data.df['Close'], forward_bars=5)
    train_cutoff = int(len(data.df) * 0.5)
    model.fit(features_df.iloc[:train_cutoff], forward_ret.iloc[:train_cutoff])

    # At bar i (after training cutoff):
    feat = {col: features_df.iloc[i][col] for col in BASE_FEATURES}
    prediction = model.predict(feat)
    if prediction.probability_up >= 0.60:
        self.buy()
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import pandas_ta_classic as ta

from rocketstocks.core.analysis.signals import signals
from rocketstocks.core.analysis.volume_pressure import compute_mfi

logger = logging.getLogger(__name__)

BASE_FEATURES = ['volume_zscore', 'mfi', 'obv_velocity', 'confluence_count']

# Minimum bars required before computing reliable features
_MIN_FEATURE_BARS = 21


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DirectionPrediction:
    """Output of a single direction prediction."""
    probability_up: float           # P(forward return > 0), in [0, 1]
    predicted_direction: str        # 'up' or 'down'
    confidence: float               # abs(probability_up - 0.5) * 2, in [0, 1]
    feature_values: dict
    feature_importances: dict | None = None


@dataclass
class ModelAccuracyReport:
    """Post-hoc accuracy analysis covering both predicted directions."""
    total_predictions: int
    predicted_up_count: int
    predicted_down_count: int
    predicted_up_win_rate: float       # % of 'predicted up' that actually went up
    predicted_down_win_rate: float     # % of 'predicted down' that actually went down
    overall_accuracy: float
    accuracy_by_confidence: dict       # {'low': %, 'medium': %, 'high': %} win rates
    confusion_matrix: dict             # {'tp': int, 'fp': int, 'tn': int, 'fn': int}
    rejected_signals: dict | None = None  # stats on signals the filter blocked


# ---------------------------------------------------------------------------
# DirectionModel
# ---------------------------------------------------------------------------

class DirectionModel:
    """Logistic regression model for directional price prediction.

    Wraps sklearn LogisticRegression + StandardScaler. Both are fitted
    together on the training data so that the scaler's mean/std are
    computed from the training window only (no data leakage).

    Args:
        forward_bars:          Number of bars ahead to predict. Label is
                               1 if close[i + forward_bars] > close[i].
        regularization_c:      Inverse of L2 regularization strength. Smaller
                               values → more regularization (default 1.0).
        min_training_samples:  Minimum labelled samples required to fit.
    """

    def __init__(
        self,
        forward_bars: int = 5,
        regularization_c: float = 1.0,
        min_training_samples: int = 50,
    ) -> None:
        self.forward_bars = forward_bars
        self.regularization_c = regularization_c
        self.min_training_samples = min_training_samples

        self._scaler = None
        self._clf = None
        self._feature_names: list[str] = BASE_FEATURES[:]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fit(self, features_df: pd.DataFrame, forward_returns: pd.Series) -> bool:
        """Fit the model on training data.

        Args:
            features_df:     DataFrame with columns matching BASE_FEATURES.
                             Must be aligned with forward_returns by index.
            forward_returns: Series of forward pct returns (from
                             compute_forward_returns()). NaN rows are dropped.

        Returns:
            True if fitting succeeded; False if insufficient data.
        """
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler

        # Align features and labels; drop NaN rows (early warmup + tail NaNs)
        combined = features_df[self._feature_names].copy()
        combined['_label'] = (forward_returns > 0).astype(int)
        combined = combined.dropna()

        if len(combined) < self.min_training_samples:
            logger.warning(
                f'DirectionModel.fit: only {len(combined)} labelled samples '
                f'(need {self.min_training_samples}); skipping fit'
            )
            return False

        X = combined[self._feature_names].values
        y = combined['_label'].values

        # Require both classes present for logistic regression
        if len(np.unique(y)) < 2:
            logger.warning('DirectionModel.fit: only one class in training labels')
            return False

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        clf = LogisticRegression(C=self.regularization_c, max_iter=1000, random_state=0)
        clf.fit(X_scaled, y)

        self._scaler = scaler
        self._clf = clf
        logger.debug(
            f'DirectionModel fitted on {len(X)} samples, '
            f'classes={clf.classes_}, '
            f'coefficients={dict(zip(self._feature_names, clf.coef_[0].round(3)))}'
        )
        return True

    def predict(self, features: dict) -> DirectionPrediction:
        """Predict direction for a single bar's feature dict.

        Args:
            features: Dict mapping feature name → value (must contain all
                      BASE_FEATURES keys).

        Returns:
            DirectionPrediction with probability_up and related metadata.
            Returns probability_up=0.5 (no confidence) if model is not fitted
            or any feature is NaN.
        """
        if not self.is_fitted:
            return _uncertain_prediction(features)

        vals = [features.get(f, float('nan')) for f in self._feature_names]
        if any(math.isnan(v) for v in vals if isinstance(v, float)):
            return _uncertain_prediction(features)

        X = np.array(vals).reshape(1, -1)
        X_scaled = self._scaler.transform(X)
        proba = self._clf.predict_proba(X_scaled)[0]

        # classes_ may be [0, 1] or [1, 0] — find index for class 1 (up)
        class_idx = list(self._clf.classes_).index(1)
        prob_up = float(proba[class_idx])
        direction = 'up' if prob_up >= 0.5 else 'down'
        confidence = abs(prob_up - 0.5) * 2.0

        return DirectionPrediction(
            probability_up=prob_up,
            predicted_direction=direction,
            confidence=confidence,
            feature_values=dict(zip(self._feature_names, vals)),
            feature_importances=self.coefficients,
        )

    def predict_batch(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Predict direction for every row in features_df.

        Args:
            features_df: DataFrame with BASE_FEATURES columns.

        Returns:
            DataFrame with columns: probability_up, predicted_direction,
            confidence.  Rows with NaN features get probability_up=0.5.
        """
        if not self.is_fitted:
            return pd.DataFrame(
                {
                    'probability_up': 0.5,
                    'predicted_direction': 'unknown',
                    'confidence': 0.0,
                },
                index=features_df.index,
            )

        df = features_df[self._feature_names].copy()
        valid_mask = df.notna().all(axis=1)

        prob_up = pd.Series(0.5, index=df.index)
        direction = pd.Series('unknown', index=df.index)
        confidence = pd.Series(0.0, index=df.index)

        if valid_mask.any():
            X = df.loc[valid_mask].values
            X_scaled = self._scaler.transform(X)
            proba = self._clf.predict_proba(X_scaled)
            class_idx = list(self._clf.classes_).index(1)
            p_up = proba[:, class_idx]

            prob_up.loc[valid_mask] = p_up
            direction.loc[valid_mask] = pd.Series(
                np.where(p_up >= 0.5, 'up', 'down'),
                index=df.index[valid_mask],
            )
            confidence.loc[valid_mask] = np.abs(p_up - 0.5) * 2.0

        return pd.DataFrame(
            {
                'probability_up': prob_up,
                'predicted_direction': direction,
                'confidence': confidence,
            },
            index=df.index,
        )

    @property
    def is_fitted(self) -> bool:
        return self._clf is not None and self._scaler is not None

    @property
    def coefficients(self) -> dict | None:
        """Return feature coefficients from the fitted model, or None."""
        if not self.is_fitted:
            return None
        return dict(zip(self._feature_names, self._clf.coef_[0].tolist()))


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------

def build_feature_vector(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
) -> dict:
    """Compute a single-bar feature dict from OHLCV Series.

    Uses the LAST value of each computed indicator — designed for
    per-bar evaluation inside a backtesting ``next()`` loop or live alerts.

    Args:
        close:  Closing price series (at least 21 bars recommended).
        high:   High price series.
        low:    Low price series.
        volume: Volume series.

    Returns:
        Dict with keys matching BASE_FEATURES.  Any uncomputable feature
        is returned as NaN.
    """
    result: dict[str, float] = {}

    # volume_zscore: current volume vs 20-bar rolling baseline
    if len(volume) >= 2:
        hist = volume.iloc[:-1].tail(20)
        curr_vol = float(volume.iloc[-1])
        result['volume_zscore'] = signals.volume_zscore(hist, curr_vol, period=20)
    else:
        result['volume_zscore'] = float('nan')

    # mfi: latest MFI value
    result['mfi'] = float(_safe_call(
        lambda: compute_mfi(high=high, low=low, close=close, volume=volume, period=14).iloc[-1]
    ))

    # obv_velocity: slope of OBV over last 10 bars
    result['obv_velocity'] = float(_safe_call(lambda: _obv_velocity(close, volume, span=10)))

    # confluence_count: count of bullish signals from RSI/MACD/ADX/OBV
    try:
        count, total, _ = signals.technical_confluence(
            close=close, high=high, low=low, volume=volume
        )
        result['confluence_count'] = float(count)
    except Exception:
        result['confluence_count'] = float('nan')

    return result


def build_features_for_backtest(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized bulk feature computation for a full backtesting DataFrame.

    Computes all BASE_FEATURES as columns using only past data at each bar
    (rolling windows that do not look forward).  Call this once in
    ``init()``; use the resulting DataFrame's rows in ``next()``.

    Args:
        df: DataFrame with capitalised columns (Open, High, Low, Close, Volume)
            as produced by ``prep_daily()`` or ``prep_5m()``.

    Returns:
        DataFrame with columns matching BASE_FEATURES, same index as *df*.
        Early bars without enough history will have NaN.
    """
    if df.empty:
        return pd.DataFrame(columns=BASE_FEATURES, index=df.index)

    close = df['Close']
    high = df['High']
    low = df['Low']
    volume = df['Volume']

    result = pd.DataFrame(index=df.index)

    # volume_zscore: (current_volume - 20-bar prior mean) / 20-bar prior std
    # shift(1) so the baseline window excludes the current bar
    vol_mean = volume.shift(1).rolling(20, min_periods=5).mean()
    vol_std = volume.shift(1).rolling(20, min_periods=5).std()
    result['volume_zscore'] = (volume - vol_mean) / vol_std

    # mfi: ta.mfi returns a Series aligned with df; safe to use directly
    try:
        mfi_series = ta.mfi(high=high, low=low, close=close, volume=volume, length=14)
        if mfi_series is not None and not mfi_series.empty:
            result['mfi'] = mfi_series.values
        else:
            result['mfi'] = float('nan')
    except Exception as exc:
        logger.warning(f'build_features_for_backtest: mfi failed: {exc}')
        result['mfi'] = float('nan')

    # obv_velocity: slope of OBV over 10 bars (diff / 10)
    try:
        obv_series = ta.obv(close=close, volume=volume)
        if obv_series is not None and not obv_series.empty:
            # Normalise by typical OBV magnitude to keep values comparable across tickers
            obv_std = obv_series.rolling(20, min_periods=5).std()
            obv_std = obv_std.replace(0, float('nan'))
            result['obv_velocity'] = obv_series.diff(10) / obv_std
        else:
            result['obv_velocity'] = float('nan')
    except Exception as exc:
        logger.warning(f'build_features_for_backtest: obv_velocity failed: {exc}')
        result['obv_velocity'] = float('nan')

    # confluence_count: computed per-row (expensive, but only called once)
    result['confluence_count'] = _vectorized_confluence(close, high, low, volume)

    return result[BASE_FEATURES]


def compute_forward_returns(close: pd.Series, forward_bars: int = 5) -> pd.Series:
    """Compute forward percentage returns for training labels.

    Args:
        close:        Closing price series.
        forward_bars: Number of bars ahead to measure return.

    Returns:
        Series of pct returns: close.shift(-forward_bars) / close - 1.
        The last *forward_bars* entries are NaN (no future data available).
    """
    return close.shift(-forward_bars) / close - 1


# ---------------------------------------------------------------------------
# Accuracy evaluation
# ---------------------------------------------------------------------------

def evaluate_model_accuracy(
    model: DirectionModel,
    features_df: pd.DataFrame,
    forward_returns: pd.Series,
) -> ModelAccuracyReport:
    """Compute post-hoc accuracy across all predictable bars.

    Runs the model's predictions over ALL bars in features_df (not just
    bars where a strategy entered), then compares predictions to realised
    forward returns.

    Args:
        model:          Fitted DirectionModel.
        features_df:    Full-length features DataFrame (from
                        build_features_for_backtest()).
        forward_returns: Full-length forward returns Series (from
                        compute_forward_returns()).

    Returns:
        ModelAccuracyReport with bidirectional win rates and confidence tiers.
    """
    if not model.is_fitted:
        logger.warning('evaluate_model_accuracy called on unfitted model')
        return ModelAccuracyReport(
            total_predictions=0,
            predicted_up_count=0,
            predicted_down_count=0,
            predicted_up_win_rate=float('nan'),
            predicted_down_win_rate=float('nan'),
            overall_accuracy=float('nan'),
            accuracy_by_confidence={},
            confusion_matrix={'tp': 0, 'fp': 0, 'tn': 0, 'fn': 0},
        )

    preds = model.predict_batch(features_df)
    preds['actual_up'] = (forward_returns > 0).values
    preds['actual_return'] = forward_returns.values

    # Drop rows where prediction is uncertain or actual return is unknown
    valid = preds[
        (preds['predicted_direction'] != 'unknown')
        & preds['actual_up'].notna()
        & preds['actual_return'].notna()
    ].copy()

    if valid.empty:
        return ModelAccuracyReport(
            total_predictions=0,
            predicted_up_count=0,
            predicted_down_count=0,
            predicted_up_win_rate=float('nan'),
            predicted_down_win_rate=float('nan'),
            overall_accuracy=float('nan'),
            accuracy_by_confidence={},
            confusion_matrix={'tp': 0, 'fp': 0, 'tn': 0, 'fn': 0},
        )

    pred_up = valid[valid['predicted_direction'] == 'up']
    pred_down = valid[valid['predicted_direction'] == 'down']

    up_win_rate = float(pred_up['actual_up'].mean()) if len(pred_up) > 0 else float('nan')
    down_win_rate = float((~pred_down['actual_up']).mean()) if len(pred_down) > 0 else float('nan')
    overall = float(
        ((valid['predicted_direction'] == 'up') & valid['actual_up']
         | (valid['predicted_direction'] == 'down') & ~valid['actual_up']).mean()
    )

    # Confusion matrix (true positive = predicted up AND went up)
    tp = int(((valid['predicted_direction'] == 'up') & valid['actual_up']).sum())
    fp = int(((valid['predicted_direction'] == 'up') & ~valid['actual_up']).sum())
    tn = int(((valid['predicted_direction'] == 'down') & ~valid['actual_up']).sum())
    fn = int(((valid['predicted_direction'] == 'down') & valid['actual_up']).sum())

    # Accuracy by confidence tier
    accuracy_by_confidence = _compute_confidence_tiers(valid)

    return ModelAccuracyReport(
        total_predictions=len(valid),
        predicted_up_count=len(pred_up),
        predicted_down_count=len(pred_down),
        predicted_up_win_rate=up_win_rate,
        predicted_down_win_rate=down_win_rate,
        overall_accuracy=overall,
        accuracy_by_confidence=accuracy_by_confidence,
        confusion_matrix={'tp': tp, 'fp': fp, 'tn': tn, 'fn': fn},
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _uncertain_prediction(features: dict) -> DirectionPrediction:
    return DirectionPrediction(
        probability_up=0.5,
        predicted_direction='unknown',
        confidence=0.0,
        feature_values=features,
        feature_importances=None,
    )


def _safe_call(fn):
    """Call fn(); return NaN on any exception or NaN result."""
    try:
        val = fn()
        if val is None:
            return float('nan')
        f = float(val)
        return f if not math.isnan(f) else float('nan')
    except Exception:
        return float('nan')


def _obv_velocity(close: pd.Series, volume: pd.Series, span: int = 10) -> float:
    """Return the normalised OBV velocity (diff over *span* bars)."""
    obv = ta.obv(close=close, volume=volume)
    if obv is None or len(obv.dropna()) < span + 1:
        return float('nan')
    obv_std = float(obv.tail(20).std())
    if obv_std == 0 or math.isnan(obv_std):
        return float('nan')
    return float(obv.iloc[-1] - obv.iloc[-span - 1]) / obv_std


def _vectorized_confluence(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
) -> pd.Series:
    """Per-row confluence count using a rolling window approach.

    Approximates the per-bar confluence count without recomputing RSI/MACD
    per-row (which would be O(n²)). Uses the last bar's indicator states
    recomputed as a whole-series operation.

    Returns:
        Series of confluence counts (0–4), aligned with input index.
    """
    result = pd.Series(float('nan'), index=close.index)

    # RSI < 30 (oversold bullish signal)
    try:
        rsi = ta.rsi(close)
        rsi_bullish = (rsi < 30).astype(float) if rsi is not None else pd.Series(float('nan'), index=close.index)
    except Exception:
        rsi_bullish = pd.Series(float('nan'), index=close.index)

    # MACD line > signal line
    try:
        macds = ta.macd(close)
        if macds is not None and not macds.empty:
            macd_bullish = (macds.iloc[:, 0] > macds.iloc[:, 1]).astype(float)
        else:
            macd_bullish = pd.Series(float('nan'), index=close.index)
    except Exception:
        macd_bullish = pd.Series(float('nan'), index=close.index)

    # ADX > 25 and +DI > -DI
    try:
        adxs = ta.adx(close=close, high=high, low=low)
        if adxs is not None and not adxs.empty:
            adx_bullish = ((adxs.iloc[:, 0] > 25) & (adxs.iloc[:, 1] > adxs.iloc[:, 2])).astype(float)
        else:
            adx_bullish = pd.Series(float('nan'), index=close.index)
    except Exception:
        adx_bullish = pd.Series(float('nan'), index=close.index)

    # OBV SMA increasing
    try:
        obv = ta.obv(close=close, volume=volume)
        if obv is not None and not obv.empty:
            obv_sma = ta.sma(obv, 10)
            obv_inc = ta.increasing(obv_sma)
            obv_bullish = obv_inc.astype(float) if obv_inc is not None else pd.Series(float('nan'), index=close.index)
        else:
            obv_bullish = pd.Series(float('nan'), index=close.index)
    except Exception:
        obv_bullish = pd.Series(float('nan'), index=close.index)

    # Sum non-NaN components
    components = [rsi_bullish, macd_bullish, adx_bullish, obv_bullish]
    stacked = pd.DataFrame(
        {i: c.values for i, c in enumerate(components)},
        index=close.index,
    )
    result = stacked.sum(axis=1, min_count=1)  # NaN if all components are NaN

    return result


def _compute_confidence_tiers(df: pd.DataFrame) -> dict:
    """Compute accuracy grouped by confidence: low < 0.33, medium < 0.67, high >= 0.67."""
    tiers = {
        'low': df[df['confidence'] < 0.33],
        'medium': df[(df['confidence'] >= 0.33) & (df['confidence'] < 0.67)],
        'high': df[df['confidence'] >= 0.67],
    }
    result = {}
    for tier_name, subset in tiers.items():
        if subset.empty:
            result[tier_name] = float('nan')
            continue
        correct = (
            ((subset['predicted_direction'] == 'up') & subset['actual_up'])
            | ((subset['predicted_direction'] == 'down') & ~subset['actual_up'])
        )
        result[tier_name] = float(correct.mean())
    return result
