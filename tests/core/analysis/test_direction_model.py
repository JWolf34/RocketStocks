"""Tests for rocketstocks.core.analysis.direction_model."""
import math

import numpy as np
import pandas as pd
import pytest

from rocketstocks.core.analysis.direction_model import (
    BASE_FEATURES,
    DirectionModel,
    DirectionPrediction,
    ModelAccuracyReport,
    build_feature_vector,
    build_features_for_backtest,
    compute_forward_returns,
    evaluate_model_accuracy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ohlcv(n=200, trend='mixed', seed=42):
    """Generate synthetic OHLCV DataFrame with capitalised columns.

    'mixed' produces enough up/down bars to give both label classes.
    """
    rng = np.random.default_rng(seed)
    if trend == 'up':
        close = pd.Series(100.0 + np.arange(n) * 0.05 + rng.normal(0, 1.5, n))
    elif trend == 'down':
        close = pd.Series(100.0 - np.arange(n) * 0.05 + rng.normal(0, 1.5, n))
    else:
        # Mixed: flat drift with enough noise to produce both up and down bars
        close = pd.Series(100.0 + rng.normal(0, 2.0, n).cumsum() * 0.1)
    high = close + rng.uniform(0.1, 1.0, n)
    low = close - rng.uniform(0.1, 1.0, n)
    open_ = close + rng.normal(0, 0.3, n)
    volume = pd.Series(1_000_000 + rng.uniform(0, 200_000, n))
    return pd.DataFrame(
        {'Open': open_, 'High': high, 'Low': low, 'Close': close, 'Volume': volume}
    )


def _fitted_model(n=200, seed=42):
    """Return a fitted DirectionModel and its training DataFrames."""
    df = _make_ohlcv(n=n, seed=seed)
    features = build_features_for_backtest(df)
    fwd = compute_forward_returns(df['Close'], forward_bars=5)
    cutoff = n // 2
    model = DirectionModel(forward_bars=5, min_training_samples=20)
    success = model.fit(features.iloc[:cutoff], fwd.iloc[:cutoff])
    return model, features, fwd, success


# ---------------------------------------------------------------------------
# compute_forward_returns
# ---------------------------------------------------------------------------

class TestComputeForwardReturns:

    def test_length_matches_input(self):
        close = pd.Series([100.0, 101, 102, 103, 104, 105])
        result = compute_forward_returns(close, forward_bars=2)
        assert len(result) == len(close)

    def test_last_n_bars_are_nan(self):
        close = pd.Series([100.0, 101, 102, 103, 104, 105])
        result = compute_forward_returns(close, forward_bars=2)
        assert result.iloc[-2:].isna().all()

    def test_values_computed_correctly(self):
        close = pd.Series([100.0, 110.0, 100.0])
        result = compute_forward_returns(close, forward_bars=1)
        # bar 0: 110/100 - 1 = 0.1
        assert result.iloc[0] == pytest.approx(0.10)
        # bar 1: 100/110 - 1 ≈ -0.0909
        assert result.iloc[1] == pytest.approx(-1 / 11)
        # bar 2: NaN (no future data)
        assert math.isnan(result.iloc[2])


# ---------------------------------------------------------------------------
# build_features_for_backtest
# ---------------------------------------------------------------------------

class TestBuildFeaturesForBacktest:

    def test_returns_dataframe_with_base_features(self):
        df = _make_ohlcv(100)
        result = build_features_for_backtest(df)
        assert isinstance(result, pd.DataFrame)
        for col in BASE_FEATURES:
            assert col in result.columns

    def test_index_matches_input(self):
        df = _make_ohlcv(100)
        result = build_features_for_backtest(df)
        assert list(result.index) == list(df.index)

    def test_early_rows_are_nan(self):
        df = _make_ohlcv(60)
        result = build_features_for_backtest(df)
        # First ~14 rows should be NaN (warmup for MFI/OBV)
        assert result.iloc[:5].isna().any().any()

    def test_later_rows_mostly_non_nan(self):
        df = _make_ohlcv(150)
        result = build_features_for_backtest(df)
        # After warmup (first 30 rows), majority should be non-NaN
        tail = result.iloc[30:]
        non_nan_pct = tail.notna().mean().mean()
        assert non_nan_pct > 0.7, f"Expected > 70% non-NaN, got {non_nan_pct:.0%}"

    def test_empty_df_returns_empty_with_columns(self):
        df = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])
        result = build_features_for_backtest(df)
        assert result.empty
        for col in BASE_FEATURES:
            assert col in result.columns

    def test_volume_zscore_magnitude(self):
        df = _make_ohlcv(100)
        result = build_features_for_backtest(df)
        valid = result['volume_zscore'].dropna()
        # Volume z-scores from near-uniform data should be small
        assert valid.abs().median() < 5.0


# ---------------------------------------------------------------------------
# build_feature_vector
# ---------------------------------------------------------------------------

class TestBuildFeatureVector:

    def test_returns_dict_with_base_features(self):
        df = _make_ohlcv(50)
        feat = build_feature_vector(df['Close'], df['High'], df['Low'], df['Volume'])
        for key in BASE_FEATURES:
            assert key in feat

    def test_returns_floats(self):
        df = _make_ohlcv(50)
        feat = build_feature_vector(df['Close'], df['High'], df['Low'], df['Volume'])
        for key in BASE_FEATURES:
            assert isinstance(feat[key], float)

    def test_insufficient_data_returns_nan(self):
        df = _make_ohlcv(3)
        feat = build_feature_vector(df['Close'], df['High'], df['Low'], df['Volume'])
        # With only 3 bars, most features should be NaN
        nan_count = sum(1 for v in feat.values() if math.isnan(v))
        assert nan_count >= 2


# ---------------------------------------------------------------------------
# DirectionModel
# ---------------------------------------------------------------------------

class TestDirectionModel:

    def test_not_fitted_initially(self):
        model = DirectionModel()
        assert not model.is_fitted

    def test_fit_returns_true_with_sufficient_data(self):
        _, _, _, success = _fitted_model()
        assert success is True

    def test_fitted_after_successful_fit(self):
        model, features, fwd, _ = _fitted_model()
        assert model.is_fitted

    def test_fit_returns_false_with_insufficient_data(self):
        model = DirectionModel(min_training_samples=1000)
        df = _make_ohlcv(50)
        features = build_features_for_backtest(df)
        fwd = compute_forward_returns(df['Close'])
        result = model.fit(features, fwd)
        assert result is False

    def test_coefficients_none_before_fit(self):
        model = DirectionModel()
        assert model.coefficients is None

    def test_coefficients_dict_after_fit(self):
        model, *_ = _fitted_model()
        coefs = model.coefficients
        assert isinstance(coefs, dict)
        for feat in BASE_FEATURES:
            assert feat in coefs

    def test_predict_returns_direction_prediction(self):
        model, features, _, _ = _fitted_model()
        last_row = features.iloc[-10].to_dict()
        result = model.predict(last_row)
        assert isinstance(result, DirectionPrediction)

    def test_predict_probability_in_0_1(self):
        model, features, _, _ = _fitted_model()
        last_row = features.iloc[-10].to_dict()
        result = model.predict(last_row)
        assert 0.0 <= result.probability_up <= 1.0

    def test_predict_direction_is_up_or_down(self):
        model, features, _, _ = _fitted_model()
        last_row = features.iloc[-10].to_dict()
        result = model.predict(last_row)
        assert result.predicted_direction in ('up', 'down')

    def test_predict_confidence_in_0_1(self):
        model, features, _, _ = _fitted_model()
        last_row = features.iloc[-10].to_dict()
        result = model.predict(last_row)
        assert 0.0 <= result.confidence <= 1.0

    def test_predict_unfitted_returns_unknown(self):
        model = DirectionModel()
        result = model.predict({'volume_zscore': 1.0, 'mfi': 60.0, 'obv_velocity': 0.5, 'confluence_count': 2.0})
        assert result.predicted_direction == 'unknown'
        assert result.probability_up == pytest.approx(0.5)

    def test_predict_nan_feature_returns_unknown(self):
        model, features, _, _ = _fitted_model()
        feat = {k: float('nan') for k in BASE_FEATURES}
        result = model.predict(feat)
        assert result.predicted_direction == 'unknown'

    def test_predict_batch_returns_dataframe(self):
        model, features, _, _ = _fitted_model()
        result = model.predict_batch(features.iloc[100:])
        assert isinstance(result, pd.DataFrame)
        assert 'probability_up' in result.columns
        assert 'predicted_direction' in result.columns
        assert 'confidence' in result.columns

    def test_predict_batch_probabilities_in_range(self):
        model, features, _, _ = _fitted_model()
        result = model.predict_batch(features.iloc[100:])
        valid = result[result['predicted_direction'] != 'unknown']
        assert (valid['probability_up'] >= 0).all()
        assert (valid['probability_up'] <= 1).all()

    def test_probability_direction_consistent(self):
        """probability_up >= 0.5 ↔ predicted_direction == 'up'."""
        model, features, _, _ = _fitted_model()
        result = model.predict_batch(features.iloc[100:])
        known = result[result['predicted_direction'] != 'unknown']
        up_mask = known['predicted_direction'] == 'up'
        assert (known.loc[up_mask, 'probability_up'] >= 0.5).all()
        assert (known.loc[~up_mask, 'probability_up'] < 0.5).all()


# ---------------------------------------------------------------------------
# evaluate_model_accuracy
# ---------------------------------------------------------------------------

class TestEvaluateModelAccuracy:

    def test_returns_model_accuracy_report(self):
        model, features, fwd, _ = _fitted_model(n=300)
        report = evaluate_model_accuracy(model, features.iloc[150:], fwd.iloc[150:])
        assert isinstance(report, ModelAccuracyReport)

    def test_total_predictions_is_positive(self):
        model, features, fwd, _ = _fitted_model(n=300)
        report = evaluate_model_accuracy(model, features.iloc[150:], fwd.iloc[150:])
        assert report.total_predictions > 0

    def test_up_plus_down_equals_total(self):
        model, features, fwd, _ = _fitted_model(n=300)
        report = evaluate_model_accuracy(model, features.iloc[150:], fwd.iloc[150:])
        assert (
            report.predicted_up_count + report.predicted_down_count
            == report.total_predictions
        )

    def test_win_rates_in_0_1(self):
        model, features, fwd, _ = _fitted_model(n=300)
        report = evaluate_model_accuracy(model, features.iloc[150:], fwd.iloc[150:])
        if not math.isnan(report.predicted_up_win_rate):
            assert 0.0 <= report.predicted_up_win_rate <= 1.0
        if not math.isnan(report.predicted_down_win_rate):
            assert 0.0 <= report.predicted_down_win_rate <= 1.0

    def test_confusion_matrix_keys(self):
        model, features, fwd, _ = _fitted_model(n=300)
        report = evaluate_model_accuracy(model, features.iloc[150:], fwd.iloc[150:])
        for key in ('tp', 'fp', 'tn', 'fn'):
            assert key in report.confusion_matrix

    def test_confusion_matrix_counts_nonnegative(self):
        model, features, fwd, _ = _fitted_model(n=300)
        report = evaluate_model_accuracy(model, features.iloc[150:], fwd.iloc[150:])
        for v in report.confusion_matrix.values():
            assert v >= 0

    def test_confidence_tiers_present(self):
        model, features, fwd, _ = _fitted_model(n=300)
        report = evaluate_model_accuracy(model, features.iloc[150:], fwd.iloc[150:])
        for tier in ('low', 'medium', 'high'):
            assert tier in report.accuracy_by_confidence

    def test_unfitted_model_returns_empty_report(self):
        model = DirectionModel()
        df = _make_ohlcv(100)
        features = build_features_for_backtest(df)
        fwd = compute_forward_returns(df['Close'])
        report = evaluate_model_accuracy(model, features, fwd)
        assert report.total_predictions == 0
        assert math.isnan(report.overall_accuracy)
