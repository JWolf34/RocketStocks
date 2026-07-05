"""LeadingIndicatorStrategy — shared base class for leading indicator strategies.

All leading indicator strategies (popularity_surge, volume_accumulation,
leading_indicator_combo) use identical exit logic and the pending-signal pattern
for 5m timeframes. This base class eliminates duplication and makes exit
behaviour configurable per run via --params.

Exit modes
----------
breakout (default)
    Z-score based. Exit when the price move since entry is statistically
    significant for this ticker. Reuses evaluate_confirmation() from
    core/analysis/alert_strategy.py, which z-scores pct_change_since_entry
    against the ticker's own daily return distribution.
    - Exit profit:   zscore_since_entry >= exit_zscore
    - Exit loss:     zscore_since_entry <= -stop_zscore
    - Max hold:      hold_bars as failsafe

momentum
    Z-score trailing exit. Tracks peak z-score after entry. Exits when z-score
    drops trail_zscore below peak while the position is profitable (move peaked
    and is fading). Captures more of the predicted move than breakout.
    - Exit trailing: peak_zscore - current_zscore >= trail_zscore AND profitable
    - Stop loss:     zscore_since_entry <= -stop_zscore
    - Max hold:      hold_bars as failsafe

bar_hold
    Simple hold N bars. Useful as baseline to compare against z-score exits.

Direction filter (opt-in)
--------------------------
When use_direction_filter=True, a logistic regression model is trained on the
first direction_train_fraction of bars in init(). Every candidate entry is
then gated by the model: entry is only allowed when P(up) >= direction_threshold
AND confidence >= direction_min_confidence.

The model is trained once on historical data — bars before the training cutoff
do not generate entries. This prevents lookahead while still letting the model
learn the ticker's own directional patterns before the out-of-sample test
window begins.
"""
from __future__ import annotations

import logging
import math

import pandas as pd
from backtesting import Strategy

from rocketstocks.backtest.data_prep import prep_for_signals
from rocketstocks.core.analysis.alert_strategy import evaluate_confirmation
from rocketstocks.core.analysis.classification import compute_return_stats
from rocketstocks.core.analysis.direction_model import (
    DirectionModel,
    build_features_for_backtest,
)

logger = logging.getLogger(__name__)


class LeadingIndicatorStrategy(Strategy):
    """Base class with configurable z-score exits, pending-signal support,
    and an opt-in logistic regression direction filter.

    Subclasses must implement ``_detect_signal()`` and set class attributes
    ``requires_popularity`` and/or ``requires_daily`` as needed by the runner.

    Optimizable parameters (all can be overridden via --params):

        exit_mode:                  'breakout', 'momentum', or 'bar_hold'
        exit_zscore:                breakout profit-take z-score threshold
        stop_zscore:                stop-loss z-score threshold (breakout + momentum)
        trail_zscore:               momentum trailing exit: drop from peak z-score
        hold_bars:                  bar_hold primary exit; breakout/momentum max-hold failsafe

        use_direction_filter:       Enable direction regression gate (default False)
        direction_threshold:        Min P(up) to allow entry (default 0.60)
        direction_forward_bars:     Forward bars used to define the label (default 5)
        direction_train_fraction:   Fraction of bars used to train the model (default 0.50)
        direction_min_confidence:   Min confidence (abs(P-0.5)*2) required (default 0.20)
    """

    exit_mode: str = 'breakout'
    exit_zscore: float = 2.0
    stop_zscore: float = 2.0
    trail_zscore: float = 1.0
    hold_bars: int = 20

    # Direction filter params (opt-in — all strategies remain unchanged by default)
    use_direction_filter: bool = False
    direction_threshold: float = 0.60
    direction_forward_bars: int = 5
    direction_train_fraction: float = 0.50
    direction_min_confidence: float = 0.20

    # Runner inspects these flags to decide what extra data to fetch/merge.
    requires_popularity: bool = False
    requires_daily: bool = False

    def init(self):
        self._pending = False
        self._peak_zscore = 0.0

        # Direction filter state
        self._direction_model: DirectionModel | None = None
        self._direction_features: pd.DataFrame | None = None
        self._direction_train_cutoff: int = 0
        self._rejections: list[dict] = []

        if self.use_direction_filter:
            self._train_direction_model()

    def next(self):
        is_regular = self._is_regular_hours()

        if self.position:
            # During 5m strategies, don't exit outside regular hours
            if self._has_regular_hours_col() and not is_regular:
                return
            self._check_exit()
            return

        if self._detect_signal():
            if self._direction_allows_entry():
                if not self._has_regular_hours_col() or is_regular:
                    self.buy()
                    self._peak_zscore = 0.0
                else:
                    self._pending = True

        if self._pending and is_regular:
            self.buy()
            self._pending = False
            self._peak_zscore = 0.0

    def _detect_signal(self) -> bool:
        """Return True when the entry condition is met. Subclasses must override."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Direction filter
    # ------------------------------------------------------------------

    def _train_direction_model(self) -> None:
        """Fit the direction model on the training window."""
        try:
            from rocketstocks.core.analysis.direction_model import compute_forward_returns

            features = build_features_for_backtest(self.data.df)
            train_cutoff = max(
                1, int(len(self.data.df) * float(self.direction_train_fraction))
            )
            forward_ret = compute_forward_returns(
                self.data.df['Close'], forward_bars=int(self.direction_forward_bars)
            )
            model = DirectionModel(
                forward_bars=int(self.direction_forward_bars),
                min_training_samples=50,
            )
            success = model.fit(
                features.iloc[:train_cutoff],
                forward_ret.iloc[:train_cutoff],
            )
            if success:
                self._direction_model = model
                self._direction_features = features
                self._direction_train_cutoff = train_cutoff
                logger.debug(
                    f'{self.__class__.__name__}: direction model fitted on '
                    f'{train_cutoff} bars; test window starts at bar {train_cutoff}'
                )
            else:
                logger.warning(
                    f'{self.__class__.__name__}: direction model fit failed '
                    f'(insufficient data); direction filter disabled'
                )
        except Exception as exc:
            logger.warning(
                f'{self.__class__.__name__}: direction model init failed: {exc}; '
                f'direction filter disabled'
            )

    def _direction_allows_entry(self) -> bool:
        """Return True if the direction filter permits entry at the current bar.

        Returns True immediately if:
        - use_direction_filter is False
        - Model failed to fit (graceful degradation)
        - Current bar is within the training window

        Otherwise evaluates the model's prediction against thresholds.
        Rejected entries are recorded in self._rejections for post-hoc analysis.
        """
        if not self.use_direction_filter:
            return True

        if self._direction_model is None or not self._direction_model.is_fitted:
            return True

        current_bar_idx = len(self.data) - 1
        if current_bar_idx < self._direction_train_cutoff:
            return True

        # Look up the pre-computed feature row for this bar
        if self._direction_features is None:
            return True

        if current_bar_idx >= len(self._direction_features):
            return True

        feat_row = self._direction_features.iloc[current_bar_idx]
        if feat_row.isna().any():
            return True  # Can't evaluate — allow entry rather than block

        prediction = self._direction_model.predict(feat_row.to_dict())

        if (
            prediction.probability_up >= float(self.direction_threshold)
            and prediction.confidence >= float(self.direction_min_confidence)
        ):
            return True

        # Blocked — record rejection for accuracy analysis
        self._rejections.append({
            'bar_idx': current_bar_idx,
            'probability_up': prediction.probability_up,
            'confidence': prediction.confidence,
            'predicted_direction': prediction.predicted_direction,
        })
        logger.debug(
            f'{self.__class__.__name__}: direction filter blocked entry at bar '
            f'{current_bar_idx} (P(up)={prediction.probability_up:.3f}, '
            f'conf={prediction.confidence:.3f})'
        )
        return False

    # ------------------------------------------------------------------
    # Exit logic (unchanged from original)
    # ------------------------------------------------------------------

    def _get_return_stats(self) -> tuple[float, float]:
        """Return (mean_return, std_return) for z-score exit computation.

        For 5m strategies: reads pre-computed daily enrichment columns
        (Daily_Return_Mean, Daily_Return_Std) so z-scoring matches the bot.

        For daily strategies: computes from the most recent lookback window.
        """
        if 'Daily_Return_Mean' in self.data.df.columns:
            mean = self.data.df['Daily_Return_Mean'].iloc[-1]
            std = self.data.df['Daily_Return_Std'].iloc[-1]
            return float(mean), float(std)

        # Daily: compute from the current lookback window
        window_len = min(60, len(self.data))
        lookback = self.data.df.iloc[max(0, len(self.data) - window_len):len(self.data)]
        lookback_lower = prep_for_signals(lookback)
        return compute_return_stats(lookback_lower, period=20)

    def _check_exit(self) -> bool:
        """Evaluate exit conditions. Returns True if position was closed."""
        if not self.trades:
            return False

        entry_price = self.trades[-1].entry_price
        current_price = float(self.data.Close[-1])
        bars_held = len(self.data) - self.trades[-1].entry_bar

        if self.exit_mode == 'bar_hold':
            if bars_held >= self.hold_bars:
                self.position.close()
                return True
            return False

        # Z-score of price move since entry
        mean_ret, std_ret = self._get_return_stats()
        if math.isnan(std_ret) or std_ret <= 0:
            if bars_held >= self.hold_bars:
                self.position.close()
                return True
            return False

        confirmation = evaluate_confirmation(
            price_at_flag=entry_price,
            current_price=current_price,
            mean_return=mean_ret,
            std_return=std_ret,
            zscore_threshold=self.exit_zscore,
        )
        zscore = confirmation.zscore_since_flag

        if math.isnan(zscore):
            if bars_held >= self.hold_bars:
                self.position.close()
                return True
            return False

        if self.exit_mode == 'breakout':
            if zscore >= self.exit_zscore:
                self.position.close()
                return True
            if zscore <= -self.stop_zscore:
                self.position.close()
                return True
            if bars_held >= self.hold_bars:
                self.position.close()
                return True

        elif self.exit_mode == 'momentum':
            if zscore > self._peak_zscore:
                self._peak_zscore = zscore
            pct_from_entry = (current_price / entry_price - 1) * 100
            if (self._peak_zscore - zscore >= self.trail_zscore
                    and pct_from_entry > 0):
                self.position.close()
                return True
            if zscore <= -self.stop_zscore:
                self.position.close()
                return True
            if bars_held >= self.hold_bars:
                self.position.close()
                return True

        return False

    def _is_regular_hours(self) -> bool:
        if not self._has_regular_hours_col():
            return True
        return bool(self.data.df['Is_Regular_Hours'].iloc[-1])

    def _has_regular_hours_col(self) -> bool:
        return 'Is_Regular_Hours' in self.data.df.columns
