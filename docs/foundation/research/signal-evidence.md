# Research: signal evidence study (issue #45)

Research date: 2026-10-19.
Data: a local copy of v1 production data (daily bars 2000-2026-07-02, ApeWisdom popularity 2025-06 to 2026-10, 4,165 posted alerts 2026-04-15 to 2026-07-05).
Only aggregates are in this file.
No vendor price rows are committed.

## Answer

No v1 alert type shows a measurable edge on the history RocketStocks already has, so none can be justified as a trade signal on this evidence.
In the posted alerts (about 2.5 months, one regime) no signal has a confidence interval that excludes zero on the positive side, and popularity surge, earnings mover and momentum confirmation drifted negative.
In 21 years of reconstructed daily-bar events, volume accumulation, breakout and plain large-move days show no stable edge (breakout flips sign between sub-periods).
The only pattern that passes the bar is continuation after an earnings-day move, at a median of +0.17% excess over 10 days with the same sign in all three sub-periods, which is real but too small to carry a push alert by itself.
Options unusual volume cannot be evaluated: the copy holds about 24 trading days of options history.
What to collect first: resume the daily options-chain snapshot with open interest and bid/ask, and log every rule firing (with alert-time price) so forward returns can be measured from the true entry.

## Method

Excess return is the ticker's return minus SPY's over the same window, at 1, 5 and 10 trading days, in percent.
Hit rate is the share of events with excess above zero.
All headline figures are medians, because the means are contaminated by bad prints and split artifacts in the vendor bars (for example a 1-day mean of +2.0% against a median of -0.07% in one large-move cell).
Confidence intervals are 95% cluster bootstraps of the median (clusters are alert dates for posted alerts and calendar months for the reconstruction), so same-day market moves do not inflate confidence.
Events are deduplicated to one per ticker per 10 days (posted) or 14 days (reconstruction).

A plain excess-versus-SPY comparison is not enough here, because the median stock underperforms SPY.
In the Apr-Jun 2026 window a random liquid ticker-day beat SPY only 47.3%, 46.0% and 45.0% of the time at 1, 5 and 10 days, so a hit rate near 47% is not evidence of anything.
So the table also shows "adj", the event's excess minus the same-date median excess of all liquid stocks (close above $5, 20-day median dollar volume above $5M), with its own cluster-bootstrap interval.
Where a signal has a natural direction (movers, breakout, momentum confirmation), the table scores continuation, meaning the return is multiplied by the sign of the move.
Volume accumulation and popularity surge are scored long, as v1 frames them.

Posted alerts use the alert timestamp to pick the entry, because daily bars have no alert-time price.
Alerts during the session enter at that day's close (conservative, it skips the rest of the day).
Alerts before 09:30 ET or on a non-trading day enter at the next open, and alerts after 16:00 ET enter at the next session's open.
This matters a great deal.
A first pass that entered every alert at the alert-date close showed earnings movers beating SPY by about +2.3% on day 1 with a 64% hit rate, which was entirely the after-hours and pre-market reaction already visible when the alert fired, not an edge.

The reconstruction reimplements v1's daily-bar definitions over the liquid universe from 2005 to mid-2026, using only data up to the event bar for every statistic:

- Volume accumulation: volume z-score at least 2.5 and absolute price z-score below 1 against the prior 20 days, and divergence (volume z minus absolute price z) at least 1.5, evaluated on the full-day bar.
- Breakout proxy: a volume-accumulation flag followed next session by a move of at least 1.5 standard deviations from the flag close, entering at that next close.
- Large move: absolute price z-score above v1's volatility-scaled threshold (3.0 down to 1.5), a stand-in for the watchlist and earnings mover trigger.
- Earnings mover: the larger-move day of the earnings date and the next day, if it passes the same threshold.
- Popularity surge: v1's mention-surge and rank-jump rules (minimum 15 mentions, tiered thresholds) over 2025-06 to 2026-06, entering at the close of the first session after the event date because the snapshot timezone is unclear.
  The acceleration gate and velocity-spike rule are not reproduced.

## Results

Cell format is median excess percent (hit rate).
"adj 10d" is the 10-day excess over the same-date liquid-universe median, with its 95% interval.
Posted rows use events deduplicated per ticker, so 10-day N is a few percent lower from truncation at 2026-07-02.

| Signal | Source | N | 1d | 5d | 10d | adj 10d [95% CI] | Verdict |
|---|---|---|---|---|---|---|---|
| Volume accumulation (long) | Posted alerts | 769 | -0.10 (49) | -0.63 (47) | -1.75 (43) | -0.56 [-1.36, +0.42] | No evidence |
| Breakout (continuation) | Posted alerts | 196 | -0.14 (48) | +1.76 (55) | -0.02 (50) | -0.13 [-1.11, +1.24] | No evidence, N small |
| Momentum confirmation (continuation) | Posted alerts | 401 | -0.10 (49) | +0.52 (51) | +0.67 (52) | +0.31 [-1.30, +1.13] | No evidence |
| Popularity surge (long) | Posted alerts | 610 | -0.39 (44) | -2.25 (39) | -2.67 (41) | -1.44 [-2.61, -0.21] | No bullish edge, negative drift |
| Watchlist mover (continuation) | Posted alerts | 147 | +0.41 (55) | -0.78 (44) | +1.40 (55) | +0.79 [-1.10, +2.78] | No evidence, 54 tickers |
| Earnings mover (continuation) | Posted alerts | 878 | -0.48 (47) | -1.02 (46) | -1.90 (43) | -0.92 [-1.72, -0.25] | No continuation edge |
| Volume accumulation, no options gate (long) | Daily bars 2005-2026 | 136,321 | -0.04 (49.0) | -0.10 (48.8) | -0.19 (48.3) | +0.02 [-0.01, +0.06] | No evidence |
| Breakout proxy (continuation) | Daily bars 2005-2026 | 38,338 | +0.02 (50.5) | +0.04 (50.4) | +0.04 (50.3) | +0.03 [-0.07, +0.14] | No edge, sign flips by period |
| Large-move day (continuation) | Daily bars 2005-2026 | 223,096 | 0.00 (49.9) | +0.03 (50.3) | +0.06 (50.4) | 0.00 [-0.06, +0.05] | No evidence |
| Earnings mover (continuation) | Daily bars 2005-2026 | 47,348 | +0.04 (50.6) | +0.09 (50.8) | +0.17 (51.3) | +0.17 [+0.09, +0.26] | Passes, economically tiny |
| Popularity surge (long) | Popularity + bars, 2025-06 to 2026-06 | 1,422 | -0.20 (47) | -0.91 (45) | -0.66 (46) | -0.07 [-0.95, +0.60] | No bullish edge |
| Options unusual volume | Options history | - | - | - | - | - | Not evaluable, about 24 sessions |

## Reading the results

Posted alerts show nothing positive.
The strongest-looking cell, breakout at 5 days (+1.76%, 55% hit), has an interval from -1.03 to +2.40 on 189 events and reverses by day 10.
Scored long-only (not shown in the table), popularity surge, momentum confirmation and earnings mover alerts underperformed SPY at 5 and 10 days with intervals that exclude zero, but these alerted names skew to small, high-volatility stocks that lagged in this window, so the negative figure is partly a universe effect.
The adjusted figure for popularity surge stays negative (-2.07 at 5 days, interval -3.09 to -0.68), and the independent reconstruction agrees in sign at 5 days (adj -0.60, interval -1.17 to -0.07, negative in both half-years) but is much smaller, so the honest summary is "no bullish edge, and possibly weak underperformance", not "short these".
The 10-day reconstruction is flat (adj -0.07).
v1's reported "confirm rate" for surges (62% for rank jumps) is a statement that price moved 1.5 standard deviations in either direction after the flag, so it is not an edge measure.

Of the daily-bar reconstructions, volume accumulation (adj 10d +0.04, +0.03, 0.00 across 2005-12, 2013-19 and 2020-26) and large-move continuation (+0.03, +0.04, -0.05) are indistinguishable from zero.
The breakout proxy looks good in the first two periods (+0.17 and +0.15 at 10 days, intervals above zero) and then reverses in 2020-26 (-0.14), which fails the sign-consistency rule.
Earnings continuation is positive in all three periods (+0.14, +0.16, +0.19 at 10 days, two of three intervals above zero, pooled interval +0.09 to +0.26), consistent with the post-earnings drift literature.
Its size is a fifth of a percent over two weeks on liquid names, with hit rates of 51%, which is well inside typical costs for smaller stocks.
The posted earnings movers in 2026 point the other way (adj 10d -0.92), which I attribute to universe and a single regime, but it is a reason not to trust either number alone.

## Caveats

- Survivorship is severe in the early years.
  Of the 1,388 liquid names in 2008, 94.5% still trade in 2026, and only 69 of 10,654 tickers end before 2025, so delisted losers are mostly absent.
  Liquid names per day grow from about 830 in 2005 to 2,600 in 2025.
  The adjusted column nets out the common survivor drift, but a survivor-only universe can still flatter any signal that selects volatile names.
- The vendor daily history had phantom bars: weekend and holiday dates (about 146,000 rows, concentrated in 2025-26 and 2006-07) that duplicate the next trading day.
  I dropped them before every statistic, since they distort horizons and rolling windows.
  The 5-minute table also has epoch-zero rows.
  These are v1 ingestion bugs v2 must not inherit.
- Posted-alert results cover 2026-04-16 to 2026-07-02 only, one regime, with forward windows cut off at the end of price collection.
  Alert intervals are wide, and watchlist mover (54 tickers) and breakout (196 events) cannot detect anything below about 2%.
- The reconstruction is a proxy.
  v1 fires intraday on partial-day volume, and only 59% of posted volume-accumulation alerts satisfy the end-of-day criteria.
  Posted volume accumulation also required an unusual-options hard gate (all 866 posted alerts are tagged volume plus options), which daily bars cannot reproduce, and it still shows no edge.
- Look-ahead is controlled: all z-scores use the prior 20 sessions, entry is at or after the event bar, and the earnings reaction day is chosen by that day's own move.
  Popularity entries are delayed one session because the snapshot timezone is not documented.
- Multiple testing: roughly 200 cells were examined at 95% confidence, so several intervals excluding zero are expected by chance.
  The earnings result survives because it is consistent across 21 years and three periods, not because of one interval.
- Prices are vendor-adjusted, so the $5 and $5M filters are applied on adjusted values in early years, and transaction costs and borrow are ignored.
- The adjusted benchmark uses the liquid universe, while alerts also fire on sub-$5 names, so alert adjustments are approximate.

## What to collect first

1. Restart the daily options-chain snapshot (it stopped 2026-07-02) and add open interest, bid, ask, implied volatility and next-morning open interest for every contract.
   As a back-of-envelope, detecting a 0.3% median effect against roughly 5% per-event dispersion needs about 1,000 independent events, and a split into two halves for stability needs more, so plan for about 250 sessions before judging.
2. Log every rule firing, including near-misses and a random control sample, with feature values, timestamp and alert-time price.
   Without an alert-time price, any posted-alert study must guess the entry, and the guess changed the earnings result from +2.3% to nothing.
3. Resume daily and 5-minute price collection, remove weekend and holiday duplicates at ingest, keep delisted tickers and corporate actions, and fix the epoch-zero rows.
4. Keep the ApeWisdom snapshots running and document their timezone.
5. Store earnings timing (before open or after close) and surprise next to each earnings event, which would sharpen the one signal with historical support.
