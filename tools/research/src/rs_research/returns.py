"""Event-study maths: forward excess returns versus SPY and their summary.

Definitions, shared by every study and quoted by the bot's label:

- A horizon of `h` is `h` benchmark (SPY) sessions after the reference
  session; the SPY session list is the trading calendar.
- Return is `close(exit) / entry_price - 1`; excess is the symbol's return minus
  SPY's return from the reference session close to the exit close. A horizon
  with no bar for the symbol on the exit session, or past the last stored
  session, has no outcome for that event.
- Hit rate is the share of events with strictly positive excess return.
  Medians are of the excess return, as a fraction (0.01 = one percentage point).
- N counts events with an entry price. Each horizon's hit rate and median use
  the events that have reached it, so a horizon's sample can be smaller than N.
- Events without an entry price are `n_unpriced`, never silently dropped.
  Horizons that matured but have no exit bar are counted in `n_missing_exit`.

Everything is vectorised and a pure function of its inputs.
"""

from dataclasses import dataclass
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

HORIZONS = (1, 5, 10)

SESSION_ZONE = ZoneInfo("America/New_York")
# Early-close days (13:00) are not modelled: an alert in their last three hours
# is referenced to the previous close instead of that day's.
MARKET_CLOSE = time(16, 0)
# The longest run of calendar days with no session (Friday to Tuesday around a
# Monday holiday, plus one for margin): an event this far before the first
# stored SPY session cannot be placed on it.
MAX_CLOSURE_DAYS = 5


@dataclass(frozen=True)
class Summary:
    n: int
    n_unpriced: int
    # Matured (event, horizon) outcomes with no exit bar, e.g. a name delisted mid-window.
    n_missing_exit: int
    hit_rate: dict[int, float | None]
    median_excess: dict[int, float | None]


def session_date(instant: datetime) -> date:
    """The exchange (Eastern) calendar date of an instant."""
    return instant.astimezone(SESSION_ZONE).date()


def first_session_after(sessions: np.ndarray, instants: list[datetime]) -> np.ndarray:
    """Index of the first session strictly after each instant's session date.
    `sessions` is sorted date ordinals; an index equal to len(sessions) means
    that session has not happened yet."""
    ordinals = np.array([session_date(t).toordinal() for t in instants], dtype=np.int64)
    return np.searchsorted(sessions, ordinals, side="right")


def last_close_before(sessions: np.ndarray, instants: list[datetime]) -> np.ndarray:
    """Index of the last session whose close is at or before each instant, or -1
    when the stored sessions start later."""
    ordinals = np.empty(len(instants), dtype=np.int64)
    for i, instant in enumerate(instants):
        local = instant.astimezone(SESSION_ZONE)
        ordinals[i] = local.date().toordinal() - (0 if local.time() >= MARKET_CLOSE else 1)
    return np.searchsorted(sessions, ordinals, side="right") - 1


def excess_returns(
    symbols: list[str],
    ref_idx: np.ndarray,
    entry_price: np.ndarray,
    sessions: np.ndarray,
    spy_close: np.ndarray,
    closes: pd.Series,
) -> pd.DataFrame:
    """Excess return per event (rows) and horizon (columns), NaN where there is
    no outcome. `ref_idx` indexes `sessions`/`spy_close` (the reference
    session); `closes` is a symbol close Series indexed by (symbol, date ordinal)."""
    out: dict[int, np.ndarray] = {}
    spy_ref = spy_close[ref_idx]
    for horizon in HORIZONS:
        exit_idx = ref_idx + horizon
        reached = exit_idx < len(sessions)
        safe = np.where(reached, exit_idx, 0)
        target = pd.MultiIndex.from_arrays([symbols, sessions[safe]])
        exit_price = closes.reindex(target).to_numpy(dtype=float)
        with np.errstate(divide="ignore", invalid="ignore"):
            excess = (exit_price / entry_price - 1.0) - (spy_close[safe] / spy_ref - 1.0)
        out[horizon] = np.where(reached & np.isfinite(excess), excess, np.nan)
    return pd.DataFrame(out)


def summarize(excess: pd.DataFrame, n_unpriced: int, n_missing_exit: int) -> Summary:
    hit_rate: dict[int, float | None] = {}
    median: dict[int, float | None] = {}
    for horizon in HORIZONS:
        values = excess[horizon].dropna()
        hit_rate[horizon] = float((values > 0).mean()) if len(values) else None
        median[horizon] = float(values.median()) if len(values) else None
    return Summary(
        n=len(excess),
        n_unpriced=n_unpriced,
        n_missing_exit=n_missing_exit,
        hit_rate=hit_rate,
        median_excess=median,
    )


def measure(
    symbols: list[str],
    ref_idx: np.ndarray,
    entry_price: np.ndarray,
    sessions: np.ndarray,
    spy_close: np.ndarray,
    closes: pd.Series,
) -> Summary:
    """Summarise events by symbol, reference session index and entry price (NaN
    when the event cannot be entered). Unpriced events are counted, then
    excluded from every statistic."""
    priced = np.isfinite(entry_price) & (entry_price > 0) & (ref_idx >= 0)
    kept = [s for s, ok in zip(symbols, priced, strict=True) if ok]
    excess = excess_returns(
        kept, ref_idx[priced], entry_price[priced], sessions, spy_close, closes
    )
    ref = ref_idx[priced]
    missing = sum(
        int(((ref + horizon < len(sessions)) & excess[horizon].isna().to_numpy()).sum())
        for horizon in HORIZONS
    )
    return summarize(excess, n_unpriced=int((~priced).sum()), n_missing_exit=missing)

