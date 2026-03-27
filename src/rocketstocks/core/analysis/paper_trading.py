"""Pure calculation functions for paper trading (no DB, no Discord)."""


_STARTING_CAPITAL = 10000.0


def calculate_position_value(shares: int, current_price: float) -> float:
    """Return the current market value of a position."""
    return shares * current_price


def calculate_gain_loss(
    shares: int, avg_cost_basis: float, current_price: float
) -> tuple[float, float]:
    """Return (gain_loss_dollars, gain_loss_pct) for a position."""
    cost = shares * avg_cost_basis
    value = shares * current_price
    gain_loss = value - cost
    gain_loss_pct = (gain_loss / cost * 100) if cost != 0 else 0.0
    return gain_loss, gain_loss_pct


def calculate_portfolio_total(cash: float, positions_value: float) -> float:
    """Return total portfolio value (cash + positions)."""
    return cash + positions_value


def calculate_total_gain_loss(
    total_value: float, starting_capital: float = _STARTING_CAPITAL
) -> tuple[float, float]:
    """Return (gain_loss_dollars, gain_loss_pct) vs starting capital."""
    gain_loss = total_value - starting_capital
    gain_loss_pct = (gain_loss / starting_capital * 100) if starting_capital != 0 else 0.0
    return gain_loss, gain_loss_pct


def calculate_new_avg_cost_basis(
    existing_shares: int,
    existing_avg: float,
    new_shares: int,
    new_price: float,
) -> float:
    """Return the weighted average cost basis after buying more shares."""
    total_shares = existing_shares + new_shares
    if total_shares == 0:
        return 0.0
    return (existing_shares * existing_avg + new_shares * new_price) / total_shares
