"""Pure calculation functions for paper trading (no DB, no Discord)."""
import statistics

from rocketstocks.core.content.models import WeeklyAward


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


# ---------------------------------------------------------------------------
# Weekly award helper
# ---------------------------------------------------------------------------

def _make_award(name: str, description: str, winner_name: str | None, detail: str | None) -> WeeklyAward:
    return WeeklyAward(award_name=name, description=description, winner_name=winner_name, detail=detail)


# ---------------------------------------------------------------------------
# Individual award functions (all pure — no DB, no Discord)
#
# Common signatures:
#   snapshots_by_user: dict[int, list[dict]]   — user_id → snapshots sorted ASC by date
#                                                each dict: {snapshot_date, portfolio_value, cash, positions_value}
#   transactions:      list[dict]              — all guild txns this week
#                                                each dict: {user_id, ticker, side, shares, price, total, executed_at}
#   positions_by_user: dict[int, list[dict]]   — user_id → positions with 'ticker', 'shares',
#                                                'avg_cost_basis', 'market_value'
#   portfolio_values_by_user: dict[int, float] — user_id → total portfolio value (cash + positions)
#   price_history:     dict[str, list[dict]]   — ticker → daily rows [{date, open, high, low, close}]
#                                                sorted ASC by date
#   ticker_sectors:    dict[str, str]          — ticker → sector string
#   all_user_ids:      list[int]               — every user with a portfolio this guild
#   user_names:        dict[int, str]          — user_id → display name
# ---------------------------------------------------------------------------

def _biggest_gainer(snapshots_by_user: dict, user_names: dict) -> WeeklyAward:
    """Highest portfolio % gain for the week (Mon → Fri from snapshots)."""
    best_user, best_pct, best_detail = None, float('-inf'), None
    for user_id, snaps in snapshots_by_user.items():
        if len(snaps) < 2:
            continue
        start_val = snaps[0]['portfolio_value']
        end_val = snaps[-1]['portfolio_value']
        if start_val <= 0:
            continue
        pct = (end_val - start_val) / start_val * 100
        if pct > best_pct:
            best_pct = pct
            best_user = user_id
            sign = '+' if pct >= 0 else ''
            best_detail = f"{sign}{pct:.1f}% (${end_val:,.0f})"
    return _make_award(
        "Biggest Gainer", "Highest portfolio % gain for the week",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _biggest_loser(snapshots_by_user: dict, user_names: dict) -> WeeklyAward:
    """Largest portfolio % loss for the week."""
    worst_user, worst_pct, worst_detail = None, float('inf'), None
    for user_id, snaps in snapshots_by_user.items():
        if len(snaps) < 2:
            continue
        start_val = snaps[0]['portfolio_value']
        end_val = snaps[-1]['portfolio_value']
        if start_val <= 0:
            continue
        pct = (end_val - start_val) / start_val * 100
        if pct < worst_pct:
            worst_pct = pct
            worst_user = user_id
            worst_detail = f"{pct:.1f}% (${end_val:,.0f})"
    return _make_award(
        "Biggest Loser", "Largest portfolio % loss for the week",
        user_names.get(worst_user) if worst_user is not None else None, worst_detail,
    )


def _sniper(transactions: list, user_names: dict) -> WeeklyAward:
    """Best single completed round-trip (FIFO buy→sell, highest % gain)."""
    best_user, best_pct, best_detail = None, float('-inf'), None
    by_user: dict = {}
    for tx in transactions:
        by_user.setdefault(tx['user_id'], []).append(tx)

    for user_id, txs in by_user.items():
        by_ticker: dict = {}
        for tx in sorted(txs, key=lambda x: x['executed_at']):
            by_ticker.setdefault(tx['ticker'], []).append(tx)
        for ticker, ticker_txs in by_ticker.items():
            buy_queue: list = []
            for tx in ticker_txs:
                if tx['side'] == 'BUY':
                    buy_queue.append(tx)
                elif tx['side'] == 'SELL' and buy_queue:
                    buy_tx = buy_queue.pop(0)
                    if buy_tx['price'] <= 0:
                        continue
                    pct = (tx['price'] - buy_tx['price']) / buy_tx['price'] * 100
                    if pct > best_pct:
                        best_pct = pct
                        best_user = user_id
                        sign = '+' if pct >= 0 else ''
                        best_detail = (
                            f"**{ticker}** {sign}{pct:.1f}% "
                            f"(${buy_tx['price']:.2f} → ${tx['price']:.2f})"
                        )
    return _make_award(
        "Sniper", "Best single round-trip — highest % gain on a completed buy→sell",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _comeback_kid(snapshots_by_user: dict, user_names: dict) -> WeeklyAward:
    """Down the most mid-week then recovered the most by Friday."""
    best_user, best_recovery, best_detail = None, float('-inf'), None
    for user_id, snaps in snapshots_by_user.items():
        if len(snaps) < 3:
            continue
        start_val = snaps[0]['portfolio_value']
        end_val = snaps[-1]['portfolio_value']
        mid_vals = [s['portfolio_value'] for s in snaps[1:-1]]
        if not mid_vals:
            continue
        min_val = min(mid_vals)
        if min_val >= start_val or start_val <= 0 or min_val <= 0:
            continue
        recovery = end_val - min_val
        if recovery > best_recovery:
            best_recovery = recovery
            best_user = user_id
            pct_down = (min_val - start_val) / start_val * 100
            pct_up = (end_val - min_val) / min_val * 100
            best_detail = f"Down {pct_down:.1f}%, recovered +{pct_up:.1f}%"
    return _make_award(
        "Comeback Kid", "Down the most mid-week but recovered the most by Friday",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _diamond_hands(
    positions_by_user: dict, price_history: dict, user_names: dict
) -> WeeklyAward:
    """Held through 5%+ drawdown from cost basis during the week."""
    best_user, best_drawdown, best_detail = None, float('inf'), None
    for user_id, positions in positions_by_user.items():
        for pos in positions:
            ticker = pos['ticker']
            cost = pos.get('avg_cost_basis', 0)
            if cost <= 0 or ticker not in price_history:
                continue
            ph = price_history[ticker]
            if not ph:
                continue
            weekly_low = min(d.get('low', d.get('close', cost)) for d in ph)
            if weekly_low <= 0:
                continue
            drawdown_pct = (weekly_low - cost) / cost * 100  # negative = below cost
            if drawdown_pct <= -5.0 and drawdown_pct < best_drawdown:
                best_drawdown = drawdown_pct
                best_user = user_id
                best_detail = f"**{ticker}** drew down {drawdown_pct:.1f}% from cost basis"
    return _make_award(
        "Diamond Hands", "Held through a 5%+ drawdown from cost basis during the week",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _paper_hands(transactions: list, price_history: dict, user_names: dict) -> WeeklyAward:
    """Sold a position that rose 5%+ after the sale."""
    best_user, best_pct, best_detail = None, float('-inf'), None
    for tx in transactions:
        if tx['side'] != 'SELL':
            continue
        ticker = tx['ticker']
        sell_price = tx['price']
        if sell_price <= 0 or ticker not in price_history:
            continue
        ph = price_history[ticker]
        sell_date = tx['executed_at']
        sell_day = sell_date.date() if hasattr(sell_date, 'date') else sell_date
        after_closes = [
            d.get('close', 0) for d in ph
            if d.get('date') is not None and d['date'] > sell_day and d.get('close', 0) > 0
        ]
        if not after_closes:
            continue
        max_after = max(after_closes)
        pct_rise = (max_after - sell_price) / sell_price * 100
        if pct_rise >= 5.0 and pct_rise > best_pct:
            best_pct = pct_rise
            best_user = tx['user_id']
            best_detail = (
                f"**{ticker}** sold at ${sell_price:.2f}, "
                f"rose +{pct_rise:.1f}% after"
            )
    return _make_award(
        "Paper Hands", "Sold a position that rose 5%+ after the sale",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _yolo(
    positions_by_user: dict, portfolio_values_by_user: dict, user_names: dict
) -> WeeklyAward:
    """Highest single-ticker concentration (% of total portfolio value)."""
    best_user, best_pct, best_detail = None, float('-inf'), None
    for user_id, positions in positions_by_user.items():
        total = portfolio_values_by_user.get(user_id, 0)
        if total <= 0:
            continue
        for pos in positions:
            market_value = pos.get('market_value', 0)
            if market_value <= 0:
                continue
            allocation = market_value / total * 100
            if allocation > best_pct:
                best_pct = allocation
                best_user = user_id
                best_detail = f"**{pos['ticker']}** at {allocation:.1f}% of portfolio"
    return _make_award(
        "YOLO", "Highest single-ticker concentration in portfolio",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _scared_money(snapshots_by_user: dict, user_names: dict) -> WeeklyAward:
    """Lowest portfolio volatility (smallest std dev of daily returns)."""
    best_user, best_std, best_detail = None, float('inf'), None
    for user_id, snaps in snapshots_by_user.items():
        if len(snaps) < 3:
            continue
        daily_returns = []
        for i in range(1, len(snaps)):
            prev = snaps[i - 1]['portfolio_value']
            curr = snaps[i]['portfolio_value']
            if prev > 0:
                daily_returns.append((curr - prev) / prev * 100)
        if len(daily_returns) < 2:
            continue
        std = statistics.stdev(daily_returns)
        if std < best_std:
            best_std = std
            best_user = user_id
            best_detail = f"Daily swing std dev: {std:.2f}%"
    return _make_award(
        "Scared Money", "Lowest portfolio volatility (smallest daily swings)",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _trendsetter(transactions: list, user_names: dict) -> WeeklyAward:
    """First to buy a ticker that 3+ other users also bought this week."""
    # Count how many distinct users bought each ticker
    ticker_buyers: dict = {}
    first_buy: dict = {}  # ticker → (executed_at, user_id) of earliest buy
    for tx in sorted(transactions, key=lambda x: x['executed_at']):
        if tx['side'] != 'BUY':
            continue
        ticker = tx['ticker']
        user_id = tx['user_id']
        ticker_buyers.setdefault(ticker, set()).add(user_id)
        if ticker not in first_buy:
            first_buy[ticker] = (tx['executed_at'], user_id)

    # Tickers bought by 3+ distinct users (trendsetter + 2 others = 3 total)
    trend_tickers = [t for t, buyers in ticker_buyers.items() if len(buyers) >= 3]
    if not trend_tickers:
        return _make_award("Trendsetter", "First to buy a ticker that 3+ others also bought", None, None)

    # Among those, find who was first overall
    earliest_time, winner_user, winner_ticker = None, None, None
    for ticker in trend_tickers:
        ts, uid = first_buy[ticker]
        if earliest_time is None or ts < earliest_time:
            earliest_time = ts
            winner_user = uid
            winner_ticker = ticker

    buyer_count = len(ticker_buyers.get(winner_ticker, set()))
    best_detail = f"**{winner_ticker}** — {buyer_count} traders followed"
    return _make_award(
        "Trendsetter", "First to buy a ticker that 3+ others also bought",
        user_names.get(winner_user) if winner_user is not None else None, best_detail,
    )


def _contrarian(transactions: list, user_names: dict) -> WeeklyAward:
    """Went against the server majority most often (sold what majority bought, or bought what majority sold)."""
    # Compute server-wide net sentiment per ticker: positive = net buyers, negative = net sellers
    ticker_net: dict = {}
    for tx in transactions:
        ticker = tx['ticker']
        ticker_net[ticker] = ticker_net.get(ticker, 0) + (1 if tx['side'] == 'BUY' else -1)

    # Count contrarian trades per user
    user_contrarian: dict = {}
    for tx in transactions:
        ticker = tx['ticker']
        net = ticker_net.get(ticker, 0)
        is_contrarian = (tx['side'] == 'SELL' and net > 0) or (tx['side'] == 'BUY' and net < 0)
        if is_contrarian:
            user_contrarian[tx['user_id']] = user_contrarian.get(tx['user_id'], 0) + 1

    if not user_contrarian:
        return _make_award("Contrarian", "Went against the server majority most often", None, None)

    best_user = max(user_contrarian, key=lambda u: user_contrarian[u])
    count = user_contrarian[best_user]
    best_detail = f"{count} contrarian trade{'s' if count != 1 else ''} this week"
    return _make_award(
        "Contrarian", "Bought what everyone sold (or sold what everyone bought)",
        user_names.get(best_user), best_detail,
    )


def _bought_the_dip(transactions: list, price_history: dict, user_names: dict) -> WeeklyAward:
    """Bought closest to a stock's weekly low price."""
    best_user, best_ratio, best_detail = None, float('inf'), None
    for tx in transactions:
        if tx['side'] != 'BUY':
            continue
        ticker = tx['ticker']
        buy_price = tx['price']
        if buy_price <= 0 or ticker not in price_history:
            continue
        ph = price_history[ticker]
        if not ph:
            continue
        weekly_low = min(d.get('low', d.get('close', buy_price)) for d in ph)
        if weekly_low <= 0:
            continue
        ratio = buy_price / weekly_low  # 1.0 = perfect dip buy
        if ratio < best_ratio:
            best_ratio = ratio
            best_user = tx['user_id']
            best_detail = f"**{ticker}** bought at ${buy_price:.2f} (weekly low ${weekly_low:.2f})"
    return _make_award(
        "Bought the Dip", "Bought closest to a stock's weekly low",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _bought_the_top(transactions: list, price_history: dict, user_names: dict) -> WeeklyAward:
    """Bought closest to a stock's weekly high price."""
    best_user, best_ratio, best_detail = None, float('-inf'), None
    for tx in transactions:
        if tx['side'] != 'BUY':
            continue
        ticker = tx['ticker']
        buy_price = tx['price']
        if buy_price <= 0 or ticker not in price_history:
            continue
        ph = price_history[ticker]
        if not ph:
            continue
        weekly_high = max(d.get('high', d.get('close', buy_price)) for d in ph)
        if weekly_high <= 0:
            continue
        ratio = buy_price / weekly_high  # 1.0 = perfect top buy
        if ratio > best_ratio:
            best_ratio = ratio
            best_user = tx['user_id']
            best_detail = f"**{ticker}** bought at ${buy_price:.2f} (weekly high ${weekly_high:.2f})"
    return _make_award(
        "Bought the Top", "Bought closest to a stock's weekly high",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _diversification(
    positions_by_user: dict, ticker_sectors: dict, user_names: dict
) -> WeeklyAward:
    """Most unique sectors represented in portfolio."""
    best_user, best_count, best_detail = None, -1, None
    for user_id, positions in positions_by_user.items():
        sectors = {
            ticker_sectors[pos['ticker']]
            for pos in positions
            if pos['ticker'] in ticker_sectors and ticker_sectors[pos['ticker']]
        }
        if len(sectors) > best_count:
            best_count = len(sectors)
            best_user = user_id
            best_detail = f"{best_count} sector{'s' if best_count != 1 else ''}"
    if best_count <= 0:
        return _make_award("Diversification Award", "Most unique sectors in portfolio", None, None)
    return _make_award(
        "Diversification Award", "Most unique sectors represented in portfolio",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _day_trader(transactions: list, user_names: dict) -> WeeklyAward:
    """Shortest average hold time (buy→sell interval) for the week."""
    by_user: dict = {}
    for tx in transactions:
        by_user.setdefault(tx['user_id'], []).append(tx)

    best_user, best_avg_seconds, best_detail = None, float('inf'), None
    for user_id, txs in by_user.items():
        by_ticker: dict = {}
        for tx in sorted(txs, key=lambda x: x['executed_at']):
            by_ticker.setdefault(tx['ticker'], []).append(tx)

        hold_times: list = []
        for ticker_txs in by_ticker.values():
            buy_queue: list = []
            for tx in ticker_txs:
                if tx['side'] == 'BUY':
                    buy_queue.append(tx['executed_at'])
                elif tx['side'] == 'SELL' and buy_queue:
                    buy_time = buy_queue.pop(0)
                    delta = (tx['executed_at'] - buy_time).total_seconds()
                    if delta >= 0:
                        hold_times.append(delta)

        if not hold_times:
            continue
        avg_seconds = sum(hold_times) / len(hold_times)
        if avg_seconds < best_avg_seconds:
            best_avg_seconds = avg_seconds
            best_user = user_id
            hours = best_avg_seconds / 3600
            if hours < 1:
                best_detail = f"Avg hold {best_avg_seconds / 60:.0f}m"
            else:
                best_detail = f"Avg hold {hours:.1f}h"
    return _make_award(
        "Day Trader", "Shortest average hold time for the week",
        user_names.get(best_user) if best_user is not None else None, best_detail,
    )


def _ghost_trader(transactions: list, all_user_ids: list, user_names: dict) -> WeeklyAward:
    """Zero trades made this week."""
    active_users = {tx['user_id'] for tx in transactions}
    inactive = [uid for uid in all_user_ids if uid not in active_users]
    if not inactive:
        return _make_award("Ghost Trader", "Didn't make a single trade all week", None, None)
    # Pick the first alphabetically by display name
    inactive.sort(key=lambda uid: user_names.get(uid, ''))
    winner = inactive[0]
    return _make_award(
        "Ghost Trader", "Didn't make a single trade all week",
        user_names.get(winner), "0 trades",
    )


# ---------------------------------------------------------------------------
# Top-level award evaluator
# ---------------------------------------------------------------------------

def evaluate_weekly_awards(
    snapshots_by_user: dict,
    transactions: list,
    positions_by_user: dict,
    portfolio_values_by_user: dict,
    price_history: dict,
    ticker_sectors: dict,
    all_user_ids: list,
    user_names: dict,
) -> list:
    """Evaluate all 15 weekly awards and return a list of WeeklyAward objects.

    Args:
        snapshots_by_user:        user_id → list[dict] sorted ASC by date
        transactions:             all guild transactions this week
        positions_by_user:        user_id → list[dict] with ticker/shares/avg_cost_basis/market_value
        portfolio_values_by_user: user_id → total portfolio value (cash + positions)
        price_history:            ticker → list[dict] with date/open/high/low/close, sorted ASC
        ticker_sectors:           ticker → sector string
        all_user_ids:             every user_id with a portfolio in this guild
        user_names:               user_id → display name string
    """
    return [
        _biggest_gainer(snapshots_by_user, user_names),
        _biggest_loser(snapshots_by_user, user_names),
        _sniper(transactions, user_names),
        _comeback_kid(snapshots_by_user, user_names),
        _diamond_hands(positions_by_user, price_history, user_names),
        _paper_hands(transactions, price_history, user_names),
        _yolo(positions_by_user, portfolio_values_by_user, user_names),
        _scared_money(snapshots_by_user, user_names),
        _trendsetter(transactions, user_names),
        _contrarian(transactions, user_names),
        _bought_the_dip(transactions, price_history, user_names),
        _bought_the_top(transactions, price_history, user_names),
        _diversification(positions_by_user, ticker_sectors, user_names),
        _day_trader(transactions, user_names),
        _ghost_trader(transactions, all_user_ids, user_names),
    ]
