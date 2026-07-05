# Testing Guide

## Running Tests

```bash
# Full suite
.venv/bin/pytest legacy/tests/ -q

# Single file
.venv/bin/pytest legacy/tests/bot/cogs/test_alerts_cog.py -v

# Single class
.venv/bin/pytest legacy/tests/bot/cogs/test_alerts_cog.py::TestEarningsPipeline -v
```

Configuration in `legacy/pyproject.toml`: `testpaths = ["tests"]`, `asyncio_mode = "auto"`.

## Directory Structure

Tests mirror the source tree:

```
legacy/tests/
├── conftest.py              # Root fixtures (mock_db, mock_stock_data, sample_df, env_override)
├── core/
│   ├── analysis/            # Pure logic — test with real DataFrames, no mocking
│   ├── charting/            # Chart rendering tests
│   ├── config/              # Environment/settings tests
│   ├── content/             # Content class build() tests
│   │   └── conftest.py      # Fixtures: quote_up, quote_down, ticker_info, price_history
│   ├── notifications/       # EventEmitter, event, config tests
│   ├── scheduler/           # Job registration tests
│   └── utils/               # Date, formatting, market hours tests
├── bot/
│   ├── cogs/                # Cog pipeline tests (heavy mocking)
│   ├── senders/             # alert_sender, report_sender, notification_sender
│   ├── views/               # Discord UI button tests
│   └── test_bot.py          # Bot setup_hook tests
└── data/
    ├── clients/             # API client tests (AsyncMock for HTTP)
    ├── test_db.py           # Postgres CRUD + cursor context manager
    ├── test_repositories.py # TickerRepository, PriceHistoryRepository
    └── test_*.py            # One file per repository (surge_store, ticker_stats, etc.)
```

## Root Fixtures (`legacy/tests/conftest.py`)

| Fixture | Purpose |
|---------|---------|
| `mock_db` | `MagicMock` Postgres with a context-managed cursor that yields a mock cursor |
| `mock_stock_data` | `MagicMock` StockData with all sub-objects (schwab, earnings, tickers, etc.) as mocks |
| `sample_df` | 60-row OHLCV DataFrame with deterministic random data (seed=42) |
| `env_override` | Patches `os.getenv` with safe test values — prevents `.env` leakage |
| `tmp_attachments` | `tmp_path`-based directory for file write tests |

## Testing by Layer

### Core (pure logic — no mocking needed)

Analysis modules (`core/analysis/`) are pure functions that take DataFrames and return results. Test them directly with synthetic data:

```python
# legacy/tests/core/analysis/test_alert_strategy.py
def _make_daily_prices(n=60, base=100.0, std_pct=2.0, seed=42) -> pd.DataFrame:
    # Build synthetic OHLCV data, call evaluate_price_alert() directly
```

Content class tests verify `build()` returns correct `EmbedSpec` fields. Use fixtures from `legacy/tests/core/content/conftest.py` (`quote_up`, `quote_down`, `ticker_info`, `price_history`).

### Bot Cogs (heavy mocking)

Cogs start task loops on `__init__`, so construction requires patching loops and `DiscordState` **before** instantiation:

```python
# legacy/tests/bot/cogs/test_alerts_cog.py:60-72
def _make_cog(earnings_df):
    bot = _make_bot()
    sd = _make_stock_data(earnings_df)
    with (
        patch.object(Alerts, "post_alerts_date"),
        patch.object(Alerts, "detect_popularity_surges"),
        patch.object(Alerts, "process_alerts"),
        patch("rocketstocks.bot.cogs.alerts.DiscordState"),
    ):
        cog = Alerts(bot=bot, stock_data=sd)
    cog.dstate = MagicMock()
    cog.mutils = MagicMock()
    return cog
```

**Critical rule:** Patch `DiscordState` at the **cog's import path** (e.g., `rocketstocks.bot.cogs.alerts.DiscordState`), not the source module. Otherwise, the real class is used and attempts a DB connection.

Each cog test file defines its own `_make_bot()`, `_make_stock_data()`, and `_make_cog()` helpers tailored to that cog's dependencies.

### Bot Senders

Sender tests mock three things: the alert/content object, the discord channel, and `DiscordState`:

```python
# legacy/tests/bot/senders/test_alert_sender.py:15-43
def _make_alert(ticker="AAPL", alert_type="VOLUME_MOVER"):
    alert = MagicMock()
    alert.build.return_value = EmbedSpec(...)
    return alert

def _make_channel(sent_id=777):
    channel = AsyncMock()
    sent_msg = AsyncMock()
    sent_msg.id = sent_id
    channel.send.return_value = sent_msg
    return channel, sent_msg

def _make_dstate(message_id=None, alert_data=None):
    dstate = MagicMock()
    dstate.get_alert_message_id = AsyncMock(return_value=message_id)
    return dstate
```

### Data Layer (repositories + DB)

Repository tests mock the `Postgres` instance and verify SQL/params:

```python
# legacy/tests/data/test_surge_store.py:41-58
def test_insert_surge_executes_sql(repo, mock_cursor):
    repo.insert_surge(ticker='GME', ...)
    mock_cursor.execute.assert_called_once()
    sql, params = mock_cursor.execute.call_args[0]
    assert 'INSERT INTO popularity_surges' in sql
    assert params == ['GME', ts, ...]
```

`test_db.py` tests the `Postgres` class itself by mocking `ThreadedConnectionPool` and configuring cursor `__enter__`/`__exit__` manually.

## Async Testing

Uses `pytest-asyncio` with `asyncio_mode = "auto"` — all `async def test_*` methods run automatically.

- **`AsyncMock`**: Use for all async functions (API clients, channel.send, dstate methods)
- **`MagicMock`**: Use for sync objects (DB, config, stock_data sub-objects)
- **`patch.object(obj, "method", new_callable=AsyncMock)`**: Patch instance methods that are async

## Common Patterns

### Verifying call arguments
```python
mock_build.assert_called_once()
_, call_kwargs = mock_build.call_args
assert call_kwargs.get("quote") == expected_quote
```

### Testing per-ticker isolation (one failure doesn't abort others)
```python
async def mock_build(ticker, **kwargs):
    if ticker == "AAPL":
        raise RuntimeError("build failed")
    built.append(ticker)
    return MagicMock()

with patch.object(cog, "build_earnings_mover", side_effect=mock_build):
    await cog._earnings_pipeline(...)
assert "MSFT" in built  # non-failing ticker still processed
```

### Validating parameterized SQL (no f-strings)
```python
sql_str, params = mock_cur.execute.call_args[0]
assert '%s' in sql_str
assert params == ('tickers',)
```

### Testing rollback on exception
```python
with pytest.raises(ValueError):
    with pg._cursor():
        raise ValueError("boom")
mock_conn.rollback.assert_called_once()
```
