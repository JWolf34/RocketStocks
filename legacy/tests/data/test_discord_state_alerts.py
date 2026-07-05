"""Tests for new get_alerts_by_type_today method in DiscordState."""
import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock

from rocketstocks.data.discord_state import DiscordState


@pytest.mark.asyncio
async def test_get_alerts_by_type_today_empty():
    """Test get_alerts_by_type_today when no alerts exist."""
    db = AsyncMock()
    db.execute = AsyncMock(return_value=[])
    dstate = DiscordState(db=db)
    
    result = await dstate.get_alerts_by_type_today('EARNINGS_MOVER')
    
    assert result == []
    db.execute.assert_called_once()
    call_args = db.execute.call_args
    assert call_args[0][1] == ['EARNINGS_MOVER', datetime.date.today()]


@pytest.mark.asyncio
async def test_get_alerts_by_type_today_multiple_tickers():
    """Test get_alerts_by_type_today returns list of tickers."""
    db = AsyncMock()
    db.execute = AsyncMock(return_value=[
        ('AAPL',),
        ('MSFT',),
        ('TSLA',),
    ])
    dstate = DiscordState(db=db)
    
    result = await dstate.get_alerts_by_type_today('EARNINGS_MOVER')
    
    assert result == ['AAPL', 'MSFT', 'TSLA']
    db.execute.assert_called_once()
    call_args = db.execute.call_args
    assert 'EARNINGS_MOVER' in str(call_args)
    assert 'DISTINCT ticker' in str(call_args[0][0])


@pytest.mark.asyncio
async def test_get_alerts_by_type_today_different_alert_types():
    """Test get_alerts_by_type_today filters by alert type correctly."""
    db = AsyncMock()
    db.execute = AsyncMock(return_value=[('NVDA',)])
    dstate = DiscordState(db=db)
    
    # Test with different alert types
    for alert_type in ['EARNINGS_MOVER', 'WATCHLIST_MOVER', 'POPULARITY_SURGE']:
        await dstate.get_alerts_by_type_today(alert_type)
        call_args = db.execute.call_args
        assert alert_type in str(call_args)
