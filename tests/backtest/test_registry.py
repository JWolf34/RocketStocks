"""Tests for rocketstocks.backtest.registry."""
import pytest
from backtesting import Strategy

from rocketstocks.backtest import registry as reg_module
from rocketstocks.backtest.registry import get_strategy, list_strategies, register


@pytest.fixture(autouse=True)
def reset_registry():
    """Reset the registry state between tests."""
    original = dict(reg_module._REGISTRY)
    original_loaded = reg_module._loaded
    yield
    reg_module._REGISTRY.clear()
    reg_module._REGISTRY.update(original)
    reg_module._loaded = original_loaded


# ---------------------------------------------------------------------------
# @register decorator
# ---------------------------------------------------------------------------

def test_register_adds_to_registry():
    @register('test_strategy_a')
    class MyStrategy(Strategy):
        def init(self): pass
        def next(self): pass

    assert reg_module._REGISTRY['test_strategy_a'] is MyStrategy


def test_register_returns_class_unchanged():
    @register('test_strategy_b')
    class MyStrategy(Strategy):
        my_attr = 42
        def init(self): pass
        def next(self): pass

    assert MyStrategy.my_attr == 42


def test_register_non_strategy_raises_typeerror():
    with pytest.raises(TypeError, match='must subclass'):
        @register('bad_strategy')
        class NotAStrategy:
            pass


# ---------------------------------------------------------------------------
# get_strategy
# ---------------------------------------------------------------------------

def test_get_strategy_returns_registered_class():
    @register('test_get_strat')
    class S(Strategy):
        def init(self): pass
        def next(self): pass

    reg_module._loaded = True  # prevent re-import during test
    assert get_strategy('test_get_strat') is S


def test_get_strategy_unknown_raises_keyerror():
    reg_module._loaded = True
    with pytest.raises(KeyError, match="Unknown strategy 'no_such_strat'"):
        get_strategy('no_such_strat')


def test_get_strategy_error_message_lists_available():
    @register('available_strategy')
    class S(Strategy):
        def init(self): pass
        def next(self): pass

    reg_module._loaded = True
    with pytest.raises(KeyError) as exc_info:
        get_strategy('missing_one')
    assert 'available_strategy' in str(exc_info.value)


# ---------------------------------------------------------------------------
# list_strategies
# ---------------------------------------------------------------------------

def test_list_strategies_returns_sorted_list():
    @register('zzz_last')
    class Z(Strategy):
        def init(self): pass
        def next(self): pass

    @register('aaa_first')
    class A(Strategy):
        def init(self): pass
        def next(self): pass

    reg_module._loaded = True
    strategies = list_strategies()
    assert strategies == sorted(strategies)


def test_list_strategies_includes_builtins():
    # Trigger lazy load
    reg_module._loaded = False
    strategies = list_strategies()
    assert 'alert_signal' in strategies
    assert 'composite_signal' in strategies
    assert 'confluence' in strategies
