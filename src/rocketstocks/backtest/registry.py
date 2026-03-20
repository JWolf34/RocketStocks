"""Strategy discovery by name via the @register decorator."""
import logging

logger = logging.getLogger(__name__)

_REGISTRY: dict[str, type] = {}
_loaded = False


def register(name: str):
    """Class decorator that registers a backtesting.py Strategy by name.

    Usage::

        from rocketstocks.backtest.registry import register
        from backtesting import Strategy

        @register('my_strategy')
        class MyStrategy(Strategy):
            ...

    The name must be unique across all registered strategies.
    """
    def decorator(cls):
        from backtesting import Strategy
        if not issubclass(cls, Strategy):
            raise TypeError(f'{cls.__name__} must subclass backtesting.Strategy')
        if name in _REGISTRY:
            logger.warning(f"Strategy '{name}' already registered; overwriting with {cls.__name__}")
        _REGISTRY[name] = cls
        logger.debug(f"Registered strategy '{name}': {cls.__name__}")
        return cls
    return decorator


def get_strategy(name: str) -> type:
    """Look up a Strategy class by its registered name.

    Triggers lazy import of all built-in strategy modules on first call.

    Raises:
        KeyError: If name is not registered.
    """
    _ensure_loaded()
    if name not in _REGISTRY:
        available = ', '.join(sorted(_REGISTRY.keys()))
        raise KeyError(f"Unknown strategy '{name}'. Available: {available}")
    return _REGISTRY[name]


def list_strategies() -> list[str]:
    """Return a sorted list of all registered strategy names."""
    _ensure_loaded()
    return sorted(_REGISTRY.keys())


def _ensure_loaded() -> None:
    """Import built-in strategy modules so their @register decorators fire."""
    global _loaded
    if _loaded:
        return
    _loaded = True
    import rocketstocks.backtest.strategies.alert_signal      # noqa: F401
    import rocketstocks.backtest.strategies.composite_signal  # noqa: F401
    import rocketstocks.backtest.strategies.confluence        # noqa: F401
