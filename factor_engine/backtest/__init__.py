"""
回测模块

基于 FactorMatrix 的回测引擎
"""
from factor_engine.backtest.backtest_engine import (
    BacktestEngine,
    BacktestConfig,
    BacktestResult
)

__all__ = [
    'BacktestEngine',
    'BacktestConfig',
    'BacktestResult'
]
