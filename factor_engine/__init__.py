"""
因子引擎模块

提供因子分析的完整框架：
- BacktestEngine: 回测引擎（新版）
- BacktestConfig: 回测配置
- BacktestResult: 回测结果
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
