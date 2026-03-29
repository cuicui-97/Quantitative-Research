"""
因子引擎模块

提供单因子分析的完整框架，包括：
- 因子定义和计算
- 分组回测
- 收益计算（等权/市值加权）
- 统计分析和可视化
"""

from .backtest.single_factor_analyzer import SingleFactorAnalyzer
from .backtest.grouping import FactorGrouper
from .backtest.weighting import WeightCalculator
from .backtest.metrics import PerformanceMetrics

__all__ = [
    'SingleFactorAnalyzer',
    'FactorGrouper',
    'WeightCalculator',
    'PerformanceMetrics',
]
