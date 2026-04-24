#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
统一因子分析框架

支持全市场、微盘股等多种股票池的因子分析
高内聚低耦合设计：股票池筛选作为可配置组件
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Callable, Union, List
import logging
from dataclasses import dataclass

from config.config import Config
from data_engine.processors.matrix_io import load_matrix, save_matrix
from .data_loader import load_index_data
from .grouping import FactorGrouper
from .weighting import WeightCalculator
from .metrics import PerformanceMetrics
from .visualization import plot_combined_returns, plot_combined_statistics


@dataclass
class StockPoolConfig:
    """股票池配置"""
    name: str  # 股票池名称，如 'all_stocks', 'microcap'
    tradability_matrix: pd.DataFrame  # 基础可交易矩阵
    filter_matrix: Optional[pd.DataFrame] = None  # 额外筛选矩阵（如微盘股矩阵）
    filter_value: int = 1  # 筛选矩阵中符合条件的值

    def get_effective_tradability(self) -> pd.DataFrame:
        """
        获取最终的可交易矩阵

        逻辑：基础可交易 OR 不符合筛选条件 → 不可交易
        """
        tradable = self.tradability_matrix.copy()

        if self.filter_matrix is not None:
            # 对齐维度
            common_dates = tradable.index.intersection(self.filter_matrix.index)
            common_stocks = tradable.columns.intersection(self.filter_matrix.columns)

            tradable_aligned = tradable.loc[common_dates, common_stocks]
            filter_aligned = self.filter_matrix.loc[common_dates, common_stocks]

            # 合并：基础不可交易(1) 或 不符合筛选条件 → 不可交易(1)
            tradable = ((tradable_aligned == 1) | (filter_aligned != self.filter_value)).astype(np.int8)

        return tradable


@dataclass
class FactorConfig:
    """因子配置"""
    name: str  # 因子名称
    matrix_file: str  # 因子矩阵文件名


class UnifiedFactorAnalyzer:
    """
    统一因子分析器

    支持不同股票池的因子分析，通过配置实现高内聚低耦合
    """

    def __init__(
        self,
        factor_config: FactorConfig,
        stock_pool_config: StockPoolConfig,
        n_groups: int = 10,
        enable_transaction_cost: bool = False,
        commission_rate: float = 0.0003,
        stamp_duty_rate: float = 0.001,
        slippage_rate: float = 0.001,
        logger: Optional[logging.Logger] = None
    ):
        """
        初始化分析器

        Args:
            factor_config: 因子配置
            stock_pool_config: 股票池配置
            n_groups: 分组数量
            enable_transaction_cost: 是否启用交易成本
            commission_rate: 佣金率
            stamp_duty_rate: 印花税率
            slippage_rate: 滑点率
            logger: 日志对象
        """
        self.factor_config = factor_config
        self.stock_pool_config = stock_pool_config
        self.n_groups = n_groups
        self.logger = logger or logging.getLogger(__name__)

        # 初始化各模块
        self.grouper = FactorGrouper(n_groups=n_groups)
        self.weight_calculator = WeightCalculator()
        self.metrics_calculator = PerformanceMetrics()

        # 初始化交易成本计算器
        if enable_transaction_cost:
            from .transaction_cost import TransactionCostCalculator
            self.transaction_cost = TransactionCostCalculator(
                commission_rate=commission_rate,
                stamp_duty_rate=stamp_duty_rate,
                slippage_rate=slippage_rate
            )
            self.logger.info(
                f"启用交易成本（佣金{commission_rate:.4%}, "
                f"印花税{stamp_duty_rate:.4%}, 滑点{slippage_rate:.4%}）"
            )
        else:
            self.transaction_cost = None

        # 数据缓存
        self._factor_matrix: Optional[pd.DataFrame] = None
        self._mv_matrix: Optional[pd.DataFrame] = None
        self._return_matrix: Optional[pd.DataFrame] = None
        self._tradability_matrix: Optional[pd.DataFrame] = None

    def _load_data(self) -> tuple:
        """
        加载并对齐所有数据

        Returns:
            (factor, mv, returns, tradable) 对齐后的矩阵元组
        """
        self.logger.info("加载数据...")

        # 加载矩阵
        factor_matrix = load_matrix(Config.MATRIX_DATA_DIR / self.factor_config.matrix_file)
        mv_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')
        return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
        tradability_matrix = self.stock_pool_config.get_effective_tradability()

        self.logger.info(f"  因子矩阵: {factor_matrix.shape}")
        self.logger.info(f"  市值矩阵: {mv_matrix.shape}")
        self.logger.info(f"  收益矩阵: {return_matrix.shape}")
        self.logger.info(f"  可交易矩阵: {tradability_matrix.shape}")

        # 对齐所有矩阵
        common_dates = (factor_matrix.index
                       .intersection(return_matrix.index)
                       .intersection(tradability_matrix.index)
                       .intersection(mv_matrix.index))
        common_stocks = (factor_matrix.columns
                        .intersection(return_matrix.columns)
                        .intersection(tradability_matrix.columns)
                        .intersection(mv_matrix.columns))

        factor = factor_matrix.loc[common_dates, common_stocks]
        mv = mv_matrix.loc[common_dates, common_stocks]
        returns = return_matrix.loc[common_dates, common_stocks]
        tradable = tradability_matrix.loc[common_dates, common_stocks]

        self.logger.info(f"  对齐后: {len(common_dates)} 个交易日, {len(common_stocks)} 只股票")

        # 统计可交易比例
        tradable_count = (tradable == 0).sum().sum()
        total_cells = len(common_dates) * len(common_stocks)
        self.logger.info(f"  可交易比例: {tradable_count / total_cells:.2%}")

        return factor, mv, returns, tradable

    def run_analysis(
        self,
        output_dir: Optional[Path] = None,
        save_results: bool = True,
        plot_results: bool = True,
        start_date: str = '2015-01-01'
    ) -> Dict[str, pd.DataFrame]:
        """
        运行完整的因子分析

        Args:
            output_dir: 输出目录
            save_results: 是否保存结果
            plot_results: 是否生成图表
            start_date: 图表起始日期

        Returns:
            包含所有分析结果的字典
        """
        self.logger.info("=" * 80)
        self.logger.info(f"{self.factor_config.name} 因子分析 - {self.stock_pool_config.name}")
        self.logger.info("=" * 80)

        # 加载数据
        factor, mv, returns, tradable = self._load_data()

        # 步骤1: 因子分组
        self.logger.info("\n[步骤1/5] 因子分组")
        group_matrix = self.grouper.group_by_factor(
            factor,
            tradable,
            logger=self.logger
        )

        # 步骤2: 计算等权收益
        self.logger.info("\n[步骤2/5] 计算等权收益")
        group_returns_equal = self.weight_calculator.calculate_group_returns(
            group_matrix,
            returns,
            weighting='equal',
            transaction_cost=self.transaction_cost,
            logger=self.logger
        )

        # 步骤3: 计算市值加权收益
        self.logger.info("\n[步骤3/5] 计算市值加权收益")
        group_returns_mv = self.weight_calculator.calculate_group_returns(
            group_matrix,
            returns,
            mv_matrix=mv,
            weighting='market_cap',
            transaction_cost=self.transaction_cost,
            logger=self.logger
        )

        # 步骤4: 统计分析
        self.logger.info("\n[步骤4/5] 统计分析")
        stats_equal = self.metrics_calculator.calculate_statistics(
            group_returns_equal,
            logger=self.logger
        )
        stats_mv = self.metrics_calculator.calculate_statistics(
            group_returns_mv,
            logger=self.logger
        )

        # IC/IR 分析
        self.logger.info("\n[步骤4b] IC/IR 分析")
        ic_series = PerformanceMetrics.calculate_ic(factor, returns, method='spearman')
        ic_ir = PerformanceMetrics.calculate_ic_ir(ic_series)
        self.logger.info(
            f"  IC均值={ic_ir['mean_ic']:.4f}  "
            f"ICIR={ic_ir['ir']:.4f}  "
            f"IC胜率={ic_ir['ic_win_rate']:.1f}%  "
            f"|IC|均值={ic_ir['mean_abs_ic']:.4f}"
        )

        # 输出统计结果
        self._print_statistics(stats_equal, stats_mv)

        # 步骤5: 保存结果和可视化
        if save_results and output_dir:
            self.logger.info("\n[步骤5/5] 保存结果和可视化")
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            self._save_results(
                output_dir,
                group_returns_equal,
                group_returns_mv,
                stats_equal,
                stats_mv
            )

            if plot_results:
                self._plot_results(
                    output_dir,
                    group_returns_equal,
                    group_returns_mv,
                    stats_equal,
                    stats_mv,
                    start_date
                )

        self.logger.info("\n" + "=" * 80)
        self.logger.info("✓ 分析完成")
        self.logger.info("=" * 80)

        return {
            'group_matrix': group_matrix,
            'group_returns_equal': group_returns_equal,
            'group_returns_mv': group_returns_mv,
            'stats_equal': stats_equal,
            'stats_mv': stats_mv,
            'ic_series': ic_series,
            'ic_ir': ic_ir,
        }

    def _print_statistics(self, stats_equal: pd.DataFrame, stats_mv: pd.DataFrame):
        """输出统计结果"""
        self.logger.info("\n" + "=" * 80)
        self.logger.info(f"{self.factor_config.name} 因子 - 等权收益统计指标:")
        self.logger.info("=" * 80)
        for line in stats_equal.to_string().splitlines():
            self.logger.info(line)
        self.logger.info("=" * 80)

        if stats_mv is not None:
            self.logger.info("\n" + "=" * 80)
            self.logger.info(f"{self.factor_config.name} 因子 - 市值加权收益统计指标:")
            self.logger.info("=" * 80)
            for line in stats_mv.to_string().splitlines():
                self.logger.info(line)
            self.logger.info("=" * 80)

    def _save_results(
        self,
        output_dir: Path,
        group_returns_equal: pd.DataFrame,
        group_returns_mv: pd.DataFrame,
        stats_equal: pd.DataFrame,
        stats_mv: pd.DataFrame
    ):
        """保存结果到文件"""
        factor_lower = self.factor_config.name.lower()

        # 合并保存统计指标
        if stats_mv is not None:
            stats_equal_copy = stats_equal.copy()
            stats_equal_copy['weighting'] = 'equal'
            stats_mv_copy = stats_mv.copy()
            stats_mv_copy['weighting'] = 'market_cap'
            combined_stats = pd.concat([stats_equal_copy, stats_mv_copy])
            combined_stats.to_csv(output_dir / f'{factor_lower}_statistics_combined.csv')
        else:
            stats_equal.to_csv(output_dir / f'{factor_lower}_statistics_combined.csv')

        # 合并保存分组收益率
        if group_returns_mv is not None:
            returns_equal_renamed = group_returns_equal.copy()
            returns_equal_renamed.columns = [f'{col}_equal' for col in returns_equal_renamed.columns]
            returns_mv_renamed = group_returns_mv.copy()
            returns_mv_renamed.columns = [f'{col}_mv' for col in returns_mv_renamed.columns]
            combined_returns = pd.concat([returns_equal_renamed, returns_mv_renamed], axis=1)
            combined_returns.to_csv(output_dir / f'{factor_lower}_returns_combined.csv')
        else:
            group_returns_equal.to_csv(output_dir / f'{factor_lower}_returns_combined.csv')

        self.logger.info(f"  结果已保存到: {output_dir}")

    def _plot_results(
        self,
        output_dir: Path,
        group_returns_equal: pd.DataFrame,
        group_returns_mv: pd.DataFrame,
        stats_equal: pd.DataFrame,
        stats_mv: pd.DataFrame,
        start_date: str
    ):
        """生成可视化图表"""
        index_returns = load_index_data(Config.SUPPLEMENTARY_DATA_DIR)

        plot_combined_returns(
            group_returns_equal=group_returns_equal,
            group_returns_mv=group_returns_mv,
            index_returns=index_returns,
            factor_name=f'{self.factor_config.name} ({self.stock_pool_config.name})',
            output_dir=output_dir,
            start_date=start_date
        )

        plot_combined_statistics(
            stats_equal=stats_equal,
            stats_mv=stats_mv,
            factor_name=f'{self.factor_config.name} ({self.stock_pool_config.name})',
            output_dir=output_dir
        )


def create_stock_pool_config(pool_type: str) -> StockPoolConfig:
    """
    创建股票池配置的工厂函数

    Args:
        pool_type: 'all_stocks' 或 'microcap'

    Returns:
        StockPoolConfig: 股票池配置对象
    """
    # 加载基础可交易矩阵
    tradability_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')

    if pool_type == 'all_stocks':
        return StockPoolConfig(
            name='AllStocks',
            tradability_matrix=tradability_matrix
        )

    elif pool_type == 'microcap':
        # 加载微盘股矩阵
        microcap_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'microcap_matrix.csv')
        return StockPoolConfig(
            name='MicroCap',
            tradability_matrix=tradability_matrix,
            filter_matrix=microcap_matrix,
            filter_value=1  # 微盘股矩阵中1表示是微盘股
        )

    elif pool_type == 'zz1000':
        # 加载中证1000矩阵（时变）
        zz1000_matrix = load_matrix(Config.MATRIX_DATA_DIR / '中证1000_matrix.csv')
        return StockPoolConfig(
            name='ZZ1000',
            tradability_matrix=tradability_matrix,
            filter_matrix=zz1000_matrix,
            filter_value=1  # 中证1000矩阵中1表示是成分股
        )

    else:
        raise ValueError(f"未知的股票池类型: {pool_type}")
