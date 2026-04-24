"""
单因子分析器

整合所有模块，提供完整的单因子分析流程
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Tuple
import logging

from .grouping import FactorGrouper
from .weighting import WeightCalculator
from .metrics import PerformanceMetrics
from .visualization import FactorVisualizer
from .transaction_cost import TransactionCostCalculator


class SingleFactorAnalyzer:
    """
    单因子分析器

    完整的单因子分析流程：
    1. 加载数据（因子矩阵、收益率矩阵、可交易矩阵、市值矩阵）
    2. 因子分组（先过滤可交易，再分组）
    3. 计算收益（等权 + 市值加权）
    4. 统计分析（各项指标）
    5. 可视化（累计收益曲线、统计柱状图）
    6. 保存结果

    使用示例：
        >>> analyzer = SingleFactorAnalyzer(
        ...     factor_name='PB',
        ...     factor_matrix=pb_matrix,
        ...     return_matrix=return_matrix,
        ...     tradability_matrix=tradability_matrix,
        ...     mv_matrix=mv_matrix
        ... )
        >>> results = analyzer.run_analysis()
    """

    def __init__(
        self,
        factor_name: str,
        factor_matrix: pd.DataFrame,
        return_matrix: pd.DataFrame,
        tradability_matrix: pd.DataFrame,
        mv_matrix: Optional[pd.DataFrame] = None,
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
            factor_name: 因子名称（如'PB', 'MV'）
            factor_matrix: 因子矩阵（dates × stocks）
            return_matrix: 收益率矩阵（dates × stocks）
            tradability_matrix: 可交易矩阵（dates × stocks，0=可交易，1=不可交易）
            mv_matrix: 市值矩阵（dates × stocks，可选，用于市值加权）
            n_groups: 分组数量，默认10
            enable_transaction_cost: 是否启用交易成本计算
            commission_rate: 佣金率（默认万3）
            stamp_duty_rate: 印花税率（默认千1）
            slippage_rate: 滑点率（默认千1）
            logger: 日志记录器
        """
        # 验证输入矩阵维度一致性
        if factor_matrix.shape != return_matrix.shape:
            raise ValueError(
                f"因子矩阵 {factor_matrix.shape} 与收益率矩阵 {return_matrix.shape} 维度不一致"
            )
        if factor_matrix.shape != tradability_matrix.shape:
            raise ValueError(
                f"因子矩阵 {factor_matrix.shape} 与可交易矩阵 {tradability_matrix.shape} 维度不一致"
            )
        if mv_matrix is not None and factor_matrix.shape != mv_matrix.shape:
            raise ValueError(
                f"因子矩阵 {factor_matrix.shape} 与市值矩阵 {mv_matrix.shape} 维度不一致"
            )

        self.factor_name = factor_name
        self.factor_matrix = factor_matrix
        self.return_matrix = return_matrix
        self.tradability_matrix = tradability_matrix
        self.mv_matrix = mv_matrix
        self.n_groups = n_groups
        self.logger = logger or logging.getLogger(__name__)

        # 初始化各模块
        self.grouper = FactorGrouper(n_groups=n_groups)
        self.weight_calculator = WeightCalculator()
        self.metrics_calculator = PerformanceMetrics()
        self.visualizer = FactorVisualizer()

        # 初始化交易成本计算器
        if enable_transaction_cost:
            self.transaction_cost = TransactionCostCalculator(
                commission_rate=commission_rate,
                stamp_duty_rate=stamp_duty_rate,
                slippage_rate=slippage_rate
            )
            self.logger.info(
                f"启用交易成本计算（佣金{commission_rate:.4%}, "
                f"印花税{stamp_duty_rate:.4%}, 滑点{slippage_rate:.4%}）"
            )
        else:
            self.transaction_cost = None

        # 结果容器
        self.group_matrix = None
        self.group_returns_equal = None
        self.group_returns_mv = None
        self.stats_equal = None
        self.stats_mv = None
        self.ic_series = None
        self.ic_ir = None

    def run_analysis(
        self,
        output_dir: Optional[Path] = None,
        save_results: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        运行完整的因子分析流程

        Args:
            output_dir: 输出目录，如果为None则不保存文件
            save_results: 是否保存结果到文件

        Returns:
            results: 包含所有分析结果的字典
                - group_matrix: 分组矩阵
                - group_returns_equal: 等权收益
                - group_returns_mv: 市值加权收益
                - stats_equal: 等权统计指标
                - stats_mv: 市值加权统计指标
        """
        self.logger.info("=" * 80)
        self.logger.info(f"{self.factor_name} 因子分析")
        self.logger.info("=" * 80)

        # 步骤1: 因子分组
        self.logger.info("\n[步骤1/5] 因子分组")
        self.group_matrix = self.grouper.group_by_factor(
            self.factor_matrix,
            self.tradability_matrix,
            logger=self.logger
        )

        # 步骤2: 计算等权收益
        self.logger.info("\n[步骤2/5] 计算等权收益")
        self.group_returns_equal = self.weight_calculator.calculate_group_returns(
            self.group_matrix,
            self.return_matrix,
            weighting='equal',
            transaction_cost=self.transaction_cost,
            logger=self.logger
        )

        # 步骤3: 计算市值加权收益
        self.logger.info("\n[步骤3/5] 计算市值加权收益")
        if self.mv_matrix is not None:
            self.group_returns_mv = self.weight_calculator.calculate_group_returns(
                self.group_matrix,
                self.return_matrix,
                mv_matrix=self.mv_matrix,
                weighting='market_cap',
                transaction_cost=self.transaction_cost,
                logger=self.logger
            )
        else:
            self.logger.warning("  未提供市值矩阵，跳过市值加权收益计算")
            self.group_returns_mv = None

        # 步骤4: 统计分析
        self.logger.info("\n[步骤4/5] 统计分析")
        self.stats_equal = self.metrics_calculator.calculate_statistics(
            self.group_returns_equal,
            logger=self.logger
        )
        if self.group_returns_mv is not None:
            self.stats_mv = self.metrics_calculator.calculate_statistics(
                self.group_returns_mv,
                logger=self.logger
            )

        # IC/IR 分析
        self.logger.info("\n[步骤4b] IC/IR 分析")
        self.ic_series = PerformanceMetrics.calculate_ic(
            self.factor_matrix,
            self.return_matrix,
            method='spearman'
        )
        self.ic_ir = PerformanceMetrics.calculate_ic_ir(self.ic_series)
        self.logger.info(
            f"  IC均值={self.ic_ir['mean_ic']:.4f}  "
            f"ICIR={self.ic_ir['ir']:.4f}  "
            f"IC胜率={self.ic_ir['ic_win_rate']:.1f}%  "
            f"|IC|均值={self.ic_ir['mean_abs_ic']:.4f}"
        )

        # 输出统计结果
        self._print_statistics()

        # 步骤5: 保存结果和可视化
        if save_results and output_dir:
            self.logger.info("\n[步骤5/5] 保存结果和可视化")
            self._save_results(output_dir)

        self.logger.info("\n" + "=" * 80)
        self.logger.info("✓ 分析完成")
        self.logger.info("=" * 80)

        # 返回结果
        return {
            'group_matrix': self.group_matrix,
            'group_returns_equal': self.group_returns_equal,
            'group_returns_mv': self.group_returns_mv,
            'stats_equal': self.stats_equal,
            'stats_mv': self.stats_mv,
            'ic_series': self.ic_series,
            'ic_ir': self.ic_ir,
        }

    def _print_statistics(self):
        """输出统计结果"""
        self.logger.info("\n" + "=" * 80)
        self.logger.info(f"{self.factor_name} 因子 - 等权收益统计指标:")
        self.logger.info("=" * 80)
        for line in self.stats_equal.to_string().splitlines():
            self.logger.info(line)
        self.logger.info("=" * 80)

        if self.stats_mv is not None:
            self.logger.info("\n" + "=" * 80)
            self.logger.info(f"{self.factor_name} 因子 - 市值加权收益统计指标:")
            self.logger.info("=" * 80)
            for line in self.stats_mv.to_string().splitlines():
                self.logger.info(line)
            self.logger.info("=" * 80)

    def _save_results(self, output_dir: Path):
        """保存结果到文件"""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        factor_lower = self.factor_name.lower()

        # 合并保存统计指标（等权 + 市值加权）
        if self.stats_mv is not None:
            # 添加加权方式列
            stats_equal_copy = self.stats_equal.copy()
            stats_equal_copy['weighting'] = 'equal'
            stats_mv_copy = self.stats_mv.copy()
            stats_mv_copy['weighting'] = 'market_cap'

            # 合并
            combined_stats = pd.concat([stats_equal_copy, stats_mv_copy])
            combined_stats.to_csv(
                output_dir / f'{factor_lower}_factor_statistics_combined.csv'
            )
        else:
            self.stats_equal.to_csv(
                output_dir / f'{factor_lower}_factor_statistics_combined.csv'
            )

        # 合并保存分组收益率（等权 + 市值加权）
        if self.group_returns_mv is not None:
            # 重命名列名以区分
            returns_equal_renamed = self.group_returns_equal.copy()
            returns_equal_renamed.columns = [f'{col}_equal' for col in returns_equal_renamed.columns]

            returns_mv_renamed = self.group_returns_mv.copy()
            returns_mv_renamed.columns = [f'{col}_mv' for col in returns_mv_renamed.columns]

            # 合并
            combined_returns = pd.concat([returns_equal_renamed, returns_mv_renamed], axis=1)
            combined_returns.to_csv(
                output_dir / f'{factor_lower}_factor_group_returns_combined.csv'
            )
        else:
            self.group_returns_equal.to_csv(
                output_dir / f'{factor_lower}_factor_group_returns_combined.csv'
            )

        self.logger.info(f"  CSV文件已保存到: {output_dir}")

        # 不再生成 PNG 图表，只在 analyze_factors_enhanced.py 中生成 HTML
        self.logger.info(f"  跳过PNG图表生成（只生成HTML）")
