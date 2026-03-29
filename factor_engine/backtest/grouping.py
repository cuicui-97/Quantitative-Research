"""
因子分组模块

负责将股票按因子值分组的逻辑
"""
import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class FactorGrouper:
    """
    因子分组器

    核心功能：
    1. 过滤T+1日可交易的股票
    2. 在可交易股票中按因子值分组（十分位数）
    3. 返回分组矩阵

    设计理念：
    - 先过滤再分组，确保各组股票池一致
    - 避免因可交易性差异导致的不公平比较
    """

    def __init__(self, n_groups: int = 10):
        """
        初始化分组器

        Args:
            n_groups: 分组数量，默认10组（十分位数）
        """
        self.n_groups = n_groups

    def group_by_factor(
        self,
        factor_matrix: pd.DataFrame,
        tradability_matrix: pd.DataFrame,
        logger: Optional[logging.Logger] = None
    ) -> pd.DataFrame:
        """
        按因子值分组

        核心逻辑：
        1. 获取T日的因子值
        2. 获取T+1日的可交易状态（通过shift）
        3. 只保留T+1日可交易且因子值非空的股票
        4. 在可交易股票中按因子值分成n组

        Args:
            factor_matrix: 因子矩阵 (dates × stocks)
            tradability_matrix: 可交易矩阵（0=可交易，1=不可交易）
            logger: 日志记录器

        Returns:
            group_matrix: 分组矩阵，值为1-n（NaN表示不参与分组）
        """
        if logger:
            logger.info(f"开始因子分组（先过滤T+1日可交易，再分成{self.n_groups}组）...")

        # shift(-1) 将T+1日的可交易状态前移到T日
        # 这样可以在T日就知道T+1日是否可交易
        tradable_next_day = tradability_matrix.shift(-1)

        # 初始化结果矩阵
        group_matrix = pd.DataFrame(
            index=factor_matrix.index,
            columns=factor_matrix.columns,
            dtype=float
        )

        # 统计信息
        skipped_dates = 0
        total_tradable = 0
        total_grouped = 0

        # 逐日分组
        for date in factor_matrix.index:
            # 1. 获取T日的因子值
            factor_values = factor_matrix.loc[date]

            # 2. 获取T+1日的可交易状态
            tradable_status = tradable_next_day.loc[date]

            # 3. 过滤：只保留T+1日可交易（值为0）且因子值非空的股票
            tradable_mask = (tradable_status == 0) & factor_values.notna()
            tradable_factors = factor_values[tradable_mask]

            total_tradable += len(tradable_factors)

            # 如果可交易股票数量不足分组数，跳过该日期
            if len(tradable_factors) < self.n_groups:
                skipped_dates += 1
                continue

            # 4. 在可交易股票中按因子值分组
            # qcut: 按分位数分组，确保每组股票数量大致相等
            # labels=False: 返回组号（0, 1, 2, ...）
            # duplicates='drop': 如果因子值重复太多，自动调整分组
            try:
                groups = pd.qcut(
                    tradable_factors,
                    q=self.n_groups,
                    labels=False,
                    duplicates='drop'
                ) + 1  # +1 使组号从1开始（1, 2, ..., n）

                group_matrix.loc[date, groups.index] = groups
                total_grouped += len(groups)
            except ValueError:
                # 如果值太少或重复太多，跳过该日期
                skipped_dates += 1
                continue

        # 输出统计信息
        if logger:
            logger.info(f"  跳过 {skipped_dates} 个日期（可交易样本不足）")
            logger.info(f"  有效日期: {len(factor_matrix.index) - skipped_dates}")
            logger.info(f"  可交易股票总数: {total_tradable:,}")
            logger.info(f"  成功分组样本数: {total_grouped:,}")

        return group_matrix
